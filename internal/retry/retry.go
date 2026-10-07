// Package retry implements viberoxy's bounded dial-stage retry policy
// (review finding F-08, stage 1): when a WAN dial fails, fail over to the
// next-best eligible path under a hard attempt cap, a shared dial budget and
// a token-bucket retry budget that keeps correlated-failure storms bounded.
//
// # Safety rule (T-RETRY-03 / T-RETRY-04): dial stage ONLY
//
// A retry is legal only BEFORE any application byte has been sent or
// received: a plain dial failure (connection refused, SOCKS handshake error,
// timeout) is safe to retry because nothing of this connection has reached
// the target yet. Once a dial succeeds the relay splices application bytes,
// and re-dialing from there would replay them — so Run hands a successful
// connection straight back to its caller and never touches it again. There is
// deliberately NO post-dial / replay API in this package, and
// TestRetry_TRETRY0304_NeverSeesPostDialConnections pins that a successful
// dial is terminal (not even another candidate is requested). Any future
// post-dial retry must not be built here.
package retry

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// Defaults bound one connection's dial stage: at most DefaultMaxAttempts
// total attempts (the initial dial plus at most two retries) inside one
// DefaultBudget wall-clock budget shared by all of them, with a retry
// budget of DefaultRetryRatio (10%) of the admitted traffic.
const (
	DefaultMaxAttempts = 3
	DefaultBudget      = 5 * time.Second
	DefaultRetryRatio  = 0.10
	DefaultBurst       = 10.0
)

// ErrNoCandidate means no candidate was ever available to dial (empty or
// fully excluded pool) — distinct from "candidates existed but every dial
// failed", so front-ends can keep 503 vs 502 / REP 0x01 semantics.
var ErrNoCandidate = errors.New("retry: no candidate to dial")

// StageDial is the only retry stage that exists: the dial stage. The
// T-RETRY-03/04 safety rule forbids any post-dial retry, so no other stage
// may ever be emitted from here.
const StageDial = "dial"

// EventHook receives one dial-stage event (SPEC-M, viberoxy_retry_total):
// stage is StageDial and outcome is one of
//
//	success      — the dial succeeded (terminal)
//	retry        — the attempt failed and another attempt follows
//	failed       — the attempt failed and the dial stage ends
//	no_candidate — no candidate was ever available to dial
//
// exactly once per attempt (plus once for the empty-pool case). The hook
// exists so package main can count retry events without internal/retry
// importing the metric registry (no import cycle).
type EventHook func(stage, outcome string)

var (
	hookMu    sync.RWMutex
	eventHook EventHook
)

// SetEventHook installs the observability hook (nil uninstalls it). It is
// wired once at process start from metrics.go's init.
func SetEventHook(fn EventHook) {
	hookMu.Lock()
	eventHook = fn
	hookMu.Unlock()
}

// emit delivers one event to the installed hook, if any. Called with no
// policy lock held (Run holds none), so the hook may take its own locks.
func emit(stage, outcome string) {
	hookMu.RLock()
	fn := eventHook
	hookMu.RUnlock()
	if fn != nil {
		fn(stage, outcome)
	}
}

// Options configures a Policy. Any non-positive numeric field selects its
// default; pass MaxAttempts=1 to disable retries outright.
type Options struct {
	// MaxAttempts is the total number of dial attempts per connection
	// (initial + retries). Default DefaultMaxAttempts (3).
	MaxAttempts int
	// Budget is the wall-clock budget for the whole dial stage, shared by
	// every attempt of one connection. Default DefaultBudget (5s).
	Budget time.Duration
	// RetryRatio is the token credit each admitted request deposits in the
	// bucket (so retries stay <= ratio x requests). Default
	// DefaultRetryRatio (0.10).
	RetryRatio float64
	// Burst is the bucket capacity and its initial fill: the largest
	// number of retries that can bank up while traffic is healthy.
	// Default DefaultBurst (10).
	Burst float64
	// Now is the clock; nil means time.Now. Tests inject a clock seeded
	// with time.Now() and advanced by their fake dials.
	Now func() time.Time
}

// Policy is the bounded dial-stage retry policy: attempt cap, deadline
// budget and token-bucket retry budget.
type Policy struct {
	// MaxAttempts is the total dial attempts allowed for one connection.
	MaxAttempts int
	// Budget is the shared wall-clock budget across those attempts.
	Budget time.Duration
	// Bucket is the token-bucket retry budget guarding correlated-failure
	// storms (T-RETRY-05).
	Bucket *Bucket

	now func() time.Time
}

// New builds a Policy from Options, applying defaults for non-positive
// fields.
func New(opts Options) *Policy {
	p := &Policy{
		MaxAttempts: opts.MaxAttempts,
		Budget:      opts.Budget,
		now:         opts.Now,
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}
	if p.Budget <= 0 {
		p.Budget = DefaultBudget
	}
	if p.now == nil {
		p.now = time.Now
	}
	ratio := opts.RetryRatio
	if ratio <= 0 {
		ratio = DefaultRetryRatio
	}
	burst := opts.Burst
	if burst <= 0 {
		burst = DefaultBurst
	}
	p.Bucket = NewBucket(ratio, burst)
	return p
}

// Bucket is the token-bucket retry budget (T-RETRY-05). Every admitted
// request deposits ratio tokens (clamped to burst); every retry attempt
// debits one token. When a token is unavailable the caller must FAIL FAST on
// its first dial error instead of retrying, which is what bounds a
// correlated-failure storm to roughly ratio x traffic plus one burst.
type Bucket struct {
	// mu guards tokens: the bucket is shared by every connection of a
	// front-end, so credit/debit happen concurrently.
	mu     sync.Mutex
	tokens float64
	ratio  float64
	burst  float64
}

// NewBucket returns a bucket with the given per-request credit and
// capacity, initially full.
func NewBucket(ratio, burst float64) *Bucket {
	return &Bucket{tokens: burst, ratio: ratio, burst: burst}
}

// Credit deposits this request's share of the retry budget: ratio tokens,
// clamped to the bucket capacity.
func (b *Bucket) Credit() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens += b.ratio
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// Take removes one token for one retry attempt. It reports false when the
// bucket is below one token: the caller must then fail fast.
func (b *Bucket) Take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Tokens reports the current fill (for diagnostics and tests).
func (b *Bucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens
}

// Run dials candidates for ONE connection under p's bounds:
//
//   - next hands out the next candidate (each at most once per connection —
//     the caller owns exclusion, e.g. WANPool.GetLeastLoadedExcluding);
//     false means none left;
//   - dial performs exactly ONE dial-stage attempt: a non-nil error is a
//     retryable dial-stage failure, a non-nil conn is terminal.
//
// On success it returns the connection and the candidate that produced it;
// the caller then owns both (and must never expect Run to touch them again).
// It fails with ErrNoCandidate when the first next() yields nothing, and with
// the last dial error otherwise. Retries stop at the first of: attempt cap,
// deadline budget exhausted, no candidate left, or an empty retry bucket
// (fail fast — T-RETRY-05).
//
// SAFETY (T-RETRY-03/04): Run only ever observes dial results, never bytes.
// A successful dial ends the sequence immediately — this function has no way
// to see, replay or re-dial a connection once dial() has returned it.
func Run[C any](ctx context.Context, p *Policy, next func() (C, bool), dial func(context.Context, C) (net.Conn, error)) (net.Conn, C, error) {
	var zero C
	if p == nil {
		p = New(Options{})
	}

	// This connection earns its share of the retry budget up front, so the
	// retry rate tracks the request rate (<= RetryRatio of traffic).
	p.Bucket.Credit()

	// One shared deadline for the whole dial stage: every attempt receives
	// a context bounded by it, so even a single hanging dial cannot blow
	// the budget.
	start := p.now()
	deadline := start.Add(p.Budget)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	cand, ok := next()
	if !ok {
		emit(StageDial, "no_candidate")
		return nil, zero, ErrNoCandidate
	}

	var lastErr error
	for attempt := 1; ; attempt++ {
		conn, err := dial(ctx, cand)
		if err == nil {
			// Terminal: hand the connection back untouched. No further
			// candidate is even requested once a dial has succeeded.
			emit(StageDial, "success")
			return conn, cand, nil
		}
		lastErr = err

		// Decide (in the pre-existing stop order: attempt cap, shared
		// budget, no candidate, empty bucket) whether this failure ends
		// the dial stage or buys one more attempt.
		outcome := "failed"
		if attempt < p.MaxAttempts && p.now().Before(deadline) {
			if nxt, ok := next(); ok && p.Bucket.Take() {
				outcome = "retry"
				cand = nxt
			}
		}
		emit(StageDial, outcome)
		if outcome != "retry" {
			return nil, zero, lastErr
		}
	}
}
