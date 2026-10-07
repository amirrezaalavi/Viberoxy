// Package path owns the identity and the per-connection accounting of ONE
// xray process lifetime.
//
// A Path is minted when a WAN slot is (re)activated and retired when that
// xray stops; it is never reused for a later occupant of the same slot. A
// handler receives the *Path when its connection is admitted and holds it for
// the connection's lifetime, so every counter the connection touches belongs
// to the generation it was reserved on. Late events from a previous occupant
// (a connection that finishes after the slot was reset) can therefore only
// ever move that occupant's own counters — never the replacement's — which is
// the F-04 root fix for the slot-index ABA problem.
//
// The pool maps slot -> current *Path through an atomic pointer; nothing in
// this package knows about slots beyond the diagnostic Slot/Port fields a
// Path carries.
package path

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
	"viberoxy/internal/health"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

// State is a Path's lifecycle state, stored as int32 in Path.State.
//
// Active, Suspect, Draining and Dead are assigned today. Suspect is the
// health-ejection state (SPEC-H.3): an ejected path is excluded from
// selection until half-open re-admission (internal/health drives it).
// Probation exists so the scheduler tasks (SPEC-S) can still grow the
// state machine without another identity change; nothing assigns it yet.
type State int32

const (
	Probation State = iota
	Active
	Suspect
	Draining
	Dead
)

func (s State) String() string {
	switch s {
	case Probation:
		return "probation"
	case Active:
		return "active"
	case Suspect:
		return "suspect"
	case Draining:
		return "draining"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

// Path is the identity of one xray process occupying one WAN slot. See the
// package documentation for the ownership rules.
//
// All methods are nil-safe: a nil *Path reads as "no occupant" (zero counts,
// not alive, no-op writes), so callers may use a slot's Current pointer
// without a nil check.
type Path struct {
	// ID is a process-wide monotonic generation number. It is minted once at
	// construction and never reused, so two paths that ever occupied the same
	// slot are always distinguishable.
	ID uint64

	// Cfg is the proxy config the xray was started from. Nil for a vacant
	// path (a slot with no occupant).
	Cfg *proxycfg.ProxyConfig

	// Proc is the lifecycle handle of the xray process. Nil for a vacant path.
	Proc *xrayproc.Handle

	// Port is the local SOCKS5 service port the process listens on, copied
	// from the slot at construction time. Callers that must tolerate the slot
	// being reconfigured in place read the slot instead.
	Port int

	// Slot is the index of the WAN slot this path occupies (or occupied, once
	// retired). It is stable: the service-port slot index never changes.
	Slot int

	// Inflight is the number of connections reserved on THIS path. Handlers
	// Reserve on the path they selected and Release the same path when they
	// finish. The raw counter may go negative; anything that exposes it to
	// the pool (selection, API) must read Conns, which clamps at zero, so a
	// drifted counter can never make the slot look under-loaded.
	Inflight atomic.Int64

	// State is the lifecycle state (type State stored as int32).
	State atomic.Int32

	// CreatedAt records when this generation was minted.
	CreatedAt time.Time

	// fails counts consecutive dial/probe failures credited to THIS path.
	// It moves with the generation: a success recorded by a handler of a
	// retired path resets that path's counter, never the replacement's.
	fails atomic.Int64

	// mu guards the health/performance samples below.
	mu sync.Mutex
	// ewmaTTFB is the peak-EWMA time-to-first-byte: worse samples jump it
	// up immediately, better samples decay it toward themselves with tau
	// ttfbTau (SPEC-H.5/S). Zero until the first sample.
	ewmaTTFB time.Duration
	// goodputBps is the peak-EWMA goodput in bits per second (worse
	// samples jump it down immediately, better ones decay up with tau
	// goodputTau), populated at relay end from the bytes actually
	// delivered (SPEC-H/SPEC-S input to the scheduler cost).
	goodputBps float64
	// healthAt is the timestamp of the last RecordHealth sample; the
	// decay between samples is measured against it. Guarded by mu.
	healthAt time.Time

	// hc is this generation's SPEC-H health state: the sliding outcome
	// window (60s/30 outcomes), ejection with exponential backoff and the
	// canary streak behind half-open re-admission. It lives here so the
	// state dies with the generation: a replacement path starts healthy.
	// Zero value is ready; all access is mutex-guarded inside health.
	hc health.State
}

// nextID is the process-wide generation counter backing Path.ID.
var nextID atomic.Uint64

// New mints the Path for one xray lifetime: a fresh never-reused ID, the
// config and process handle of the occupant, and Active state.
func New(cfg *proxycfg.ProxyConfig, proc *xrayproc.Handle, slot, port int) *Path {
	p := vacant(slot, port, Active)
	p.Cfg = cfg
	p.Proc = proc
	return p
}

// NewVacant mints the placeholder Path a slot holds while no xray occupies
// it: no config, no process, Dead state, zero counters. It still gets a fresh
// ID so that a slot's current pointer is never nil and so that the empty
// generation has counters of its own — reservations and releases held by
// handlers of a retired occupant never land on whatever comes next.
func NewVacant(slot, port int) *Path {
	return vacant(slot, port, Dead)
}

func vacant(slot, port int, state State) *Path {
	p := &Path{
		ID:        nextID.Add(1),
		Slot:      slot,
		Port:      port,
		CreatedAt: time.Now(),
	}
	p.State.Store(int32(state))
	return p
}

// Reserve books one connection on this path. Handlers call it on the path the
// selector handed them, never on a slot index.
func (p *Path) Reserve() {
	if p == nil {
		return
	}
	p.Inflight.Add(1)
}

// Release drops one connection reserved by Reserve. Release always targets
// the receiver: a handler that outlives its path's slot generation releases
// its own path, so the replacement occupying the slot keeps its own count
// (the F-04 ABA guard — there is deliberately no slot lookup here).
func (p *Path) Release() {
	if p == nil {
		return
	}
	p.Inflight.Add(-1)
}

// Conns is the pool-facing connection count: Inflight clamped at zero. The
// raw counter may drift negative if a caller releases more than it reserved,
// but a negative load must never leak into selection or the API.
func (p *Path) Conns() int64 {
	if p == nil {
		return 0
	}
	if n := p.Inflight.Load(); n > 0 {
		return n
	}
	return 0
}

// RecordFailure credits one consecutive failure to this path.
func (p *Path) RecordFailure() {
	if p == nil {
		return
	}
	p.fails.Add(1)
}

// RecordSuccess clears this path's consecutive-failure counter. It is
// unconditional today; a later health task tightens the success rule.
func (p *Path) RecordSuccess() {
	if p == nil {
		return
	}
	p.fails.Store(0)
}

// ConsecutiveFails returns this path's consecutive-failure count.
func (p *Path) ConsecutiveFails() int64 {
	if p == nil {
		return 0
	}
	return p.fails.Load()
}

// GetState returns the lifecycle state of this path.
func (p *Path) GetState() State {
	if p == nil {
		return Dead
	}
	return State(p.State.Load())
}

// SetState stores the lifecycle state of this path.
func (p *Path) SetState(s State) {
	if p == nil {
		return
	}
	p.State.Store(int32(s))
}

// Alive reports whether this path's xray process is still running (a vacant
// path and an exited process both report false).
func (p *Path) Alive() bool {
	if p == nil {
		return false
	}
	return p.Proc.Alive()
}

// Stop stops this path's xray process and removes its temp config file. It
// is safe on a vacant path and safe to call twice.
//
// Note: the pool's ResetEmpty still stops through the slot's recorded
// cmd/config-path pair, which is the same handle for an occupied slot; this
// method is the path-owned entry point for lifecycle callers.
func (p *Path) Stop() error {
	if p == nil || p.Proc == nil {
		return nil
	}
	return p.Proc.Stop("")
}

// peak-EWMA time constants: a WORSE sample jumps the estimate
// immediately, a BETTER sample only decays it toward itself with this
// time constant (SPEC-H.5/S — see RecordHealth).
const (
	ttfbTau    = 10 * time.Second
	goodputTau = 30 * time.Second
)

// RecordOutcome feeds one classified relay/dial outcome (SPEC-H.1) into
// this path's health window (SPEC-H.2/H.3): OK clears the consecutive
// failure counter, HARD_FAIL increments it, NEUTRAL changes nothing. It
// returns true exactly when this outcome newly EJECTS the path — the
// caller then sees the path state move Active -> Suspect (not-selected);
// Draining/Dead states are never overwritten, drain policy belongs to the
// drain task. An ejection also notifies the observer (SPEC-M) with the
// SPEC-H.3 rule that fired.
func (p *Path) RecordOutcome(now time.Time, dest string, o health.Outcome) bool {
	if p == nil {
		return false
	}
	switch o {
	case health.OK:
		p.RecordSuccess()
	case health.HardFail:
		p.RecordFailure()
	}
	ejected, reason := p.hc.RecordWithReason(now, dest, o)
	if ejected && p.GetState() == Active {
		p.SetState(Suspect)
	}
	if ejected {
		notify(Event{Kind: EventEjection, Path: p, Reason: reason})
	}
	return ejected
}

// NoteCanary folds one canary interval result (SPEC-H.5) into this path's
// health state. It returns true when the path is RE-ADMITTED: an ejected
// (Suspect) path whose backoff elapsed and which just collected
// HalfOpenSuccesses consecutive canary successes moves Suspect -> Active
// again. Draining/Dead states are never touched.
func (p *Path) NoteCanary(now time.Time, ok bool) (recovered bool) {
	if p == nil {
		return false
	}
	recovered = p.hc.NoteCanary(now, ok)
	if recovered && p.GetState() == Suspect {
		p.SetState(Active)
	}
	return recovered
}

// Ejected reports whether health has ejected this path (SPEC-H.3/H.4):
// an ejected path is not admitted for new traffic until half-open
// re-admission.
func (p *Path) Ejected() bool {
	if p == nil {
		return false
	}
	return p.hc.Ejected()
}

// CanaryStreak reports this generation's consecutive canary successes
// since the last failed canary (nil-safe; 0 for a nil path). The pool
// uses it as the recovery signal for a health-drained slot (F-02).
func (p *Path) CanaryStreak() int {
	if p == nil {
		return 0
	}
	return p.hc.CanaryStreak()
}

// HealthSnapshot returns the path's window/backoff snapshot as of now,
// for diagnostics and tests.
func (p *Path) HealthSnapshot(now time.Time) health.Snapshot {
	if p == nil {
		return health.Snapshot{}
	}
	return p.hc.Snapshot(now)
}

// RecordHealth folds one observation into the path's performance EWMAs:
// the time-to-first-byte observed for this connection (0 = unknown) and
// the goodput in bits per second. Both feed the scheduler's cost function
// (internal/sched).
//
// Semantics are peak-EWMA, adverse-direction-first:
//
//   - TTFB: a HIGHER sample (slower) replaces the estimate immediately;
//     a lower one only decays the estimate toward it with tau
//     ttfbTau (10s).
//   - goodput: a LOWER sample (slower delivery) replaces the estimate
//     immediately; a higher one decays toward it with tau goodputTau
//     (30s).
//
// The decay is wall-clock based: between samples the stored estimate
// moves a fraction 1-exp(-dt/tau) of the way to the better sample, so a
// path that recovers earns its standing back gradually instead of on one
// lucky relay, while a regression is priced in on the very next sample.
// A non-zero sample also notifies the observer (SPEC-M:
// viberoxy_path_ttfb_seconds), once per sample, from the caller's values.
func (p *Path) RecordHealth(ttfb time.Duration, goodputBps float64) {
	p.recordHealthAt(time.Now(), ttfb, goodputBps)
}

// recordHealthAt is RecordHealth with an injected clock (tests drive the
// decay deterministically); caller-facing semantics are identical.
func (p *Path) recordHealthAt(now time.Time, ttfb time.Duration, goodputBps float64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if ttfb > 0 {
		switch {
		case p.ewmaTTFB == 0:
			p.ewmaTTFB = ttfb
		case ttfb > p.ewmaTTFB:
			p.ewmaTTFB = ttfb // worse latency: jump up immediately
		default:
			p.ewmaTTFB = decayDur(p.ewmaTTFB, ttfb, now.Sub(p.healthAt), ttfbTau)
		}
	}
	if goodputBps > 0 {
		switch {
		case p.goodputBps == 0:
			p.goodputBps = goodputBps
		case goodputBps < p.goodputBps:
			p.goodputBps = goodputBps // worse goodput: jump down immediately
		default:
			p.goodputBps = decayF64(p.goodputBps, goodputBps, now.Sub(p.healthAt), goodputTau)
		}
	}
	if now.After(p.healthAt) {
		p.healthAt = now
	}
	p.mu.Unlock()

	if ttfb > 0 || goodputBps > 0 {
		notify(Event{Kind: EventSample, Path: p, TTFB: ttfb, GoodputBps: goodputBps})
	}
}

// decayDur moves cur toward the better sample target by the fraction
// 1-exp(-dt/tau) accumulated over dt (dt <= 0 moves nothing).
func decayDur(cur, target time.Duration, dt, tau time.Duration) time.Duration {
	if dt <= 0 {
		return cur
	}
	f := 1 - math.Exp(-float64(dt)/float64(tau))
	v := float64(target) + (float64(cur)-float64(target))*(1-f)
	return time.Duration(v)
}

// decayF64 is decayDur for float64 samples (goodput bps).
func decayF64(cur, target float64, dt, tau time.Duration) float64 {
	if dt <= 0 {
		return cur
	}
	f := 1 - math.Exp(-float64(dt)/float64(tau))
	return target + (cur-target)*(1-f)
}

// TTFB returns the path's time-to-first-byte EWMA (0 = no sample yet).
func (p *Path) TTFB() time.Duration {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ewmaTTFB
}

// GoodputBps returns the path's goodput EWMA in bits per second
// (0 = no sample yet).
func (p *Path) GoodputBps() float64 {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.goodputBps
}
