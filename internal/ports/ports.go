// Package ports provides a bounded, concurrency-safe allocator of local TCP
// ports for the xray instances viberoxy starts.
//
// Why this exists (review finding F-13): a speed test today derives its
// listener port as TEST_BASE_PORT+tested, and the drop/replace API reuses
// TEST_BASE_PORT+0 — so two concurrent speed tests can pick the same port
// and one dies with a confusing "address already in use". The upcoming
// make-before-break swap needs a second, disjoint set of ports: spare
// service listeners above the WAN_COUNT live slots
// (WAN_BASE_PORT+WAN_COUNT .. +N). Both problems are the same shape — a
// bounded set of port numbers handed to exactly one owner at a time — so
// both are served by an Allocator.
//
// Semantics:
//
//   - New(base, n) manages exactly the range base..base+n-1 (ports must be
//     valid TCP ports, 1..65535). Anything outside the range is rejected.
//   - Acquire never blocks: it returns ErrExhausted once every port in the
//     range is checked out. Exhaustion is an error, never a duplicate
//     hand-out — a caller that cannot get a port must skip or retry later,
//     never reuse a port it does not hold.
//   - Release returns a port to the pool. Releasing a port the allocator
//     does not currently hold (never acquired, already released) returns
//     ErrNotOwned and leaves the free list untouched, so a double Release
//     cannot inflate the pool and later hand the same port to two owners.
//     Releasing an out-of-range port returns a descriptive error, likewise
//     without touching state. In other words Release is idempotent-safe: a
//     stray or repeated call can narrow availability but can never create a
//     phantom port.
//   - All methods are safe for concurrent use (a single sync.Mutex guards
//     the free list and the in-use set, which are kept exact inverses of
//     each other). Ports are handed out lowest-first on a fresh allocator;
//     after a Release the order is unspecified and must not be relied on.
//   - Len is the constant capacity (size of the range), InUse the number of
//     currently checked-out ports, Free the number still available;
//     Free() + InUse() == Len() always holds at rest.
//
// Deliberately omitted: AcquireWithin(ctx, timeout). Every in-tree caller —
// the speed-test loop and the future swap — is already driven by a bounded
// cycle or timer and can retry on the next tick; a blocking acquire would
// just hide backpressure behind sleeping goroutines and introduce a
// timeout-grants-a-port race (did the port arrive before or after the
// deadline?) for no gain. Non-blocking Acquire plus a documented
// ErrExhausted keeps the failure mode explicit.
//
// This package only decides which port number an owner may use; it never
// binds a socket. Actually listening (and coping with a foreign process
// already holding the port, EADDRINUSE) stays with the caller.
package ports

import (
	"errors"
	"fmt"
	"sync"
)

// ErrExhausted is returned by Acquire when every port in the allocator's
// range is currently checked out. It is terminal for that call: the caller
// owns no port and must not assume it can use one.
var ErrExhausted = errors.New("ports: no free port in range")

// ErrNotOwned is returned by Release for a port that is inside the
// allocator's range but is not currently checked out (never acquired, or
// already released). The call is a no-op on the allocator's state.
var ErrNotOwned = errors.New("ports: port not held by this allocator")

// maxPort is the highest valid TCP port number.
const maxPort = 65535

// Allocator hands out ports from one fixed, bounded range [base, base+n-1]
// with at most one live owner per port. It is the shared mechanism behind
// both port pools the daemon needs (see the package doc); create one with
// New or with the convenience constructors NewTestPorts / NewSpareWANPorts.
//
// The zero value is not usable; construct with New. A nil *Allocator is not
// valid either — callers are expected to hold a non-nil allocator.
type Allocator struct {
	// mu guards free and inUse. They are kept exact inverses: a port is in
	// exactly one of the two sets at all times, which is what makes a
	// duplicate hand-out impossible while mu is held across the whole
	// check-and-mutate of Acquire/Release.
	mu    sync.Mutex
	free  []int            // available ports, not currently owned
	inUse map[int]struct{} // ports checked out via Acquire
	base  int              // first port of the range
	size  int              // number of ports in the range (== cap(free))
}

// New returns an allocator covering exactly base..base+n-1. It fails if the
// range is empty or leaves the valid TCP port space (1..65535).
func New(base, n int) (*Allocator, error) {
	if n < 1 {
		return nil, fmt.Errorf("ports: size %d: must be at least 1", n)
	}
	if base < 1 {
		return nil, fmt.Errorf("ports: base %d: must be at least 1", base)
	}
	if base+n-1 > maxPort {
		return nil, fmt.Errorf("ports: range [%d, %d] exceeds maximum port %d", base, base+n-1, maxPort)
	}
	// Seed the free list so the lowest port comes out first: fill in
	// descending order and pop from the back.
	free := make([]int, 0, n)
	for i := n - 1; i >= 0; i-- {
		free = append(free, base+i)
	}
	return &Allocator{
		free:  free,
		inUse: make(map[int]struct{}, n),
		base:  base,
		size:  n,
	}, nil
}

// NewTestPorts returns the allocator for speed-test xray listeners: the
// TEST_BASE_PORT .. TEST_BASE_PORT+maxTests-1 range that runCycle walks as
// TEST_BASE_PORT+tested (maxTests is MAX_TEST_PER_CYCLE-ish — one slot per
// concurrent test instead of a hand-computed index). It is New with the
// test-range arguments; no callers are wired up yet.
func NewTestPorts(testBase, maxTests int) (*Allocator, error) {
	return New(testBase, maxTests)
}

// NewSpareWANPorts returns the allocator for spare service (non-test) xray
// listeners used by make-before-break swaps: the spares ports sitting
// directly above the wanCount live service slots, i.e.
// wanBase+wanCount .. wanBase+wanCount+spares-1, where live services occupy
// wanBase .. wanBase+wanCount-1. It is New with the offset already applied;
// no callers are wired up yet.
func NewSpareWANPorts(wanBase, wanCount, spares int) (*Allocator, error) {
	if wanCount < 0 {
		return nil, fmt.Errorf("ports: wanCount %d: must not be negative", wanCount)
	}
	return New(wanBase+wanCount, spares)
}

// Acquire checks out a free port and returns it. It never blocks: when the
// whole range is held it returns (0, ErrExhausted). The returned port is
// owned by the caller until a matching Release; ownership is what prevents
// two speed tests (or a swap's spare listener) from binding the same port.
func (a *Allocator) Acquire() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.free) == 0 {
		return 0, ErrExhausted
	}
	p := a.free[len(a.free)-1]
	a.free = a.free[:len(a.free)-1]
	a.inUse[p] = struct{}{}
	return p, nil
}

// Release returns p to the pool. Releasing a port this allocator does not
// currently hold — because it was never acquired, was already released, or
// lies outside the configured range — returns an error and leaves the free
// list exactly as it was; a double Release therefore cannot inflate the
// pool or hand a held port to a second owner. Releasing a valid held port
// returns nil.
func (a *Allocator) Release(p int) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p < a.base || p >= a.base+a.size {
		return fmt.Errorf("ports: release %d: outside range [%d, %d]", p, a.base, a.base+a.size-1)
	}
	if _, ok := a.inUse[p]; !ok {
		return fmt.Errorf("ports: release %d: %w", p, ErrNotOwned)
	}
	delete(a.inUse, p)
	a.free = append(a.free, p)
	return nil
}

// Len reports the allocator's capacity: how many ports the range spans.
// It is constant for the life of the allocator.
func (a *Allocator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.size
}

// InUse reports how many ports are currently checked out via Acquire and
// not yet returned via Release — i.e. the number of live owners.
func (a *Allocator) InUse() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.inUse)
}

// Free reports how many ports are currently available for Acquire.
// At rest, Free() + InUse() == Len().
func (a *Allocator) Free() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.free)
}
