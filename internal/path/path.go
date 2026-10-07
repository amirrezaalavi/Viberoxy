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
	"sync"
	"sync/atomic"
	"time"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

// State is a Path's lifecycle state, stored as int32 in Path.State.
//
// Only Active, Draining and Dead are assigned today. Probation and Suspect
// exist so the later health/scheduler tasks (SPEC-H/SPEC-S) can grow the
// state machine here without another identity change; nothing selects or
// ejects on them yet.
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
	// ewmaTTFB is the time-to-first-byte EWMA — placeholder field, zero-valued;
	// populated by later health/scheduler tasks.
	ewmaTTFB time.Duration
	// goodputBps is the goodput EWMA in bits per second — placeholder field,
	// zero-valued; populated by later health/scheduler tasks.
	goodputBps float64
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
