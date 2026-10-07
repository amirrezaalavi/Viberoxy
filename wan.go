package main

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"viberoxy/internal/cands"
	"viberoxy/internal/health"
	"viberoxy/internal/path"
	"viberoxy/internal/ports"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

type WANState int

const (
	StateEmpty WANState = iota
	StateTesting
	StateActive
	StateDraining
)

func (s WANState) String() string {
	switch s {
	case StateEmpty:
		return "empty"
	case StateTesting:
		return "testing"
	case StateActive:
		return "active"
	case StateDraining:
		return "draining"
	default:
		return "unknown"
	}
}

// DrainReason records WHY a slot went Draining (F-02). It decides whether
// health recovery may bring the slot back (un-drain) or whether it stays
// draining until the drain completes.
type DrainReason int

const (
	// DrainReplace is a drain ordered because the slot is being replaced:
	// the cycle's replacement marking (runCycle) and the generic
	// MarkDraining callers. It stays draining until the drain completes —
	// canary recovery never un-drains the retirement.
	DrainReplace DrainReason = iota
	// DrainHealth is a drain ordered because the slot's health failed
	// (runCanaries at WAN_FAIL_THRESHOLD). When its canary streak
	// recovers, the slot returns to Active instead of being reaped.
	DrainHealth
)

type WANSlot struct {
	Index       int
	State       WANState
	Config      *proxycfg.ProxyConfig
	Cmd         *xrayproc.Handle
	ConfigPath  string
	ServicePort int
	// Current is the identity of the xray process occupying this slot right
	// now: one *path.Path per occupant lifetime, swapped (never reused) on
	// every (re)activation. Handlers reserve and release through the *Path
	// they held at connection start — never through this slot index — so
	// events left over from a previous occupant can never reach the current
	// one (F-04). The pointer is never nil: NewWANPool and ResetEmpty always
	// install one, vacant when the slot is empty.
	Current   *atomic.Pointer[path.Path]
	SpeedMbps float64
	// StabilityScore ranks upstream exit churn (distinct exit IPs minus
	// 1; 0 = stable or never probed). Used for replacement preference
	// only — a churny slot is never rejected on this alone.
	StabilityScore int
	DrainAt        time.Time
	// drainReason is why DrainAt was set (DrainReplace/DrainHealth);
	// meaningful only while State == StateDraining. Guarded by mu. The
	// zero value is DrainReplace: a slot put into StateDraining by hand
	// (fixtures) is never un-drained, only reaped.
	drainReason DrainReason
	// ExitIP is the last observed exit IP from a keepalive probe through
	// this slot's SOCKS5 listener. Empty when never probed.
	ExitIP string
	// LastProbe is the timestamp of the last successful keepalive probe.
	// Zero when never probed.
	LastProbe time.Time

	// releasePort hands ServicePort back to the allocator it was checked
	// out of, or nil when the slot serves on its own base port. Non-nil
	// implies ServicePort is a held spare (F-13 make-before-break): the
	// spare is released only after its listener is down — by the swap that
	// retires it or by ResetEmpty — and the slot then returns to its base
	// port. Guarded by mu.
	releasePort func()

	mu sync.Mutex
}

type WANPool struct {
	Slots    []*WANSlot
	BasePort int

	// replaceMu serializes the two short critical sections of a make-
	// before-break replacement — the candidate pick and the Swap cutover —
	// so concurrent drops cannot interleave their slot bookkeeping. It is
	// deliberately NOT held across the speed test or the process start
	// (F-13): replacements of different slots test in parallel and the old
	// occupant serves throughout. Listener collisions are prevented by the
	// port allocators now, not by this lock.
	replaceMu sync.Mutex

	// testPorts and sparePorts are the shared F-13 port budgets, wired
	// once by startup (SetPortAllocators) before any goroutine that reads
	// them starts: every speed test — the cycle's and the drop/replace
	// API's — checks its port out of testPorts, and make-before-break
	// replacements boot on ports from sparePorts. Both stay nil on
	// hand-built pools (unit tests), which keeps the legacy fixed-port
	// behavior. Read through the nil-safe accessors.
	testPorts  *ports.Allocator
	sparePorts *ports.Allocator
}

// SetPortAllocators wires the shared port budgets (F-13): the speed-test
// allocator used by runCycle/startup/DropAndReplace alike, and the spare
// service-port allocator replacements boot on. Call it once at wiring time
// (startup); nil means "no allocator" (legacy fixed-port behavior).
func (p *WANPool) SetPortAllocators(test, spare *ports.Allocator) {
	p.testPorts = test
	p.sparePorts = spare
}

// TestPorts returns the shared speed-test port allocator, or nil when the
// pool was built without one. Nil-receiver safe.
func (p *WANPool) TestPorts() *ports.Allocator {
	if p == nil {
		return nil
	}
	return p.testPorts
}

// SparePorts returns the spare service-port allocator replacements boot on,
// or nil when the pool was built without one. Nil-receiver safe.
func (p *WANPool) SparePorts() *ports.Allocator {
	if p == nil {
		return nil
	}
	return p.sparePorts
}

var (
	// ErrNoReplacementCandidate means every candidate is either failed or
	// excluded (including the config that was just dropped).
	ErrNoReplacementCandidate = errors.New("no replacement candidate")
	// ErrReplacementTestFailed means a cached candidate failed its mandatory
	// re-test and was not promoted to a WAN slot.
	ErrReplacementTestFailed = errors.New("replacement failed test")
)

// ReplacementTester re-tests a cached candidate before it is promoted.
type ReplacementTester func(*proxycfg.ProxyConfig, int, time.Duration, string, int64, int) *TestResult

// ReplacementStarter starts the persistent xray process for a replacement.
type ReplacementStarter func(*proxycfg.ProxyConfig, int, ...bool) (*xrayproc.Handle, string, error)

// DropAndReplaceOptions contains the candidate source, test settings, and
// injectable process operations used by DropAndReplace.
type DropAndReplaceOptions struct {
	Candidates      *cands.Pool
	TestPort        int
	Timeout         time.Duration
	DownloadURL     string
	DownloadSize    int64
	StabilityProbes int
	XrayMux         bool
	TestCandidate   ReplacementTester
	StartCandidate  ReplacementStarter
	// TestPorts is the shared speed-test port budget (F-13): when wired,
	// each candidate test checks a port out of it and returns it when the
	// test ends, so a cycle test and an API-triggered replacement test can
	// never collide on one listener port; exhaustion is a clean
	// ErrNoReplacementCandidate-family error, never a reused port. Nil
	// falls back to TestPort.
	TestPorts *ports.Allocator
	// SparePorts holds the spare service ports (WAN_BASE_PORT+WAN_COUNT..)
	// an occupied slot boots its replacement on, so the old process keeps
	// listening on its own port until the cutover. Nil makes an occupied
	// slot start on its own service port (legacy callers / tests with
	// injected starters).
	SparePorts *ports.Allocator
}

func NewWANPool(count int, basePort int) *WANPool {
	slots := make([]*WANSlot, count)
	for i := 0; i < count; i++ {
		slot := &WANSlot{
			Index:       i,
			State:       StateEmpty,
			Current:     new(atomic.Pointer[path.Path]),
			ServicePort: basePort + i,
		}
		// Every slot starts with its own vacant generation, so Current is
		// never nil and even an empty slot accounts for itself.
		slot.Current.Store(path.NewVacant(i, basePort+i))
		slots[i] = slot
	}
	return &WANPool{Slots: slots, BasePort: basePort}
}

// DropAndReplace replaces the config serving a WAN slot make-before-break
// (F-13): the candidate is picked, speed-tested and started while the OLD
// occupant keeps serving, and only a validated, running replacement is
// swapped in (Swap). The retired occupant is stopped strictly AFTER that
// cutover — never before it — so no traffic is dropped while the
// replacement's test runs. Every failure path (no candidate, exhausted port
// budget, failed test, failed start, lost swap race) leaves the slot exactly
// as it was: the old path keeps serving and a failed replacement never
// empties the slot.
//
// Locking: replaceMu guards only the two short critical sections — the pick
// below and the Swap cutover — and is deliberately not held across the
// speed test or the process start, so concurrent replacements of different
// slots test in parallel (the review's lock complaint).
//
// Ports: the test port comes from opts.TestPorts when wired (the shared
// TEST_BASE_PORT allocator the cycle's tests draw from, so the two can
// never collide); an occupied slot starts its replacement on a spare
// service port from opts.SparePorts (WAN_BASE_PORT+WAN_COUNT..), never on
// the slot's own port while the old process still listens there. Without
// allocators (unit tests, hand-wired callers) it falls back to opts.TestPort
// and the slot's own port.
func (p *WANPool) DropAndReplace(index int, opts DropAndReplaceOptions) (*TestResult, error) {
	if index < 0 || index >= len(p.Slots) {
		return nil, errors.New("slot index out of range")
	}

	// Critical section 1: snapshot the occupant and pick the candidate.
	// Nothing below mutates the slot until the Swap cutover, so the old
	// occupant stays Active and serving for the whole test (RT-10).
	p.replaceMu.Lock()
	slot := p.Slots[index]
	slot.mu.Lock()
	state := slot.State
	current := slot.Config
	expected := slot.Current.Load()
	servicePort := slot.ServicePort
	oldCmd := slot.Cmd
	oldConfigPath := slot.ConfigPath
	oldRelease := slot.releasePort
	slot.mu.Unlock()

	if opts.Candidates == nil {
		p.replaceMu.Unlock()
		return nil, ErrNoReplacementCandidate
	}
	// F-12: never hand back the config just dropped (Exclude below covers
	// its Raw; the identity checks below also cover an empty Raw), and
	// never a config whose server:port is already serving another slot —
	// the same rule runCycle applies before promoting a config
	// (HasServerPort: active or draining slots own their server:port).
	if current != nil && current.Raw != "" {
		opts.Candidates.Exclude(current.Raw)
	}
	candidate := opts.Candidates.Best(func(cfg *proxycfg.ProxyConfig) bool {
		if current != nil && cfg == current {
			return true
		}
		if current != nil && current.Raw != "" && cfg.Raw == current.Raw {
			return true
		}
		return p.HasServerPort(cfg.Server, cfg.Port)
	})
	p.replaceMu.Unlock()
	if candidate == nil || candidate.Config == nil {
		return nil, ErrNoReplacementCandidate
	}

	// Validate the candidate on a port checked out of the shared allocator
	// (F-13 port discipline): exhaustion is a clean retry-later error, and
	// the port goes back the moment the test ends — never reused while
	// another test still holds it.
	testPort := opts.TestPort
	releaseTest := func() {}
	if opts.TestPorts != nil {
		tp, err := opts.TestPorts.Acquire()
		if err != nil {
			return nil, fmt.Errorf("%w: no free speed-test port", ErrNoReplacementCandidate)
		}
		testPort = tp
		releaseTest = func() { _ = opts.TestPorts.Release(tp) }
	}
	testCandidate := opts.TestCandidate
	if testCandidate == nil {
		testCandidate = TestSpeedWithStability
	}
	result := testCandidate(candidate.Config, testPort, opts.Timeout, opts.DownloadURL, opts.DownloadSize, opts.StabilityProbes)
	releaseTest()
	if result == nil || result.Error != nil {
		if result != nil && result.Error != nil {
			return nil, fmt.Errorf("%w: %v", ErrReplacementTestFailed, result.Error)
		}
		return nil, ErrReplacementTestFailed
	}

	// Start the replacement. An occupied slot starts on a SPARE service
	// port so the old process keeps listening on its own port until the
	// cutover; an empty slot has no occupant to protect and reuses its own
	// (free) port.
	startPort := servicePort
	var releaseSpare func()
	if state != StateEmpty && opts.SparePorts != nil {
		sp, err := opts.SparePorts.Acquire()
		if err != nil {
			return nil, fmt.Errorf("%w: no free spare service port", ErrNoReplacementCandidate)
		}
		startPort = sp
		releaseSpare = func() { _ = opts.SparePorts.Release(sp) }
	}
	startCandidate := opts.StartCandidate
	if startCandidate == nil {
		startCandidate = StartXray
	}
	cmd, configPath, err := startCandidate(candidate.Config, startPort, opts.XrayMux)
	if err != nil {
		if releaseSpare != nil {
			releaseSpare()
		}
		return nil, fmt.Errorf("start replacement xray: %w", err)
	}

	// Critical section 2: the atomic cutover. Swap refuses when the slot no
	// longer holds the path this replacement was validated against (another
	// replacement won, or the slot was reset while we tested); the loser
	// simply discards the process it started — the slot's current occupant
	// keeps serving.
	newPath := path.New(candidate.Config, cmd, index, startPort)
	p.replaceMu.Lock()
	_, swapErr := p.Swap(index, expected, newPath, configPath, releaseSpare)
	p.replaceMu.Unlock()
	if swapErr != nil {
		_ = StopXray(cmd, configPath)
		if releaseSpare != nil {
			releaseSpare()
		}
		return nil, swapErr
	}

	// Post-cutover teardown of the retired occupant: stop its process
	// first, then hand its spare port back (the listener must be down
	// before the port can be re-issued). This is the first moment any part
	// of the old WAN is touched — that ordering is the F-13 fix.
	if oldCmd != nil {
		_ = StopXray(oldCmd, oldConfigPath)
	} else if expected != nil && expected.Proc != nil {
		_ = expected.Stop()
	}
	if oldRelease != nil {
		oldRelease()
	}

	p.SetSlotSpeedMbps(index, result.Speed)
	p.SetSlotStability(index, result.StabilityScore)
	return result, nil
}

// Swap is the make-before-break cutover (F-13): it atomically installs
// newPath as the occupant of slot index — config, process handle, temp
// config path, service port (front-end dials follow the new listener) and
// spare-port ownership all move to the replacement — and retires the
// previous occupant's Path to Draining so its counters stay intact for
// handlers still in flight. The slot lands StateActive with cleared
// speed/stability/probe bookkeeping; the caller re-records what it
// measured.
//
// expected is the Path the caller observed before validating the
// replacement. If the slot no longer holds it — another replacement won
// the race, or the slot was reset while the candidate was tested — Swap
// fails with an ErrNoReplacementCandidate-wrapped error and changes
// NOTHING, so the caller can discard the process it started.
//
// Swap performs no I/O: the caller stops the retired occupant strictly
// after Swap returns, never before it — that ordering is the whole point
// of make-before-break.
func (p *WANPool) Swap(index int, expected, newPath *path.Path, configPath string, releasePort func()) (*path.Path, error) {
	if index < 0 || index >= len(p.Slots) {
		return nil, errors.New("slot index out of range")
	}
	if newPath == nil {
		return nil, errors.New("nil replacement path")
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	old := slot.Current.Load()
	if old == nil || old != expected {
		slot.mu.Unlock()
		return nil, fmt.Errorf("%w: slot changed while the replacement was being validated", ErrNoReplacementCandidate)
	}
	// The generation being replaced goes Draining: it keeps its own
	// counters for in-flight handlers until its teardown. A vacant
	// placeholder (fixture-built slot) stays as it is.
	if old.Cfg != nil || old.Proc != nil {
		old.SetState(path.Draining)
	}
	slot.State = StateActive
	slot.Config = newPath.Cfg
	slot.Cmd = newPath.Proc
	slot.ConfigPath = configPath
	// The slot adopts the replacement's listener port: relay and probe
	// dials read ServicePort, so it must follow the cutover — the
	// replacement boots on a spare port while the old process keeps its
	// own until it is stopped after this point.
	slot.ServicePort = newPath.Port
	slot.releasePort = releasePort
	slot.SpeedMbps = 0
	slot.StabilityScore = 0
	slot.DrainAt = time.Time{}
	slot.drainReason = DrainReplace
	slot.ExitIP = ""
	slot.LastProbe = time.Time{}
	slot.Current.Store(newPath)
	slot.mu.Unlock()
	return old, nil
}

func (p *WANPool) StartTesting(index int, cfg *proxycfg.ProxyConfig) error {
	if index < 0 || index >= len(p.Slots) {
		return errors.New("slot index out of range")
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.State != StateEmpty {
		return errors.New("slot is not empty")
	}
	slot.State = StateTesting
	slot.Config = cfg
	return nil
}

func (p *WANPool) SetActive(index int, cmd *xrayproc.Handle, configPath string) error {
	if index < 0 || index >= len(p.Slots) {
		return errors.New("slot index out of range")
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.State != StateTesting {
		return errors.New("slot is not in testing state")
	}
	slot.State = StateActive
	slot.Cmd = cmd
	slot.ConfigPath = configPath
	// (Re)activation mints a brand-new generation: the replacement gets its
	// own Path with a fresh ID, so reservations and credits held on the
	// previous occupant stay with that occupant.
	slot.Current.Store(path.New(slot.Config, cmd, index, slot.ServicePort))
	// Any drain bookkeeping belonged to the previous occupant.
	slot.drainReason = DrainReplace
	return nil
}

// MarkDraining moves an Active slot into StateDraining because it is
// BEING REPLACED (the cycle's replacement marking, and the generic
// callers): the slot serves its existing flows and is reaped when the
// drain completes (inflight == 0 or DRAIN_MAX) — health recovery never
// un-drains a retirement. See MarkDrainingHealth for the health flavor.
func (p *WANPool) MarkDraining(index int) error {
	return p.markDraining(index, DrainReplace)
}

// MarkDrainingHealth moves an Active slot into StateDraining because its
// HEALTH failed (runCanaries at WAN_FAIL_THRESHOLD). Unlike MarkDraining,
// this drain is reversible: when the path's canary streak recovers the
// slot returns to Active instead of being reaped (F-02, un-drain).
func (p *WANPool) MarkDrainingHealth(index int) error {
	return p.markDraining(index, DrainHealth)
}

func (p *WANPool) markDraining(index int, reason DrainReason) error {
	if index < 0 || index >= len(p.Slots) {
		return errors.New("slot index out of range")
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.State != StateActive {
		return errors.New("slot is not active")
	}
	slot.State = StateDraining
	slot.DrainAt = time.Now()
	slot.drainReason = reason
	if cur := slot.Current.Load(); cur != nil {
		cur.SetState(path.Draining)
	}
	return nil
}

// UnDrainIfRecovered returns a HEALTH-drained slot (F-02) to Active when
// its health has recovered: either this canary interval re-admitted the
// path (halfOpenRecovery, the return of Path.NoteCanary) or its
// consecutive canary-success streak reached health.HalfOpenSuccesses.
// A slot marked Draining for replacement (DrainReplace) is never
// un-drained — its drain is the retirement, not an ejection. Reports
// whether the slot was un-drained. Safe on out-of-range indices.
func (p *WANPool) UnDrainIfRecovered(index int, halfOpenRecovery bool) bool {
	if index < 0 || index >= len(p.Slots) {
		return false
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.State != StateDraining || slot.drainReason != DrainHealth {
		return false
	}
	cur := slot.Current.Load()
	if !halfOpenRecovery && cur.CanaryStreak() < health.HalfOpenSuccesses {
		return false
	}
	if cur.GetState() != path.Draining {
		return false // retired/vacant generation: nothing to bring back
	}
	slot.State = StateActive
	slot.DrainAt = time.Time{}
	slot.drainReason = DrainReplace
	cur.SetState(path.Active)
	slog.Info("wan un-drained: health recovered, back to active", "index", index)
	return true
}

func (p *WANPool) ResetEmpty(index int) error {
	if index < 0 || index >= len(p.Slots) {
		return errors.New("slot index out of range")
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	old := slot.Current.Load()
	cmd := slot.Cmd
	configPath := slot.ConfigPath
	release := slot.releasePort
	slot.mu.Unlock()

	// Stop the retired occupant. For an activated slot old.Proc and cmd are
	// the same handle, so the process and its temp config are torn down
	// exactly as before the Path split.
	if cmd != nil {
		StopXray(cmd, configPath)
	} else if old != nil && old.Proc != nil {
		_ = old.Stop()
	}

	// F-13: a slot serving on a spare service port hands the port back only
	// after its listener is down, and returns to its own base port so the
	// next activation binds exactly what runCycle/startup expect.
	if release != nil {
		release()
	}

	slot.mu.Lock()
	slot.State = StateEmpty
	slot.Config = nil
	slot.Cmd = nil
	slot.ConfigPath = ""
	slot.SpeedMbps = 0
	slot.StabilityScore = 0
	slot.DrainAt = time.Time{}
	slot.drainReason = DrainReplace
	slot.ExitIP = ""
	slot.LastProbe = time.Time{}
	if release != nil {
		slot.ServicePort = p.BasePort + index
		slot.releasePort = nil
	}
	// Retire the old generation and swap in a fresh vacant one. The retired
	// Path keeps its own counters for handlers still in flight — they can no
	// longer touch whatever occupies this slot next (F-04).
	if old != nil {
		old.SetState(path.Dead)
	}
	slot.Current.Store(path.NewVacant(index, slot.ServicePort))
	slot.mu.Unlock()
	return nil
}

func (p *WANPool) GetState(index int) WANState {
	if index < 0 || index >= len(p.Slots) {
		return StateEmpty
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.State
}

func (p *WANPool) GetSlotsByState(states ...WANState) []int {
	var result []int
	for i, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		slot.mu.Unlock()
		for _, st := range states {
			if s == st {
				result = append(result, i)
				break
			}
		}
	}
	return result
}

func (p *WANPool) ActiveCount() int {
	count := 0
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		slot.mu.Unlock()
		if s == StateActive || s == StateDraining {
			count++
		}
	}
	return count
}

// HasServerPort reports whether any active or draining slot already serves
// the given upstream server:port. Used to avoid promoting the same endpoint
// into two WAN slots (WAN dedupe). Testing and empty slots are ignored.
func (p *WANPool) HasServerPort(server string, port int) bool {
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cfg := slot.Config
		slot.mu.Unlock()
		if s == StateActive || s == StateDraining {
			if cfg != nil && cfg.Server == server && cfg.Port == port {
				return true
			}
		}
	}
	return false
}

// HealthyActiveCount returns the number of active/draining slots considered
// healthy. Called with no arguments it health-checks the underlying xray
// process (legacy behavior). Called with a threshold it counts slots whose
// ConsecutiveFails is strictly below the threshold.
func (p *WANPool) HealthyActiveCount(thresholds ...int) int {
	if len(thresholds) > 0 {
		threshold := thresholds[0]
		count := 0
		for _, slot := range p.Slots {
			slot.mu.Lock()
			s := slot.State
			fails := slot.Current.Load().ConsecutiveFails()
			slot.mu.Unlock()
			if s == StateActive || s == StateDraining {
				if fails < int64(threshold) {
					count++
				}
			}
		}
		return count
	}

	count := 0
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cmd := slot.Cmd
		slot.mu.Unlock()
		if s == StateActive || s == StateDraining {
			if HealthCheckXray(cmd) {
				count++
			}
		}
	}
	return count
}

// RecordFailure atomically increments the consecutive-failure counter of the
// path currently occupying the slot: a probe targets the xray listening on
// that service port right now, and its credit must not outlive it.
func (p *WANPool) RecordFailure(index int) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	p.Slots[index].Current.Load().RecordFailure()
}

// RecordSuccess atomically resets the consecutive-failure counter of the
// path currently occupying the slot.
func (p *WANPool) RecordSuccess(index int) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	p.Slots[index].Current.Load().RecordSuccess()
}

// SlotConsecutiveFails returns the current consecutive-failure count for a
// slot (0 for out-of-range indices).
func (p *WANPool) SlotConsecutiveFails(index int) int64 {
	if index < 0 || index >= len(p.Slots) {
		return 0
	}
	return p.Slots[index].Current.Load().ConsecutiveFails()
}

// SlotSpeedMbps returns the last measured speed for a slot (0 if unknown).
func (p *WANPool) SlotSpeedMbps(index int) float64 {
	if index < 0 || index >= len(p.Slots) {
		return 0
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.SpeedMbps
}

// SetSlotSpeedMbps records the measured speed for a slot.
func (p *WANPool) SetSlotSpeedMbps(index int, speed float64) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.SpeedMbps = speed
}

// SlotStabilityScore returns the last measured stability score for a slot
// (0 = unknown/stable when never probed).
func (p *WANPool) SlotStabilityScore(index int) int {
	if index < 0 || index >= len(p.Slots) {
		return 0
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.StabilityScore
}

// SetSlotStability records the measured stability score for a slot and
// exposes it as the viberoxy_wan_stability gauge.
func (p *WANPool) SetSlotStability(index int, score int) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	slot.StabilityScore = score
	slot.mu.Unlock()
	metricWanStability.Set(float64(score), strconv.Itoa(index))
}

// PickReplacementSlot returns the index of the active slot to prefer when
// replacing a WAN: the one with the highest stability score (least stable
// exit IP, i.e. most churn). Equal scores resolve to the lowest index, so
// with stability probing disabled (all scores 0/unknown) it returns the
// first active slot — the historical behavior. Returns -1 for an empty
// slice.
func (p *WANPool) PickReplacementSlot(activeSlots []int) int {
	if len(activeSlots) == 0 {
		return -1
	}
	best := activeSlots[0]
	for _, idx := range activeSlots[1:] {
		if p.SlotStabilityScore(idx) > p.SlotStabilityScore(best) {
			best = idx
		}
	}
	return best
}

// DefaultFailThreshold is the number of consecutive failures after which a
// WAN slot is considered unhealthy: GetLeastLoaded excludes it from load
// balancing and the keepalive loop marks it draining for replacement.
const DefaultFailThreshold = 2

// RoutableCount returns the number of ACTIVE slots that are routable:
// their ConsecutiveFails is strictly below the given threshold and their
// Cmd (xray process handle) is non-nil. A slot with a nil Cmd cannot
// serve traffic even if its state is active.
//
// Draining slots never count (D-03, F-02): draining means "no NEW
// connections", so /readyz is ready iff at least one ACTIVE routable
// path exists — a pool whose only live paths are draining is not ready.
func (p *WANPool) RoutableCount(threshold int) int {
	count := 0
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cmd := slot.Cmd
		cur := slot.Current.Load()
		slot.mu.Unlock()
		fails := cur.ConsecutiveFails()
		if s == StateActive {
			if fails < int64(threshold) && cmd != nil {
				count++
			}
		}
	}
	return count
}

// GetLeastLoaded returns the *path.Path of the ACTIVE slot with the
// fewest connections — nil when no selectable slot exists. A Draining
// slot is NEVER selected while an Active routable path exists (F-02);
// it is only handed out by the degraded last-resort fallback when no
// active path is available at all. Slots whose current path's
// ConsecutiveFails has reached the given threshold are skipped as
// unhealthy; with no argument the default DefaultFailThreshold (2)
// applies.
//
// The selection rule itself is unchanged: least-loaded wins, ties resolve
// to the lowest slot index, load comes from the current occupant's clamped
// connection count. What the callers hold is no longer the index but the
// returned *Path: the handler reserves and releases that exact generation
// for the connection's lifetime, which is what keeps slot-index accounting
// (and the F-04 ABA bug) out of the relay.
//
// If no routable slot exists (all active slots are over the threshold),
// it falls back to the least-loaded among all active slots and logs a
// warning so the degradation is visible; only when no active path is
// available there either does the fallback consider draining slots. This
// prevents the proxy from blackholing traffic when every WAN is degraded
// but at least one is still alive.
func (p *WANPool) GetLeastLoaded(thresholds ...int) *path.Path {
	return p.GetLeastLoadedExcluding(nil, thresholds...)
}

// GetLeastLoadedExcluding is GetLeastLoaded with an exclusion set: any path
// present in tried (the paths a connection has already attempted on, F-08
// dial-stage failover) is skipped — in the routable pass AND in the degraded
// fallback — so a retry always lands on a path not yet tried. Everything else
// is the selection rule of GetLeastLoaded, byte-for-byte: least-loaded
// active under threshold, ties to the lowest index, degraded fallback last.
// A nil or empty tried map is the plain GetLeastLoaded call.
//
// Draining (F-02): StateDraining is skipped by the routable pass exactly
// like a tried or over-threshold path, and the degraded fallback walks
// ACTIVE slots first — a draining slot is considered only when no active
// path is available (last resort, logged), so a drain never attracts NEW
// connections while any Active routable path exists.
//
// Health ejection (SPEC-H.3, F-01): a path the outcome window ejected
// (path state Suspect) is skipped in the routable pass exactly like an
// over-threshold path; it remains part of the degraded fallback (no
// blackhole when nothing is routable), and half-open re-admission
// (internal/health) clears Suspect again.
func (p *WANPool) GetLeastLoadedExcluding(tried map[*path.Path]bool, thresholds ...int) *path.Path {
	threshold := DefaultFailThreshold
	if len(thresholds) > 0 {
		threshold = thresholds[0]
	}

	var best *path.Path
	var bestCount int64 = -1

	// First pass: pick the least-loaded among routable ACTIVE slots.
	// Draining slots are never eligible here (F-02): they serve their
	// existing flows but take no new connections while any Active
	// path is available — the degraded fallback below decides
	// availability for the last-resort case.
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cur := slot.Current.Load()
		cmd := slot.Cmd
		slot.mu.Unlock()
		if tried[cur] {
			continue // already attempted on this connection
		}
		fails := cur.ConsecutiveFails()
		if s == StateActive {
			if fails < int64(threshold) && cmd != nil && cur.GetState() != path.Suspect {
				c := cur.Conns()
				if best == nil || c < bestCount {
					best = cur
					bestCount = c
				}
			}
		}
	}

	if best != nil {
		return best
	}

	// Fallback: no routable slot. Pick the least-loaded among ALL
	// ACTIVE slots (even over threshold) to avoid blackhole.
	slog.Warn("no routable WAN: falling back to degraded slots", "threshold", threshold)
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cur := slot.Current.Load()
		slot.mu.Unlock()
		if tried[cur] {
			continue // already attempted on this connection
		}
		if s == StateActive {
			c := cur.Conns()
			if best == nil || c < bestCount {
				best = cur
				bestCount = c
			}
		}
	}
	if best != nil {
		return best
	}

	// Last resort (F-02): NO active path is available, so a draining slot
	// may take the connection rather than blackhole — logged so the
	// degradation is visible alongside the "no routable WAN" warning.
	slog.Warn("no active WAN: draining slot selected as last resort", "threshold", threshold)
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cur := slot.Current.Load()
		slot.mu.Unlock()
		if tried[cur] {
			continue // already attempted on this connection
		}
		if s == StateDraining {
			c := cur.Conns()
			if best == nil || c < bestCount {
				best = cur
				bestCount = c
			}
		}
	}
	return best
}

// DrainExpired returns the indices of draining slots whose drain is
// COMPLETE (F-02): the current path has no in-flight connections left
// (existing flows finished) OR maxDrain — DRAIN_MAX — has elapsed since
// the slot started draining, whichever comes first. The caller reaps what
// it returns (ResetEmpty); a slot still carrying flows before DRAIN_MAX
// keeps serving. Before that decision, a HEALTH-drained slot whose canary
// streak has recovered is un-drained back to Active instead of being
// killed (F-02 un-drain); a replacement drain is never un-drained.
func (p *WANPool) DrainExpired(maxDrain time.Duration) []int {
	var result []int
	now := time.Now()
	for i := range p.Slots {
		// Recovery beats the kill: an un-drained slot is Active again
		// and must not appear in the reap list.
		if p.UnDrainIfRecovered(i, false) {
			continue
		}
		slot := p.Slots[i]
		slot.mu.Lock()
		s := slot.State
		drainAt := slot.DrainAt
		cur := slot.Current.Load()
		slot.mu.Unlock()
		if s != StateDraining {
			continue
		}
		if cur.Conns() == 0 {
			result = append(result, i)
			continue
		}
		if !drainAt.IsZero() && now.Sub(drainAt) >= maxDrain {
			result = append(result, i)
		}
	}
	return result
}

func (p *WANPool) HealthCheckAll() []int {
	var result []int
	for i, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cmd := slot.Cmd
		slot.mu.Unlock()
		if s == StateActive || s == StateDraining {
			if !HealthCheckXray(cmd) {
				result = append(result, i)
				// Reap the orphaned slot: reset it to StateEmpty so the
				// process is stopped and the slot can be reused.
				p.ResetEmpty(i)
			}
		}
	}
	return result
}

func (p *WANPool) ShutdownAll() {
	for i := range p.Slots {
		slot := p.Slots[i]
		slot.mu.Lock()
		if slot.State == StateActive {
			slot.State = StateDraining
			slot.DrainAt = time.Now()
		}
		slot.mu.Unlock()
	}

	for i := range p.Slots {
		p.ResetEmpty(i)
	}
}
