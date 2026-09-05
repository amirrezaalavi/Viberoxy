package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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

type WANSlot struct {
	Index            int
	State            WANState
	Config           *ProxyConfig
	Cmd              *exec.Cmd
	ConfigPath       string
	ServicePort      int
	ConnCount        int64
	ConsecutiveFails int64
	SpeedMbps        float64
	// StabilityScore ranks upstream exit churn (distinct exit IPs minus
	// 1; 0 = stable or never probed). Used for replacement preference
	// only — a churny slot is never rejected on this alone.
	StabilityScore int
	DrainAt        time.Time
	// ExitIP is the last observed exit IP from a keepalive probe through
	// this slot's SOCKS5 listener. Empty when never probed.
	ExitIP string
	// LastProbe is the timestamp of the last successful keepalive probe.
	// Zero when never probed.
	LastProbe time.Time

	mu sync.Mutex
}

type WANPool struct {
	Slots    []*WANSlot
	BasePort int

	// replaceMu serializes user-requested drop operations. A replacement
	// temporarily clears a slot while testing and starting a candidate; without
	// this guard, concurrent requests could start multiple xray processes for
	// the same service port.
	replaceMu sync.Mutex
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
type ReplacementTester func(*ProxyConfig, int, time.Duration, string, int64, int) *TestResult

// ReplacementStarter starts the persistent xray process for a replacement.
type ReplacementStarter func(*ProxyConfig, int, ...bool) (*exec.Cmd, string, error)

// DropAndReplaceOptions contains the candidate source, test settings, and
// injectable process operations used by DropAndReplace.
type DropAndReplaceOptions struct {
	Candidates      *CandidatePool
	TestPort        int
	Timeout         time.Duration
	DownloadURL     string
	DownloadSize    int64
	StabilityProbes int
	XrayMux         bool
	TestCandidate   ReplacementTester
	StartCandidate  ReplacementStarter
}

func NewWANPool(count int, basePort int) *WANPool {
	slots := make([]*WANSlot, count)
	for i := 0; i < count; i++ {
		slots[i] = &WANSlot{
			Index:       i,
			State:       StateEmpty,
			ServicePort: basePort + i,
		}
	}
	return &WANPool{Slots: slots, BasePort: basePort}
}

// DropAndReplace removes the current config from a WAN slot, excludes it from
// future selection, re-tests the best remaining candidate, and starts it on
// the slot's existing service port. The slot remains empty whenever no
// candidate is available or promotion fails.
func (p *WANPool) DropAndReplace(index int, opts DropAndReplaceOptions) (*TestResult, error) {
	if index < 0 || index >= len(p.Slots) {
		return nil, errors.New("slot index out of range")
	}

	p.replaceMu.Lock()
	defer p.replaceMu.Unlock()

	slot := p.Slots[index]
	slot.mu.Lock()
	current := slot.Config
	servicePort := slot.ServicePort
	slot.mu.Unlock()

	if opts.Candidates != nil && current != nil && current.Raw != "" {
		opts.Candidates.Exclude(current.Raw)
	}
	if err := p.ResetEmpty(index); err != nil {
		return nil, fmt.Errorf("drop WAN: %w", err)
	}

	if opts.Candidates == nil {
		return nil, ErrNoReplacementCandidate
	}
	candidate := opts.Candidates.Best()
	if candidate == nil || candidate.Config == nil {
		return nil, ErrNoReplacementCandidate
	}

	testCandidate := opts.TestCandidate
	if testCandidate == nil {
		testCandidate = TestSpeedWithStability
	}
	result := testCandidate(candidate.Config, opts.TestPort, opts.Timeout, opts.DownloadURL, opts.DownloadSize, opts.StabilityProbes)
	if result == nil || result.Error != nil {
		if result != nil && result.Error != nil {
			return nil, fmt.Errorf("%w: %v", ErrReplacementTestFailed, result.Error)
		}
		return nil, ErrReplacementTestFailed
	}

	if err := p.StartTesting(index, candidate.Config); err != nil {
		return nil, fmt.Errorf("prepare replacement slot: %w", err)
	}

	startCandidate := opts.StartCandidate
	if startCandidate == nil {
		startCandidate = StartXray
	}
	cmd, path, err := startCandidate(candidate.Config, servicePort, opts.XrayMux)
	if err != nil {
		_ = p.ResetEmpty(index)
		return nil, fmt.Errorf("start replacement xray: %w", err)
	}
	if err := p.SetActive(index, cmd, path); err != nil {
		_ = StopXray(cmd, path)
		_ = p.ResetEmpty(index)
		return nil, fmt.Errorf("activate replacement slot: %w", err)
	}
	p.SetSlotSpeedMbps(index, result.Speed)
	p.SetSlotStability(index, result.StabilityScore)
	return result, nil
}

func (p *WANPool) StartTesting(index int, cfg *ProxyConfig) error {
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

func (p *WANPool) SetActive(index int, cmd *exec.Cmd, configPath string) error {
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
	return nil
}

func (p *WANPool) MarkDraining(index int) error {
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
	return nil
}

func (p *WANPool) ResetEmpty(index int) error {
	if index < 0 || index >= len(p.Slots) {
		return errors.New("slot index out of range")
	}
	slot := p.Slots[index]
	slot.mu.Lock()
	cmd := slot.Cmd
	configPath := slot.ConfigPath
	slot.mu.Unlock()

	if cmd != nil {
		StopXray(cmd, configPath)
	}

	slot.mu.Lock()
	slot.State = StateEmpty
	slot.Config = nil
	slot.Cmd = nil
	slot.ConfigPath = ""
	slot.ConnCount = 0
	slot.ConsecutiveFails = 0
	slot.SpeedMbps = 0
	slot.StabilityScore = 0
	slot.DrainAt = time.Time{}
	slot.ExitIP = ""
	slot.LastProbe = time.Time{}
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
			fails := atomic.LoadInt64(&slot.ConsecutiveFails)
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

// RecordFailure atomically increments a slot's consecutive-failure counter.
func (p *WANPool) RecordFailure(index int) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	atomic.AddInt64(&p.Slots[index].ConsecutiveFails, 1)
}

// RecordSuccess atomically resets a slot's consecutive-failure counter.
func (p *WANPool) RecordSuccess(index int) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	atomic.StoreInt64(&p.Slots[index].ConsecutiveFails, 0)
}

// SlotConsecutiveFails returns the current consecutive-failure count for a
// slot (0 for out-of-range indices).
func (p *WANPool) SlotConsecutiveFails(index int) int64 {
	if index < 0 || index >= len(p.Slots) {
		return 0
	}
	return atomic.LoadInt64(&p.Slots[index].ConsecutiveFails)
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

// RoutableCount returns the number of active/draining slots that are
// routable: their ConsecutiveFails is strictly below the given threshold
// and their Cmd (xray process handle) is non-nil. A slot with a nil Cmd
// cannot serve traffic even if its state is active.
func (p *WANPool) RoutableCount(threshold int) int {
	count := 0
	for _, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		cmd := slot.Cmd
		slot.mu.Unlock()
		fails := atomic.LoadInt64(&slot.ConsecutiveFails)
		if s == StateActive || s == StateDraining {
			if fails < int64(threshold) && cmd != nil {
				count++
			}
		}
	}
	return count
}

// GetLeastLoaded returns the index of the active/draining slot with the
// fewest connections. Slots whose ConsecutiveFails has reached the given
// threshold are skipped as unhealthy; with no argument the default
// DefaultFailThreshold (2) applies.
//
// If no routable slot exists (all active/draining slots are over the
// threshold), it falls back to the least-loaded among all active/draining
// slots and logs a warning so the degradation is visible. This prevents
// the proxy from blackholing traffic when every WAN is degraded but at
// least one is still alive.
func (p *WANPool) GetLeastLoaded(thresholds ...int) int {
	threshold := DefaultFailThreshold
	if len(thresholds) > 0 {
		threshold = thresholds[0]
	}

	best := -1
	var bestCount int64 = -1

	// First pass: pick the least-loaded among routable slots.
	for i, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		c := atomic.LoadInt64(&slot.ConnCount)
		cmd := slot.Cmd
		slot.mu.Unlock()
		fails := atomic.LoadInt64(&slot.ConsecutiveFails)
		if s == StateActive || s == StateDraining {
			if fails < int64(threshold) && cmd != nil {
				if best == -1 || c < bestCount {
					best = i
					bestCount = c
				}
			}
		}
	}

	if best != -1 {
		return best
	}

	// Fallback: no routable slot. Pick the least-loaded among ALL
	// active/draining slots (even over threshold) to avoid blackhole.
	slog.Warn("no routable WAN: falling back to degraded slots", "threshold", threshold)
	for i, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		c := atomic.LoadInt64(&slot.ConnCount)
		slot.mu.Unlock()
		if s == StateActive || s == StateDraining {
			if best == -1 || c < bestCount {
				best = i
				bestCount = c
			}
		}
	}
	return best
}

func (p *WANPool) IncConnCount(index int) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	atomic.AddInt64(&p.Slots[index].ConnCount, 1)
}

func (p *WANPool) DecConnCount(index int) {
	if index < 0 || index >= len(p.Slots) {
		return
	}
	atomic.AddInt64(&p.Slots[index].ConnCount, -1)
}

func (p *WANPool) DrainExpired(graceDuration time.Duration) []int {
	var result []int
	now := time.Now()
	for i, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		drainAt := slot.DrainAt
		slot.mu.Unlock()
		if s == StateDraining && !drainAt.IsZero() && now.Sub(drainAt) >= graceDuration {
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
