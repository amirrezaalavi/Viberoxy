package main

import (
	"errors"
	"os/exec"
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
	DrainAt          time.Time

	mu sync.Mutex
}

type WANPool struct {
	Slots    []*WANSlot
	BasePort int
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
	slot.DrainAt = time.Time{}
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

// DefaultFailThreshold is the number of consecutive failures after which a
// WAN slot is considered unhealthy: GetLeastLoaded excludes it from load
// balancing and the keepalive loop marks it draining for replacement.
const DefaultFailThreshold = 2

// GetLeastLoaded returns the index of the active/draining slot with the
// fewest connections. Slots whose ConsecutiveFails has reached the given
// threshold are skipped as unhealthy; with no argument the default
// DefaultFailThreshold (2) applies.
func (p *WANPool) GetLeastLoaded(thresholds ...int) int {
	threshold := DefaultFailThreshold
	if len(thresholds) > 0 {
		threshold = thresholds[0]
	}
	best := -1
	var bestCount int64 = -1
	for i, slot := range p.Slots {
		slot.mu.Lock()
		s := slot.State
		c := atomic.LoadInt64(&slot.ConnCount)
		fails := atomic.LoadInt64(&slot.ConsecutiveFails)
		slot.mu.Unlock()
		if s == StateActive || s == StateDraining {
			if fails >= int64(threshold) {
				continue
			}
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
