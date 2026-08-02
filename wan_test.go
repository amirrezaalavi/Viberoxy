package main

import (
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewWANPool(t *testing.T) {
	pool := NewWANPool(4, 10700)
	if len(pool.Slots) != 4 {
		t.Fatalf("expected 4 slots, got %d", len(pool.Slots))
	}
	if pool.BasePort != 10700 {
		t.Errorf("BasePort = %d, want 10700", pool.BasePort)
	}
	for i, slot := range pool.Slots {
		if slot.Index != i {
			t.Errorf("slot[%d].Index = %d, want %d", i, slot.Index, i)
		}
		if slot.State != StateEmpty {
			t.Errorf("slot[%d].State = %v, want empty", i, slot.State)
		}
		if slot.ServicePort != 10700+i {
			t.Errorf("slot[%d].ServicePort = %d, want %d", i, slot.ServicePort, 10700+i)
		}
	}
}

func TestStartTesting(t *testing.T) {
	pool := NewWANPool(2, 10700)
	cfg := &ProxyConfig{Server: "1.2.3.4", Port: 443}
	err := pool.StartTesting(0, cfg)
	if err != nil {
		t.Fatalf("StartTesting error: %v", err)
	}
	if pool.Slots[0].State != StateTesting {
		t.Errorf("state = %v, want testing", pool.Slots[0].State)
	}
	if pool.Slots[0].Config != cfg {
		t.Error("config not set")
	}
}

func TestStartTesting_NonEmpty(t *testing.T) {
	pool := NewWANPool(2, 10700)
	cfg := &ProxyConfig{Server: "1.2.3.4", Port: 443}
	if err := pool.StartTesting(0, cfg); err != nil {
		t.Fatalf("first StartTesting error: %v", err)
	}
	err := pool.StartTesting(0, cfg)
	if err == nil {
		t.Fatal("expected error for non-empty slot")
	}
}

func TestSetActive(t *testing.T) {
	pool := NewWANPool(2, 10700)
	cfg := &ProxyConfig{Server: "1.2.3.4", Port: 443}
	if err := pool.StartTesting(0, cfg); err != nil {
		t.Fatalf("StartTesting error: %v", err)
	}
	cmd := exec.Command("sleep", "9999")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmd.Process.Kill()

	err := pool.SetActive(0, cmd, "/tmp/test-config.json")
	if err != nil {
		t.Fatalf("SetActive error: %v", err)
	}
	if pool.Slots[0].State != StateActive {
		t.Errorf("state = %v, want active", pool.Slots[0].State)
	}
	if pool.Slots[0].Cmd != cmd {
		t.Error("cmd not set")
	}
	if pool.Slots[0].ConfigPath != "/tmp/test-config.json" {
		t.Error("configPath not set")
	}
}

func TestSetActive_WrongState(t *testing.T) {
	pool := NewWANPool(2, 10700)
	err := pool.SetActive(0, nil, "/tmp/test-config.json")
	if err == nil {
		t.Fatal("expected error for empty slot")
	}
}

func TestMarkDraining(t *testing.T) {
	pool := NewWANPool(2, 10700)
	cfg := &ProxyConfig{Server: "1.2.3.4", Port: 443}
	if err := pool.StartTesting(0, cfg); err != nil {
		t.Fatalf("StartTesting error: %v", err)
	}
	cmd := exec.Command("sleep", "9999")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmd.Process.Kill()
	if err := pool.SetActive(0, cmd, "/tmp/test-config.json"); err != nil {
		t.Fatalf("SetActive error: %v", err)
	}

	before := time.Now()
	err := pool.MarkDraining(0)
	if err != nil {
		t.Fatalf("MarkDraining error: %v", err)
	}
	if pool.Slots[0].State != StateDraining {
		t.Errorf("state = %v, want draining", pool.Slots[0].State)
	}
	if pool.Slots[0].DrainAt.Before(before) {
		t.Error("DrainAt should be set after before")
	}
}

func TestMarkDraining_WrongState(t *testing.T) {
	pool := NewWANPool(2, 10700)
	err := pool.MarkDraining(0)
	if err == nil {
		t.Fatal("expected error for empty slot")
	}
}

func TestResetEmpty(t *testing.T) {
	pool := NewWANPool(2, 10700)
	cfg := &ProxyConfig{Server: "1.2.3.4", Port: 443}

	pool.StartTesting(0, cfg)
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, cmd, "/tmp/test-config.json")
	pool.IncConnCount(0)

	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}

	slot := pool.Slots[0]
	if slot.State != StateEmpty {
		t.Errorf("state = %v, want empty", slot.State)
	}
	if slot.Config != nil {
		t.Error("config should be nil")
	}
	if slot.Cmd != nil {
		t.Error("cmd should be nil")
	}
	if slot.ConfigPath != "" {
		t.Error("configPath should be empty")
	}
	if atomic.LoadInt64(&slot.ConnCount) != 0 {
		t.Error("ConnCount should be 0")
	}
	if !slot.DrainAt.IsZero() {
		t.Error("DrainAt should be zero")
	}
}

func TestGetState(t *testing.T) {
	pool := NewWANPool(2, 10700)
	if pool.GetState(0) != StateEmpty {
		t.Error("expected empty")
	}
	pool.StartTesting(0, &ProxyConfig{})
	if pool.GetState(0) != StateTesting {
		t.Error("expected testing")
	}
}

func TestGetSlotsByState(t *testing.T) {
	pool := NewWANPool(4, 10700)
	pool.StartTesting(0, &ProxyConfig{})
	pool.StartTesting(1, &ProxyConfig{})

	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, cmd, "/tmp/cfg.json")

	emptySlots := pool.GetSlotsByState(StateEmpty)
	if len(emptySlots) != 2 || emptySlots[0] != 2 || emptySlots[1] != 3 {
		t.Errorf("empty slots = %v, want [2 3]", emptySlots)
	}

	testingSlots := pool.GetSlotsByState(StateTesting)
	if len(testingSlots) != 1 || testingSlots[0] != 1 {
		t.Errorf("testing slots = %v, want [1]", testingSlots)
	}

	activeSlots := pool.GetSlotsByState(StateActive)
	if len(activeSlots) != 1 || activeSlots[0] != 0 {
		t.Errorf("active slots = %v, want [0]", activeSlots)
	}

	multi := pool.GetSlotsByState(StateActive, StateTesting)
	if len(multi) != 2 {
		t.Errorf("multi state slots = %v, want 2", multi)
	}
}

func TestActiveCount(t *testing.T) {
	pool := NewWANPool(4, 10700)
	if pool.ActiveCount() != 0 {
		t.Errorf("expected 0, got %d", pool.ActiveCount())
	}

	pool.StartTesting(0, &ProxyConfig{})
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, cmd, "/tmp/cfg.json")
	if pool.ActiveCount() != 1 {
		t.Errorf("expected 1, got %d", pool.ActiveCount())
	}

	pool.MarkDraining(0)
	if pool.ActiveCount() != 1 {
		t.Errorf("expected 1 (draining), got %d", pool.ActiveCount())
	}
}

func TestHasServerPort(t *testing.T) {
	pool := NewWANPool(3, 10700)

	// Empty pool: never a match.
	if pool.HasServerPort("1.2.3.4", 443) {
		t.Error("HasServerPort on empty pool = true, want false")
	}

	// Active slot matches its own server:port.
	pool.StartTesting(0, &ProxyConfig{Server: "1.2.3.4", Port: 443})
	cmd := exec.Command("sleep", "9999")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmd.Process.Kill()
	if err := pool.SetActive(0, cmd, "/tmp/cfg.json"); err != nil {
		t.Fatalf("SetActive error: %v", err)
	}

	if !pool.HasServerPort("1.2.3.4", 443) {
		t.Error("HasServerPort(active match) = false, want true")
	}
	if pool.HasServerPort("1.2.3.4", 8443) {
		t.Error("HasServerPort(same server, different port) = true, want false")
	}
	if pool.HasServerPort("5.6.7.8", 443) {
		t.Error("HasServerPort(different server) = true, want false")
	}

	// Draining slots still count as occupied (they serve traffic until the
	// drain grace period expires).
	if err := pool.MarkDraining(0); err != nil {
		t.Fatalf("MarkDraining error: %v", err)
	}
	if !pool.HasServerPort("1.2.3.4", 443) {
		t.Error("HasServerPort(draining match) = false, want true")
	}

	// A testing slot is NOT a match: the config may fail to activate.
	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	pool.StartTesting(0, &ProxyConfig{Server: "9.9.9.9", Port: 9999})
	if pool.HasServerPort("9.9.9.9", 9999) {
		t.Error("HasServerPort(testing slot) = true, want false")
	}

	// Reset clears the config, so no stale match remains.
	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	if pool.HasServerPort("9.9.9.9", 9999) {
		t.Error("HasServerPort(after reset) = true, want false")
	}

	// Empty slots with a leftover config pointer are ignored.
	pool.Slots[1].State = StateEmpty
	pool.Slots[1].Config = &ProxyConfig{Server: "1.1.1.1", Port: 80}
	if pool.HasServerPort("1.1.1.1", 80) {
		t.Error("HasServerPort(empty slot with stale config) = true, want false")
	}
}

func TestGetLeastLoaded(t *testing.T) {
	pool := NewWANPool(3, 10700)

	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		cmd.Start()
		defer cmd.Process.Kill()
		pool.SetActive(i, cmd, "/tmp/cfg.json")
	}

	atomic.StoreInt64(&pool.Slots[0].ConnCount, 10)
	atomic.StoreInt64(&pool.Slots[1].ConnCount, 3)

	idx := pool.GetLeastLoaded()
	if idx != 1 {
		t.Errorf("expected slot 1 (3 conns), got %d", idx)
	}
}

func TestGetLeastLoaded_AllEmpty(t *testing.T) {
	pool := NewWANPool(3, 10700)
	idx := pool.GetLeastLoaded()
	if idx != -1 {
		t.Errorf("expected -1, got %d", idx)
	}
}

func TestGetLeastLoaded_SkipsUnhealthy(t *testing.T) {
	pool := NewWANPool(2, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, cmd, "/tmp/cfg.json")
	}

	// Slot 0 is unhealthy (2 fails >= threshold 2) despite fewer connections.
	atomic.StoreInt64(&pool.Slots[0].ConnCount, 1)
	atomic.StoreInt64(&pool.Slots[1].ConnCount, 5)
	pool.RecordFailure(0)
	pool.RecordFailure(0)

	if idx := pool.GetLeastLoaded(2); idx != 1 {
		t.Errorf("GetLeastLoaded(2) = %d, want 1 (skip unhealthy slot 0)", idx)
	}

	// The default threshold (2) behaves identically.
	if idx := pool.GetLeastLoaded(); idx != 1 {
		t.Errorf("GetLeastLoaded() = %d, want 1 (default threshold)", idx)
	}

	// A successful probe clears slot 0, which then wins on connection count.
	pool.RecordSuccess(0)
	if idx := pool.GetLeastLoaded(2); idx != 0 {
		t.Errorf("GetLeastLoaded(2) after recovery = %d, want 0", idx)
	}
}

func TestGetLeastLoaded_AllUnhealthy(t *testing.T) {
	pool := NewWANPool(2, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, cmd, "/tmp/cfg.json")
	}
	pool.RecordFailure(0)
	pool.RecordFailure(0)
	pool.RecordFailure(1)
	pool.RecordFailure(1)

	if idx := pool.GetLeastLoaded(2); idx != -1 {
		t.Errorf("GetLeastLoaded(2) = %d, want -1 (all slots unhealthy)", idx)
	}
}

func TestGetLeastLoaded_ThresholdOne(t *testing.T) {
	pool := NewWANPool(2, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, cmd, "/tmp/cfg.json")
	}

	// With threshold 1, a single failure excludes a slot.
	pool.RecordFailure(0)
	if idx := pool.GetLeastLoaded(1); idx != 1 {
		t.Errorf("GetLeastLoaded(1) = %d, want 1", idx)
	}
}

func TestIncDecConnCount(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.IncConnCount(0)
	pool.IncConnCount(0)
	pool.IncConnCount(0)
	if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c != 3 {
		t.Errorf("expected 3, got %d", c)
	}
	pool.DecConnCount(0)
	if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c != 2 {
		t.Errorf("expected 2, got %d", c)
	}
	pool.DecConnCount(0)
	pool.DecConnCount(0)
	if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c != 0 {
		t.Errorf("expected 0, got %d", c)
	}
}

func TestDrainExpired(t *testing.T) {
	pool := NewWANPool(3, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		cmd.Start()
		defer cmd.Process.Kill()
		pool.SetActive(i, cmd, "/tmp/cfg.json")
	}
	pool.MarkDraining(0)
	pool.MarkDraining(1)

	pool.Slots[0].DrainAt = time.Now().Add(-2 * time.Minute)
	pool.Slots[1].DrainAt = time.Now().Add(-30 * time.Second)

	expired := pool.DrainExpired(time.Minute)
	if len(expired) != 1 || expired[0] != 0 {
		t.Errorf("expected [0], got %v", expired)
	}
}

func TestDrainExpired_NotYet(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.StartTesting(0, &ProxyConfig{})
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, cmd, "/tmp/cfg.json")
	pool.MarkDraining(0)

	pool.Slots[0].DrainAt = time.Now()

	expired := pool.DrainExpired(time.Minute)
	if len(expired) != 0 {
		t.Errorf("expected empty, got %v", expired)
	}
}

func TestHealthCheckAll(t *testing.T) {
	pool := NewWANPool(3, 10700)

	pool.StartTesting(0, &ProxyConfig{})
	cmdAlive := exec.Command("sleep", "9999")
	if err := cmdAlive.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmdAlive.Process.Kill()
	pool.SetActive(0, cmdAlive, "/tmp/cfg1.json")

	pool.StartTesting(1, &ProxyConfig{})
	cmdDead := exec.Command("sleep", "0")
	cmdDead.Start()
	cmdDead.Wait()
	pool.SetActive(1, cmdDead, "/tmp/cfg2.json")

	pool.StartTesting(2, &ProxyConfig{})
	cmdAlive2 := exec.Command("sleep", "9999")
	if err := cmdAlive2.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmdAlive2.Process.Kill()
	pool.SetActive(2, cmdAlive2, "/tmp/cfg3.json")

	dead := pool.HealthCheckAll()
	if len(dead) != 1 || dead[0] != 1 {
		t.Errorf("expected [1], got %v", dead)
	}
}

func TestShutdownAll(t *testing.T) {
	pool := NewWANPool(2, 10700)

	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, cmd, "/tmp/cfg.json")
	}

	pool.ShutdownAll()

	for i, slot := range pool.Slots {
		if slot.State != StateEmpty {
			t.Errorf("slot[%d] state = %v, want empty", i, slot.State)
		}
	}
}

func TestConcurrentConnCount(t *testing.T) {
	pool := NewWANPool(1, 10700)
	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.IncConnCount(0)
		}()
	}
	wg.Wait()

	if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c != int64(n) {
		t.Errorf("expected %d, got %d", n, c)
	}

	for i := 0; i < n/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.DecConnCount(0)
		}()
	}
	wg.Wait()

	if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c != int64(n/2) {
		t.Errorf("expected %d, got %d", n/2, c)
	}
}

func TestHealthyActiveCount(t *testing.T) {
	pool := NewWANPool(3, 10700)

	pool.StartTesting(0, &ProxyConfig{})
	cmdAlive := exec.Command("sleep", "9999")
	if err := cmdAlive.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmdAlive.Process.Kill()
	pool.SetActive(0, cmdAlive, "/tmp/cfg1.json")

	pool.StartTesting(1, &ProxyConfig{})
	cmdDead := exec.Command("sleep", "0")
	cmdDead.Start()
	cmdDead.Wait()
	pool.SetActive(1, cmdDead, "/tmp/cfg2.json")

	if c := pool.HealthyActiveCount(); c != 1 {
		t.Errorf("expected 1, got %d", c)
	}
}

func TestRecordFailureSuccess(t *testing.T) {
	pool := NewWANPool(2, 10700)

	pool.RecordFailure(0)
	pool.RecordFailure(0)
	if f := atomic.LoadInt64(&pool.Slots[0].ConsecutiveFails); f != 2 {
		t.Errorf("ConsecutiveFails = %d, want 2", f)
	}

	pool.RecordSuccess(0)
	if f := atomic.LoadInt64(&pool.Slots[0].ConsecutiveFails); f != 0 {
		t.Errorf("ConsecutiveFails after success = %d, want 0", f)
	}

	// Out-of-range indices must be no-ops.
	pool.RecordFailure(99)
	pool.RecordSuccess(-1)
	if f := atomic.LoadInt64(&pool.Slots[1].ConsecutiveFails); f != 0 {
		t.Errorf("slot 1 ConsecutiveFails = %d, want 0", f)
	}
}

func TestSlotConsecutiveFails(t *testing.T) {
	pool := NewWANPool(2, 10700)
	if f := pool.SlotConsecutiveFails(0); f != 0 {
		t.Errorf("initial fails = %d, want 0", f)
	}
	pool.RecordFailure(0)
	pool.RecordFailure(0)
	pool.RecordFailure(0)
	if f := pool.SlotConsecutiveFails(0); f != 3 {
		t.Errorf("fails = %d, want 3", f)
	}
	if f := pool.SlotConsecutiveFails(99); f != 0 {
		t.Errorf("out-of-range fails = %d, want 0", f)
	}
}

func TestHealthyActiveCount_Threshold(t *testing.T) {
	pool := NewWANPool(3, 10700)
	pool.Slots[0].State = StateActive
	pool.Slots[1].State = StateActive
	pool.Slots[2].State = StateActive

	pool.RecordFailure(1)
	pool.RecordFailure(1)
	pool.RecordFailure(1)
	pool.RecordFailure(2)

	// Threshold 3: healthy means ConsecutiveFails < 3 → slots 0 and 2.
	if c := pool.HealthyActiveCount(3); c != 2 {
		t.Errorf("HealthyActiveCount(3) = %d, want 2", c)
	}
	// Threshold 1: only slots with zero consecutive failures → slot 0.
	if c := pool.HealthyActiveCount(1); c != 1 {
		t.Errorf("HealthyActiveCount(1) = %d, want 1", c)
	}
	// Threshold 0: no slot has fails < 0.
	if c := pool.HealthyActiveCount(0); c != 0 {
		t.Errorf("HealthyActiveCount(0) = %d, want 0", c)
	}
	// Draining slots participate too.
	pool.Slots[1].State = StateDraining
	if c := pool.HealthyActiveCount(3); c != 2 {
		t.Errorf("HealthyActiveCount(3) with draining = %d, want 2", c)
	}
}

func TestSlotSpeedMbps(t *testing.T) {
	pool := NewWANPool(2, 10700)

	if s := pool.SlotSpeedMbps(0); s != 0 {
		t.Errorf("initial speed = %v, want 0", s)
	}
	pool.SetSlotSpeedMbps(0, 42.5)
	if s := pool.SlotSpeedMbps(0); s != 42.5 {
		t.Errorf("speed = %v, want 42.5", s)
	}
	if s := pool.SlotSpeedMbps(99); s != 0 {
		t.Errorf("out-of-range speed = %v, want 0", s)
	}
}

func TestSlotStabilityScore(t *testing.T) {
	pool := NewWANPool(2, 10700)

	if s := pool.SlotStabilityScore(0); s != 0 {
		t.Errorf("initial stability = %d, want 0 (unknown/stable)", s)
	}
	pool.SetSlotStability(0, 3)
	if s := pool.SlotStabilityScore(0); s != 3 {
		t.Errorf("stability = %d, want 3", s)
	}
	if s := pool.SlotStabilityScore(99); s != 0 {
		t.Errorf("out-of-range stability = %d, want 0", s)
	}

	// Out-of-range writes are no-ops.
	pool.SetSlotStability(99, 7)
	pool.SetSlotStability(-1, 7)
	if s := pool.SlotStabilityScore(1); s != 0 {
		t.Errorf("slot 1 stability = %d, want 0 (untouched)", s)
	}

	// ResetEmpty clears the score.
	pool.SetSlotStability(0, 2)
	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	if s := pool.SlotStabilityScore(0); s != 0 {
		t.Errorf("stability after reset = %d, want 0", s)
	}
}

func TestPickReplacementSlot(t *testing.T) {
	pool := NewWANPool(4, 10700)

	// Empty list -> -1.
	if idx := pool.PickReplacementSlot(nil); idx != -1 {
		t.Errorf("PickReplacementSlot(nil) = %d, want -1", idx)
	}

	// All scores unknown (0): historical behavior — first active slot.
	slots := []int{0, 1, 2}
	if idx := pool.PickReplacementSlot(slots); idx != 0 {
		t.Errorf("PickReplacementSlot(all unknown) = %d, want 0", idx)
	}

	// The least stable slot (highest score) wins.
	pool.SetSlotStability(0, 2)
	pool.SetSlotStability(1, 4)
	pool.SetSlotStability(2, 1)
	if idx := pool.PickReplacementSlot(slots); idx != 1 {
		t.Errorf("PickReplacementSlot = %d, want 1 (highest stability score)", idx)
	}

	// Ties resolve to the lowest index in the given order.
	pool.SetSlotStability(2, 4)
	if idx := pool.PickReplacementSlot(slots); idx != 1 {
		t.Errorf("PickReplacementSlot(tie) = %d, want 1 (first of tied)", idx)
	}
	if idx := pool.PickReplacementSlot([]int{2, 1, 0}); idx != 2 {
		t.Errorf("PickReplacementSlot(ordered [2 1 0]) = %d, want 2", idx)
	}

	// A single active slot is always itself.
	if idx := pool.PickReplacementSlot([]int{3}); idx != 3 {
		t.Errorf("PickReplacementSlot([3]) = %d, want 3", idx)
	}
}

func TestResetEmpty_FromTesting(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.StartTesting(0, &ProxyConfig{Server: "1.2.3.4", Port: 443})

	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	if pool.Slots[0].State != StateEmpty {
		t.Error("expected empty state")
	}
}

func TestResetEmpty_FromDraining(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.StartTesting(0, &ProxyConfig{})
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, cmd, "/tmp/cfg.json")
	pool.MarkDraining(0)
	pool.IncConnCount(0)

	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	if pool.Slots[0].State != StateEmpty {
		t.Error("expected empty state")
	}
}
