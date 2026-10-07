package main

import (
	"os/exec"
	"sync"
	"testing"
	"time"
	"viberoxy/internal/path"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

// slotOf maps a selection result to the slot index the assertions below use
// (-1 when no path was selected), keeping index-worded expectations readable.
func slotOf(p *path.Path) int {
	if p == nil {
		return -1
	}
	return p.Slot
}

// slotInflight reads the raw inflight counter of the path currently occupying
// a slot. Connection accounting lives on the Path now, not on the slot.
func slotInflight(pool *WANPool, index int) int64 {
	return pool.Slots[index].Current.Load().Inflight.Load()
}

// setSlotInflight seeds the inflight counter of the slot's current path, the
// way tests used to seed slot.ConnCount directly.
func setSlotInflight(pool *WANPool, index int, n int64) {
	pool.Slots[index].Current.Load().Inflight.Store(n)
}

// setFails records n consecutive failures on the slot's current path, the way
// tests used to seed slot.ConsecutiveFails directly.
func setFails(pool *WANPool, index, n int) {
	for i := 0; i < n; i++ {
		pool.RecordFailure(index)
	}
}

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
	cfg := &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443}
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
	cfg := &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443}
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
	cfg := &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443}
	if err := pool.StartTesting(0, cfg); err != nil {
		t.Fatalf("StartTesting error: %v", err)
	}
	cmd := exec.Command("sleep", "9999")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmd.Process.Kill()

	proc := xrayproc.Wrap(cmd, "/tmp/test-config.json")
	err := pool.SetActive(0, proc, "/tmp/test-config.json")
	if err != nil {
		t.Fatalf("SetActive error: %v", err)
	}
	if pool.Slots[0].State != StateActive {
		t.Errorf("state = %v, want active", pool.Slots[0].State)
	}
	if pool.Slots[0].Cmd != proc {
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
	cfg := &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443}
	if err := pool.StartTesting(0, cfg); err != nil {
		t.Fatalf("StartTesting error: %v", err)
	}
	cmd := exec.Command("sleep", "9999")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmd.Process.Kill()
	if err := pool.SetActive(0, xrayproc.Wrap(cmd, "/tmp/test-config.json"), "/tmp/test-config.json"); err != nil {
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
	cfg := &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443}

	pool.StartTesting(0, cfg)
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, xrayproc.Wrap(cmd, "/tmp/test-config.json"), "/tmp/test-config.json")
	before := pool.Slots[0].Current.Load()
	before.Reserve()

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
	if slot.Current.Load().Inflight.Load() != 0 {
		t.Error("ConnCount should be 0")
	}
	// The reset installed a NEW generation: the retired Path (with the
	// reservation still on it) is no longer the slot's current occupant.
	if after := slot.Current.Load(); after == before || after.ID == before.ID {
		t.Errorf("ResetEmpty must swap in a fresh Path, got the same generation ID %d", after.ID)
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
	pool.StartTesting(0, &proxycfg.ProxyConfig{})
	if pool.GetState(0) != StateTesting {
		t.Error("expected testing")
	}
}

func TestGetSlotsByState(t *testing.T) {
	pool := NewWANPool(4, 10700)
	pool.StartTesting(0, &proxycfg.ProxyConfig{})
	pool.StartTesting(1, &proxycfg.ProxyConfig{})

	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")

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

	pool.StartTesting(0, &proxycfg.ProxyConfig{})
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
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
	pool.StartTesting(0, &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443})
	cmd := exec.Command("sleep", "9999")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmd.Process.Kill()
	if err := pool.SetActive(0, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json"); err != nil {
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
	pool.StartTesting(0, &proxycfg.ProxyConfig{Server: "9.9.9.9", Port: 9999})
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
	pool.Slots[1].Config = &proxycfg.ProxyConfig{Server: "1.1.1.1", Port: 80}
	if pool.HasServerPort("1.1.1.1", 80) {
		t.Error("HasServerPort(empty slot with stale config) = true, want false")
	}
}

func TestGetLeastLoaded(t *testing.T) {
	pool := NewWANPool(3, 10700)

	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &proxycfg.ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		cmd.Start()
		defer cmd.Process.Kill()
		pool.SetActive(i, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
	}

	setSlotInflight(pool, 0, 10)
	setSlotInflight(pool, 1, 3)

	idx := slotOf(pool.GetLeastLoaded())
	if idx != 1 {
		t.Errorf("expected slot 1 (3 conns), got %d", idx)
	}
}

func TestGetLeastLoaded_AllEmpty(t *testing.T) {
	pool := NewWANPool(3, 10700)
	idx := slotOf(pool.GetLeastLoaded())
	if idx != -1 {
		t.Errorf("expected -1, got %d", idx)
	}
}

func TestGetLeastLoaded_SkipsUnhealthy(t *testing.T) {
	pool := NewWANPool(2, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &proxycfg.ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
	}

	// Slot 0 is unhealthy (2 fails >= threshold 2) despite fewer connections.
	setSlotInflight(pool, 0, 1)
	setSlotInflight(pool, 1, 5)
	pool.RecordFailure(0)
	pool.RecordFailure(0)

	if idx := slotOf(pool.GetLeastLoaded(2)); idx != 1 {
		t.Errorf("GetLeastLoaded(2) = %d, want 1 (skip unhealthy slot 0)", idx)
	}

	// The default threshold (2) behaves identically.
	if idx := slotOf(pool.GetLeastLoaded()); idx != 1 {
		t.Errorf("GetLeastLoaded() = %d, want 1 (default threshold)", idx)
	}

	// A successful probe clears slot 0, which then wins on connection count.
	pool.RecordSuccess(0)
	if idx := slotOf(pool.GetLeastLoaded(2)); idx != 0 {
		t.Errorf("GetLeastLoaded(2) after recovery = %d, want 0", idx)
	}
}

func TestGetLeastLoaded_AllUnhealthy_FallsBack(t *testing.T) {
	pool := NewWANPool(2, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &proxycfg.ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
	}
	pool.RecordFailure(0)
	pool.RecordFailure(0)
	pool.RecordFailure(1)
	pool.RecordFailure(1)

	// With the new fallback semantics, GetLeastLoaded never returns -1
	// when active slots exist — it picks the least-loaded among all
	// active/draining slots even if all are over the threshold.
	idx := slotOf(pool.GetLeastLoaded(2))
	if idx != 0 {
		t.Errorf("GetLeastLoaded(2) = %d, want 0 (fallback to least-loaded)", idx)
	}
}

func TestGetLeastLoaded_ThresholdOne(t *testing.T) {
	pool := NewWANPool(2, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &proxycfg.ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
	}

	// With threshold 1, a single failure excludes a slot.
	pool.RecordFailure(0)
	if idx := slotOf(pool.GetLeastLoaded(1)); idx != 1 {
		t.Errorf("GetLeastLoaded(1) = %d, want 1", idx)
	}
}

// TestIncDecConnCount pins reserve/release accounting on the slot's Path —
// connection counts no longer live on the slot itself.
func TestIncDecConnCount(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pth := pool.Slots[0].Current.Load()
	pth.Reserve()
	pth.Reserve()
	pth.Reserve()
	if c := pth.Inflight.Load(); c != 3 {
		t.Errorf("expected 3, got %d", c)
	}
	pth.Release()
	if c := pth.Inflight.Load(); c != 2 {
		t.Errorf("expected 2, got %d", c)
	}
	pth.Release()
	pth.Release()
	if c := pth.Inflight.Load(); c != 0 {
		t.Errorf("expected 0, got %d", c)
	}
}

func TestDrainExpired(t *testing.T) {
	pool := NewWANPool(3, 10700)
	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &proxycfg.ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		cmd.Start()
		defer cmd.Process.Kill()
		pool.SetActive(i, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
	}
	pool.MarkDraining(0)
	pool.MarkDraining(1)

	pool.Slots[0].DrainAt = time.Now().Add(-2 * time.Minute)
	pool.Slots[1].DrainAt = time.Now().Add(-30 * time.Second)
	// Both still carry an in-flight connection: completion by
	// inflight == 0 must not fire, so the AGE rule (DRAIN_MAX clock)
	// is what distinguishes them.
	pool.Slots[0].Current.Load().Reserve()
	pool.Slots[1].Current.Load().Reserve()

	expired := pool.DrainExpired(time.Minute)
	if len(expired) != 1 || expired[0] != 0 {
		t.Errorf("expected [0], got %v", expired)
	}
}

func TestDrainExpired_NotYet(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.StartTesting(0, &proxycfg.ProxyConfig{})
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
	pool.MarkDraining(0)

	pool.Slots[0].DrainAt = time.Now()
	// The flow is still in flight: neither inflight == 0 nor the clock
	// may complete the drain yet.
	pool.Slots[0].Current.Load().Reserve()

	expired := pool.DrainExpired(time.Minute)
	if len(expired) != 0 {
		t.Errorf("expected empty, got %v", expired)
	}
}

func TestHealthCheckAll(t *testing.T) {
	pool := NewWANPool(3, 10700)

	pool.StartTesting(0, &proxycfg.ProxyConfig{})
	cmdAlive := exec.Command("sleep", "9999")
	if err := cmdAlive.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmdAlive.Process.Kill()
	pool.SetActive(0, xrayproc.Wrap(cmdAlive, "/tmp/cfg1.json"), "/tmp/cfg1.json")

	pool.StartTesting(1, &proxycfg.ProxyConfig{})
	cmdDead := exec.Command("sleep", "0")
	cmdDead.Start()
	cmdDead.Wait()
	pool.SetActive(1, xrayproc.Wrap(cmdDead, "/tmp/cfg2.json"), "/tmp/cfg2.json")

	pool.StartTesting(2, &proxycfg.ProxyConfig{})
	cmdAlive2 := exec.Command("sleep", "9999")
	if err := cmdAlive2.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmdAlive2.Process.Kill()
	pool.SetActive(2, xrayproc.Wrap(cmdAlive2, "/tmp/cfg3.json"), "/tmp/cfg3.json")

	dead := pool.HealthCheckAll()
	if len(dead) != 1 || dead[0] != 1 {
		t.Errorf("expected [1], got %v", dead)
	}
}

func TestShutdownAll(t *testing.T) {
	pool := NewWANPool(2, 10700)

	for i := 0; i < 2; i++ {
		pool.StartTesting(i, &proxycfg.ProxyConfig{})
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		defer cmd.Process.Kill()
		pool.SetActive(i, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
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
	pth := pool.Slots[0].Current.Load()
	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pth.Reserve()
		}()
	}
	wg.Wait()

	if c := pth.Inflight.Load(); c != int64(n) {
		t.Errorf("expected %d, got %d", n, c)
	}

	for i := 0; i < n/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pth.Release()
		}()
	}
	wg.Wait()

	if c := pth.Inflight.Load(); c != int64(n/2) {
		t.Errorf("expected %d, got %d", n/2, c)
	}
}

func TestHealthyActiveCount(t *testing.T) {
	pool := NewWANPool(3, 10700)

	pool.StartTesting(0, &proxycfg.ProxyConfig{})
	cmdAlive := exec.Command("sleep", "9999")
	if err := cmdAlive.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmdAlive.Process.Kill()
	pool.SetActive(0, xrayproc.Wrap(cmdAlive, "/tmp/cfg1.json"), "/tmp/cfg1.json")

	pool.StartTesting(1, &proxycfg.ProxyConfig{})
	cmdDead := exec.Command("sleep", "0")
	cmdDead.Start()
	cmdDead.Wait()
	pool.SetActive(1, xrayproc.Wrap(cmdDead, "/tmp/cfg2.json"), "/tmp/cfg2.json")

	if c := pool.HealthyActiveCount(); c != 1 {
		t.Errorf("expected 1, got %d", c)
	}
}

func TestRecordFailureSuccess(t *testing.T) {
	pool := NewWANPool(2, 10700)

	pool.RecordFailure(0)
	pool.RecordFailure(0)
	if f := pool.SlotConsecutiveFails(0); f != 2 {
		t.Errorf("ConsecutiveFails = %d, want 2", f)
	}

	pool.RecordSuccess(0)
	if f := pool.SlotConsecutiveFails(0); f != 0 {
		t.Errorf("ConsecutiveFails after success = %d, want 0", f)
	}

	// Out-of-range indices must be no-ops.
	pool.RecordFailure(99)
	pool.RecordSuccess(-1)
	if f := pool.SlotConsecutiveFails(1); f != 0 {
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

func TestExitIPAndLastProbe(t *testing.T) {
	pool := NewWANPool(2, 10700)

	// Initially empty.
	if pool.Slots[0].ExitIP != "" {
		t.Errorf("initial ExitIP = %q, want empty", pool.Slots[0].ExitIP)
	}
	if !pool.Slots[0].LastProbe.IsZero() {
		t.Error("initial LastProbe should be zero")
	}

	// Set values.
	pool.Slots[0].ExitIP = "203.0.113.1"
	pool.Slots[0].LastProbe = time.Now()

	if pool.Slots[0].ExitIP != "203.0.113.1" {
		t.Errorf("ExitIP = %q, want 203.0.113.1", pool.Slots[0].ExitIP)
	}
	if pool.Slots[0].LastProbe.IsZero() {
		t.Error("LastProbe should be set")
	}

	// ResetEmpty clears them.
	pool.Slots[0].State = StateActive
	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	if pool.Slots[0].ExitIP != "" {
		t.Errorf("ExitIP after reset = %q, want empty", pool.Slots[0].ExitIP)
	}
	if !pool.Slots[0].LastProbe.IsZero() {
		t.Error("LastProbe should be zero after reset")
	}
}

func TestResetEmpty_FromTesting(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.StartTesting(0, &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443})

	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	if pool.Slots[0].State != StateEmpty {
		t.Error("expected empty state")
	}
}

func TestResetEmpty_FromDraining(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.StartTesting(0, &proxycfg.ProxyConfig{})
	cmd := exec.Command("sleep", "9999")
	cmd.Start()
	defer cmd.Process.Kill()
	pool.SetActive(0, xrayproc.Wrap(cmd, "/tmp/cfg.json"), "/tmp/cfg.json")
	pool.MarkDraining(0)
	pool.Slots[0].Current.Load().Reserve()

	if err := pool.ResetEmpty(0); err != nil {
		t.Fatalf("ResetEmpty error: %v", err)
	}
	if pool.Slots[0].State != StateEmpty {
		t.Error("expected empty state")
	}
}

func TestRoutableWANConcept(t *testing.T) {
	pool := NewWANPool(3, 10700)

	// Slot 0: active but over fail threshold (3 >= 2).
	pool.Slots[0].State = StateActive
	pool.Slots[0].Cmd = xrayproc.Wrap(exec.Command("sleep", "9999"), "")
	setFails(pool, 0, 3)

	// Slot 1: active but over fail threshold (3 >= 2).
	pool.Slots[1].State = StateActive
	pool.Slots[1].Cmd = xrayproc.Wrap(exec.Command("sleep", "9999"), "")
	setFails(pool, 1, 3)

	// Slot 2: active, healthy (0 < 2), with running xray.
	pool.Slots[2].State = StateActive
	pool.Slots[2].Cmd = xrayproc.Wrap(exec.Command("sleep", "9999"), "")
	setFails(pool, 2, 0)

	// Only slot 2 is routable.
	if c := pool.RoutableCount(DefaultFailThreshold); c != 1 {
		t.Errorf("RoutableCount(2) = %d, want 1", c)
	}

	// GetLeastLoaded must return the healthy slot (nil only for an empty pool).
	idx := slotOf(pool.GetLeastLoaded(DefaultFailThreshold))
	if idx != 2 {
		t.Errorf("GetLeastLoaded(2) = %d, want 2 (healthy slot)", idx)
	}
}

// TestOrphanedProcessReap verifies that HealthCheckAll resets a slot to
// StateActive when its xray process has already exited, ensuring no orphaned
// processes linger. This is the TDD RED test for the orphan-reap fix.
func TestOrphanedProcessReap(t *testing.T) {
	pool := NewWANPool(2, 10700)

	// Put slot 0 into StateActive with a long-running command.
	pool.StartTesting(0, &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443})
	cmd := exec.Command("sleep", "3600")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	proc := xrayproc.Wrap(cmd, "/tmp/test-config.json")
	if err := pool.SetActive(0, proc, "/tmp/test-config.json"); err != nil {
		t.Fatalf("SetActive error: %v", err)
	}

	// Simulate the process exiting: kill it; the handle's reaper reaps the
	// zombie, so the test must wait on Exited instead of calling Wait.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill cmd: %v", err)
	}
	select {
	case <-proc.Exited:
	case <-time.After(2 * time.Second):
		t.Fatal("child was not reaped within 2s")
	}

	// Confirm the process is truly gone (reaped via Wait).
	if cmd.ProcessState == nil {
		t.Fatal("ProcessState should be non-nil after Wait")
	}
	// A signal-killed process has Exited()==false but is still reaped.
	// The key property: Success() is false (it did not exit cleanly).
	if cmd.ProcessState.Success() {
		t.Fatal("process should not have exited cleanly")
	}

	// Run the health check — it should detect the dead process and reap
	// the slot, resetting it to StateEmpty.
	dead := pool.HealthCheckAll()
	if len(dead) != 1 || dead[0] != 0 {
		t.Fatalf("HealthCheckAll = %v, want [0]", dead)
	}

	// The critical assertion: after detecting the dead process, the slot
	// must be reset to StateEmpty so no orphan remains and the slot can
	// be reused.
	if pool.GetState(0) != StateEmpty {
		t.Errorf("slot state after reap = %v, want empty", pool.GetState(0))
	}
	if pool.Slots[0].Cmd != nil {
		t.Error("slot.Cmd should be nil after reap")
	}
	if pool.Slots[0].Config != nil {
		t.Error("slot.Config should be nil after reap")
	}
	if pool.Slots[0].ConfigPath != "" {
		t.Error("configPath should be empty after reap")
	}
}

// TestPath_TPATH01_OldPathCannotMutateReplacement is T-PATH-01: the ABA guard
// end to end. Handlers reserve on the *Path the selector handed them and
// release that same pointer; when the slot is reset and re-occupied while
// they are still in flight, their late release (and success credit) must land
// on their own generation and never move the replacement's counters.
func TestPath_TPATH01_OldPathCannotMutateReplacement(t *testing.T) {
	pool := NewWANPool(1, 20000)
	defer pool.ShutdownAll()
	occupy := func() {
		t.Helper()
		if err := pool.StartTesting(0, &proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443}); err != nil {
			t.Fatalf("StartTesting: %v", err)
		}
		cmd := exec.Command("sleep", "30")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cmd: %v", err)
		}
		if err := pool.SetActive(0, xrayproc.Wrap(cmd, ""), ""); err != nil {
			cmd.Process.Kill()
			t.Fatalf("SetActive: %v", err)
		}
	}

	occupy()
	relay := &wanRelay{pool: pool}
	old := pool.Slots[0].Current.Load()
	for i := 0; i < 3; i++ {
		relay.beginWAN(old, "test")
	}
	if got := old.Inflight.Load(); got != 3 {
		t.Fatalf("old path inflight = %d, want 3", got)
	}

	// Replacement while the old handlers are still in flight.
	pool.ResetEmpty(0)
	occupy()
	cur := pool.Slots[0].Current.Load()
	if cur == old || cur.ID == old.ID {
		t.Fatalf("replacement reuses the old Path (ID %d): generations must never be shared", cur.ID)
	}

	// The old handlers finish afterwards.
	for i := 0; i < 3; i++ {
		relay.endWAN(old)
	}
	old.RecordSuccess()

	if got := cur.Inflight.Load(); got != 0 {
		t.Errorf("replacement inflight = %d, want 0 (old handler releases leaked into it)", got)
	}
	if got := cur.ConsecutiveFails(); got != 0 {
		t.Errorf("replacement fails = %d, want 0 (old handler success leaked into it)", got)
	}
	if got := old.Inflight.Load(); got != 0 {
		t.Errorf("old path inflight = %d, want 0 (its own accounting still balances)", got)
	}

	// And the replacement's own accounting still works after the leak attempt.
	cur.Reserve()
	if got := cur.Inflight.Load(); got != 1 {
		t.Errorf("replacement inflight = %d, want 1", got)
	}
}
