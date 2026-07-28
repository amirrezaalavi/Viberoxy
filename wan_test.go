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
