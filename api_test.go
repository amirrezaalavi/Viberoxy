package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"sync/atomic"
	"testing"
)

func TestHandleGetWANSlots(t *testing.T) {
	pool := NewWANPool(3, 10700)

	// Slot 0: empty (default).
	// Slot 1: active with some state.
	pool.StartTesting(1, &ProxyConfig{Server: "1.2.3.4", Port: 443})
	cmd := exec.Command("sleep", "9999")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	defer cmd.Process.Kill()
	if err := pool.SetActive(1, cmd, "/tmp/cfg.json"); err != nil {
		t.Fatalf("SetActive error: %v", err)
	}
	pool.SetSlotSpeedMbps(1, 42.5)
	pool.Slots[1].ExitIP = "203.0.113.1"
	atomic.StoreInt64(&pool.Slots[1].ConnCount, 7)

	// Slot 2: draining.
	pool.StartTesting(2, &ProxyConfig{Server: "5.6.7.8", Port: 8443})
	cmd2 := exec.Command("sleep", "9999")
	if err := cmd2.Start(); err != nil {
		t.Fatalf("start cmd2: %v", err)
	}
	defer cmd2.Process.Kill()
	if err := pool.SetActive(2, cmd2, "/tmp/cfg2.json"); err != nil {
		t.Fatalf("SetActive(2) error: %v", err)
	}
	if err := pool.MarkDraining(2); err != nil {
		t.Fatalf("MarkDraining error: %v", err)
	}
	atomic.StoreInt64(&pool.Slots[2].ConsecutiveFails, 1)

	// Call the handler.
	handler := handleGetWANSlots(pool)
	req := httptest.NewRequest(http.MethodGet, "/api/viberoxy/wans", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Check status and content type.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	// Decode the response.
	var infos []WANSlotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &infos); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(infos) != 3 {
		t.Fatalf("got %d slots, want 3", len(infos))
	}

	// Verify slot 0 (empty).
	if infos[0].State != "empty" {
		t.Errorf("slot 0 state = %q, want empty", infos[0].State)
	}
	if infos[0].ExitIP != "" {
		t.Errorf("slot 0 exit_ip = %q, want empty", infos[0].ExitIP)
	}

	// Verify slot 1 (active).
	if infos[1].State != "active" {
		t.Errorf("slot 1 state = %q, want active", infos[1].State)
	}
	if infos[1].SpeedMbps != 42.5 {
		t.Errorf("slot 1 speed_mbps = %v, want 42.5", infos[1].SpeedMbps)
	}
	if infos[1].Conns != 7 {
		t.Errorf("slot 1 conns = %d, want 7", infos[1].Conns)
	}
	if infos[1].ExitIP != "203.0.113.1" {
		t.Errorf("slot 1 exit_ip = %q, want 203.0.113.1", infos[1].ExitIP)
	}

	// Verify slot 2 (draining).
	if infos[2].State != "draining" {
		t.Errorf("slot 2 state = %q, want draining", infos[2].State)
	}
	if infos[2].ConsecutiveFails != 1 {
		t.Errorf("slot 2 consecutive_fails = %d, want 1", infos[2].ConsecutiveFails)
	}
}

func TestHandleGetWANSlots_EmptyPool(t *testing.T) {
	pool := NewWANPool(0, 10700)
	handler := handleGetWANSlots(pool)
	req := httptest.NewRequest(http.MethodGet, "/api/viberoxy/wans", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var infos []WANSlotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &infos); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("got %d slots, want 0", len(infos))
	}
}