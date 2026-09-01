package main

import (
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"
)

func TestReadyzRequiresRoutableWAN_Unhealthy(t *testing.T) {
	pool := NewWANPool(1, 10700)
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

	// Mark the slot as unhealthy: ConsecutiveFails above threshold
	pool.RecordFailure(0)
	pool.RecordFailure(0)
	pool.RecordFailure(0)

	handler := NewObservabilityHandler(pool)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("got status %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if rec.Body.String() == "" {
		t.Error("expected non-empty body")
	}
}

func TestReadyzRequiresRoutableWAN_Healthy(t *testing.T) {
	pool := NewWANPool(1, 10700)
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

	// ConsecutiveFails=0, below threshold
	pool.RecordSuccess(0)

	handler := NewObservabilityHandler(pool)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "ready\n" {
		t.Errorf("got body %q, want %q", rec.Body.String(), "ready\n")
	}
}