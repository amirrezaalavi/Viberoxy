package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
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

func activeTestSlot(t *testing.T, pool *WANPool, index int, cfg *ProxyConfig) {
	t.Helper()
	if err := pool.StartTesting(index, cfg); err != nil {
		t.Fatalf("StartTesting: %v", err)
	}
	if err := pool.SetActive(index, nil, ""); err != nil {
		t.Fatalf("SetActive: %v", err)
	}
}

func TestHandleDropWAN_ReplacesFromCandidatePool(t *testing.T) {
	defer metricWanStability.Set(0, "0")

	pool := NewWANPool(1, 10700)
	current := &ProxyConfig{Protocol: "ss", Server: "old.example", Port: 443, Raw: "ss://old"}
	replacement := &ProxyConfig{Protocol: "ss", Server: "new.example", Port: 8443, Raw: "ss://new"}
	activeTestSlot(t, pool, 0, current)

	candidates := NewCandidatePool(10)
	candidates.Update([]*TestResult{
		{Config: current, Speed: 100},
		{Config: replacement, Speed: 80},
	})

	var testedConfig, startedConfig *ProxyConfig
	var testPort, startPort int
	opts := DropAndReplaceOptions{
		Candidates:      candidates,
		TestPort:        10800,
		Timeout:         3 * time.Second,
		DownloadURL:     "https://example.test/file",
		DownloadSize:    1_000_000,
		StabilityProbes: 2,
		XrayMux:         true,
		TestCandidate: func(cfg *ProxyConfig, port int, timeout time.Duration, downloadURL string, downloadSize int64, stabilityProbes int) *TestResult {
			testedConfig = cfg
			testPort = port
			if timeout != 3*time.Second || downloadURL != "https://example.test/file" || downloadSize != 1_000_000 || stabilityProbes != 2 {
				t.Errorf("tester options were not forwarded")
			}
			return &TestResult{Config: cfg, Speed: 73.5, StabilityScore: 1}
		},
		StartCandidate: func(cfg *ProxyConfig, port int, muxEnabled ...bool) (*exec.Cmd, string, error) {
			startedConfig = cfg
			startPort = port
			if len(muxEnabled) != 1 || !muxEnabled[0] {
				t.Errorf("mux option = %v, want [true]", muxEnabled)
			}
			return nil, "/tmp/replacement.json", nil
		},
	}

	trigger := make(chan struct{}, 1)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/viberoxy/wans/0/drop", nil)
	handleDropWAN(pool, opts, trigger).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		Status string      `json:"status"`
		WAN    WANSlotInfo `json:"wan"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Status != "replaced" {
		t.Errorf("status = %q, want replaced", response.Status)
	}
	if response.WAN.Index != 0 || response.WAN.State != "active" || response.WAN.SpeedMbps != 73.5 {
		t.Errorf("wan = %+v, want active slot 0 at 73.5 Mbps", response.WAN)
	}
	if testedConfig != replacement || startedConfig != replacement {
		t.Errorf("tested=%p started=%p, want replacement %p", testedConfig, startedConfig, replacement)
	}
	if testPort != 10800 {
		t.Errorf("test port = %d, want 10800", testPort)
	}
	if startPort != 10700 {
		t.Errorf("start port = %d, want slot service port 10700", startPort)
	}
	if pool.Slots[0].Config != replacement || pool.GetState(0) != StateActive {
		t.Errorf("slot was not activated with replacement")
	}
	select {
	case <-trigger:
		t.Error("cycle trigger should not be queued after successful replacement")
	default:
	}
}

func TestHandleDropWAN_NoCandidatesTriggersCycle(t *testing.T) {
	pool := NewWANPool(1, 10700)
	current := &ProxyConfig{Protocol: "ss", Server: "old.example", Port: 443, Raw: "ss://old"}
	activeTestSlot(t, pool, 0, current)
	candidates := NewCandidatePool(1)
	candidates.Update([]*TestResult{{Config: current, Speed: 50}})

	trigger := make(chan struct{}, 1)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/viberoxy/wans/0/drop", nil)
	handleDropWAN(pool, DropAndReplaceOptions{Candidates: candidates}, trigger).ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["status"] != "replacing" || response["message"] != "no candidates, fetching..." {
		t.Errorf("response = %#v", response)
	}
	if pool.GetState(0) != StateEmpty || pool.Slots[0].Config != nil {
		t.Error("dropped slot was not cleared")
	}
	select {
	case <-trigger:
	default:
		t.Fatal("cycle trigger was not queued")
	}
}

func TestHandleDropWAN_ReplacementTestFailure(t *testing.T) {
	pool := NewWANPool(1, 10700)
	activeTestSlot(t, pool, 0, &ProxyConfig{Raw: "ss://old"})
	replacement := &ProxyConfig{Protocol: "ss", Server: "new.example", Port: 443, Raw: "ss://new"}
	candidates := NewCandidatePool(1)
	candidates.Update([]*TestResult{{Config: replacement, Speed: 50}})
	started := false
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		TestCandidate: func(cfg *ProxyConfig, port int, timeout time.Duration, downloadURL string, downloadSize int64, stabilityProbes int) *TestResult {
			return &TestResult{Config: cfg, Error: errors.New("probe failed")}
		},
		StartCandidate: func(cfg *ProxyConfig, port int, muxEnabled ...bool) (*exec.Cmd, string, error) {
			started = true
			return nil, "", nil
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/viberoxy/wans/0/drop", nil)
	handleDropWAN(pool, opts, make(chan struct{}, 1)).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadGateway, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["status"] != "error" || response["message"] != "replacement failed test" {
		t.Errorf("response = %#v", response)
	}
	if started {
		t.Error("StartCandidate called after failed test")
	}
	if pool.GetState(0) != StateEmpty {
		t.Errorf("slot state = %v, want empty", pool.GetState(0))
	}
}

func TestHandleDropWAN_RejectsInvalidRequests(t *testing.T) {
	handler := handleDropWAN(NewWANPool(1, 10700), DropAndReplaceOptions{Candidates: NewCandidatePool(1)}, make(chan struct{}, 1))
	tests := []struct {
		name   string
		method string
		path   string
		status int
	}{
		{name: "method", method: http.MethodGet, path: "/api/viberoxy/wans/0/drop", status: http.StatusMethodNotAllowed},
		{name: "non numeric", method: http.MethodPost, path: "/api/viberoxy/wans/nope/drop", status: http.StatusBadRequest},
		{name: "missing suffix", method: http.MethodPost, path: "/api/viberoxy/wans/0", status: http.StatusBadRequest},
		{name: "negative", method: http.MethodPost, path: "/api/viberoxy/wans/-1/drop", status: http.StatusBadRequest},
		{name: "out of range", method: http.MethodPost, path: "/api/viberoxy/wans/1/drop", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Errorf("status = %d, want %d; body=%s", rec.Code, tt.status, rec.Body.String())
			}
			if tt.status == http.StatusMethodNotAllowed && rec.Header().Get("Allow") != http.MethodPost {
				t.Errorf("Allow = %q, want POST", rec.Header().Get("Allow"))
			}
			if rec.Header().Get("Content-Type") != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", rec.Header().Get("Content-Type"))
			}
		})
	}
}

func TestWANPoolDropAndReplace_StartFailureLeavesSlotEmpty(t *testing.T) {
	pool := NewWANPool(1, 10700)
	activeTestSlot(t, pool, 0, &ProxyConfig{Raw: "ss://old"})
	replacement := &ProxyConfig{Protocol: "ss", Server: "new.example", Port: 443, Raw: "ss://new"}
	candidates := NewCandidatePool(1)
	candidates.Update([]*TestResult{{Config: replacement, Speed: 50}})
	wantErr := errors.New("start failed")

	_, err := pool.DropAndReplace(0, DropAndReplaceOptions{
		Candidates: candidates,
		TestCandidate: func(cfg *ProxyConfig, port int, timeout time.Duration, downloadURL string, downloadSize int64, stabilityProbes int) *TestResult {
			return &TestResult{Config: cfg, Speed: 45}
		},
		StartCandidate: func(cfg *ProxyConfig, port int, muxEnabled ...bool) (*exec.Cmd, string, error) {
			return nil, "", wantErr
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want wrapped %v", err, wantErr)
	}
	if pool.GetState(0) != StateEmpty || pool.Slots[0].Config != nil {
		t.Error("slot should remain empty after start failure")
	}
}
