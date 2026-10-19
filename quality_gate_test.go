package main

// Tests for the promotion quality gate (Admissible) and its inputs: the
// TTFB ceiling, the direct-egress (leak) detector and the duplicate exit-IP
// gate. These exist because the pool used to admit nodes on Mbps alone: a
// 5-second-latency path, or a config that was not proxying at all, could
// become a WAN and only fail later in front of the client.

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"viberoxy/internal/proxycfg"
)

func cfgNamed(name string) *proxycfg.ProxyConfig {
	return &proxycfg.ProxyConfig{Protocol: "socks5", Server: name, Port: 1080, Name: name, Raw: "socks5://" + name + ":1080"}
}

func TestAdmissible(t *testing.T) {
	const hostIP = "203.0.113.5"

	fast := &TestResult{Config: cfgNamed("fast"), Speed: 40, TTFB: 80 * time.Millisecond, ExitIP: "198.51.100.1"}

	tests := []struct {
		name      string
		result    *TestResult
		minSpeed  float64
		maxTTFB   time.Duration
		hostIP    string
		active    map[string]bool
		wantEmpty bool
		wantSub   string
	}{
		{name: "nil", result: nil, wantSub: "nil result"},
		{name: "failed", result: &TestResult{Config: cfgNamed("bad"), Error: errors.New("boom")}, wantSub: "test failed"},
		{name: "slow speed", result: &TestResult{Config: cfgNamed("slow"), Speed: 1, TTFB: time.Millisecond}, minSpeed: 5, wantSub: "below speed floor"},
		{name: "high ttfb", result: &TestResult{Config: cfgNamed("laggy"), Speed: 40, TTFB: 3 * time.Second}, minSpeed: 5, maxTTFB: 2500 * time.Millisecond, wantSub: "above ttfb ceiling"},
		{name: "ttfb ceiling disabled", result: &TestResult{Config: cfgNamed("laggy"), Speed: 40, TTFB: 30 * time.Second}, minSpeed: 5, maxTTFB: 0, wantEmpty: true},
		{name: "direct egress", result: &TestResult{Config: cfgNamed("leak"), Speed: 40, TTFB: time.Millisecond, ExitIP: hostIP}, minSpeed: 5, maxTTFB: 0, hostIP: hostIP, wantSub: "direct egress"},
		{name: "duplicate exit", result: &TestResult{Config: cfgNamed("dup"), Speed: 40, TTFB: time.Millisecond, ExitIP: "198.51.100.9"}, minSpeed: 5, active: map[string]bool{"198.51.100.9": true}, wantSub: "duplicate exit ip"},
		{name: "unknown exit skips ip gates", result: &TestResult{Config: cfgNamed("unknown"), Speed: 40, TTFB: time.Millisecond, ExitIP: ""}, minSpeed: 5, maxTTFB: time.Second, hostIP: hostIP, active: map[string]bool{"198.51.100.9": true}, wantEmpty: true},
		{name: "unknown host ip skips leak gate", result: &TestResult{Config: cfgNamed("unknownhost"), Speed: 40, TTFB: time.Millisecond, ExitIP: "198.51.100.9"}, minSpeed: 5, hostIP: "", active: nil, wantEmpty: true},
		{name: "admissible", result: fast, minSpeed: 5, maxTTFB: 2500 * time.Millisecond, hostIP: hostIP, active: map[string]bool{"198.51.100.9": true}, wantEmpty: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Admissible(tc.result, tc.minSpeed, tc.maxTTFB, tc.hostIP, tc.active)
			if tc.wantEmpty {
				if got != "" {
					t.Fatalf("Admissible = %q, want \"\" (admissible)", got)
				}
				return
			}
			if got == "" {
				t.Fatalf("Admissible = \"\", want a rejection containing %q", tc.wantSub)
			}
			if !strings.Contains(got, tc.wantSub) {
				t.Fatalf("Admissible = %q, want it to contain %q", got, tc.wantSub)
			}
		})
	}
}

// resetHostExitIP clears the memoized direct exit IP between tests.
func resetHostExitIP() {
	hostExitIPMu.Lock()
	hostExitIPValue = ""
	hostExitIPAt = time.Time{}
	hostExitIPFailAt = time.Time{}
	hostExitIPMu.Unlock()
}

func TestHostExitIPCachesAndBacksOff(t *testing.T) {
	oldFn, oldTTL, oldBackoff := directExitIPFunc, exitIPTTL, exitIPFailureBackoff
	defer func() {
		directExitIPFunc, exitIPTTL, exitIPFailureBackoff = oldFn, oldTTL, oldBackoff
		resetHostExitIP()
	}()
	resetHostExitIP()

	calls := 0
	directExitIPFunc = func(time.Duration) (string, error) {
		calls++
		return "198.51.100.7", nil
	}

	if got := hostExitIP(time.Second); got != "198.51.100.7" {
		t.Fatalf("hostExitIP = %q, want the probed IP", got)
	}
	if got := hostExitIP(time.Second); got != "198.51.100.7" {
		t.Fatalf("cached hostExitIP = %q", got)
	}
	if calls != 1 {
		t.Fatalf("direct probe calls = %d, want 1 (cached)", calls)
	}

	// A failed probe must be remembered: without the backoff a host that
	// cannot reach the probe endpoint would pay one timeout per candidate.
	resetHostExitIP()
	calls = 0
	directExitIPFunc = func(time.Duration) (string, error) {
		calls++
		return "", errors.New("no route")
	}
	if got := hostExitIP(time.Second); got != "" {
		t.Fatalf("hostExitIP failed probe = %q, want \"\"", got)
	}
	if got := hostExitIP(time.Second); got != "" {
		t.Fatalf("hostExitIP after failure = %q, want \"\"", got)
	}
	if calls != 1 {
		t.Fatalf("direct probe calls = %d after failure, want 1 (backoff)", calls)
	}
}

func TestDownloadMeasurerTTFB(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	const bodyDelay = 60 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush() // headers now, body after the delay
		}
		time.Sleep(bodyDelay)
		w.Write(make([]byte, 4096))
	}))
	defer srv.Close()

	speed, ttfb, err := DownloadMeasurerTTFB(socksAddr, srv.URL, 4096, 5*time.Second)
	if err != nil {
		t.Fatalf("DownloadMeasurerTTFB error: %v", err)
	}
	if speed <= 0 {
		t.Errorf("speed = %f, want > 0", speed)
	}
	if ttfb < bodyDelay/2 {
		t.Errorf("ttfb = %s, want >= %s (time to FIRST body byte)", ttfb, bodyDelay/2)
	}
}

func TestBestAdmissibleCandidate(t *testing.T) {
	hostIP := "203.0.113.5"
	activeExit := map[string]bool{"198.51.100.9": true}

	leaky := &TestResult{Config: cfgNamed("leaky"), Speed: 90, TTFB: 10 * time.Millisecond, ExitIP: hostIP}
	duplicate := &TestResult{Config: cfgNamed("duplicate"), Speed: 80, TTFB: 10 * time.Millisecond, ExitIP: "198.51.100.9"}
	laggy := &TestResult{Config: cfgNamed("laggy"), Speed: 70, TTFB: 4 * time.Second, ExitIP: "198.51.100.20"}
	good := &TestResult{Config: cfgNamed("good"), Speed: 10, TTFB: 60 * time.Millisecond, ExitIP: "198.51.100.21"}
	slower := &TestResult{Config: cfgNamed("slower"), Speed: 5, TTFB: 80 * time.Millisecond, ExitIP: "198.51.100.22"}
	results := []*TestResult{leaky, duplicate, laggy, good, slower}

	// The gates eliminate every faster-but-unusable candidate: the pick is
	// the 10 Mbps node, not the 90 Mbps leak.
	got := bestAdmissibleCandidate(results, 5, 2500*time.Millisecond, hostIP, activeExit, nil)
	if got == nil || got.Config.Name != "good" {
		t.Fatalf("bestAdmissibleCandidate = %v, want the \"good\" config", got)
	}

	// With the gates off, speed wins as before (backwards compatibility of
	// the speed-only path).
	if got := bestNewCandidate(results, 5, nil); got == nil || got.Config.Name != "leaky" {
		t.Fatalf("bestNewCandidate = %v, want the fastest config", got)
	}

	// blocked still excludes a config the caller rejects.
	blocked := func(c *proxycfg.ProxyConfig) bool { return c != nil && c.Name == "good" }
	if got := bestAdmissibleCandidate(results, 5, 2500*time.Millisecond, hostIP, activeExit, blocked); got == nil || got.Config.Name != "slower" {
		t.Fatalf("bestAdmissibleCandidate with blocked = %v, want \"slower\"", got)
	}
}

func TestSlotExitIPDiversity(t *testing.T) {
	pool := NewWANPool(2, 0)

	// An EMPTY slot never counts as serving its exit IP.
	pool.SetSlotExitIP(0, "198.51.100.1")
	pool.SetSlotExitIP(1, "198.51.100.2")
	if got := pool.ActiveExitIPs(); len(got) != 0 {
		t.Fatalf("active exit IPs = %v, want none (no active slots)", got)
	}

	// Promoting slot 0 publishes its exit IP to the gate.
	pool.Slots[0].State = StateActive
	if got := pool.ActiveExitIPs(); len(got) != 1 || !got["198.51.100.1"] {
		t.Fatalf("active exit IPs = %v, want {198.51.100.1}", got)
	}

	// Slot 1 is still empty, so its recorded IP stays out of the set.
	if got := pool.ActiveExitIPs(); len(got) != 1 {
		t.Fatalf("active exit IPs = %v, want 1 entry while slot 1 is empty", got)
	}

	// Activating it adds the second exit.
	pool.Slots[1].State = StateActive
	if got := pool.ActiveExitIPs(); len(got) != 2 {
		t.Fatalf("active exit IPs = %v, want 2 entries", got)
	}

	// An empty IP is a no-op write (unknown must never overwrite a known
	// value, or a single failed canary would disable the gate).
	pool.SetSlotExitIP(1, "")
	if got := pool.ActiveExitIPs(); len(got) != 2 {
		t.Fatalf("active exit IPs after empty set = %v, want 2 entries", got)
	}
}
