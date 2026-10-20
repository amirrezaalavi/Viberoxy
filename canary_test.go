package main

import (
	"net"
	"os/exec"
	"testing"
	"time"

	"viberoxy/internal/health"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

// TestCanaryBaseInterval_MapsKeepaliveKnob pins the env-knob mapping:
// KEEPALIVE_INTERVAL drives the CANARY interval, clamped into SPEC-H.5's
// 15–30s band (the configured default of 300s therefore runs canaries
// every 30s; the config-time minimum of 10s runs them every 15s).
func TestCanaryBaseInterval_MapsKeepaliveKnob(t *testing.T) {
	cases := []struct {
		keepaliveSec int
		want         time.Duration
	}{
		{0, 15 * time.Second},   // hand-built configs clamp up
		{5, 15 * time.Second},   // below the band
		{10, 15 * time.Second},  // config minimum
		{20, 20 * time.Second},  // in band, used as-is
		{30, 30 * time.Second},  // band ceiling
		{300, 30 * time.Second}, // configured default
		{3600, 30 * time.Second},
	}
	for _, c := range cases {
		if got := canaryBaseInterval(c.keepaliveSec); got != c.want {
			t.Errorf("canaryBaseInterval(%d) = %s, want %s", c.keepaliveSec, got, c.want)
		}
	}
}

// TestRunCanaries_EnvironmentalFailureDrainsNothing is the production
// wiring of T-HLT-03/T-CHAOS-05: when every path fails every canary
// endpoint in the same interval, the >=75% breaker declares an
// environmental failure and runCanaries must record NOTHING — no failure
// counters, no MarkDraining, no ejection — while the env_degraded gauge is
// set and the pool keeps serving from its (unchanged) selection state.
//
// The simulated failure is a SOCKS listener that ACCEPTS and then closes:
// the tunnel is up, the endpoint never answers. A listener that is simply
// GONE (connection refused) is a different case — a dead local process —
// and is handled by handleDeadTunnels, not by the environmental breaker
// (see TestRunCanariesDeadTunnelIsNotEnvironmental).
func TestRunCanaries_EnvironmentalFailureDrainsNothing(t *testing.T) {
	t.Setenv("HEALTH_ENDPOINTS", "") // built-in defaults; the SOCKS dials below never leave loopback

	pool := NewWANPool(2, 0)
	for i := range pool.Slots {
		if err := pool.StartTesting(i, &proxycfg.ProxyConfig{Raw: "x", Server: "1.2.3.4", Port: 1}); err != nil {
			t.Fatal(err)
		}
		// Non-nil handle so the slot is a real occupant. The service
		// port points at a listener that accepts and closes: the dial
		// succeeds, the tunnel never carries a request.
		pool.Slots[i].ServicePort = acceptingThenClosingPort(t)
		if err := pool.SetActive(i, xrayproc.Wrap(&exec.Cmd{}, ""), ""); err != nil {
			t.Fatal(err)
		}
	}

	cfg := &proxycfg.Config{TestTimeout: 1, WanFailThreshold: 2, KeepaliveInterval: 10}
	runCanaries(cfg, pool, health.EndpointsFromEnv())

	if !health.EnvDegraded() {
		t.Error("env_degraded gauge not set although 2/2 paths failed their canaries")
	}
	defer health.SetEnvDegraded(false)

	for i := range pool.Slots {
		if got := pool.GetState(i); got != StateActive {
			t.Errorf("slot %d state = %s during an environmental failure: nothing may be drained", i, got)
		}
		if fails := pool.SlotConsecutiveFails(i); fails != 0 {
			t.Errorf("slot %d consecutive fails = %d during an environmental failure: nothing may be recorded", i, fails)
		}
		if pool.Slots[i].Current.Load().Ejected() {
			t.Errorf("slot %d path ejected during an environmental failure", i)
		}
	}
}

// acceptingThenClosingPort starts a listener that accepts connections and
// closes them immediately: the SOCKS handshake never completes, but the
// connection is NOT refused — the local process is alive.
func acceptingThenClosingPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}
