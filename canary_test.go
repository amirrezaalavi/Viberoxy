package main

import (
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
// endpoint in the same interval (here: every path's SOCKS listener is
// dead), the >=75% breaker declares an environmental failure and
// runCanaries must record NOTHING — no failure counters, no MarkDraining,
// no ejection — while the env_degraded gauge is set and the pool keeps
// serving from its (unchanged) selection state.
func TestRunCanaries_EnvironmentalFailureDrainsNothing(t *testing.T) {
	t.Setenv("HEALTH_ENDPOINTS", "") // built-in defaults; the SOCKS dials below never leave loopback

	pool := NewWANPool(2, 0)
	for i := range pool.Slots {
		if err := pool.StartTesting(i, &proxycfg.ProxyConfig{Raw: "x", Server: "1.2.3.4", Port: 1}); err != nil {
			t.Fatal(err)
		}
		// Non-nil handle so the slot is a real occupant; the service
		// port is 0/1, so every canary dial fails instantly on loopback
		// without touching the network.
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
