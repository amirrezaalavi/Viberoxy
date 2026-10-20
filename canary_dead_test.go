package main

import (
	"strconv"
	"syscall"
	"testing"
	"time"

	"viberoxy/internal/health"
	"viberoxy/internal/path"
	"viberoxy/internal/proxycfg"
)

// TestCanaryProbeDetailedDetectsDeadTunnel: a refused local SOCKS port is a
// DEAD tunnel, not an endpoint failure. The distinction is what keeps the
// environmental breaker from swallowing a local outage.
func TestCanaryProbeDetailedDetectsDeadTunnel(t *testing.T) {
	port := freePort(t)
	addr := "127.0.0.1:" + strconv.Itoa(port)

	ok, rtt, dead := canaryProbeDetailed(addr, []string{"https://www.gstatic.com/generate_204"}, 2*time.Second)
	if ok {
		t.Error("probe of a dead tunnel reported success")
	}
	if !dead {
		t.Error("probe of a refused local port was not classified as a dead tunnel")
	}
	if rtt != 0 {
		t.Errorf("rtt = %v, want 0 for a dead tunnel", rtt)
	}
}

// TestIsTunnelGone covers the error classification: a refused local dial is
// "gone", a remote error is not.
func TestIsTunnelGone(t *testing.T) {
	if isTunnelGone(nil) {
		t.Error("nil error classified as a dead tunnel")
	}
	if !isTunnelGone(syscall.ECONNREFUSED) {
		t.Error("ECONNREFUSED not classified as a dead tunnel")
	}
	if isTunnelGone(syscall.ECONNRESET) {
		t.Error("ECONNRESET classified as a dead tunnel")
	}
}

// TestHandleDeadTunnelsQueuesARefill is the regression guard for the outage:
// a slot whose tunnel is gone must be drained AND queue a cycle, so the pool
// refills immediately instead of waiting for the (12h) fetch interval.
func TestHandleDeadTunnelsQueuesARefill(t *testing.T) {
	pool := NewWANPool(1, 10700)
	// Drain the trigger channel: a previous test may have queued one.
	select {
	case <-triggerCycle:
	default:
	}

	handleDeadTunnels(pool, &proxycfg.Config{WanFailThreshold: 2}, map[int]bool{0: true})

	select {
	case <-triggerCycle:
	default:
		t.Error("a dead tunnel did not queue a cycle")
	}
}

// TestRunCanariesDeadTunnelIsNotEnvironmental: with every path dead
// LOCALLY, the canary must not blame the environment (env_degraded stays
// false) and must queue a refill.
func TestRunCanariesDeadTunnelIsNotEnvironmental(t *testing.T) {
	pool := NewWANPool(1, 10700)
	// A slot whose service port has no listener: the tunnel is gone.
	port := freePort(t)
	pool.Slots[0].ServicePort = port
	pool.Slots[0].State = StateActive
	pool.Slots[0].Current.Store(path.New(&proxycfg.ProxyConfig{Server: "dead.example", Port: 443}, nil, 0, port))

	select {
	case <-triggerCycle:
	default:
	}

	cfg := &proxycfg.Config{TestTimeout: 3, WanFailThreshold: 2, KeepaliveInterval: 30}
	runCanaries(cfg, pool, []string{"https://www.gstatic.com/generate_204"})

	if health.EnvDegraded() {
		t.Error("a dead local tunnel was classified as an environmental failure")
	}
	select {
	case <-triggerCycle:
	default:
		t.Error("runCanaries did not queue a refill for a dead tunnel")
	}
	if state := pool.GetState(0); state == StateActive {
		t.Error("the dead slot is still active; it should be draining")
	}
}
