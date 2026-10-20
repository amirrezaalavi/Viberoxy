package main

import (
	"testing"

	"viberoxy/internal/proxycfg"
)

// TestDegradedFallbackDefaultIsEnabled pins the default: a pool that has no
// routable WAN still hands out a degraded path (availability first), which
// is the historical behavior. ALLOW_DEGRADED_FALLBACK=false turns it off.
func TestDegradedFallbackDefaultIsEnabled(t *testing.T) {
	pool := NewWANPool(1, 10700)
	if !pool.AllowDegradedFallback {
		t.Fatal("new pool has AllowDegradedFallback=false, want true (historical behavior)")
	}
}

// TestDegradedFallbackDisabledFailsFast is the fail-closed mode: with the
// knob off, a connection that has no routable WAN gets NO candidate at all
// (the caller answers 502/503) instead of being routed through a path that
// is already known to be failing.
func TestDegradedFallbackDisabledFailsFast(t *testing.T) {
	newPool := func(t *testing.T) *WANPool {
		t.Helper()
		pool := NewWANPool(2, 10700)
		drainMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Server: "degraded.example", Port: 443}, 20000)
		// Over the fail threshold: the slot is Active but no longer
		// routable, so it only lives in the degraded tier.
		setFails(pool, 0, 2)
		return pool
	}

	// Default (knob on): the degraded path is still handed out.
	pool := newPool(t)
	picked, release := pool.Select(SelectOptions{Threshold: 2})
	if picked == nil {
		t.Fatal("Select returned no candidate with the degraded fallback enabled")
	}
	if release != nil {
		release()
	}

	// Knob off: nothing is handed out.
	pool = newPool(t)
	pool.AllowDegradedFallback = false
	picked, release = pool.Select(SelectOptions{Threshold: 2})
	if picked != nil {
		t.Errorf("Select returned slot %d with the degraded fallback disabled, want none", picked.Slot)
	}
	if release != nil {
		release()
	}
}

// TestDegradedFallbackDisabledDropsDrainingLastResort: the draining
// last-resort tier is dropped as well — a draining slot is not a better
// answer than a fast failure.
func TestDegradedFallbackDisabledDropsDrainingLastResort(t *testing.T) {
	pool := NewWANPool(1, 10700)
	drainMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Server: "drain.example", Port: 443}, 20000)
	if err := pool.MarkDraining(0); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}

	picked, release := pool.Select(SelectOptions{Threshold: 2})
	if picked == nil {
		t.Fatal("Select returned no candidate with the degraded fallback enabled")
	}
	if release != nil {
		release()
	}

	pool.AllowDegradedFallback = false
	picked, release = pool.Select(SelectOptions{Threshold: 2})
	if picked != nil {
		t.Errorf("Select returned slot %d with the degraded fallback disabled, want none", picked.Slot)
	}
	if release != nil {
		release()
	}
}
