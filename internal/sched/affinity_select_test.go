package sched

import (
	"testing"
	"viberoxy/internal/affinity"
	"viberoxy/internal/path"
)

// without returns cands minus the given path (the eligibility filter the
// WAN pool applies when a path goes Suspect: it leaves the routable tier).
func without(cands []*path.Path, drop *path.Path) []*path.Path {
	out := make([]*path.Path, 0, len(cands))
	for _, p := range cands {
		if p != drop {
			out = append(out, p)
		}
	}
	return out
}

// T-AFF-01 (SLO): one simulated 10-minute run of sequential requests with
// one (client, site) key — every request while the sticky path is healthy
// must land on it (SLO >= 99.5%; the implementation is deterministic and
// scores 100%). Ten ticks of simulated ejection in the middle verify the
// fallback: the ejected path must NOT be selected while it is out of the
// routable tier, and stickiness resumes when it returns.
func TestTAFF01_SameClientSiteSticksWhileHealthy(t *testing.T) {
	SeedRand(11)
	paths := mkPaths(4)
	req := Request{Routable: paths, ClientID: "client-a", SiteKey: "example.com"}

	sticky, release := Select(req)
	if sticky == nil {
		t.Fatal("initial selection returned nil")
	}
	release()

	const ticks = 600 // 10 simulated minutes, one request per second
	healthyTicks, stickyPicks := 0, 0
	for tick := 0; tick < ticks; tick++ {
		r := req
		healthy := true
		if tick >= 300 && tick < 310 {
			// The sticky path is ejected for 10 simulated seconds:
			// it leaves the routable tier (wan.go drops Suspect), so
			// selection must fall back among the remaining paths.
			healthy = false
			r.Routable = without(paths, sticky)
		}
		p, rel := Select(r)
		if p == nil {
			t.Fatalf("tick %d: Select returned nil with %d candidates", tick, len(r.Routable))
		}
		if healthy {
			healthyTicks++
			if p == sticky {
				stickyPicks++
			}
		} else if p == sticky {
			t.Fatalf("tick %d: ejected sticky path selected from the fallback tier", tick)
		}
		rel()
	}
	if healthyTicks == 0 {
		t.Fatal("no healthy ticks recorded")
	}
	slo := float64(stickyPicks) / float64(healthyTicks)
	if slo < 0.995 {
		t.Errorf("stickiness over %d healthy ticks = %.4f (>= 99.5%% required); sticky slot %d got %d",
			healthyTicks, slo, sticky.Slot, stickyPicks)
	}
}

// T-AFF-03: when the HRW pick is unhealthy it leaves the eligible tier —
// the fallback must be the deterministic next-highest rendezvous winner
// over the remaining paths, not a fresh coin flip.
func TestTAFF03_UnhealthyStickyPickFallsBackDeterministically(t *testing.T) {
	SeedRand(13)
	paths := mkPaths(4)
	req := Request{Routable: paths, ClientID: "client-b", SiteKey: "site.test"}

	sticky, release := Select(req)
	if sticky == nil {
		t.Fatal("initial selection returned nil")
	}
	release()

	// The sticky path is ejected: the pool drops it from the routable
	// tier before calling Select.
	tier := without(paths, sticky)
	r := Request{Routable: tier, ClientID: "client-b", SiteKey: "site.test"}
	want := affinity.Pick("client-b", "site.test", tier)
	if want == nil || want == sticky {
		t.Fatalf("fallback expectation invalid: want a non-sticky candidate, got %v", want)
	}
	for i := 0; i < 3; i++ {
		got, rel := Select(r)
		if got != want {
			t.Fatalf("fallback pick %d = slot %d, want the deterministic next HRW winner (slot %d)", i, got.Slot, want.Slot)
		}
		rel()
	}
}

// T-AFF-04 (AFFINITY_BULK_SPILL): the sticky path at exactly
// AffinitySpillFactor x the tier median keeps stickiness; one reservation
// above that (bulk load concentrated on the sticky path) spills every
// selection to P2C — availability over stickiness.
func TestTAFF04_BulkSpillToP2C(t *testing.T) {
	SeedRand(14)
	paths := mkPaths(4)
	req := Request{Routable: paths, ClientID: "client-c", SiteKey: "bulk.test"}

	sticky, release := Select(req)
	if sticky == nil {
		t.Fatal("initial selection returned nil")
	}
	release()

	// Every path at 1, sticky at 3: median 1, threshold 3x1 = 3 —
	// exactly at the boundary, stickiness must hold.
	for _, p := range paths {
		p.Reserve()
	}
	sticky.Reserve()
	sticky.Reserve()
	got, rel := Select(req)
	if got != sticky {
		t.Errorf("sticky at exactly 3x median (3 vs 1) spilled to slot %d: boundary must not spill", got.Slot)
	}
	rel()

	// Bulk transfer on the sticky path: 10 > 3x1 — spill to P2C. The
	// P2C cost comparison cannot return the 10-inflight path against
	// 1-inflight peers either, so this is deterministic.
	for i := 0; i < 7; i++ {
		sticky.Reserve()
	}
	for i := 0; i < 8; i++ {
		got, rel := Select(req)
		if got == nil {
			t.Fatalf("spill pick %d returned nil", i)
		}
		if got == sticky {
			t.Fatalf("spill pick %d returned the bulk-loaded sticky path (inflight 10, median 1): spill did not trigger", i)
		}
		rel()
	}

	for _, p := range paths {
		for p.Inflight.Load() > 0 {
			p.Release()
		}
	}
}
