package sched

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"viberoxy/internal/path"
)

// mkPaths builds n fresh Active paths for one test and matures them past
// the slow-start ramp window, so ramp(age) is exactly 1 for every path and
// the ranking under test is decided by load/latency/weight only — never by
// which path happened to be created first.
func mkPaths(n int) []*path.Path {
	paths := make([]*path.Path, n)
	for i := range paths {
		paths[i] = path.New(nil, nil, i, 0)
		paths[i].CreatedAt = time.Now().Add(-2 * RampWindow)
	}
	return paths
}

// selectHold picks without releasing (a held connection), failing the test
// when Select yields nothing where a candidate exists.
func selectHold(t *testing.T, req Request) *path.Path {
	t.Helper()
	p, release := Select(req)
	if p == nil {
		t.Fatal("Select returned nil although candidates were provided")
	}
	if release == nil {
		t.Fatal("Select returned a path but no release function")
	}
	return p
}

// T-SEL-02: four equal paths must split 1e5 picks within +-15% each.
// Picks are HELD (no release), so the counts are the raw inflight
// distribution the cost function converges to.
func TestTSEL02_EqualPathsFairSplit(t *testing.T) {
	SeedRand(2)
	paths := mkPaths(4)
	req := Request{Routable: paths}

	const picks = 100000
	for i := 0; i < picks; i++ {
		selectHold(t, req)
	}

	want := picks / len(paths)
	tol := want * 15 / 100
	for i, p := range paths {
		got := p.Inflight.Load()
		if got < int64(want-tol) || got > int64(want+tol) {
			t.Errorf("path %d received %d of %d picks, want %d +-15%% (%d..%d)",
				i, got, picks, want, want-tol, want+tol)
		}
	}
	var sum int64
	for _, p := range paths {
		sum += p.Inflight.Load()
		p.Release()
	}
	if sum != picks {
		t.Errorf("sum of picks = %d, want %d (every Select must reserve exactly once)", sum, picks)
	}
}

// T-SEL-03: 2x goodput EWMA => approximately 2x the load at equilibrium.
func TestTSEL03_GoodputScalesLoad(t *testing.T) {
	SeedRand(3)
	paths := mkPaths(2)
	paths[0].RecordHealth(0, 2_000_000) // 2 Mbps
	paths[1].RecordHealth(0, 1_000_000) // 1 Mbps
	req := Request{Routable: paths}

	const picks = 100000
	for i := 0; i < picks; i++ {
		selectHold(t, req)
	}
	fast := paths[0].Inflight.Load()
	slow := paths[1].Inflight.Load()
	if slow == 0 {
		t.Fatalf("2x-goodput path took ALL %d picks (fast=%d slow=0); want ≈2x split", fast, fast)
	}
	ratio := float64(fast) / float64(slow)
	if ratio < 1.6 || ratio > 2.5 {
		t.Errorf("load ratio fast/slow = %.3f, want ≈2.0 (bounds 1.6..2.5; fast=%d slow=%d)",
			ratio, fast, slow)
	}
	for _, p := range paths {
		p.Release()
	}
}

// T-SEL-04: 256 goroutines x 1000 selects each. Every Select must have
// reserved before it returns (atomic select+reserve): the observed inflight
// is >= 1 the instant Select hands the path back, and the final sum equals
// the number of Selects exactly — no lost, duplicated or deferred
// reservation. A Select that merely ranks (reservation split out to the
// caller, the pre-fix TOCTOU) fails both assertions.
func TestTSEL04_AtomicReservation(t *testing.T) {
	SeedRand(4)
	paths := mkPaths(4)
	req := Request{Routable: paths}

	const goroutines = 256
	const perGoroutine = 1000

	var (
		wg          sync.WaitGroup
		nilPicks    atomic.Int64
		unreserved  atomic.Int64
		noReleaseFn atomic.Int64
	)
	// Each goroutine writes only its own slot; main reads after wg.Wait().
	held := make([][]func(), goroutines)
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			releases := make([]func(), 0, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				p, release := Select(req)
				if p == nil {
					nilPicks.Add(1)
					continue
				}
				if release == nil {
					noReleaseFn.Add(1)
					continue
				}
				// The reservation must be observable NOW, before
				// Select returns — nothing releases during this phase.
				if p.Inflight.Load() < 1 {
					unreserved.Add(1)
				}
				releases = append(releases, release)
			}
			held[g] = releases
		}()
	}
	wg.Wait()

	if n := nilPicks.Load(); n != 0 {
		t.Errorf("%d of %d selects returned nil with candidates present", n, goroutines*perGoroutine)
	}
	if n := noReleaseFn.Load(); n != 0 {
		t.Errorf("%d selects returned a path without a release function", n)
	}
	if n := unreserved.Load(); n != 0 {
		t.Errorf("%d selects returned a path whose inflight was still 0: reservation is not part of Select", n)
	}

	var sum int64
	for _, p := range paths {
		sum += p.Inflight.Load()
	}
	want := int64(goroutines * perGoroutine)
	if sum != want {
		t.Errorf("total inflight after %d selects = %d, want exactly %d (over/under reservation)", want, sum, want)
	}

	// Exercise every returned release: the pool must drain to zero.
	for _, releases := range held {
		for _, release := range releases {
			release()
		}
	}
	for _, p := range paths {
		if n := p.Inflight.Load(); n != 0 {
			t.Errorf("path %d inflight = %d after all releases, want 0", p.Slot, n)
		}
	}
}

// T-SEL-05: the slow-start ramp. A fresh path (age ~0) starts at 10% of
// its weight and reaches 1.0 after RampWindow; while one path is young and
// one mature, the young one must be strongly avoided.
func TestTSEL05_SlowStartRamp(t *testing.T) {
	if got := ramp(0); got != RampStart {
		t.Errorf("ramp(0) = %v, want %v", got, RampStart)
	}
	if got := ramp(RampWindow / 2); got < 0.549 || got > 0.551 {
		t.Errorf("ramp(30s) = %v, want ≈0.55", got)
	}
	if got := ramp(RampWindow); got != 1 {
		t.Errorf("ramp(60s) = %v, want 1", got)
	}
	if got := ramp(10 * RampWindow); got != 1 {
		t.Errorf("ramp(600s) = %v, want 1 (clamped)", got)
	}
	if !(ramp(10*time.Second) < ramp(20*time.Second) && ramp(20*time.Second) < ramp(50*time.Second)) {
		t.Error("ramp is not strictly increasing over its window")
	}

	SeedRand(5)
	paths := mkPaths(2)
	paths[0].CreatedAt = time.Now().Add(-2 * RampWindow) // mature: weight 1.0
	paths[1].CreatedAt = time.Now()                      // fresh: weight ≈0.1 => 10x cost
	req := Request{Routable: paths}

	const picks = 1000
	freshPicks := 0
	for i := 0; i < picks; i++ {
		p, release := Select(req)
		if p == nil {
			t.Fatal("Select returned nil with two candidates")
		}
		if p == paths[1] {
			freshPicks++
		}
		release()
	}
	// At 10x cost the fresh path must lose every fair comparison; the
	// ramp is broken if it picks up meaningful load.
	if freshPicks > picks/20 {
		t.Errorf("fresh path received %d of %d picks, want <= %d (slow-start ramp not applied)",
			freshPicks, picks, picks/20)
	}
}

// T-SEL-06: equal costs must resolve RANDOMLY — over 1000 tied selects both
// paths take a substantial share, and neither is locked out (in particular
// the lowest index must not win everything).
func TestTSEL06_TiesBreakRandom(t *testing.T) {
	SeedRand(6)
	paths := mkPaths(2) // identical: same age, no latency, no goodput, no load
	req := Request{Routable: paths}

	const picks = 1000
	counts := map[*path.Path]int{}
	for i := 0; i < picks; i++ {
		p, release := Select(req)
		if p == nil {
			t.Fatal("Select returned nil with two tied candidates")
		}
		counts[p]++
		release()
	}
	for i, p := range paths {
		got := counts[p]
		if got < picks*2/5 || got > picks*3/5 {
			t.Errorf("path %d won %d of %d tied picks, want a ~50/50 coin flip (bounds 400..600)", i, got, picks)
		}
	}
}
