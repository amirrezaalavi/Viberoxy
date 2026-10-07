package main

import (
	"os/exec"
	"testing"
	"viberoxy/internal/path"
	"viberoxy/internal/sched"
	"viberoxy/internal/xrayproc"
)

// selActivate marks a slot Active with a live (non-nil) process handle and
// mints a fresh Active path generation — the production shape of an
// occupied slot — and returns that path. The generation is matured past
// the scheduler's slow-start ramp so ramp(age) is exactly 1 for every
// path: selection under test is decided by eligibility and load, never by
// activation order.
func selActivate(p *WANPool, i int) *path.Path {
	slot := p.Slots[i]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.State = StateActive
	slot.Cmd = xrayproc.Wrap(&exec.Cmd{}, "")
	cur := path.New(slot.Config, slot.Cmd, i, slot.ServicePort)
	cur.CreatedAt = cur.CreatedAt.Add(-2 * sched.RampWindow)
	slot.Current.Store(cur)
	return cur
}

// T-SEL-01 (property, 1e5 seeded picks): while any healthy routable path
// exists, selection NEVER hands out a Probation, Draining, Dead or
// ejected (Suspect) generation — nor an over-threshold one — no matter
// what else the pool contains. With the healthy paths exhausted (tried),
// the documented last-resorts take over in order: degraded Active slots
// first, draining slots only when no active path is left. Nothing ever
// blackholes while any candidate tier is non-empty.
func TestTSEL01_NeverSelectsIneligible(t *testing.T) {
	sched.SeedRand(101)
	pool := NewWANPool(7, 30000)

	healthy0 := selActivate(pool, 0)
	healthy1 := selActivate(pool, 1)

	// Degraded-tier members under Active slots: over threshold...
	over := selActivate(pool, 2)
	pool.RecordFailure(2)
	pool.RecordFailure(2)
	// ...ejected by the health window...
	ejected := selActivate(pool, 3)
	ejected.SetState(path.Suspect)
	// ...a Probation generation (nothing assigns it in production; the
	// property must hold regardless)...
	probation := selActivate(pool, 4)
	probation.SetState(path.Probation)
	// ...and an Active slot still holding a retired/vacant (Dead)
	// generation — a fixture-only shape the degraded pass keeps
	// handing out exactly as before.
	pool.Slots[5].mu.Lock()
	pool.Slots[5].State = StateActive
	pool.Slots[5].Cmd = xrayproc.Wrap(&exec.Cmd{}, "")
	vacant := pool.Slots[5].Current.Load()
	vacant.CreatedAt = vacant.CreatedAt.Add(-2 * sched.RampWindow)
	pool.Slots[5].mu.Unlock()

	// Draining slot: the documented last resort only.
	selActivate(pool, 6)
	if err := pool.MarkDraining(6); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	draining := pool.Slots[6].Current.Load()

	healthy := map[*path.Path]bool{healthy0: true, healthy1: true}
	counts := map[*path.Path]int{}

	// Phase 1: 1e5 picks with a healthy tier available — only the two
	// healthy generations may ever come back.
	for i := 0; i < 100000; i++ {
		p, release := pool.Select(SelectOptions{Threshold: DefaultFailThreshold})
		if p == nil {
			t.Fatalf("pick %d: Select returned nil while healthy paths exist", i)
		}
		if !healthy[p] {
			t.Fatalf("pick %d: selected ineligible path (slot %d, state %s, fails %d, wants only healthy Active)",
				i, p.Slot, p.GetState(), p.ConsecutiveFails())
		}
		counts[p]++
		release()
	}
	if counts[healthy0] < 45000 || counts[healthy1] < 45000 {
		t.Errorf("healthy split = %d/%d, want both paths carrying traffic (≥45000 each)",
			counts[healthy0], counts[healthy1])
	}

	// Phase 2: the healthy paths are tried (a retry's view) — the
	// degraded fallback must answer from ACTIVE slots only: never the
	// draining slot, never nil.
	tried := map[*path.Path]bool{healthy0: true, healthy1: true}
	degradedOK := map[*path.Path]bool{over: true, ejected: true, probation: true, vacant: true}
	for i := 0; i < 1000; i++ {
		p, release := pool.Select(SelectOptions{Tried: tried, Threshold: DefaultFailThreshold})
		if p == nil {
			t.Fatalf("degraded pick %d: Select returned nil while active slots exist (blackhole)", i)
		}
		if !degradedOK[p] {
			t.Fatalf("degraded pick %d: selected slot %d (state %s); draining/healthy tiers are out of scope here",
				i, p.Slot, p.GetState())
		}
		if p == draining {
			t.Fatalf("degraded pick %d: draining slot selected while an active path exists (F-02)", i)
		}
		release()
	}

	// Phase 3: everything active is tried — the documented F-02 last
	// resort hands out the draining slot rather than blackholing.
	tried[over], tried[ejected], tried[probation], tried[vacant] = true, true, true, true
	p, release := pool.Select(SelectOptions{Tried: tried, Threshold: DefaultFailThreshold})
	if p != draining {
		t.Fatalf("last-resort Select = %v (slot -1), want the draining slot's path", p)
	}
	release()
}

// TestWANSelect_ReservesAndExcludes pins the pool-level contract:
// Select reserves atomically (inflight observable at return), the tried
// set excludes exactly the marked paths, and (nil, nil) comes back only
// when no tier has a candidate.
func TestWANSelect_ReservesAndExcludes(t *testing.T) {
	pool := NewWANPool(3, 30000)
	a := selActivate(pool, 0)
	b := selActivate(pool, 1)

	p, release := pool.Select(SelectOptions{Threshold: DefaultFailThreshold})
	if p == nil || release == nil {
		t.Fatalf("Select returned nil path or nil release (path=%v)", p)
	}
	if got := p.Inflight.Load(); got != 1 {
		t.Errorf("inflight right after Select = %d, want 1 (reservation is part of Select)", got)
	}
	if p != a && p != b {
		t.Errorf("Select returned slot %d, want one of {0,1}", p.Slot)
	}
	release()
	if got := p.Inflight.Load(); got != 0 {
		t.Errorf("inflight after release = %d, want 0", got)
	}

	// Tried: the selected generation is excluded on the next round.
	other := map[*path.Path]bool{p: true}
	q, release2 := pool.Select(SelectOptions{Tried: other, Threshold: DefaultFailThreshold})
	if q == nil {
		t.Fatal("Select with one tried path returned nil although a candidate remains")
	}
	if q == p {
		t.Errorf("tried path %d was handed out again", q.Slot)
	}
	release2()

	// Both tried: no routable candidate (slot 2 is empty), and with
	// every tier empty Select reports (nil, nil).
	other[p], other[q] = true, true
	r, release3 := pool.Select(SelectOptions{Tried: other, Threshold: DefaultFailThreshold})
	if r != nil {
		t.Errorf("Select with every path tried = slot %d, want nil (degraded tier holds the same tried paths)", r.Slot)
	}
	if release3 != nil {
		t.Error("nil selection must not come with a release function")
	}
}
