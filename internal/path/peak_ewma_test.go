package path

import (
	"math"
	"testing"
	"time"
)

// TestRecordHealth_PeakEWMA pins the peak-EWMA semantics the scheduler
// cost relies on (SPEC-H.5/S): adverse samples land immediately, favorable
// samples decay in with the documented taus (10s TTFB / 30s goodput).
func TestRecordHealth_PeakEWMA(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)

	t.Run("first sample seeds", func(t *testing.T) {
		p := New(nil, nil, 0, 0)
		p.recordHealthAt(base, 200*time.Millisecond, 5_000_000)
		if got := p.TTFB(); got != 200*time.Millisecond {
			t.Errorf("TTFB after first sample = %v, want 200ms", got)
		}
		if got := p.GoodputBps(); got != 5_000_000 {
			t.Errorf("goodput after first sample = %v, want 5e6", got)
		}
	})

	t.Run("worse samples jump immediately", func(t *testing.T) {
		p := New(nil, nil, 0, 0)
		p.recordHealthAt(base, 100*time.Millisecond, 8_000_000)
		p.recordHealthAt(base.Add(time.Second), 750*time.Millisecond, 1_000_000)
		if got := p.TTFB(); got != 750*time.Millisecond {
			t.Errorf("TTFB after worse sample = %v, want 750ms (no smoothing on regression)", got)
		}
		if got := p.GoodputBps(); got != 1_000_000 {
			t.Errorf("goodput after worse sample = %v, want 1e6 (no smoothing on regression)", got)
		}
	})

	t.Run("better samples decay with tau", func(t *testing.T) {
		// TTFB: after exactly one tau (10s) the remaining gap must be
		// exp(-1) ≈ 0.368 of the original — i.e. the estimate moved
		// 63.2% of the way to the better sample, not all of it.
		pt := New(nil, nil, 0, 0)
		pt.recordHealthAt(base, 1*time.Second, 0)
		pt.recordHealthAt(base.Add(ttfbTau), 10*time.Millisecond, 0)
		got := float64(pt.TTFB())
		expected := float64(10*time.Millisecond) + (float64(1*time.Second)-float64(10*time.Millisecond))*math.Exp(-1)
		if diff := got - expected; diff > float64(time.Millisecond) || diff < -float64(time.Millisecond) {
			t.Errorf("TTFB after 1 tau of improvement = %v, want ≈%v (exp(-1) of the gap remaining)",
				time.Duration(got), time.Duration(expected))
		}
		if pt.TTFB() >= 1*time.Second {
			t.Errorf("TTFB did not decay toward the better sample: %v", pt.TTFB())
		}

		// Goodput decay uses its own tau (30s) and the same exp rule.
		pg := New(nil, nil, 0, 0)
		pg.recordHealthAt(base, 0, 1_000_000)
		pg.recordHealthAt(base.Add(goodputTau), 0, 9_000_000)
		expectedGP := 9_000_000.0 + (1_000_000.0-9_000_000.0)*math.Exp(-1)
		if diff := pg.GoodputBps() - expectedGP; diff > 10_000 || diff < -10_000 {
			t.Errorf("goodput after 1 tau of improvement = %v, want ≈%v", pg.GoodputBps(), expectedGP)
		}
	})

	t.Run("zero samples are skipped and nil is safe", func(t *testing.T) {
		var nilPath *Path
		nilPath.recordHealthAt(base, 100*time.Millisecond, 1_000_000) // no panic
		p := New(nil, nil, 0, 0)
		p.recordHealthAt(base, 0, 0)
		if p.TTFB() != 0 || p.GoodputBps() != 0 {
			t.Errorf("zero samples must leave the EWMAs unset, got TTFB=%v goodput=%v", p.TTFB(), p.GoodputBps())
		}
	})
}
