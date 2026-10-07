package health

import (
	"fmt"
	"testing"
	"time"
)

// fakeClock is the injected clock for the health tests: every State method
// takes its `now` from here, so window pruning, backoff and decay are
// deterministic (no sleeps, no time.Now).
type fakeClock struct{ t time.Time }

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// ejectViaStreak drives a State to an ejection through SPEC-H.3 rule A:
// EjectConsecutive consecutive HARD_FAILs across distinct hosts.
func ejectViaStreak(s *State, now time.Time) {
	for i := 0; i < EjectConsecutive; i++ {
		s.Record(now, fmt.Sprintf("host%d.example:443", i), HardFail)
	}
}

// admitViaCanaries walks a State through half-open re-admission: skip the
// backoff, then HalfOpenSuccesses consecutive canary successes.
func admitViaCanaries(s *State, now time.Time) bool {
	recovered := false
	for i := 0; i < HalfOpenSuccesses; i++ {
		if s.NoteCanary(now, true) {
			recovered = true
		}
	}
	return recovered
}

// ---------- Classify (SPEC-H.1) ----------

func TestHealth_ClassifyOutcomeSPECH1(t *testing.T) {
	cases := []struct {
		name string
		res  Result
		want Outcome
	}{
		{"down>0 is OK", Result{Up: 10, Down: 5}, OK},
		{"down>0 beats a splice error", Result{Up: 10, Down: 5, Err: fmt.Errorf("reset")}, OK},
		{"upstream-first still OK", Result{Up: 0, Down: 5}, OK},
		{"dial error is HARD_FAIL", Result{DialErr: fmt.Errorf("refused")}, HardFail},
		{"dial error beats bytes", Result{Up: 1, Down: 1, DialErr: fmt.Errorf("refused")}, HardFail},
		{"up>0 down==0 before T_FIRST", Result{Up: 1, Down: 0, Duration: 100 * time.Millisecond}, HardFail},
		{"up>0 down==0 at/after T_FIRST", Result{Up: 1, Down: 0, Duration: 9 * time.Second}, HardFail},
		{"T_FIRST override is honoured", Result{Up: 1, Down: 0, Duration: time.Second, TFirst: 500 * time.Millisecond}, HardFail},
		{"up==0 down==0 is NEUTRAL", Result{Up: 0, Down: 0}, Neutral},
		{"up==0 down==0 with splice error is NEUTRAL", Result{Up: 0, Down: 0, Err: fmt.Errorf("eof")}, Neutral},
	}
	for _, c := range cases {
		if got := Classify(c.res); got != c.want {
			t.Errorf("%s: Classify(%+v) = %v, want %v", c.name, c.res, got, c.want)
		}
	}
}

// ---------- T-HLT-01: window math + ejection thresholds ----------

func TestHealth_THLT01_WindowMathAndEjectionThresholds(t *testing.T) {
	clk := newClock()

	// (a) Window AGE: outcomes older than WindowAge are pruned. Seven
	// hard failures (below both eject rules) vanish once they age out.
	var aged State
	now := clk.Now()
	for i := 0; i < EjectMinOutcomes-1; i++ {
		if aged.Record(now, "a.example:443", HardFail) {
			t.Fatalf("ejected at outcome %d: rule A needs 3 distinct hosts, rule B needs %d outcomes",
				i+1, EjectMinOutcomes)
		}
	}
	clk.Advance(WindowAge + time.Second)
	if snap := aged.Snapshot(clk.Now()); snap.Total != 0 {
		t.Errorf("window holds %d outcomes after %s: want 0 (age prune)", snap.Total, WindowAge)
	}

	// (b) Window SIZE: at most WindowSize outcomes survive, whichever
	// bound is smaller — 40 fresh OKs keep the newest WindowSize.
	var sized State
	for i := 0; i < WindowSize+10; i++ {
		sized.Record(clk.Now(), fmt.Sprintf("h%d.example:80", i%3), OK)
		clk.Advance(time.Millisecond)
	}
	if snap := sized.Snapshot(clk.Now()); snap.Total != WindowSize {
		t.Errorf("window holds %d outcomes: want %d (size cap)", snap.Total, WindowSize)
	}

	// (c) Rule B (ratio): >= EjectMinOutcomes outcomes with failRatio >=
	// 50% ejects — exactly at the 8th outcome (4 fails / 4 OKs), not
	// before.
	var ratio State
	ejectedAt := -1
	for i := 0; i < EjectMinOutcomes; i++ {
		o := HardFail
		if i%2 == 1 {
			o = OK
		}
		if ratio.Record(clk.Now(), fmt.Sprintf("r%d.example:80", i%4), o) && ejectedAt < 0 {
			ejectedAt = i + 1
		}
		clk.Advance(time.Millisecond)
	}
	if ejectedAt != EjectMinOutcomes {
		t.Errorf("ratio rule ejected at outcome %d, want exactly %d (4 fails/4 OKs = 50%%)", ejectedAt, EjectMinOutcomes)
	}

	// (d) Below the thresholds: 8 outcomes, 3 fails (37.5%) — no ejection.
	var below State
	pattern := []Outcome{HardFail, OK, HardFail, OK, HardFail, OK, OK, OK}
	for i, o := range pattern {
		if below.Record(clk.Now(), fmt.Sprintf("b%d.example:80", i), o) {
			t.Fatalf("ejected at outcome %d of a 37.5%%-fail window: rule B needs >= %v", i+1, EjectFailRatio)
		}
		clk.Advance(time.Millisecond)
	}
	if snap := below.Snapshot(clk.Now()); snap.FailRatio >= EjectFailRatio {
		t.Errorf("failRatio = %v, want < %v", snap.FailRatio, EjectFailRatio)
	}

	// (e) Rule A (consecutive + distinct): 3 consecutive HARD_FAILs across
	// 3 distinct destination hosts ejects even with < 8 outcomes.
	var cons State
	for i := 0; i < EjectDistinctDest-1; i++ {
		if cons.Record(clk.Now(), fmt.Sprintf("c%d.example:443", i), HardFail) {
			t.Fatalf("ejected after only %d consecutive failures", i+1)
		}
	}
	if !cons.Record(clk.Now(), "c2.example:443", HardFail) {
		t.Fatal("3 consecutive HARD_FAILs to 3 distinct hosts did not eject")
	}
	if snap := cons.Snapshot(clk.Now()); !snap.Ejected || snap.Consecutive != EjectConsecutive || snap.DistinctRun != EjectDistinctDest {
		t.Errorf("snapshot = %+v, want ejected with consecutive=%d distinct=%d",
			snap, EjectConsecutive, EjectDistinctDest)
	}

	// Two consecutive failures never eject (rule A needs >= 3).
	var two State
	two.Record(clk.Now(), "x.example:443", HardFail)
	if two.Record(clk.Now(), "y.example:443", HardFail) {
		t.Fatal("2 consecutive HARD_FAILs ejected; rule A requires 3")
	}
}

// ---------- T-HLT-02: backoff doubling / decay ----------

func TestHealth_THLT02_BackoffDoublesAndDecays(t *testing.T) {
	clk := newClock()
	var s State

	wantBackoff := []time.Duration{
		30 * time.Second,  // 30s * 2^0
		60 * time.Second,  // 30s * 2^1
		120 * time.Second, // 30s * 2^2
		240 * time.Second,
		480 * time.Second,
		600 * time.Second, // 960s capped at 10min
	}

	for round, want := range wantBackoff {
		ejectViaStreak(&s, clk.Now())
		if !s.Ejected() {
			t.Fatalf("round %d: state not ejected", round)
		}
		if got := s.BackoffRemaining(clk.Now()); got != want {
			t.Errorf("round %d: backoff = %s, want %s (30s*2^%d, cap %s)",
				round, got, want, round, MaxBackoff)
		}
		if s.EjectCount() != round+1 {
			t.Errorf("round %d: ejectCount = %d, want %d", round, s.EjectCount(), round+1)
		}

		// Walk back to healthy: skip the backoff, re-admit via 2 canary
		// successes. Every later round needs the full sequence again.
		clk.Advance(want + time.Second)
		if !admitViaCanaries(&s, clk.Now()) {
			t.Fatalf("round %d: not re-admitted after backoff + %d canary successes", round, HalfOpenSuccesses)
		}
		if s.Ejected() {
			t.Fatalf("round %d: still ejected after re-admission", round)
		}
	}
	if s.EjectCount() != len(wantBackoff) {
		t.Fatalf("ejectCount = %d, want %d", s.EjectCount(), len(wantBackoff))
	}

	// Decay: HealthDecayAfter of continuous health drops the count one
	// step per window — two steps take the exponent from 6 back to 4, so
	// the next ejection backs off 30s*2^4 = 480s instead of 600s.
	for wantCount := s.EjectCount() - 1; s.EjectCount() > 4; wantCount-- {
		clk.Advance(HealthDecayAfter + time.Second)
		s.Record(clk.Now(), "fresh.example:443", OK)
		if s.EjectCount() != wantCount {
			t.Fatalf("ejectCount = %d after %s of health, want %d", s.EjectCount(), HealthDecayAfter, wantCount)
		}
	}
	ejectViaStreak(&s, clk.Now())
	if got := s.BackoffRemaining(clk.Now()); got != 480*time.Second {
		t.Errorf("post-decay backoff = %s, want 480s (ejectCount decayed to 4)", got)
	}

	// Backoff() itself: doubling + cap.
	if Backoff(0) != 30*time.Second || Backoff(1) != 60*time.Second || Backoff(4) != 480*time.Second {
		t.Errorf("Backoff doubling wrong: %s %s %s", Backoff(0), Backoff(1), Backoff(4))
	}
	if Backoff(100) != MaxBackoff {
		t.Errorf("Backoff(100) = %s, want cap %s", Backoff(100), MaxBackoff)
	}
}

// ---------- T-HLT-05: Suspect -> healthy recovery ----------

func TestHealth_THLT05_SuspectToHealthyRecovery(t *testing.T) {
	clk := newClock()
	var s State

	ejectViaStreak(&s, clk.Now())
	if !s.Ejected() {
		t.Fatal("state not ejected")
	}

	// Successful relays alone must NOT re-admit an ejected path: only the
	// canary half-open does (SPEC-H.5). While suspect, the window keeps
	// recording and prunes, so the failure evidence ages out cleanly.
	for i := 0; i < 10; i++ {
		clk.Advance(7 * time.Second)
		if s.Record(clk.Now(), fmt.Sprintf("live%d.example:80", i), OK) {
			t.Fatal("OK outcome re-admitted an ejected path (only canaries may)")
		}
		if !s.Ejected() {
			t.Fatalf("ejected state recovered without a canary at iteration %d", i)
		}
	}
	snap := s.Snapshot(clk.Now())
	if snap.FailRatio >= EjectFailRatio || snap.Consecutive != 0 {
		t.Errorf("window after recovery traffic = %+v: old failures must age out of the 60s window", snap)
	}

	// Recovery: backoff elapses, then HalfOpenSuccesses consecutive canary
	// successes flip the path back to healthy.
	clk.Advance(s.BackoffRemaining(clk.Now()) + time.Second)
	if s.NoteCanary(clk.Now(), true) {
		t.Fatal("first canary success re-admitted the path; half-open needs 2")
	}
	if !s.NoteCanary(clk.Now(), true) {
		t.Fatal("second consecutive canary success did not re-admit the path")
	}
	if s.Ejected() {
		t.Fatal("path still ejected after backoff + 2 canary successes")
	}

	// The system stays live: a fresh distinct-destination failure streak
	// on the recovered path ejects it again.
	ejectViaStreak(&s, clk.Now())
	if !s.Ejected() {
		t.Fatal("recovered path did not re-eject on a new failure streak")
	}
}

// ---------- T-HLT-06: one dead TARGET must not eject a healthy path ----------

func TestHealth_THLT06_OneDeadTargetDoesNotEjectHealthyPath(t *testing.T) {
	clk := newClock()
	var s State

	// A healthy path where ONE target site is dead: live traffic keeps
	// flowing (11 OKs across several live hosts), the dead site fails 5
	// times — including a burst of EjectConsecutive+2 consecutive retries.
	ejected := false
	record := func(dest string, o Outcome) {
		if s.Record(clk.Now(), dest, o) {
			ejected = true
		}
		clk.Advance(time.Second)
	}
	for i := 0; i < 6; i++ {
		record(fmt.Sprintf("live%d.example:443", i%3), OK)
	}
	for i := 0; i < 5; i++ {
		record("dead-target.example:8443", HardFail) // consecutive, but ONE host
	}
	for i := 0; i < 6; i++ {
		record(fmt.Sprintf("live%d.example:443", i%3), OK)
	}

	if ejected {
		snap := s.Snapshot(clk.Now())
		t.Fatalf("one dead target ejected the path: %+v (distinct-destination guard must hold)", snap)
	}
	snap := s.Snapshot(clk.Now())
	if snap.FailRatio >= EjectFailRatio {
		t.Fatalf("test setup broken: failRatio %v should stay below %v so only the distinctness guard is under test", snap.FailRatio, EjectFailRatio)
	}
	// The consecutive run to that one host never counted as rule A:
	// distinct destinations in the run stayed at 1 (checked via a fresh
	// run of the same shape).
	var run State
	for i := 0; i < 5; i++ {
		run.Record(clk.Now(), "dead-target.example:8443", HardFail)
	}
	if run.Ejected() {
		t.Fatal("5 consecutive HARD_FAILs to one host ejected the path; rule A needs 3 DISTINCT hosts")
	}
	if snap := run.Snapshot(clk.Now()); snap.Consecutive != 5 || snap.DistinctRun != 1 {
		t.Errorf("tail run = consecutive %d / distinct %d, want 5 / 1", snap.Consecutive, snap.DistinctRun)
	}

	// Contrast: the same volume across 3 distinct hosts does eject.
	var contrast State
	contrast.Record(clk.Now(), "a.example:443", HardFail)
	contrast.Record(clk.Now(), "b.example:443", HardFail)
	if !contrast.Record(clk.Now(), "c.example:443", HardFail) {
		t.Fatal("3 consecutive failures across 3 distinct hosts did not eject")
	}
}
