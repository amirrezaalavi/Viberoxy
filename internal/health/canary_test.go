package health

import (
	"math/rand"
	"testing"
	"time"
)

// ---------- T-HLT-03: all canary endpoints failing => nobody ejected ----------

func TestHealth_THLT03_AllCanariesFailNobodyEjected(t *testing.T) {
	// Endpoint layer: a path's canary fails only if ALL endpoints failed.
	if PathOK(false, false) {
		t.Error("PathOK(false,false) = true: all endpoints failed, the path canary must fail")
	}
	if !PathOK(false, true) || !PathOK(true, false) {
		t.Error("PathOK must pass when any single endpoint succeeded")
	}

	// Interval layer: every path fails its canary in the SAME interval
	// (all endpoints down, local network broken, DNS gone — T-CHAOS-05).
	results := []PathCanary{
		{Index: 0}, {Index: 1}, {Index: 2}, {Index: 3}, // OK: false everywhere
	}
	degraded, failing := ApplyCanary(results)
	if !degraded {
		t.Fatal("4/4 canary failures did not trip the environmental breaker (>=75%)")
	}
	if len(failing) != 0 {
		t.Fatalf("environmental failure returned %d paths to fail: %v — nobody may be ejected", len(failing), failing)
	}

	// main.go's runCanaries mirrors exactly this policy: degraded =>
	// only the env_degraded gauge moves, no path sees a failure, no State
	// is touched, nothing is ejected.
	SetEnvDegraded(degraded)
	defer SetEnvDegraded(false)
	if !EnvDegraded() {
		t.Fatal("env_degraded gauge not set on environmental failure")
	}
	states := make([]State, len(results))
	now := time.Unix(1_700_000_000, 0)
	for _, idx := range failing { // empty by contract
		states[idx].NoteCanary(now, false)
	}
	for i := range states {
		if states[i].Ejected() {
			t.Fatalf("path %d ejected during an environmental failure", i)
		}
	}

	// The breaker boundary: 75% trips, below does not — and a partial
	// failure hands its paths back for per-path handling.
	if !EnvFailure(3, 4) {
		t.Error("EnvFailure(3,4) = false: 75% must trip the breaker")
	}
	if EnvFailure(2, 4) {
		t.Error("EnvFailure(2,4) = true: 50% is not environmental")
	}
	partial := []PathCanary{{Index: 0}, {Index: 1, OK: true}, {Index: 2}, {Index: 3, OK: true}}
	degraded, failing = ApplyCanary(partial)
	if degraded || len(failing) != 2 || failing[0] != 0 || failing[1] != 2 {
		t.Errorf("ApplyCanary(partial) = degraded %v, failing %v; want false, [0 2]", degraded, failing)
	}
	SetEnvDegraded(degraded)
	if EnvDegraded() {
		t.Error("env_degraded stayed set after a partial (non-environmental) failure")
	}
	SetEnvDegraded(false)
}

// ---------- T-HLT-04: half-open re-admission after 2 canary OKs ----------

func TestHealth_THLT04_HalfOpenReadmitAfterTwoCanaryOKs(t *testing.T) {
	clk := newClock()
	var s State
	ejectViaStreak(&s, clk.Now())
	if !s.Ejected() {
		t.Fatal("state not ejected")
	}

	// While the backoff runs, canary successes are not half-open probing
	// yet: they neither re-admit nor accumulate a streak.
	if s.NoteCanary(clk.Now().Add(5*time.Second), true) {
		t.Fatal("re-admitted during the ejection backoff")
	}
	clk.Advance(BaseBackoff + time.Second) // backoff elapsed

	// First post-backoff success is not enough.
	if s.NoteCanary(clk.Now(), true) {
		t.Fatal("1 consecutive canary success re-admitted the path; half-open needs 2")
	}
	if !s.Ejected() {
		t.Fatal("path admitted after a single canary success")
	}

	// A failed canary resets the streak.
	if s.NoteCanary(clk.Now().Add(20*time.Second), false) {
		t.Fatal("failed canary reported recovery")
	}
	if s.NoteCanary(clk.Now().Add(40*time.Second), true) {
		t.Fatal("streak survived a failed canary: 1 success after a failure re-admitted")
	}

	// Two consecutive successes re-admit (the one after the reset plus
	// this one).
	if !s.NoteCanary(clk.Now().Add(60*time.Second), true) {
		t.Fatal("2 consecutive canary successes did not re-admit the path")
	}
	if s.Ejected() {
		t.Fatal("path still ejected after half-open re-admission")
	}
}

// ---------- canary endpoints + jittered interval (SPEC-H.5) ----------

func TestCanaryEndpointsFromEnv(t *testing.T) {
	t.Setenv("HEALTH_ENDPOINTS", "")
	if got := EndpointsFromEnv(); len(got) != 2 || got[0] != "https://www.gstatic.com/generate_204" || got[1] != "https://www.cloudflare.com/cdn-cgi/trace" {
		t.Errorf("unset/empty HEALTH_ENDPOINTS = %v, want the two built-in defaults", got)
	}

	t.Setenv("HEALTH_ENDPOINTS", "https://a.example/204, https://b.example/trace;https://c.example/")
	got := EndpointsFromEnv()
	want := []string{"https://a.example/204", "https://b.example/trace", "https://c.example/"}
	if len(got) != len(want) {
		t.Fatalf("EndpointsFromEnv() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("endpoint[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Whitespace-only must fall back rather than yield a canary that can
	// never fail.
	t.Setenv("HEALTH_ENDPOINTS", "  , ;  ")
	if got := EndpointsFromEnv(); len(got) != 2 {
		t.Errorf("blank HEALTH_ENDPOINTS = %v, want defaults", got)
	}
}

func TestCanaryJitterIntervalBounds(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for _, base := range []time.Duration{0, CanaryIntervalMin, 22 * time.Second, CanaryIntervalMax, time.Minute} {
		for i := 0; i < 500; i++ {
			d := JitterInterval(base, r)
			if d < CanaryIntervalMin || d > CanaryIntervalMax {
				t.Fatalf("JitterInterval(%s) = %s: outside the 15-30s canary band", base, d)
			}
		}
	}
	// ±20% jitter around an in-band base must actually vary.
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		seen[JitterInterval(22*time.Second, r)] = true
	}
	if len(seen) < 100 {
		t.Errorf("only %d distinct intervals in 200 draws: jitter not applied", len(seen))
	}
}
