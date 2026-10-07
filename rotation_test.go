package main

// SPEC-R rotation tests (F-03's second half): a full pool keeps
// evaluating candidates, and a swap only happens on a CLEAR win —
// hysteresis (>= +30%), MIN_DWELL, FETCH_INTERVAL cooldown and exit-IP
// dedupe — at most one make-before-break swap per cycle.
//
// Every test injects the clock (rotator.now) and the swap's test/start
// hooks, so there is no sleeping and no real xray anywhere in here.

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
	"viberoxy/internal/cands"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

// rotNow is the fixed "current time" the T-ROT tests inject via
// rotator.now; fixtures backdate path.CreatedAt against it.
var rotNow = time.Now().Round(time.Second)

// rotPool builds a one-slot pool whose single slot is Active with the
// given incumbent benchmark speed, stability score, activation age (the
// current path's CreatedAt is backdated against the injected clock) and
// last canary-recorded exit IP ("" = never probed / unknown).
func rotPool(t *testing.T, speed float64, stability int, age time.Duration, exitIP string) *WANPool {
	t.Helper()
	pool := NewWANPool(1, 24000)
	slot := pool.Slots[0]
	slot.State = StateActive
	slot.Config = &proxycfg.ProxyConfig{Protocol: "ss", Server: "1.1.1.1", Port: 8388, Raw: "ss://incumbent"}
	slot.SpeedMbps = speed
	slot.StabilityScore = stability
	slot.ExitIP = exitIP
	slot.Current.Load().CreatedAt = rotNow.Add(-age)
	return pool
}

// rotResult builds a passing speed-test result for a candidate. The
// server is a hostname so the production exit-IP resolver reports the
// candidate's exit IP as unknown (gate skipped) unless a test says
// otherwise.
func rotResult(server string, port int, speed float64) *TestResult {
	return &TestResult{
		Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: server, Port: port, Raw: "ss://" + server},
		Speed:  speed,
	}
}

// rotConfig is a hand-built cycle config for the rotation tests: the
// 300s FETCH_INTERVAL doubles as the rotation cooldown.
func rotConfig() *proxycfg.Config {
	return &proxycfg.Config{
		SubscriberURL: "https://example.invalid/sub",
		FetchInterval: 300,
		TestTimeout:   3,
		DownloadSize:  1000000,
		MinimumSpeed:  5,
		WanCount:      1,
		TestBasePort:  25000,
		WanBasePort:   24000,
	}
}

// newTestRotator is the injected-clock constructor for the T-ROT tests.
func newTestRotator() *rotator {
	r := newRotator()
	r.now = func() time.Time { return rotNow }
	return r
}

// T-ROT-01 (SPEC-R): an equal-quality candidate must NOT displace the
// incumbent — neither an exact tie nor a +10% (within the 30%
// hysteresis band) improvement may cause churn.
func TestRotation_TROT01_EqualQualityCandidateNoSwap(t *testing.T) {
	r := newTestRotator()
	pool := rotPool(t, 10, 0, 2*time.Hour, "")
	cfg := rotConfig()

	var starts int
	r.start = func(*proxycfg.ProxyConfig, int, ...bool) (*xrayproc.Handle, string, error) {
		starts++
		return swpDummyHandle(), "", nil
	}
	r.test = func(c *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
		return &TestResult{Config: c, Speed: 40}
	}

	results := []*TestResult{
		rotResult("cand-a.example", 1001, 10),   // equal to the incumbent
		rotResult("cand-b.example", 1002, 11),   // +10%
		rotResult("cand-c.example", 1003, 11.9), // just under the +30% bar
	}
	if d := r.decide(pool, results, cfg.MinimumSpeed, 300*time.Second); d.Victim != -1 {
		t.Fatalf("decide() = victim %d candidate %+v (%s); equal-quality candidates must not churn the pool",
			d.Victim, d.Candidate, d.Reason)
	}
	if swapped := r.maybeSwap(pool, cfg, results); swapped {
		t.Errorf("maybeSwap() = true; a candidate inside the 30%% hysteresis band must not swap (starts=%d)", starts)
	}
	if got := pool.Slots[0].Config.Raw; got != "ss://incumbent" {
		t.Errorf("incumbent config = %q, want ss://incumbent untouched", got)
	}
	if starts != 0 {
		t.Errorf("started %d replacements, want 0 (no churn)", starts)
	}
}

// T-ROT-02 (SPEC-R): a clearly better candidate (>= +30%) swaps the
// worst incumbent — exactly ONCE per cycle even when several candidates
// qualify — and the just-recorded swap then blocks the next one until
// the cooldown elapses.
func TestRotation_TROT02_ClearlyBetterCandidateSwapsExactlyOnce(t *testing.T) {
	r := newTestRotator()
	pool := rotPool(t, 10, 0, 2*time.Hour, "")
	cfg := rotConfig()

	var starts, retests int
	r.start = func(*proxycfg.ProxyConfig, int, ...bool) (*xrayproc.Handle, string, error) {
		starts++
		return swpDummyHandle(), "", nil
	}
	r.test = func(c *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
		retests++
		return &TestResult{Config: c, Speed: 42}
	}

	results := []*TestResult{
		rotResult("cand-a.example", 1001, 13.5), // +35% — qualifies
		rotResult("cand-b.example", 1002, 40),   // fastest — must win
		rotResult("cand-c.example", 1003, 20),   // +100% — qualifies too
	}
	if !r.maybeSwap(pool, cfg, results) {
		t.Fatalf("maybeSwap() = false; a %v candidate vs a %v incumbent must swap", 40.0, 10.0)
	}
	if starts != 1 {
		t.Fatalf("swaps started = %d, want exactly 1 (at most one swap per cycle)", starts)
	}
	if retests != 1 {
		t.Errorf("replacement re-tests = %d, want exactly 1", retests)
	}
	if got := pool.Slots[0].Config.Server; got != "cand-b.example" {
		t.Errorf("swapped in %q, want the fastest qualifying candidate cand-b.example", got)
	}
	// The cooldown clock was stamped: an immediate second cycle cannot swap.
	if r.maybeSwap(pool, cfg, results) {
		t.Errorf("second swap inside the FETCH_INTERVAL cooldown window")
	}
	if starts != 1 {
		t.Errorf("swaps started after the second call = %d, want 1", starts)
	}
}

// T-ROT-03 (SPEC-R): MIN_DWELL and the FETCH_INTERVAL cooldown are hard
// gates — a too-fresh incumbent is never rotated out, and a swap
// suppresses the next one until cooldown has elapsed.
func TestRotation_TROT03_DwellAndCooldownRespected(t *testing.T) {
	r := newTestRotator()
	cfg := rotConfig()
	better := []*TestResult{rotResult("cand.example", 1001, 40)}

	// MIN_DWELL: activated 1 minute ago (< 10 min) -> refused even
	// though the candidate is 4x better.
	fresh := rotPool(t, 10, 0, time.Minute, "")
	if d := r.decide(fresh, better, cfg.MinimumSpeed, 300*time.Second); d.Victim != -1 {
		t.Fatalf("decide() swapped after only 1m of dwell (%s); MIN_DWELL %s must hold", d.Reason, rotationMinDwell)
	}
	// Dwell satisfied (11 min >= 10 min): the same candidate qualifies.
	aged := rotPool(t, 10, 0, 11*time.Minute, "")
	if d := r.decide(aged, better, cfg.MinimumSpeed, 300*time.Second); d.Victim != 0 {
		t.Fatalf("decide() = victim %d (%s), want the aged incumbent swapped", d.Victim, d.Reason)
	}

	// Cooldown: a swap 30s ago blocks (cooldown = 300s = FETCH_INTERVAL)...
	r.lastSwap = rotNow.Add(-30 * time.Second)
	if d := r.decide(aged, better, cfg.MinimumSpeed, 300*time.Second); d.Victim != -1 {
		t.Fatalf("decide() swapped %s after the last swap; cooldown %s must hold", 30*time.Second, 300*time.Second)
	}
	// ...and once it elapses, rotation is allowed again.
	r.lastSwap = rotNow.Add(-301 * time.Second)
	if d := r.decide(aged, better, cfg.MinimumSpeed, 300*time.Second); d.Victim != 0 {
		t.Fatalf("decide() = victim %d (%s) after cooldown elapsed, want the swap", d.Victim, d.Reason)
	}
}

// T-ROT-04 (SPEC-R): a candidate whose exit IP is already served by an
// active path is rejected; a different exit IP passes, and an UNKNOWN
// exit IP (either side) skips the gate instead of blocking rotation.
func TestRotation_TROT04_ExitIPDuplicateRejected(t *testing.T) {
	r := newTestRotator()
	cfg := rotConfig()

	// Production resolver: a direct config's exit IP is its server
	// address when that is an IP literal; a hostname is unknown.
	if got := candidateExitIP(&proxycfg.ProxyConfig{Protocol: "ss", Server: "203.0.113.7", Port: 8388}); got != "203.0.113.7" {
		t.Errorf("candidateExitIP(203.0.113.7) = %q, want 203.0.113.7", got)
	}
	if got := candidateExitIP(&proxycfg.ProxyConfig{Protocol: "ss", Server: "fast.example.com", Port: 8388}); got != "" {
		t.Errorf("candidateExitIP(fast.example.com) = %q, want \"\" (unknown)", got)
	}

	// The incumbent's canary loop recorded exit IP 203.0.113.7; a
	// candidate egressing through the same IP must not rotate in.
	pool := rotPool(t, 10, 0, 2*time.Hour, "203.0.113.7")
	dup := []*TestResult{{Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "203.0.113.7", Port: 9999, Raw: "ss://203.0.113.7:9999"}, Speed: 40}}
	if d := r.decide(pool, dup, cfg.MinimumSpeed, 300*time.Second); d.Victim != -1 {
		t.Fatalf("decide() = victim %d; a candidate on the already-served exit IP 203.0.113.7 must be rejected (%s)", d.Victim, d.Reason)
	}

	// A different known exit IP clears the gate.
	r.exitIP = func(*proxycfg.ProxyConfig) string { return "203.0.113.9" }
	if d := r.decide(pool, dup, cfg.MinimumSpeed, 300*time.Second); d.Victim != 0 {
		t.Fatalf("decide() = victim %d (%s), want the swap for a distinct exit IP", d.Victim, d.Reason)
	}

	// Unknown candidate exit IP -> the check is skipped, not blocking.
	r.exitIP = func(*proxycfg.ProxyConfig) string { return "" }
	if d := r.decide(pool, dup, cfg.MinimumSpeed, 300*time.Second); d.Victim != 0 {
		t.Fatalf("decide() = victim %d (%s); an unknown candidate exit IP must skip the dedupe gate", d.Victim, d.Reason)
	}
}

// T-ROT-05 (F-03): with a full pool one cycle evaluates at most
// MAX_TEST_PER_CYCLE_FULL (= 2) NEW candidates — never zero, never the
// whole subscription.
func TestRotation_TROT05_FullPoolBudgetIsTwo(t *testing.T) {
	orig := startTestXray
	startTestXray = func(*proxycfg.ProxyConfig, int, ...bool) (*xrayproc.Handle, string, error) {
		return nil, "", errors.New("spy: no xray for this test")
	}
	t.Cleanup(func() { startTestXray = orig })

	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	// Four distinct NEW configs (none serving, none recently tested).
	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzcw@10.0.0.1:443#n1",
		"ss://YWVzLTEyOC1nY206cGFzcw@10.0.0.2:443#n2",
		"ss://YWVzLTEyOC1nY206cGFzcw@10.0.0.3:443#n3",
		"ss://YWVzLTEyOC1nY206cGFzcw@10.0.0.4:443#n4",
	}
	body := base64.StdEncoding.EncodeToString([]byte(strings.Join(lines, "\n")))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()

	pool := NewWANPool(1, 26000)
	// A live occupant so HealthCheckAll does not reap the slot and the
	// pool really is full (the RT-11 fixture's pattern).
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Skip("sleep unavailable")
	}
	defer cmd.Process.Kill()
	pool.Slots[0].State = StateActive
	pool.Slots[0].Cmd = xrayproc.Wrap(cmd, "")
	pool.Slots[0].Config = &proxycfg.ProxyConfig{Protocol: "ss", Server: "1.1.1.1", Port: 1, Raw: "ss://incumbent"}
	pool.Slots[0].SpeedMbps = 5.1

	cfg := &proxycfg.Config{SubscriberURL: srv.URL, FetchInterval: 300, TestTimeout: 3,
		DownloadSize: 1000000, MinimumSpeed: 1e9, WanCount: 1,
		TestBasePort: 26100, WanBasePort: 26000}
	runCycle(cfg, pool, cands.NewPool(10), time.Minute)

	b, err := os.ReadFile("sorted.txt")
	if err != nil {
		t.Fatalf("read sorted.txt: %v", err)
	}
	// One line per evaluated candidate (writeSortedTxt: errors too).
	evaluated := strings.Count(string(b), "\n")
	if evaluated < 1 {
		t.Errorf("full pool evaluated %d candidates; F-03 requires >= 1 per cycle", evaluated)
	}
	if evaluated > 2 {
		t.Errorf("full pool evaluated %d candidates; MAX_TEST_PER_CYCLE_FULL default is 2", evaluated)
	}
}
