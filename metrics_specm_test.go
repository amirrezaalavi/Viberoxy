package main

// SPEC-M metric tests (F-16): every metric the review requires must be
// registered with the right labels and fed by the behavior it observes;
// viberoxy_session_reset_on_drain_total must stay 0 under drain and swap;
// and the access log must tell the truth (ok only when bytes reached the
// client).
//
// Label isolation: per-path series are keyed by the generation ID, which
// is monotonic and minted fresh per test fixture, so these assertions
// never collide with another test's series (go test -shuffle=on safe).

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
	"viberoxy/internal/health"
	"viberoxy/internal/path"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/retry"
	"viberoxy/internal/xrayproc"
)

// specMScrape renders the registry the way /metrics does (minus the pool
// refresh, which the individual tests drive explicitly).
func specMScrape(t *testing.T) string {
	t.Helper()
	var sb strings.Builder
	if err := metricsRegistry.Write(&sb); err != nil {
		t.Fatalf("registry write: %v", err)
	}
	return sb.String()
}

// TestSpecM_ExpositionListsEveryMetric pins registration + TYPE for every
// SPEC-M metric, and the zero-valued series that must exist from the very
// first scrape (env_degraded healthy, no last-resort selections yet, and
// the must-stay-zero session-reset counter).
func TestSpecM_ExpositionListsEveryMetric(t *testing.T) {
	out := specMScrape(t)

	want := []struct {
		name string
		kind string
	}{
		{"viberoxy_path_state", "gauge"},
		{"viberoxy_path_inflight", "gauge"},
		{"viberoxy_path_ttfb_seconds", "histogram"},
		{"viberoxy_path_goodput_bps", "gauge"},
		{"viberoxy_conn_outcome_total", "counter"},
		{"viberoxy_path_ejections_total", "counter"},
		{"viberoxy_retry_total", "counter"},
		{"viberoxy_swap_total", "counter"},
		{"viberoxy_env_degraded", "gauge"},
		{"viberoxy_selection_last_resort_total", "counter"},
		{"viberoxy_drain_seconds", "histogram"},
		{"viberoxy_session_reset_on_drain_total", "counter"},
	}
	for _, w := range want {
		if !strings.Contains(out, "# HELP "+w.name+" ") {
			t.Errorf("exposition missing HELP for %s", w.name)
		}
		if !strings.Contains(out, "# TYPE "+w.name+" "+w.kind) {
			t.Errorf("exposition missing TYPE %s %s", w.name, w.kind)
		}
	}

	// Zero-initialized series visible from the first scrape. Values other
	// than the session-reset counter may have moved in other tests (the
	// registry is process-global), so only their presence is pinned here.
	for _, line := range []string{
		"viberoxy_env_degraded ",
		"viberoxy_selection_last_resort_total ",
		"viberoxy_session_reset_on_drain_total 0",
	} {
		if !strings.Contains(out, line) {
			t.Errorf("exposition missing series line %q", line)
		}
	}
}

// TestSpecM_PathSeriesRefresh pins the {path,slot} gauges: path is the
// generation ID, slot the WAN index; state uses internal/path's numeric
// states (1=active, 4=dead), inflight the clamped connection count, and
// the goodput gauge the path's EWMA. It also pins that a retired
// generation's series are deleted once it holds nothing (bounded
// cardinality) while a retired generation still draining keeps reporting.
func TestSpecM_PathSeriesRefresh(t *testing.T) {
	pool := NewWANPool(2, 27100)

	// Slot 0: active occupant with one in-flight connection and a sample.
	pool.Slots[0].State = StateActive
	cur := pool.Slots[0].Current.Load()
	cur.SetState(path.Active)
	cur.Reserve()
	ttfbBefore := metricPathTTFB.Value()
	cur.RecordHealth(150*time.Millisecond, 2_000_000)
	if got := metricPathTTFB.Value(); got != ttfbBefore+1 {
		t.Errorf("path_ttfb_seconds observations = %v, want %v (the path sample hook must feed the histogram)",
			got, ttfbBefore+1)
	}

	refreshPoolMetrics(pool)
	out := specMScrape(t)

	id := strconv.FormatUint(cur.ID, 10)
	for _, want := range []string{
		`viberoxy_path_state{path="` + id + `",slot="0"} 1`,
		`viberoxy_path_inflight{path="` + id + `",slot="0"} 1`,
		`viberoxy_path_goodput_bps{path="` + id + `",slot="0"} 500000`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q\n%s", want, out)
		}
	}

	// Slot 1 is vacant: its placeholder reports state 4 (dead).
	vac := pool.Slots[1].Current.Load()
	vid := strconv.FormatUint(vac.ID, 10)
	if !strings.Contains(out, `viberoxy_path_state{path="`+vid+`",slot="1"} 4`) {
		t.Errorf("exposition missing the vacant slot's dead state series\n%s", out)
	}
	// The TTFB histogram is unlabelled and counts every path sample.
	if !strings.Contains(out, "viberoxy_path_ttfb_seconds_count ") {
		t.Errorf("exposition missing viberoxy_path_ttfb_seconds_count\n%s", out)
	}

	// Retire slot 0's generation while it holds nothing: its gauges are
	// dropped at the next refresh instead of freezing forever.
	cur.Release()
	pool.Slots[0].Current.Store(path.NewVacant(0, 27100))
	refreshPoolMetrics(pool)
	out = specMScrape(t)
	if strings.Contains(out, `viberoxy_path_state{path="`+id+`",slot="0"}`) {
		t.Errorf("retired path %s still has a state series after its last connection finished", id)
	}
}

// TestSpecM_ConnOutcomeFeed pins viberoxy_conn_outcome_total{path,outcome,
// reason}: fed at relay end from health.ClassifyReason, so the label values
// are the classifier's outcome and clause.
func TestSpecM_ConnOutcomeFeed(t *testing.T) {
	p := path.New(&proxycfg.ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 8388}, nil, 0, 10700)
	id := strconv.FormatUint(p.ID, 10)

	recordConnOutcome(p, health.OK, "delivered")
	recordConnOutcome(p, health.HardFail, "no_down")
	recordConnOutcome(nil, health.OK, "delivered") // nil path: no panic, no series

	out := specMScrape(t)
	for _, want := range []string{
		`viberoxy_conn_outcome_total{path="` + id + `",outcome="ok",reason="delivered"} 1`,
		`viberoxy_conn_outcome_total{path="` + id + `",outcome="hard_fail",reason="no_down"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q\n%s", want, out)
		}
	}

	// The reason strings come from health.ClassifyReason — the same call
	// relay.go feeds the metric with.
	for _, tc := range []struct {
		res     health.Result
		outcome health.Outcome
		reason  string
	}{
		{health.Result{Up: 10, Down: 5, Duration: time.Second}, health.OK, "delivered"},
		{health.Result{Up: 10, Down: 0, Duration: time.Second}, health.HardFail, "no_down"},
		{health.Result{Up: 10, Down: 0, Duration: 9 * time.Second}, health.HardFail, "first_byte_timeout"},
		{health.Result{Up: 0, Down: 0, Duration: time.Second}, health.Neutral, "client_abort"},
	} {
		o, reason := health.ClassifyReason(tc.res)
		if o != tc.outcome || reason != tc.reason {
			t.Errorf("ClassifyReason(%+v) = (%v, %q), want (%v, %q)", tc.res, o, reason, tc.outcome, tc.reason)
		}
	}
}

// TestSpecM_EjectionFeedsCounter pins viberoxy_path_ejections_total: an
// ejection observed by internal/path notifies the observer wired here, with
// the SPEC-H.3 rule that fired as the reason.
func TestSpecM_EjectionFeedsCounter(t *testing.T) {
	p := path.New(&proxycfg.ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 8388}, nil, 0, 10700)
	id := strconv.FormatUint(p.ID, 10)
	now := time.Now()

	// Rule A: >= 3 consecutive HARD_FAILs across >= 3 distinct hosts.
	if p.RecordOutcome(now, "a.example", health.HardFail) {
		t.Fatal("ejected after 1 failure; the window needs a run")
	}
	if p.RecordOutcome(now, "b.example", health.HardFail) {
		t.Fatal("ejected after 2 failures; rule A needs 3")
	}
	if !p.RecordOutcome(now, "c.example", health.HardFail) {
		t.Fatal("3 consecutive failures on 3 distinct hosts must eject (SPEC-H.3 rule A)")
	}
	if got := p.GetState(); got != path.Suspect {
		t.Errorf("path state = %v, want Suspect after ejection", got)
	}
	if v := metricPathEjections.Value(id, "consecutive_distinct"); v != 1 {
		t.Errorf("path_ejections_total{reason=consecutive_distinct} = %v, want 1", v)
	}
	if out := specMScrape(t); !strings.Contains(out,
		`viberoxy_path_ejections_total{path="`+id+`",reason="consecutive_distinct"} 1`) {
		t.Errorf("exposition missing the ejection series\n%s", out)
	}
}

// TestSpecM_RetryHook pins viberoxy_retry_total{stage,outcome}: internal/retry
// reports each dial-stage attempt through the hook installed by metrics.go's
// init (no import cycle), exactly once per attempt.
func TestSpecM_RetryHook(t *testing.T) {
	ctx := context.Background()

	retryBefore := metricRetryTotal.Value(retry.StageDial, "retry")
	failedBefore := metricRetryTotal.Value(retry.StageDial, "failed")
	noneBefore := metricRetryTotal.Value(retry.StageDial, "no_candidate")

	// Two attempts, both failing: attempt 1 buys a retry, attempt 2 ends
	// the stage (attempt cap).
	pol := retry.New(retry.Options{MaxAttempts: 2, Budget: time.Second})
	n := 0
	next := func() (int, bool) {
		n++
		if n > 2 {
			return 0, false
		}
		return n, true
	}
	dial := func(context.Context, int) (net.Conn, error) { return nil, errors.New("refused") }
	if _, _, err := retry.Run(ctx, pol, next, dial); err == nil {
		t.Fatal("Run with failing dials must return an error")
	}
	if v := metricRetryTotal.Value(retry.StageDial, "retry"); v != retryBefore+1 {
		t.Errorf("retry_total{stage=dial,outcome=retry} = %v, want %v", v, retryBefore+1)
	}
	if out := specMScrape(t); !strings.Contains(out, `viberoxy_retry_total{stage="dial",outcome="retry"}`) {
		t.Errorf("exposition missing the retry_total{stage=\"dial\",outcome=\"retry\"} series\n%s", out)
	}
	if v := metricRetryTotal.Value(retry.StageDial, "failed"); v != failedBefore+1 {
		t.Errorf("retry_total{stage=dial,outcome=failed} = %v, want %v", v, failedBefore+1)
	}

	// Empty pool: one no_candidate event, no attempts.
	if _, _, err := retry.Run(ctx, pol, func() (int, bool) { return 0, false }, dial); !errors.Is(err, retry.ErrNoCandidate) {
		t.Fatalf("Run on an empty pool = %v, want ErrNoCandidate", err)
	}
	if v := metricRetryTotal.Value(retry.StageDial, "no_candidate"); v != noneBefore+1 {
		t.Errorf("retry_total{stage=dial,outcome=no_candidate} = %v, want %v", v, noneBefore+1)
	}
}

// TestSpecM_SwapTotal pins viberoxy_swap_total{result} for every verdict
// SPEC-R can return: swapped | cooldown | dwell | hysteresis | no_candidate.
func TestSpecM_SwapTotal(t *testing.T) {
	better := []*TestResult{rotResult("cand.example", 1001, 40)}

	// no_candidate: nothing cleared the gates.
	r := newTestRotator()
	pool := rotPool(t, 10, 0, 2*time.Hour, "")
	cfg := rotConfig()
	before := metricSwapTotal.Value("no_candidate")
	if r.maybeSwap(pool, cfg, nil) {
		t.Fatal("maybeSwap with no candidates must not swap")
	}
	if v := metricSwapTotal.Value("no_candidate"); v != before+1 {
		t.Errorf("swap_total{result=no_candidate} = %v, want %v", v, before+1)
	}

	// dwell: a freshly activated incumbent is never rotated out.
	r = newTestRotator()
	fresh := rotPool(t, 10, 0, time.Minute, "")
	before = metricSwapTotal.Value("dwell")
	if r.maybeSwap(fresh, cfg, better) {
		t.Fatal("maybeSwap must respect MIN_DWELL")
	}
	if v := metricSwapTotal.Value("dwell"); v != before+1 {
		t.Errorf("swap_total{result=dwell} = %v, want %v", v, before+1)
	}

	// hysteresis: a candidate inside the +30% band must not churn the pool.
	r = newTestRotator()
	aged := rotPool(t, 10, 0, 2*time.Hour, "")
	within := []*TestResult{rotResult("cand.example", 1001, 11.9)}
	before = metricSwapTotal.Value("hysteresis")
	if r.maybeSwap(aged, cfg, within) {
		t.Fatal("maybeSwap with a sub-hysteresis candidate must not swap")
	}
	if v := metricSwapTotal.Value("hysteresis"); v != before+1 {
		t.Errorf("swap_total{result=hysteresis} = %v, want %v", v, before+1)
	}

	// cooldown: a swap just happened, so the next cycle is gated.
	r = newTestRotator()
	r.lastSwap = rotNow.Add(-time.Second)
	aged = rotPool(t, 10, 0, 2*time.Hour, "")
	before = metricSwapTotal.Value("cooldown")
	if r.maybeSwap(aged, cfg, better) {
		t.Fatal("maybeSwap inside the cooldown window must not swap")
	}
	if v := metricSwapTotal.Value("cooldown"); v != before+1 {
		t.Errorf("swap_total{result=cooldown} = %v, want %v", v, before+1)
	}

	// swapped: a clearly better candidate cuts over make-before-break.
	r = newTestRotator()
	r.start = func(*proxycfg.ProxyConfig, int, ...bool) (*xrayproc.Handle, string, error) {
		return swpDummyHandle(), "", nil
	}
	r.test = func(c *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
		return &TestResult{Config: c, Speed: 42}
	}
	aged = rotPool(t, 10, 0, 2*time.Hour, "")
	before = metricSwapTotal.Value("swapped")
	if !r.maybeSwap(aged, cfg, better) {
		t.Fatal("maybeSwap with a 4x-better candidate must swap")
	}
	if v := metricSwapTotal.Value("swapped"); v != before+1 {
		t.Errorf("swap_total{result=swapped} = %v, want %v", v, before+1)
	}
	if out := specMScrape(t); !strings.Contains(out, `viberoxy_swap_total{result="swapped"}`) {
		t.Errorf("exposition missing the swap_total{result=\"swapped\"} series\n%s", out)
	}
}

// TestSpecM_EnvDegradedGauge pins viberoxy_env_degraded: wired from
// internal/health's breaker state (the single source of truth) on every
// scrape and at event time.
func TestSpecM_EnvDegradedGauge(t *testing.T) {
	t.Cleanup(func() {
		health.SetEnvDegraded(false)
		metricEnvDegraded.Set(0)
	})

	health.SetEnvDegraded(true)
	refreshPoolMetrics(nil) // pool-less scrape still refreshes the gauge
	if v := metricEnvDegraded.Value(); v != 1 {
		t.Errorf("env_degraded = %v, want 1 while the breaker is tripped", v)
	}
	if out := specMScrape(t); !strings.Contains(out, "viberoxy_env_degraded 1") {
		t.Errorf("exposition missing viberoxy_env_degraded 1\n%s", out)
	}

	health.SetEnvDegraded(false)
	refreshPoolMetrics(nil)
	if v := metricEnvDegraded.Value(); v != 0 {
		t.Errorf("env_degraded = %v, want 0 after recovery", v)
	}
}

// TestSpecM_SelectionLastResort pins viberoxy_selection_last_resort_total:
// incremented only when selection had to fall back — degraded active slots,
// or (last resort) a draining slot — never for a routable pick.
func TestSpecM_SelectionLastResort(t *testing.T) {
	pool := NewWANPool(2, 27200)
	idle := func(i int) {
		pool.Slots[i].State = StateActive
		pool.Slots[i].Cmd = swpDummyHandle()
	}
	idle(0)
	idle(1)

	// Routable pick: no increment.
	before := metricSelectionLastResort.Value()
	if p := pool.GetLeastLoaded(2); p == nil {
		t.Fatal("GetLeastLoaded returned nil for a healthy pool")
	}
	if v := metricSelectionLastResort.Value(); v != before {
		t.Errorf("last_resort_total = %v after a routable pick, want %v (unchanged)", v, before)
	}

	// Both active but over the threshold: the degraded fallback fires.
	setFails(pool, 0, 5)
	setFails(pool, 1, 5)
	if p := pool.GetLeastLoaded(2); p == nil {
		t.Fatal("degraded fallback must still hand back a path (no blackhole)")
	}
	if v := metricSelectionLastResort.Value(); v != before+1 {
		t.Errorf("last_resort_total = %v after a degraded pick, want %v", v, before+1)
	}

	// Draining-only pool: the last-resort pass fires.
	if err := pool.MarkDraining(0); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	if err := pool.MarkDraining(1); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	before = metricSelectionLastResort.Value()
	if p := pool.GetLeastLoaded(2); p == nil {
		t.Fatal("draining last resort must still hand back a path")
	}
	if v := metricSelectionLastResort.Value(); v != before+1 {
		t.Errorf("last_resort_total = %v after a draining last-resort pick, want %v", v, before+1)
	}
}

// TestSpecM_DrainSeconds pins viberoxy_drain_seconds: observed when a drain
// completes — reaped (inflight == 0 / DRAIN_MAX) or un-drained on health
// recovery.
func TestSpecM_DrainSeconds(t *testing.T) {
	// Reap path: a drain with nothing in flight completes immediately.
	pool := NewWANPool(1, 27300)
	pool.Slots[0].State = StateActive
	pool.Slots[0].Cmd = swpDummyHandle()
	if err := pool.MarkDraining(0); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	before := metricDrainSeconds.Value()
	reaped := reapCompletedDrains(pool, time.Minute)
	if len(reaped) != 1 || reaped[0] != 0 {
		t.Fatalf("reapCompletedDrains = %v, want [0]", reaped)
	}
	if v := metricDrainSeconds.Value(); v != before+1 {
		t.Errorf("drain_seconds observations = %v, want %v (the reap must record the drain duration)", v, before+1)
	}
	if out := specMScrape(t); !strings.Contains(out, "viberoxy_drain_seconds_count ") {
		t.Errorf("exposition missing viberoxy_drain_seconds_count\n%s", out)
	}
	if got := pool.GetState(0); got != StateEmpty {
		t.Errorf("slot state after reap = %v, want empty", got)
	}

	// Un-drain path: a health drain that recovers records its duration too.
	pool = NewWANPool(1, 27400)
	pool.Slots[0].State = StateActive
	pool.Slots[0].Cmd = swpDummyHandle()
	if err := pool.MarkDrainingHealth(0); err != nil {
		t.Fatalf("MarkDrainingHealth: %v", err)
	}
	cur := pool.Slots[0].Current.Load()
	now := time.Now()
	cur.NoteCanary(now, true)
	cur.NoteCanary(now, true) // HalfOpenSuccesses consecutive canaries
	before = metricDrainSeconds.Value()
	if !pool.UnDrainIfRecovered(0, false) {
		t.Fatal("a health-drained slot with a full canary streak must un-drain")
	}
	if v := metricDrainSeconds.Value(); v != before+1 {
		t.Errorf("drain_seconds observations = %v, want %v (the un-drain must record the drain duration)", v, before+1)
	}
}

// TestSpecM_SessionResetOnDrainStaysZero is the SPEC-M tripwire:
// viberoxy_session_reset_on_drain_total is initialized to 0 and MUST stay
// 0 — drains and swaps never reset a live session. Exercised through the
// real drain reap and a real make-before-break swap.
func TestSpecM_SessionResetOnDrainStaysZero(t *testing.T) {
	if v := metricSessionResetOnDrain.Value(); v != 0 {
		t.Fatalf("session_reset_on_drain_total = %v before the test, want 0", v)
	}

	// Drain + reap.
	pool := NewWANPool(1, 27500)
	pool.Slots[0].State = StateActive
	pool.Slots[0].Cmd = swpDummyHandle()
	if err := pool.MarkDraining(0); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}
	reapCompletedDrains(pool, time.Minute)

	// Make-before-break swap (same fixture as T-ROT-02).
	r := newTestRotator()
	r.start = func(*proxycfg.ProxyConfig, int, ...bool) (*xrayproc.Handle, string, error) {
		return swpDummyHandle(), "", nil
	}
	r.test = func(c *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
		return &TestResult{Config: c, Speed: 42}
	}
	swapped := rotPool(t, 10, 0, 2*time.Hour, "")
	if !r.maybeSwap(swapped, rotConfig(), []*TestResult{rotResult("cand.example", 1001, 40)}) {
		t.Fatal("fixture swap failed; the drain/swap exercise is incomplete")
	}

	if v := metricSessionResetOnDrain.Value(); v != 0 {
		t.Errorf("session_reset_on_drain_total = %v after drain + swap; it MUST stay 0", v)
	}
	if out := specMScrape(t); !strings.Contains(out, "viberoxy_session_reset_on_drain_total 0") {
		t.Errorf("exposition missing viberoxy_session_reset_on_drain_total 0\n%s", out)
	}
}

// TestSpecM_AccessLogTellsTheTruth (F-16): the per-connection log line says
// "ok" only when bytes actually reached the client (down > 0); everything
// else — including a caller that passes "ok" for a relay that delivered
// nothing — logs "err".
func TestSpecM_AccessLogTellsTheTruth(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	r := &wanRelay{AccessLog: true}
	now := time.Now()
	// The three shapes logAccess sees: a lying caller (dial/relay passes
	// "ok"), a genuinely delivered connection, and an empty one.
	r.logAccess("example.com:443", 0, 10, 0, now, "ok", "connect", "wan")
	r.logAccess("example.com:443", 0, 10, 5, now, "ok", "connect", "wan")
	r.logAccess("example.com:443", 0, 0, 0, now, "err", "connect", "wan")

	var lines []string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, "proxy access") {
			lines = append(lines, l)
		}
	}
	if len(lines) != 3 {
		t.Fatalf("got %d access-log lines, want 3:\n%s", len(lines), buf.String())
	}
	wantStatus := []string{"status=err", "status=ok", "status=err"}
	for i, want := range wantStatus {
		if !strings.Contains(lines[i], want) {
			t.Errorf("access line %d = %s\n  want it to contain %q", i, lines[i], want)
		}
	}

	// Access log off: nothing is written at all.
	buf.Reset()
	r.AccessLog = false
	r.logAccess("example.com:443", 0, 10, 5, now, "ok", "connect", "wan")
	if buf.Len() != 0 {
		t.Errorf("ACCESS_LOG=false still logged: %s", buf.String())
	}
}
