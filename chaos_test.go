//go:build chaos

package main

// Hermetic chaos budget suite (review §7 layer L3): T-CHAOS-01..09 pin the
// review's chaos budgets with asserted wall-clock and count limits. Every
// test is hermetic (loopback FakeWAN/test fixtures only, no network, no
// xray binary), seeded where randomness exists, and uses the health
// subsystem's injectable clocks wherever realtime would blow the budget.
//
//	01  kill one backend        => its path leaves routable selection in < 2s,
//	                               traffic continues on the survivors and the
//	                               dead listener takes no new connections
//	02  black-hole one backend  => health-window ejection (SPEC-H.3) out of
//	                               selection within 20s of wall traffic
//	03  slow-TTFB path          => measurably less traffic than either fast
//	                               path within a bounded window
//	04  flaky path              => deprioritized: ejected within budget, hit
//	                               share collapses, no fresh traffic after
//	05  all canary endpoints    => nothing ejected/drained, env_degraded set
//	06  garbage/empty sub fetch => zero pool churn (0 swaps, paths untouched)
//	07  flapping backend        => backoff keeps it out: <= 1 re-admit / 60s
//	08  swap under load         => zero failed connections (extends
//	                               TestSwap_TSWAP01 with a flapping sibling,
//	                               so dial-stage retries run across the
//	                               validation window and the cutover)
//	09  swap during a T-INT-02-style transfer => completes, SHA-256 matched,
//	                               while a sibling backend flaps
//
// T-INT-01/T-INT-02 live in integrity_test.go (untagged) and are compiled
// into this build too, at their full 25-iteration profile.

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"viberoxy/internal/cands"
	"viberoxy/internal/health"
	"viberoxy/internal/path"
	"viberoxy/internal/ports"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/sched"
	"viberoxy/internal/testutil"
	"viberoxy/internal/xrayproc"
)

// chaosFixture is the shared T-CHAOS stage: n healthy FakeWANs wired into
// a real pool behind the real SOCKS5 front-end, with a provenance
// collector for cross-talk classification. failThreshold is what the
// front-end hands to selection; tests that must observe the SPEC-H.3
// window eject pass a high threshold so the fail counter cannot quietly
// remove the path before the window gets its evidence.
type chaosFixture struct {
	t         *testing.T
	pool      *WANPool
	wans      []*testutil.FakeWAN
	front     string
	col       *integrityCollector
	threshold int
}

func newChaosFixture(t *testing.T, n int, idle time.Duration, failThreshold int) *chaosFixture {
	t.Helper()
	setIntegrityEnv(t)
	pool := NewWANPool(n, 0)
	fx := &chaosFixture{t: t, pool: pool, col: newIntegrityCollector(), threshold: failThreshold}
	fx.col.beginIter(0)
	for i := 0; i < n; i++ {
		fx.addWAN(fmt.Sprintf("ch-w%d", i), testutil.Good(0, 0), i,
			&proxycfg.ProxyConfig{Protocol: "ss", Server: fmt.Sprintf("ch-w%d.example", i), Port: 443, Raw: fmt.Sprintf("ss://ch-w%d", i)})
	}
	fx.front = startSocksFront(t, pool, idle, failThreshold)
	return fx
}

// addWAN brings one more FakeWAN up on pool slot index (the fixture's own
// tags join the provenance set immediately).
func (fx *chaosFixture) addWAN(id string, mode testutil.Mode, index int, cfg *proxycfg.ProxyConfig) *testutil.FakeWAN {
	fx.t.Helper()
	w, err := testutil.NewFakeWAN(id, mode)
	if err != nil {
		fx.t.Fatal(err)
	}
	fx.t.Cleanup(func() { _ = w.Close() })
	fx.col.addTag(w.ID)
	swpMarkActive(fx.t, fx.pool, index, cfg, w.Port())
	fx.wans = append(fx.wans, w)
	return w
}

// routableReports whether p is in the pool's routable selection tier at
// the given fail threshold (the exact tier production Select draws from
// first).
func routableReports(pool *WANPool, p *path.Path, threshold int) bool {
	routable, _, _ := pool.eligibleTries(nil, threshold, true)
	for _, c := range routable {
		if c == p {
			return true
		}
	}
	return false
}

// tagCounter is a concurrency-safe per-tag OK counter for burst windows.
type tagCounter struct {
	mu sync.Mutex
	m  map[string]int
	n  int
}

func newTagCounter() *tagCounter { return &tagCounter{m: map[string]int{}} }

func (c *tagCounter) record(res testutil.Result) {
	if !res.OK {
		return
	}
	c.mu.Lock()
	c.m[res.Tag]++
	c.n++
	c.mu.Unlock()
}

func (c *tagCounter) snapshot() (total int, m map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m = make(map[string]int, len(c.m))
	for k, v := range c.m {
		m[k] = v
	}
	return c.n, m
}

// ---------------------------------------------------------------------------
// T-CHAOS-01: kill one backend
// ---------------------------------------------------------------------------

// TestChaos01_KilledBackendLeavesSelectionUnder2s kills one FakeWAN's
// listener under live traffic: BUDGET — the path must leave the routable
// selection within 2s wall, traffic must keep flowing on the survivors,
// and the dead backend must accept no NEW connection afterwards.
func TestChaos01_KilledBackendLeavesSelectionUnder2s(t *testing.T) {
	fx := newChaosFixture(t, 3, 30*time.Second, DefaultFailThreshold)
	sched.SeedRand(101) // deterministic P2C picks for the budget measurement
	dead := fx.pool.Slots[1].Current.Load()

	clients := make([]*testutil.FakeClient, 4)
	for i := range clients {
		clients[i] = &testutil.FakeClient{ID: fmt.Sprintf("c01-w%d", i), Socks5: true, Timeout: 3 * time.Second}
	}
	send := func(w int, seq int64) testutil.Result {
		clients[w].Target = integrityTargets[(seq+int64(w))%int64(len(integrityTargets))]
		return clients[w].Send(fx.front, seq)
	}
	// 4 workers x 45 conns x 40ms pace: ~1.8s of continuous traffic, so
	// the 2s budget below is measured against a live load, not an idle
	// pool.
	workers := startLoadWorkers(4, 180, 40*time.Millisecond, send, fx.col.record)
	t.Cleanup(workers.stopAll)
	if !waitUntil(5*time.Second, func() bool { return workers.completed.Load() >= 10 }) {
		t.Fatal("traffic never ramped up")
	}

	t0 := time.Now()
	if err := fx.wans[1].Die(); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(2*time.Second, func() bool { return !routableReports(fx.pool, dead, fx.threshold) }) {
		t.Fatalf("killed backend's path still in the routable selection %v after Die (budget 2s): fails=%d hits=%d completed=%d state=%s snapshot=%+v",
			time.Since(t0), fx.pool.SlotConsecutiveFails(1), fx.wans[1].Hits(),
			workers.completed.Load(), dead.GetState(), dead.HealthSnapshot(time.Now()))
	}
	leave := time.Since(t0)
	if leave >= 2*time.Second {
		t.Fatalf("path left selection after %v, budget 2s", leave)
	}

	// The surviving traffic runs to completion, then a deterministic burst
	// proves the dead backend takes no NEW connection.
	select {
	case <-workers.done:
	case <-time.After(60 * time.Second):
		t.Fatal("load did not finish in 60s")
	}
	deadHits := fx.wans[1].Hits()
	for seq := int64(1000); seq < 1012; seq++ {
		r := send(0, seq)
		fx.col.record(r)
	}
	if got := fx.wans[1].Hits() - deadHits; got != 0 {
		t.Errorf("dead backend accepted %d new connection(s) after leaving selection, want 0", got)
	}
	ok, clean, cross, tags, samples := fx.col.endIter()
	if cross > 0 {
		t.Fatalf("cross-talk during kill chaos: %d exchanges, samples: %s", cross, strings.Join(samples, "; "))
	}
	if len(tags) < 2 {
		t.Errorf("post-kill traffic landed on tags %v, want >= 2 live WANs", tags)
	}
	if ok < 60 {
		t.Errorf("only %d/%d exchanges verified OK — traffic did not continue on the survivors", ok, ok+clean)
	}
	t.Logf("T-CHAOS-01: dead path left selection after %v (budget 2s); ok=%d clean=%d tags=%v",
		leave, ok, clean, tagKeys(tags))
}

// ---------------------------------------------------------------------------
// T-CHAOS-02: black-hole one backend
// ---------------------------------------------------------------------------

// TestChaos02_BlackholedBackendEjectedWithin20s flips one backend to
// AcceptClose — the handshake completes and then the connection dies with
// zero bytes down, the classic black-hole shape (probe-verified: the relay
// classifies it HardFail/no_down immediately). BUDGET — the SPEC-H.3
// window must eject the path out of selection within 20s of wall traffic.
// The front-end's fail threshold is raised to WindowSize so the
// consecutive-failure counter cannot quietly remove the path first: this
// test pins the WINDOW's ejection, exactly as the review's fail-ratio
// window budget describes.
func TestChaos02_BlackholedBackendEjectedWithin20s(t *testing.T) {
	fx := newChaosFixture(t, 3, 6*time.Second, health.WindowSize)
	victim := fx.pool.Slots[1].Current.Load()

	// Black-hole the backend BEFORE traffic so its window fills with hard
	// failures only.
	if err := fx.wans[1].SetMode(testutil.AcceptClose()); err != nil {
		t.Fatal(err)
	}

	clients := make([]*testutil.FakeClient, 6)
	for i := range clients {
		clients[i] = &testutil.FakeClient{ID: fmt.Sprintf("c02-w%d", i), Socks5: true, Timeout: 2 * time.Second}
	}
	send := func(w int, seq int64) testutil.Result {
		clients[w].Target = integrityTargets[(seq+int64(w))%int64(len(integrityTargets))]
		return clients[w].Send(fx.front, seq)
	}

	t0 := time.Now()
	if !driveBurstsUntil(20*time.Second, victim.Ejected, func() *loadWorkers {
		return startLoadWorkers(6, 180, 6*time.Millisecond, send, fx.col.record)
	}) {
		t.Fatalf("black-holed path never ejected within 20s (state=%s, snapshot=%+v, hits=%d)",
			victim.GetState(), victim.HealthSnapshot(time.Now()), fx.wans[1].Hits())
	}
	ejectFor := time.Since(t0)
	if routableReports(fx.pool, victim, fx.threshold) {
		t.Errorf("path ejected after %v but still in the routable selection", ejectFor)
	}

	victimHits := fx.wans[1].Hits()
	for seq := int64(1000); seq < 1012; seq++ {
		fx.col.record(send(0, seq))
	}
	if got := fx.wans[1].Hits() - victimHits; got != 0 {
		t.Errorf("ejected backend accepted %d fresh connection(s), want 0", got)
	}
	ok, clean, cross, tags, samples := fx.col.endIter()
	if cross > 0 {
		t.Fatalf("cross-talk during black-hole chaos: samples: %s", strings.Join(samples, "; "))
	}
	if len(tags) < 2 {
		t.Errorf("survivor traffic landed on tags %v, want >= 2 live WANs", tags)
	}
	_ = clean
	t.Logf("T-CHAOS-02: path ejected after %v (budget 20s, snapshot %+v); ok=%d tags=%v",
		ejectFor, victim.HealthSnapshot(time.Now()), ok, tagKeys(tags))
}

// ---------------------------------------------------------------------------
// T-CHAOS-03: slow-TTFB path gets less traffic
// ---------------------------------------------------------------------------

// TestChaos03_SlowTTFBPathGetsLessTraffic gives one backend a 400ms
// time-to-first-byte and drives two bounded bursts of concurrent traffic.
// BUDGET — in the second burst the slow path must carry measurably less
// traffic than EITHER fast path, and less than it did in the first burst
// (the peak-EWMA weight drop, observable within the burst window).
func TestChaos03_SlowTTFBPathGetsLessTraffic(t *testing.T) {
	fx := newChaosFixture(t, 3, 30*time.Second, DefaultFailThreshold)
	sched.SeedRand(3)
	if err := fx.wans[2].SetMode(testutil.SlowTTFB(400 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	clients := make([]*testutil.FakeClient, 6)
	for i := range clients {
		clients[i] = &testutil.FakeClient{ID: fmt.Sprintf("c03-w%d", i), Socks5: true, Timeout: 5 * time.Second}
	}
	send := func(w int, seq int64) testutil.Result {
		clients[w].Target = integrityTargets[(seq+int64(w))%int64(len(integrityTargets))]
		return clients[w].Send(fx.front, seq)
	}

	burst := func(total int) (int, map[string]int) {
		cnt := newTagCounter()
		w := startLoadWorkers(6, total, 5*time.Millisecond, send, func(r testutil.Result) {
			fx.col.record(r)
			cnt.record(r)
		})
		select {
		case <-w.done:
		case <-time.After(60 * time.Second):
			t.Fatal("burst did not finish in 60s")
		}
		return cnt.snapshot()
	}

	start := time.Now()
	_, first := burst(60) // warm-up: the slow path takes its TTFB sample(s)
	_, second := burst(60)
	elapsed := time.Since(start)

	slow := "ch-w2"
	if first[slow] < 1 {
		t.Fatalf("slow path received no traffic at all in the warm-up burst (%v) — the drop claim would be vacuous", first)
	}
	for _, fast := range []string{"ch-w0", "ch-w1"} {
		if second[slow] >= second[fast] {
			t.Errorf("second burst: slow path %s got %d, fast path %s got %d — slow path must get less",
				slow, second[slow], fast, second[fast])
		}
	}
	if second[slow] >= first[slow] {
		t.Errorf("slow path share did not drop: first burst %d, second burst %d", first[slow], second[slow])
	}
	ok, clean, cross, _, samples := fx.col.endIter()
	if cross > 0 {
		t.Fatalf("cross-talk during slow-TTFB traffic: samples: %s", strings.Join(samples, "; "))
	}
	if ok < 90 {
		t.Errorf("only %d OK / %d clean exchanges — fixture unhealthy for a traffic-share claim", ok, clean)
	}
	t.Logf("T-CHAOS-03: slow-path share first=%d second=%d of 60 (fast second: w0=%d w1=%d); elapsed %v (budget 60s)",
		first[slow], second[slow], second["ch-w0"], second["ch-w1"], elapsed)
}

// ---------------------------------------------------------------------------
// T-CHAOS-04: flaky path is deprioritized
// ---------------------------------------------------------------------------

// TestChaos04_FlakyPathDeprioritized flips one backend to Flaky(0.9)
// (seeded draws; ~90% of its connections die black-hole style). BUDGET —
// the path must be health-ejected within 15s of wall traffic, its hit
// share must collapse to less than half of either healthy path, and it
// must take no fresh connection after ejection.
func TestChaos04_FlakyPathDeprioritized(t *testing.T) {
	fx := newChaosFixture(t, 3, 6*time.Second, health.WindowSize)
	victim := fx.pool.Slots[1].Current.Load()
	if err := fx.wans[1].SetMode(testutil.Flaky(0.9, 42)); err != nil {
		t.Fatal(err)
	}

	clients := make([]*testutil.FakeClient, 6)
	for i := range clients {
		clients[i] = &testutil.FakeClient{ID: fmt.Sprintf("c04-w%d", i), Socks5: true, Timeout: 2 * time.Second}
	}
	send := func(w int, seq int64) testutil.Result {
		clients[w].Target = integrityTargets[(seq+int64(w))%int64(len(integrityTargets))]
		return clients[w].Send(fx.front, seq)
	}
	hitsAtStart := fx.wans[1].Hits()

	// Continuous bursts until the window ejects: an ended burst would
	// starve the pool of the outcomes the ejection waits for (a victim
	// whose weight already diverged gets few picks per burst).
	t0 := time.Now()
	if !driveBurstsUntil(15*time.Second, victim.Ejected, func() *loadWorkers {
		return startLoadWorkers(6, 180, 6*time.Millisecond, send, fx.col.record)
	}) {
		t.Fatalf("flaky path never ejected within 15s (state=%s snapshot=%+v hits=%d)",
			victim.GetState(), victim.HealthSnapshot(time.Now()), fx.wans[1].Hits())
	}
	ejectFor := time.Since(t0)
	hitsAtEject := fx.wans[1].Hits()

	// A dedicated post-ejection burst: the ejected path must take NO fresh
	// connection (driveBurstsUntil always drains its last burst first).
	post := startLoadWorkers(6, 60, 6*time.Millisecond, send, fx.col.record)
	<-post.done
	hitsEnd := fx.wans[1].Hits()
	fresh := hitsEnd - hitsAtEject
	total := hitsEnd - hitsAtStart

	ok, clean, cross, tags, samples := fx.col.endIter()
	attempts := ok + clean
	// Deprioritization: the flaky path's share of ALL attempts stays below
	// a third — i.e. less than half of what either healthy path carries.
	if attempts > 0 && total*3 >= int64(attempts) {
		t.Errorf("flaky path took %d of %d connections — not deprioritized", total, attempts)
	}
	if fresh > 6 {
		t.Errorf("flaky path took %d fresh connections after ejection, want <= 6 (in-flight slack only)", fresh)
	}
	if cross > 0 {
		t.Fatalf("cross-talk during flaky chaos: samples: %s", strings.Join(samples, "; "))
	}
	if len(tags) < 2 {
		t.Errorf("survivor traffic on tags %v, want >= 2 live WANs", tags)
	}
	t.Logf("T-CHAOS-04: flaky path ejected after %v (budget 15s), hits=%d/%d (fresh after eject=%d); ok=%d tags=%v",
		ejectFor, total, attempts, fresh, ok, tagKeys(tags))
}

// ---------------------------------------------------------------------------
// T-CHAOS-05: all canary endpoints failing
// ---------------------------------------------------------------------------

// TestChaos05_AllCanaryEndpointsFailingDrainsNothing wires healthy paths
// backed by REAL SOCKS relays (so a canary COULD succeed) and points
// HEALTH_ENDPOINTS at refused loopback ports. BUDGET — every path fails
// every endpoint in the same interval: the >=75% environmental breaker
// must set env_degraded and record NOTHING — no consecutive failures, no
// drains, no ejections — while the pool keeps selection state and keeps
// serving traffic, across repeated intervals.
func TestChaos05_AllCanaryEndpointsFailingDrainsNothing(t *testing.T) {
	setIntegrityEnv(t)
	dead := freePort(t) // reserved by convention: nothing listens there
	t.Setenv("HEALTH_ENDPOINTS", fmt.Sprintf(
		"http://127.0.0.1:%d/a http://127.0.0.1:%d/b http://127.0.0.1:%d/c", dead, dead, dead))
	t.Cleanup(func() { health.SetEnvDegraded(false) })

	pool := NewWANPool(3, 0)
	for i := 0; i < 3; i++ {
		ln, addr := startTestSocksServer(t)
		t.Cleanup(func() { ln.Close() })
		_, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatal(err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatal(err)
		}
		swpMarkActive(t, pool, i, &proxycfg.ProxyConfig{
			Protocol: "ss", Server: fmt.Sprintf("c05-%d.example", i), Port: 443, Raw: fmt.Sprintf("ss://c05-%d", i),
		}, port)
	}
	cfg := &proxycfg.Config{TestTimeout: 1, WanFailThreshold: 2, KeepaliveInterval: 10}
	endpoints := health.EndpointsFromEnv()
	if len(endpoints) != 3 {
		t.Fatalf("HEALTH_ENDPOINTS parsed to %d endpoints, want 3", len(endpoints))
	}

	paths := make([]*path.Path, 3)
	for i := range paths {
		paths[i] = pool.Slots[i].Current.Load()
	}
	ejectionsBefore := metricPathEjections.Value(fmt.Sprintf("%d", paths[0].ID), "fail_ratio") +
		metricPathEjections.Value(fmt.Sprintf("%d", paths[0].ID), "consecutive_distinct")

	// Two intervals: the breaker must hold and record nothing every time.
	for interval := 1; interval <= 2; interval++ {
		runCanaries(cfg, pool, endpoints)
		if !health.EnvDegraded() {
			t.Fatalf("interval %d: env_degraded not set although 3/3 paths failed every endpoint", interval)
		}
		for i, p := range paths {
			if got := pool.GetState(i); got != StateActive {
				t.Errorf("interval %d: slot %d state = %s, want active (environmental failure drains nothing)", interval, i, got)
			}
			if got := pool.SlotConsecutiveFails(i); got != 0 {
				t.Errorf("interval %d: slot %d consecutive fails = %d, want 0 (nothing recorded)", interval, i, got)
			}
			if p.Ejected() {
				t.Errorf("interval %d: path %d ejected during an environmental failure", interval, i)
			}
			if got := p.GetState(); got != path.Active {
				t.Errorf("interval %d: path %d state = %s, want active", interval, i, got)
			}
			if !routableReports(pool, p, DefaultFailThreshold) {
				t.Errorf("interval %d: path %d left the routable selection during an environmental failure", interval, i)
			}
		}
	}
	if got := metricPathEjections.Value(fmt.Sprintf("%d", paths[0].ID), "fail_ratio") +
		metricPathEjections.Value(fmt.Sprintf("%d", paths[0].ID), "consecutive_distinct"); got != ejectionsBefore {
		t.Errorf("ejection counter moved by %.0f during an environmental failure, want 0", got-ejectionsBefore)
	}

	// The pool still serves: a local echo target through the real front.
	echoAddr := startEchoServer(t)
	setenvLocalTargets(t)
	front := startIntegritySocks(t, pool, 30*time.Second)
	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i * 13)
	}
	for i := 0; i < 12; i++ {
		if err := rawEchoRoundTrip(front, echoAddr, payload); err != nil {
			t.Fatalf("traffic through the (healthy) pool failed during env_degraded: %v", err)
		}
	}
	t.Logf("T-CHAOS-05: env_degraded held across 2 intervals; 0 drains, 0 ejections, 0 recorded failures; 12/12 conns served")
}

// setenvLocalTargets opts the SSRF guard in for loopback test targets
// (T-CHAOS-05's echo server) without touching the public-target default
// the other chaos tests rely on.
func setenvLocalTargets(t *testing.T) {
	t.Helper()
	t.Setenv("ALLOW_PRIVATE_TARGETS", "true")
	t.Setenv("NO_AFFINITY_DOMAINS", "")
}

// rawEchoRoundTrip sends payload through the SOCKS front-end to a raw
// echo target and requires the exact bytes back (no [ID] framing: the
// target is a plain TCP echo server, not a FakeWAN).
func rawEchoRoundTrip(addr, target string, payload []byte) error {
	conn, err := integritySocks5Connect(addr, target, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("echo mismatch: got %d bytes differing from the %d sent", len(got), len(payload))
	}
	return nil
}

// ---------------------------------------------------------------------------
// T-CHAOS-06: garbage/empty subscription -> zero churn
// ---------------------------------------------------------------------------

// TestChaos06_GarbageSubscriptionZeroPoolChurn feeds runCycle a garbage
// and then an empty subscription body against a fully active pool. BUDGET
// — zero churn: no slot state change, no Path generation swap, no service
// port move, no candidate-pool pollution and viberoxy_swap_total flat on
// every label.
func TestChaos06_GarbageSubscriptionZeroPoolChurn(t *testing.T) {
	// runCycle writes sorted.txt into the working directory on success;
	// keep the repo clean (same pattern as the swap tests).
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	pool := NewWANPool(2, 10700)
	for i := 0; i < 2; i++ {
		if err := pool.StartTesting(i, &proxycfg.ProxyConfig{Protocol: "ss", Server: fmt.Sprintf("c06-%d.example", i), Port: 443, Raw: fmt.Sprintf("ss://c06-%d", i)}); err != nil {
			t.Fatal(err)
		}
		// A live handle: runCycle's HealthCheckAll must see a healthy
		// occupant, so any churn observed below comes from the fetch.
		cmd := exec.Command("sleep", "9999")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start liveness stand-in: %v", err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		if err := pool.SetActive(i, xrayproc.Wrap(cmd, ""), ""); err != nil {
			t.Fatal(err)
		}
	}
	pool.Slots[0].ExitIP = "203.0.113.1"
	pool.Slots[0].SpeedMbps = 42

	type slotSnap struct {
		state WANState
		p     *path.Path
		port  int
	}

	swapResults := []string{"swapped", "cooldown", "dwell", "hysteresis", "no_candidate"}
	swapDelta := func() float64 {
		var d float64
		for _, r := range swapResults {
			d += metricSwapTotal.Value(r)
		}
		return d
	}

	before := make([]slotSnap, len(pool.Slots))
	for i, s := range pool.Slots {
		before[i] = slotSnap{state: pool.GetState(i), p: s.Current.Load(), port: s.ServicePort}
	}
	swapsBefore := swapDelta()
	candidatePool := cands.NewPool(10)
	cfg := &proxycfg.Config{SubscriberURL: "", FetchInterval: 300, TestTimeout: 1, WanCount: 2}

	cases := []struct {
		name string
		body string
	}{
		{"garbage", "!!! not a subscription !!!\n(random noise, zero parseable share links)\n"},
		{"empty", ""},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(tc.body))
		}))
		cfg.SubscriberURL = srv.URL
		runCycle(cfg, pool, candidatePool, time.Minute)
		srv.Close()

		for i, s := range pool.Slots {
			gotState := pool.GetState(i)
			gotPath := s.Current.Load()
			gotPort := s.ServicePort
			if gotState != before[i].state {
				t.Errorf("%s fetch: slot %d state %s -> %s (churn)", tc.name, i, before[i].state, gotState)
			}
			if gotPath != before[i].p {
				t.Errorf("%s fetch: slot %d Path generation swapped %v -> %v (churn)", tc.name, i, before[i].p, gotPath)
			}
			if gotPort != before[i].port {
				t.Errorf("%s fetch: slot %d service port %d -> %d (churn)", tc.name, i, before[i].port, gotPort)
			}
		}
		if got := len(candidatePool.List()); got != 0 {
			t.Errorf("%s fetch: candidate pool holds %d entries, want 0 (no configs were parsed)", tc.name, got)
		}
	}
	if d := swapDelta() - swapsBefore; d != 0 {
		t.Errorf("viberoxy_swap_total moved by %.0f across garbage/empty fetches, want 0", d)
	}
	t.Logf("T-CHAOS-06: garbage + empty fetches: 0 swaps, 0 state changes, 0 Path swaps, 0 candidate entries")
}

// ---------------------------------------------------------------------------
// T-CHAOS-07: flapping backend backoff
// ---------------------------------------------------------------------------

// TestChaos07_FlappingBackendBackoffLimitsReadmits drives a flapping path
// through the health window with an INJECTED clock (no realtime waits):
// eject -> half-open re-admit -> immediate re-flap -> re-eject with the
// doubled backoff. BUDGET — at most ONE re-admission per 60s window, and
// while backoff runs the path stays out of selection.
func TestChaos07_FlappingBackendBackoffLimitsReadmits(t *testing.T) {
	pool := NewWANPool(2, 10700)
	swpMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Protocol: "ss", Server: "c07-flap.example", Port: 443, Raw: "ss://c07-flap"}, 20700)
	swpMarkActive(t, pool, 1, &proxycfg.ProxyConfig{Protocol: "ss", Server: "c07-steady.example", Port: 443, Raw: "ss://c07-steady"}, 20701)
	flap := pool.Slots[0].Current.Load()
	steady := pool.Slots[1].Current.Load()
	_ = steady

	// The ejection loop must never hand the flap path out while it is out.
	assertSelectedAway := func(when string) {
		t.Helper()
		for i := 0; i < 50; i++ {
			picked, release := pool.Select(SelectOptions{Threshold: DefaultFailThreshold})
			if picked == nil {
				t.Fatalf("%s: selection returned no path at all (steady path must keep serving)", when)
			}
			if picked == flap {
				release()
				t.Fatalf("%s: ejected/flapping path was selected", when)
			}
			release()
		}
	}

	t0 := time.Now()
	// Eject on SPEC-H.3 rule A evidence, at t0.
	for k := 1; k <= 3; k++ {
		flap.RecordOutcome(t0, fmt.Sprintf("f%d.flap.example:80", k), health.HardFail)
	}
	if !flap.Ejected() {
		t.Fatal("flap path not ejected by 3 consecutive distinct hard failures")
	}
	if got := flap.HealthSnapshot(t0).BackoffLeft; got != health.BaseBackoff {
		t.Errorf("first backoff = %s, want %s", got, health.BaseBackoff)
	}
	assertSelectedAway("first ejection")

	var readmits []time.Time
	tryReadmit := func(at time.Time, want bool, label string) {
		t.Helper()
		got := flap.NoteCanary(at, true)
		if got != want {
			t.Errorf("%s: NoteCanary re-admitted=%v, want %v", label, got, want)
		}
		if got {
			readmits = append(readmits, at)
			// Production mirrors a canary success with RecordSuccess
			// (runCanaries), which clears the consecutive-failure counter
			// the strict routable rule keys on.
			flap.RecordSuccess()
		}
	}
	// While the 30s backoff runs: successes do not count.
	tryReadmit(t0.Add(10*time.Second), false, "t0+10s (in backoff)")
	tryReadmit(t0.Add(20*time.Second), false, "t0+20s (in backoff)")
	if !flap.Ejected() {
		t.Error("path re-admitted while its backoff still ran")
	}
	// Half-open: 2 consecutive successes past the backoff re-admit it.
	tryReadmit(t0.Add(31*time.Second), false, "t0+31s (1st half-open success)")
	tryReadmit(t0.Add(32*time.Second), true, "t0+32s (2nd half-open success)")
	if flap.Ejected() {
		t.Error("path still ejected after the half-open streak")
	}
	if !routableReports(pool, flap, DefaultFailThreshold) {
		t.Error("re-admitted path is not routable again")
	}

	// Flap immediately: re-eject with the DOUBLED backoff (ejectCount 1).
	tFlap := t0.Add(33 * time.Second)
	for k := 1; k <= 3; k++ {
		flap.RecordOutcome(tFlap, fmt.Sprintf("g%d.flap.example:80", k), health.HardFail)
	}
	if !flap.Ejected() {
		t.Fatal("second flap did not eject")
	}
	if got := flap.HealthSnapshot(tFlap).BackoffLeft; got < time.Minute {
		t.Errorf("second backoff = %s, want >= 60s (30s * 2^1)", got)
	}
	assertSelectedAway("second ejection")

	// Successes through the whole first minute do not re-admit a path
	// whose doubled backoff still runs.
	tryReadmit(t0.Add(45*time.Second), false, "t0+45s")
	tryReadmit(t0.Add(55*time.Second), false, "t0+55s")
	tryReadmit(t0.Add(59*time.Second), false, "t0+59s")
	tryReadmit(t0.Add(61*time.Second), false, "t0+61s (second backoff runs to t0+93s)")

	// Count re-admissions inside ANY 60s window: [t0, t0+60s] holds
	// exactly the one at t0+32s.
	inFirstMinute := 0
	for _, r := range readmits {
		if !r.After(t0.Add(60 * time.Second)) {
			inFirstMinute++
		}
	}
	if inFirstMinute != 1 {
		t.Errorf("re-admissions within the first 60s = %d, want <= 1 (got %v)", inFirstMinute, readmits)
	}

	// The doubled backoff ends at t0+93s: two successes re-admit there,
	// and the gap from the previous re-admit must still be >= 60s.
	tryReadmit(t0.Add(94*time.Second), false, "t0+94s (1st success of the 2nd streak)")
	tryReadmit(t0.Add(95*time.Second), true, "t0+95s (2nd success)")
	if len(readmits) != 2 {
		t.Fatalf("total re-admissions = %d, want 2 (%v)", len(readmits), readmits)
	}
	if gap := readmits[1].Sub(readmits[0]); gap < time.Minute {
		t.Errorf("gap between re-admissions = %s, want >= 60s", gap)
	}
	t.Logf("T-CHAOS-07: re-admits at %v and %v (gap %v >= 60s budget; backoffs %s then %s)",
		readmits[0].Format(time.RFC3339), readmits[1].Format(time.RFC3339),
		readmits[1].Sub(readmits[0]), health.BaseBackoff, health.Backoff(1))
}

// ---------------------------------------------------------------------------
// T-CHAOS-08: swap under load (extends swap_test's T-SWAP-01)
// ---------------------------------------------------------------------------

// TestChaos08_SwapUnderLoadWithFlappingSiblingZeroFailures EXTENDS
// TestSwap_TSWAP01_FullSwapUnderLoadDropsZeroConnections (swap_test.go)
// rather than duplicating it: same zero-dropped-connections guarantee for
// a full make-before-break swap under synthetic load, plus a sibling
// backend that dies inside the candidate-validation window and revives
// after the cutover — so dial-stage retries run across the swap. BUDGET —
// zero failed connections out of >= 100 attempts, >= 1 retried dial, the
// old WAN serving throughout the validation window and the replacement
// taking traffic after it.
func TestChaos08_SwapUnderLoadWithFlappingSiblingZeroFailures(t *testing.T) {
	setIntegrityEnv(t)
	oldWAN, err := testutil.NewFakeWAN("c08-old", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oldWAN.Close() })
	sibWAN, err := testutil.NewFakeWAN("c08-sib", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sibWAN.Close() })
	newWAN, err := testutil.NewFakeWAN("c08-new", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newWAN.Close() })

	pool := NewWANPool(2, 0)
	swpMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Protocol: "ss", Server: "c08-old.example", Port: 443, Raw: "ss://c08-old"}, oldWAN.Port())
	swpMarkActive(t, pool, 1, &proxycfg.ProxyConfig{Protocol: "ss", Server: "c08-sib.example", Port: 443, Raw: "ss://c08-sib"}, sibWAN.Port())
	front := startIntegritySocks(t, pool, 30*time.Second)

	spare, err := ports.New(newWAN.Port(), 1)
	if err != nil {
		t.Fatal(err)
	}
	candidates := cands.NewPool(4)
	candidates.Update([]*cands.Entry{{
		Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "c08-new.example", Port: 8443, Raw: "ss://c08-new"},
		Speed:  7,
	}})

	col := newIntegrityCollector()
	col.beginIter(0)
	// Provenance: every WAN tag this test can legitimately see.
	col.addTag("c08-old")
	col.addTag("c08-sib")
	col.addTag("c08-new")
	clients := make([]*testutil.FakeClient, 6)
	for i := range clients {
		clients[i] = &testutil.FakeClient{ID: fmt.Sprintf("c08-w%d", i), Socks5: true, Timeout: 3 * time.Second}
	}
	send := func(w int, seq int64) testutil.Result {
		clients[w].Target = integrityTargets[(seq+int64(w))%int64(len(integrityTargets))]
		return clients[w].Send(front, seq)
	}
	workers := startLoadWorkers(6, 180, 25*time.Millisecond, send, col.record)
	t.Cleanup(workers.stopAll)

	retriesBefore := metricRetryTotal.Value("dial", "retry")
	var stateDuringTest WANState = -1
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		SparePorts: spare,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			// The sibling dies INSIDE the validation window: from here on
			// new dials that land on it must fail over via the retry
			// policy while the old WAN keeps serving.
			_ = sibWAN.Die()
			time.Sleep(400 * time.Millisecond)
			// Quiesce for the cutover (the rule swap_test.go documents):
			// pause new sends, drain the target slot's reservations so
			// every service-port read is ordered before the swap write.
			closeChan(workers.pause)
			for i := 0; i < 6; i++ {
				select {
				case <-workers.paused:
				case <-time.After(8 * time.Second):
					t.Errorf("worker %d never paused for the cutover", i)
					return &TestResult{Config: cfg, Error: errChaosQuiesce}
				}
			}
			target := pool.Slots[0].Current.Load()
			if !waitUntil(10*time.Second, func() bool { return target.Inflight.Load() == 0 }) {
				t.Errorf("slot 0 reservations never drained before the cutover (inflight=%d)", target.Inflight.Load())
				return &TestResult{Config: cfg, Error: errChaosQuiesce}
			}
			stateDuringTest = pool.GetState(0)
			return &TestResult{Config: cfg, Speed: 7}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, port int, _ ...bool) (*xrayproc.Handle, string, error) {
			return swpDummyHandle(), "", nil
		},
	}

	if !waitUntil(5*time.Second, func() bool { return workers.completed.Load() >= 20 }) {
		t.Fatal("load too light: < 20 connections before the swap")
	}
	if _, err := pool.DropAndReplace(0, opts); err != nil {
		closeChan(workers.resume)
		t.Fatalf("DropAndReplace: %v", err)
	}
	closeChan(workers.resume)
	_ = sibWAN.Revive()

	select {
	case <-workers.done:
	case <-time.After(60 * time.Second):
		t.Fatal("load did not finish in 60s")
	}

	ok, clean, cross, tags, samples := col.endIter()
	if cross > 0 {
		t.Fatalf("cross-talk during the swap window: samples: %s", strings.Join(samples, "; "))
	}
	attempts := ok + clean
	if attempts < 100 {
		t.Errorf("load too light: %d attempts, want >= 100 to prove zero failures", attempts)
	}
	if clean != 0 {
		t.Errorf("%d/%d connections FAILED across the swap; want 0", clean, attempts)
	}
	if got := metricRetryTotal.Value("dial", "retry") - retriesBefore; got < 1 {
		t.Errorf("sibling death never triggered a dial-stage retry (%.0f) — the flap did not exercise failover", got)
	}
	if stateDuringTest != StateActive {
		t.Errorf("slot 0 was %s while the candidate was tested; the old WAN must keep serving", stateDuringTest)
	}
	if got := pool.Slots[0].ServicePort; got != newWAN.Port() {
		t.Errorf("slot 0 service port = %d, want the replacement %d", got, newWAN.Port())
	}
	if newWAN.Hits() == 0 {
		t.Error("the replacement never received traffic after the cutover")
	}
	if len(tags) < 2 {
		t.Errorf("traffic landed on tags %v, want >= 2 (both old and new WANs served)", tagKeys(tags))
	}
	t.Logf("T-CHAOS-08: %d attempts, 0 failed, %.0f retries across swap+sibling-death; tags=%v",
		attempts, metricRetryTotal.Value("dial", "retry")-retriesBefore, tagKeys(tags))
}

// errChaosQuiesce aborts a DropAndReplace test window when the quiesce
// cannot be established (reported through the normal test error path).
var errChaosQuiesce = fmt.Errorf("chaos quiesce failed")

// ---------------------------------------------------------------------------
// T-CHAOS-09: swap during a T-INT-02-style transfer
// ---------------------------------------------------------------------------

// TestChaos09_SwapDuringLargeTransferSiblingChaosHashMatched streams a
// 128 MiB SHA-256-verified transfer through slot 0, flaps a sibling
// backend underneath (kill, AcceptClose, revive — background conns retry),
// and swaps slot 0 MID-STREAM. BUDGET — the transfer completes with a
// matching hash on the pre-swap WAN's tag, the retired generation still
// holds its reservation at cutover, zero session resets, and every
// background exchange stays cross-talk clean.
func TestChaos09_SwapDuringLargeTransferSiblingChaosHashMatched(t *testing.T) {
	setIntegrityEnv(t)
	size := int64(128 << 20)
	expected := transferExpectedHash(size)

	pool := NewWANPool(2, 0) // slot 1 starts EMPTY: the transfer is pinned to slot 0
	oldWAN, err := testutil.NewFakeWAN("ch9-old", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oldWAN.Close() })
	oldPath := swpMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Protocol: "ss", Server: "ch9-old.example", Port: 443, Raw: "ss://ch9-old"}, oldWAN.Port())
	front := startIntegritySocks(t, pool, 60*time.Second)

	col := newIntegrityCollector()
	col.beginIter(0)
	col.addTag(oldWAN.ID)

	resCh := make(chan transferResult, 1)
	go func() {
		resCh <- streamTransfer(front, "1.2.3.4:80", size, 3*time.Minute)
	}()

	// Wait for a solid slice of the stream to flow (this is also the
	// -race ordering cover for slot 0's service-port read).
	swapAt := metricProxyBytes.Value("0", "up") + float64(size)/8
	if !waitUntil(60*time.Second, func() bool { return metricProxyBytes.Value("0", "up") >= swapAt }) {
		t.Fatal("transfer never started flowing")
	}

	// Activate the sibling and flap it while the transfer streams.
	sibWAN, err := testutil.NewFakeWAN("ch9-sib", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sibWAN.Close() })
	col.addTag(sibWAN.ID)
	swpMarkActive(t, pool, 1, &proxycfg.ProxyConfig{Protocol: "ss", Server: "ch9-sib.example", Port: 443, Raw: "ss://ch9-sib"}, sibWAN.Port())

	clients := make([]*testutil.FakeClient, 3)
	for i := range clients {
		clients[i] = &testutil.FakeClient{ID: fmt.Sprintf("c09-w%d", i), Socks5: true, Timeout: 3 * time.Second}
	}
	send := func(w int, seq int64) testutil.Result {
		clients[w].Target = integrityTargets[(seq+int64(w))%int64(len(integrityTargets))]
		return clients[w].Send(front, seq)
	}
	bg := startLoadWorkers(3, 60, 30*time.Millisecond, send, col.record)
	t.Cleanup(bg.stopAll)

	var died, faulted, revived bool
	for done := false; !done; {
		c := bg.completed.Load()
		if c >= 5 && !died {
			_ = sibWAN.Die()
			died = true
		}
		if c >= 20 && !faulted {
			_ = sibWAN.SetMode(testutil.AcceptClose())
			faulted = true
		}
		if c >= 30 && !revived {
			_ = sibWAN.SetMode(testutil.Good(0, 0))
			_ = sibWAN.Revive()
			revived = true
		}
		select {
		case <-bg.done:
			done = true
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !died || !faulted || !revived {
		t.Fatalf("sibling chaos incomplete: died=%v faulted=%v revived=%v", died, faulted, revived)
	}

	// Background load is done; its handlers get a grace period, then the
	// metric covers order every service-port read before the swap write.
	time.Sleep(300 * time.Millisecond)
	coverServicePortReads(0)

	if oldPath.Inflight.Load() < 1 {
		t.Fatal("the transfer finished before the swap: cannot prove a mid-stream cutover")
	}
	newWAN, err := testutil.NewFakeWAN("ch9-new", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newWAN.Close() })
	col.addTag(newWAN.ID)
	spare, err := ports.New(newWAN.Port(), 1)
	if err != nil {
		t.Fatal(err)
	}
	candidates := cands.NewPool(2)
	candidates.Update([]*cands.Entry{{
		Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "ch9-new.example", Port: 8443, Raw: "ss://ch9-new"},
		Speed:  9,
	}})
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		SparePorts: spare,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			time.Sleep(50 * time.Millisecond)
			return &TestResult{Config: cfg, Speed: 9}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ ...bool) (*xrayproc.Handle, string, error) {
			return swpDummyHandle(), "", nil
		},
	}
	if _, err := pool.DropAndReplace(0, opts); err != nil {
		t.Fatalf("DropAndReplace mid-stream: %v", err)
	}
	metricProxyConnections.Value("0", "socks5")
	if got := oldPath.Inflight.Load(); got < 1 {
		t.Errorf("retired path inflight = %d at cutover, want >= 1 (the transfer must still be held)", got)
	}
	closeChan(bg.resume)

	sessionResetsBefore := metricSessionResetOnDrain.Value()

	var res transferResult
	select {
	case res = <-resCh:
	case <-time.After(3 * time.Minute):
		t.Fatal("transfer never completed")
	}
	<-bg.done

	if res.Err != nil {
		t.Errorf("transfer failed across the swap: %v", res.Err)
	}
	if res.Recv != size || res.Sent != size {
		t.Errorf("transfer bytes: sent=%d recv=%d, want %d", res.Sent, res.Recv, size)
	}
	if res.SendHash != expected || res.RecvHash != expected {
		t.Errorf("transfer SHA-256 mismatch after the mid-stream swap")
	}
	if res.Tag != "ch9-old" {
		t.Errorf("transfer echoed by tag %q, want the pre-swap WAN \"ch9-old\"", res.Tag)
	}
	if got := pool.GetState(0); got != StateActive {
		t.Errorf("slot 0 state = %s after the cutover, want active", got)
	}
	if got := pool.Slots[0].ServicePort; got != newWAN.Port() {
		t.Errorf("slot 0 service port = %d, want the replacement %d", got, newWAN.Port())
	}
	if got := oldPath.GetState(); got != path.Draining {
		t.Errorf("retired path state = %s, want draining", got)
	}
	if got := metricSessionResetOnDrain.Value() - sessionResetsBefore; got != 0 {
		t.Errorf("viberoxy_session_reset_on_drain_total delta = %.0f, want 0 (swap must never reset a session)", got)
	}
	ok, clean, cross, tags, samples := col.endIter()
	if cross > 0 {
		t.Fatalf("cross-talk during sibling chaos: samples: %s", strings.Join(samples, "; "))
	}
	if len(tags) < 2 {
		t.Errorf("background traffic landed on tags %v, want >= 2", tagKeys(tags))
	}
	t.Logf("T-CHAOS-09: %d-byte transfer hash-matched across the mid-stream swap; bg ok=%d clean=%d tags=%v",
		size, ok, clean, tagKeys(tags))
}

// ---------------------------------------------------------------------------
// small shared helpers
// ---------------------------------------------------------------------------

// driveBurstsUntil launches consecutive bursts of load through launch()
// until cond holds or the budget elapses, draining every burst before the
// next decision. An ended burst would starve the pool of the very outcomes
// a health-window condition waits on (a victim whose scheduler weight has
// already diverged earns few picks per burst), so a fresh burst starts
// whenever the previous one exhausts the condition. Returns cond's value.
func driveBurstsUntil(budget time.Duration, cond func() bool, launch func() *loadWorkers) bool {
	deadline := time.Now().Add(budget)
	for !cond() && time.Now().Before(deadline) {
		w := launch()
		select {
		case <-w.done:
			// Trailing relay outcomes land just after the last send.
			time.Sleep(50 * time.Millisecond)
		case <-time.After(time.Until(deadline)):
			w.stopAll()
			<-w.done
			return cond()
		}
	}
	return cond()
}

// tagKeys renders a tag histogram for log lines.
func tagKeys(tags map[string]int) []string {
	out := make([]string, 0, len(tags))
	for k, n := range tags {
		out = append(out, fmt.Sprintf("%s=%d", k, n))
	}
	return out
}
