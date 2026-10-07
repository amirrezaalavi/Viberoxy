package main

// Integrity verification suite (review §7 layer L2): the cross-talk proof
// and the transfer-integrity proof, both wired through the REAL front-end
// stack — proxy/socks front-ends + WANPool selection (internal/sched) +
// dial-stage retry (internal/retry) + relay (internal/relayio) — against
// scriptable testutil FakeWAN backends.
//
//	T-INT-01  cross-talk under seeded chaos: N fake clients × K connections
//	          through both front-ends while the run continuously kills,
//	          revives, mode-flips, drains, health-ejects and performs ONE
//	          make-before-break swap. Every response must be either (a) the
//	          requesting client's own payload echoed by exactly one known
//	          WAN tag, or (b) a clean protocol error (502/503, REP 0x01,
//	          EOF/reset/timeout, connection refused). Another client's
//	          bytes, a crc-valid corruption or a truncated-but-"ok" frame
//	          is fatal. Iterations: 2 × 200 conns in the normal suite (also
//	          under -short), 25 behind `-tags chaos`; CHAOS_ITERATIONS
//	          overrides both (nightly 1000-run).
//	T-INT-02  a 256 MiB streamed transfer (32 MiB under -short), SHA-256
//	          verified end to end, survives a make-before-break swap of its
//	          OWN slot mid-stream with zero resets: the stream keeps
//	          flowing on the retired generation, the replacement receives
//	          no traffic mid-transfer, and no session is ever reset.
//
// DETERMINISM: every iteration seeds the chaos schedule (math/rand), the
// selection RNG (sched.SeedRand) and the flaky-backend draws; the health
// subsystem's clock knobs are injected where the suite drives them directly.
// OS-level scheduling of concurrent sockets cannot be seeded, so the
// assertions are floors and invariants (never byte-exact sequences), and
// every iteration asserts that each scheduled chaos event actually fired.
//
// RACE NOTE (relay.go reads slot.ServicePort without the slot lock, by
// design — see swap_test.go): a swap may only write that port once every
// handler that has read it is ordered before the write. The pause below
// stops new sends, and every post-read action a handler can take (relay
// byte callbacks, dial-failure outcome/retry events, latency recording) is
// published through a metric mutex the swap acquires BEFORE the write; the
// post-write metricProxyConnections acquire orders every FUTURE read after
// it. That replaces swap_test's drain-to-zero with a cover that also works
// while a transfer (the long stream) is deliberately held in flight.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

// integrityTargets are the three distinct public destinations every fake
// client rotates through. Three distinct destination HOSTS is exactly what
// SPEC-H.3's consecutive-distinct ejection rule keys on, so the chaos run
// feeds the health window realistic evidence.
var integrityTargets = []string{"1.2.3.4:80", "5.6.7.8:80", "9.10.11.12:80"}

// setIntegrityEnv pins the front-end env knobs the integrity/chaos suites
// depend on: no per-(client, site) stickiness (the cross-talk proof wants
// traffic mixed across every path), no proxy auth (plain front-ends) and
// the SSRF guard left ON with public fake targets.
func setIntegrityEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NO_AFFINITY_DOMAINS", strings.Join([]string{"1.2.3.4", "5.6.7.8", "9.10.11.12"}, " "))
	t.Setenv("PROXY_USERS", "")
	t.Setenv("ALLOW_PRIVATE_TARGETS", "")
}

// integrityIterations is T-INT-01's seeded iteration budget (see the file
// header). CHAOS_ITERATIONS wins, then -short, then the chaos build tag.
func integrityIterations(t *testing.T) int {
	t.Helper()
	if v := os.Getenv("CHAOS_ITERATIONS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("CHAOS_ITERATIONS=%q: want a positive integer", v)
		}
		return n
	}
	if testing.Short() {
		return 2
	}
	if chaosBuildTag {
		return 25
	}
	return 2
}

// waitUntil polls cond until it holds or timeout elapses (final check
// included), so budget assertions never race the last state change.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return cond()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// closeChan closes ch exactly once; only the test goroutine calls it.
func closeChan(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// ---------------------------------------------------------------------------
// front-end fixtures
// ---------------------------------------------------------------------------

// startIntegritySocks runs the real SOCKS5 front-end on an ephemeral port
// against pool. idle bounds the relay's inactivity timeout so a wedged
// backend cannot pin handlers (and their reservations) for the default
// 300s beyond a test's budget. The fail threshold is the production
// default; chaos tests that pin the health window's own ejection pass a
// custom one through startSocksFront.
func startIntegritySocks(t *testing.T, pool *WANPool, idle time.Duration) string {
	t.Helper()
	return startSocksFront(t, pool, idle, DefaultFailThreshold)
}

// startSocksFront is startIntegritySocks with an explicit WanFailThreshold
// (what the front-end hands to pool.Select on every dial).
func startSocksFront(t *testing.T, pool *WANPool, idle time.Duration, failThreshold int) string {
	t.Helper()
	port := freePort(t)
	srv := NewSocksServer(port, pool)
	srv.AccessLog = false
	srv.IdleTimeout = idle
	srv.WanFailThreshold = failThreshold
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Listen(ctx) }()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForPort(addr, 3*time.Second); err != nil {
		t.Fatalf("integrity socks front did not start: %v", err)
	}
	return addr
}

// startIntegrityProxy runs the real HTTPS CONNECT front-end on an
// ephemeral port against the same pool.
func startIntegrityProxy(t *testing.T, pool *WANPool, idle time.Duration) string {
	t.Helper()
	port := freePort(t)
	proxy := NewProxyServer(port, pool)
	proxy.AccessLog = false
	proxy.IdleTimeout = idle
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = proxy.Start(ctx) }()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForPort(addr, 3*time.Second); err != nil {
		t.Fatalf("integrity proxy front did not start: %v", err)
	}
	return addr
}

// httpFakeClient is the HTTPS-CONNECT twin of testutil.FakeClient: the
// same self-verifying "clientID|seq|random|crc32" payload and the same
// response verification (tag + exact payload + crc), driven through the
// proxy front-end instead of the SOCKS5 one. One instance per worker
// goroutine — never shared.
type httpFakeClient struct {
	ID      string
	Timeout time.Duration
}

// Send performs one CONNECT exchange and verifies the echoed payload with
// testutil's own verifier, so both front-ends are held to identical
// cross-talk rules.
func (c *httpFakeClient) Send(proxyAddr, target string, seq int64) testutil.Result {
	payload, err := testutil.MakePayload(c.ID, seq)
	if err != nil {
		return testutil.Result{Err: err}
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = testutil.DefaultTimeout
	}
	conn, err := net.DialTimeout("tcp", proxyAddr, timeout)
	if err != nil {
		return testutil.Result{Err: err}
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		return testutil.Result{Err: fmt.Errorf("send connect: %w", err)}
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return testutil.Result{Err: fmt.Errorf("read connect status: %w", err)}
	}
	code := 0
	var version string
	fmt.Sscanf(strings.TrimSpace(line), "HTTP/%s %d", &version, &code)
	if code != 200 {
		// 502 (every attempt failed) / 503 (no WAN eligible) and any
		// other non-200 are clean protocol errors for the proxy front.
		return testutil.Result{Err: fmt.Errorf("proxy-connect status %d", code)}
	}
	// Consume the rest of the response headers (through the blank line);
	// anything buffered there is still part of the HTTP handshake, not the
	// tunnelled stream.
	for {
		hl, err := br.ReadString('\n')
		if err != nil {
			return testutil.Result{Err: fmt.Errorf("read connect headers: %w", err)}
		}
		if strings.TrimSpace(hl) == "" {
			break
		}
	}
	if _, err := conn.Write(payload); err != nil {
		return testutil.Result{Err: fmt.Errorf("send payload: %w", err)}
	}
	tag, body, n, err := readTaggedResponseLocal(br, len(payload))
	if err != nil {
		return testutil.Result{Tag: tag, Bytes: n, Err: fmt.Errorf("read response: %w", err)}
	}
	if err := testutil.VerifyPayload(body); err != nil {
		return testutil.Result{Tag: tag, Bytes: n, Err: err}
	}
	if !bytes.Equal(body, payload) {
		return testutil.Result{Tag: tag, Bytes: n, Err: errors.New("testutil: echoed payload differs from the one sent")}
	}
	return testutil.Result{OK: true, Tag: tag, Bytes: n}
}

// readTaggedResponseLocal mirrors testutil's unexported framing reader
// ( "[ID]" tag + exactly want body bytes), used by the HTTP-front client.
func readTaggedResponseLocal(r io.Reader, want int) (string, []byte, int64, error) {
	var tb []byte
	one := make([]byte, 1)
	for {
		if len(tb) > 512 {
			return "", nil, int64(len(tb)), errors.New("testutil: response tag exceeds 512 bytes or is missing ']'")
		}
		if _, err := io.ReadFull(r, one); err != nil {
			return "", nil, int64(len(tb)), err
		}
		tb = append(tb, one[0])
		if one[0] == ']' {
			break
		}
	}
	if tb[0] != '[' {
		return "", nil, int64(len(tb)), errors.New("testutil: response does not start with an [ID] tag")
	}
	body := make([]byte, want)
	n, err := io.ReadFull(r, body)
	return string(tb[1 : len(tb)-1]), body[:n], int64(len(tb)) + int64(n), err
}

// ---------------------------------------------------------------------------
// classification: the cross-talk verdict itself
// ---------------------------------------------------------------------------

// exchange kinds returned by classifyExchange.
const (
	kindOK    = "ok"
	kindCross = "cross-talk" // fatal: foreign/corrupted bytes
)

// crossTalkSignatures are the testutil verification failures that prove a
// response carried bytes that were NOT this client's own payload. Any of
// them is the bug class T-INT-01 exists to catch.
var crossTalkSignatures = []string{
	"echoed payload differs", // another client's payload (or mixed bytes)
	"crc32 mismatch",         // corrupted payload that reached the client
	"no crc32 field",         // payload shape broken
	"bad crc32 field",        // payload shape broken
	"does not start with an", // non-frame bytes where a tag must be
	"tag exceeds 512",        // runaway/garbage tag
}

// classifyExchange turns one exchange into (kind, detail). OK only when the
// client got exactly its own payload framed by a KNOWN WAN tag; everything
// that is not a verification failure must be a clean protocol error
// (connect-502/503, rep-0x01, eof, reset, timeout, refused, no-wan).
func classifyExchange(res testutil.Result, known map[string]bool) (kind, class string) {
	if res.Err == nil {
		if !res.OK {
			return kindCross, "ok-without-verification"
		}
		if !known[res.Tag] {
			// Payload verified but no live WAN produced it: provenance
			// violated (a tag from a fake this run never created).
			return kindCross, "unknown-tag:" + res.Tag
		}
		return kindOK, res.Tag
	}
	s := res.Err.Error()
	for _, sig := range crossTalkSignatures {
		if strings.Contains(s, sig) {
			return kindCross, s
		}
	}
	switch {
	case strings.Contains(s, "proxy-connect status"):
		// "proxy-connect status 502" -> class "502"
		fields := strings.Fields(s)
		return "clean", fields[len(fields)-1]
	case strings.Contains(s, "CONNECT failed, reply code"), errors.Is(res.Err, testutil.ErrHandshake):
		return "clean", "rep-0x01"
	case strings.Contains(s, "connection refused"):
		return "clean", "refused"
	case strings.Contains(s, "i/o timeout"), strings.Contains(s, "deadline exceeded"):
		return "clean", "timeout"
	case strings.Contains(s, "reset by peer"):
		return "clean", "reset"
	case strings.Contains(s, "EOF"):
		return "clean", "eof"
	case strings.Contains(s, "no candidate"), strings.Contains(s, "No WAN"):
		return "clean", "no-wan"
	default:
		return "clean", "other:" + s
	}
}

// integrityCollector accumulates per-iteration exchange verdicts. All
// workers record through its mutex; the known-tag set only grows between
// iterations (never during a run), so workers classify against a stable
// provenance universe.
type integrityCollector struct {
	mu    sync.Mutex
	known map[string]bool

	// current iteration bucket
	curIter  int
	curKinds map[string]int
	curTags  map[string]int
	curCross []string
	// whole-suite totals
	totalKinds map[string]int
	iters      []string
}

func newIntegrityCollector() *integrityCollector {
	return &integrityCollector{
		known:      map[string]bool{},
		curKinds:   map[string]int{},
		curTags:    map[string]int{},
		totalKinds: map[string]int{},
	}
}

func (c *integrityCollector) addTag(tag string) {
	c.mu.Lock()
	c.known[tag] = true
	c.mu.Unlock()
}

func (c *integrityCollector) beginIter(it int) {
	c.mu.Lock()
	c.curIter = it
	c.curKinds = map[string]int{}
	c.curTags = map[string]int{}
	c.curCross = nil
	c.mu.Unlock()
}

// record classifies one exchange and files it in the current bucket.
func (c *integrityCollector) record(res testutil.Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	kind, class := classifyExchange(res, c.known)
	c.curKinds[kind+":"+class]++
	c.totalKinds[kind+":"+class]++
	if kind == kindCross {
		c.curCross = append(c.curCross, fmt.Sprintf("tag=%q err=%v", res.Tag, res.Err))
	}
	if kind == kindOK {
		c.curTags[res.Tag]++
	}
}

// endIter closes the bucket and returns (ok, clean, cross, distinctOKTags,
// crossSamples).
func (c *integrityCollector) endIter() (ok, clean, cross int, tags map[string]int, samples []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, n := range c.curKinds {
		switch {
		case strings.HasPrefix(k, kindOK+":"):
			ok += n
		case strings.HasPrefix(k, kindCross+":"):
			cross += n
		default:
			clean += n
		}
	}
	tags = map[string]int{}
	for k, n := range c.curTags {
		tags[k] = n
	}
	samples = append(samples, c.curCross...)
	return
}

// summary renders the whole-suite class histogram for the final report.
func (c *integrityCollector) summary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := make([]string, 0, len(c.totalKinds))
	for k, n := range c.totalKinds {
		parts = append(parts, fmt.Sprintf("%s=%d", k, n))
	}
	// stable-ish order is not required for the log; sort for readability.
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			if parts[j] < parts[i] {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	return strings.Join(parts, " ")
}

// ---------------------------------------------------------------------------
// shared load generator
// ---------------------------------------------------------------------------

// loadWorkers is the fixed-size fake-client load generator shared by
// T-INT-01 and the chaos suite: n workers, each owning its own client
// identity, sending conns exchanges (paced) through send until finished or
// stopped. The pause/resume pair is the swap quiesce: a worker that sees
// pause finishes its current exchange, ACKS, and issues nothing new until
// resume — so "all workers acked" means zero in-flight client sends.
type loadWorkers struct {
	n         int
	conns     int
	pace      time.Duration
	record    func(testutil.Result)
	pause     chan struct{}
	resume    chan struct{}
	paused    chan struct{}
	stop      chan struct{}
	done      chan struct{}
	wg        sync.WaitGroup
	completed atomic.Int64
}

// startLoadWorkers splits total conns across n workers (the first
// total%nWorkers get one extra). record may be called concurrently.
func startLoadWorkers(n, total int, pace time.Duration, send func(worker int, seq int64) testutil.Result, record func(testutil.Result)) *loadWorkers {
	w := &loadWorkers{
		n:      n,
		conns:  total,
		pace:   pace,
		record: record,
		pause:  make(chan struct{}),
		resume: make(chan struct{}),
		paused: make(chan struct{}, n),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	base := total / n
	extra := total % n
	for id := 0; id < n; id++ {
		count := base
		if id < extra {
			count++
		}
		w.wg.Add(1)
		go func(id, count int) {
			defer w.wg.Done()
			acked := false
			for seq := int64(1); seq <= int64(count); seq++ {
				select {
				case <-w.stop:
					return
				default:
				}
				select {
				case <-w.pause:
					if !acked {
						acked = true
						w.paused <- struct{}{}
					}
					select {
					case <-w.resume:
					case <-w.stop:
						return
					}
				default:
				}
				w.record(send(id, seq))
				w.completed.Add(1)
				if w.pace > 0 {
					time.Sleep(w.pace)
				}
			}
		}(id, count)
	}
	go func() {
		w.wg.Wait()
		close(w.done)
	}()
	return w
}

// stopAll releases every worker (idempotent).
func (w *loadWorkers) stopAll() { closeChan(w.stop) }

// ---------------------------------------------------------------------------
// T-INT-01
// ---------------------------------------------------------------------------

// TestIntegrity_TINT01_CrossTalkUnderChaos is the review's ORIGINAL
// question — "can a response reach the wrong client?" — answered under
// continuous seeded chaos. See the file header for the full contract.
func TestIntegrity_TINT01_CrossTalkUnderChaos(t *testing.T) {
	setIntegrityEnv(t)
	iterations := integrityIterations(t)
	const (
		nWorkers   = 12 // 8 SOCKS5 FakeClients + 4 HTTPS-CONNECT clients
		connsTotal = 200
		// okFloor is the per-iteration availability floor: chaos may fail
		// connections (cleanly), but the run must stay dominated by
		// verified exchanges or the cross-talk proof is vacuous.
		okFloor = connsTotal * 30 / 100
	)

	col := newIntegrityCollector()
	retryBefore := metricRetryTotal.Value("dial", "retry")
	sessionResetsBefore := metricSessionResetOnDrain.Value()

	for it := 0; it < iterations; it++ {
		runTINT01Iteration(t, it, int64(0x71C000)+int64(it), nWorkers, connsTotal, okFloor, col)
	}

	if got := metricRetryTotal.Value("dial", "retry") - retryBefore; got < 1 {
		t.Errorf("T-INT-01 chaos never forced a dial-stage retry (%.0f): Die/Refuse chaos did not exercise failover", got)
	}
	if got := metricSessionResetOnDrain.Value() - sessionResetsBefore; got != 0 {
		t.Errorf("viberoxy_session_reset_on_drain_total = %.0f, want 0 (drains/swaps never reset a live session)", got)
	}
	t.Logf("T-INT-01 summary: iterations=%d conns/iter=%d | %s", iterations, connsTotal, col.summary())
}

// runTINT01Iteration runs one seeded chaos iteration: fixture, long
// transfer, load, chaos schedule, the quiesced make-before-break swap, and
// every per-iteration assertion.
func runTINT01Iteration(t *testing.T, it int, seed int64, nWorkers, connsTotal, okFloor int, col *integrityCollector) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	sched.SeedRand(seed) // deterministic P2C picks for this iteration

	const (
		nWAN    = 4
		paceBps = 32768 // every backend paces echoes so the long stream spans the swap
	)
	pool := NewWANPool(nWAN, 0)
	col.beginIter(it)

	// Fixture WANs + their pool slots. Tag IDs embed the iteration so the
	// known-tag set is globally unique and only grows between iterations.
	wans := make([]*testutil.FakeWAN, nWAN)
	initialPaths := make([]*path.Path, 0, nWAN)
	for i := 0; i < nWAN; i++ {
		w, err := testutil.NewFakeWAN(fmt.Sprintf("i%d-w%d", it, i), testutil.Good(0, paceBps))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close() })
		wans[i] = w
		col.addTag(w.ID)
		swpMarkActive(t, pool, i, &proxycfg.ProxyConfig{
			Protocol: "ss",
			Server:   fmt.Sprintf("i%d-w%d.example", it, i),
			Port:     443,
			Raw:      fmt.Sprintf("ss://i%d-w%d", it, i),
		}, w.Port())
		initialPaths = append(initialPaths, pool.Slots[i].Current.Load())
	}

	// The make-before-break replacement: created up front so its tag is
	// known before any worker classifies (the known set stays immutable
	// while workers run).
	newWAN, err := testutil.NewFakeWAN(fmt.Sprintf("i%d-new", it), testutil.Good(0, paceBps))
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
		Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: fmt.Sprintf("i%d-new.example", it), Port: 8443, Raw: fmt.Sprintf("ss://i%d-new", it)},
		Speed:  42,
	}})
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		SparePorts: spare,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			// The candidate is validated while the load runs (RT-10): the
			// old WAN serves throughout the test window.
			time.Sleep(40 * time.Millisecond)
			return &TestResult{Config: cfg, Speed: 42}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ ...bool) (*xrayproc.Handle, string, error) {
			return swpDummyHandle(), "", nil
		},
	}

	socksAddr := startIntegritySocks(t, pool, 6*time.Second)
	proxyAddr := startIntegrityProxy(t, pool, 6*time.Second)

	// ---- the long transfer: started BEFORE the load, so the slot it
	// lands on is exactly the slot the swap will target. It must still be
	// in flight at the cutover — that is what makes the swap a
	// make-before-break proof (and what would expose a release that hits
	// the slot's CURRENT generation instead of the held one).
	const longSize = 96 << 10
	longExpected := transferExpectedHash(longSize)
	longDone := make(chan transferResult, 1)
	go func() {
		longDone <- streamTransfer(socksAddr, integrityTargets[0], longSize, 45*time.Second)
	}()
	targetIdx := -1
	if !waitUntil(3*time.Second, func() bool {
		for i := 0; i < nWAN; i++ {
			if slotInflight(pool, i) > 0 {
				targetIdx = i
				return true
			}
		}
		return false
	}) {
		t.Fatal("the long transfer never reached a WAN slot")
	}
	targetPath := pool.Slots[targetIdx].Current.Load()

	// ---- load: one client identity per worker
	socksClients := make([]*testutil.FakeClient, 8)
	for i := range socksClients {
		socksClients[i] = &testutil.FakeClient{
			ID:      fmt.Sprintf("i%d-sc%d", it, i),
			Socks5:  true,
			Timeout: 2 * time.Second,
		}
	}
	httpClients := make([]*httpFakeClient, nWorkers-8)
	for i := range httpClients {
		httpClients[i] = &httpFakeClient{ID: fmt.Sprintf("i%d-hc%d", it, i), Timeout: 2 * time.Second}
	}
	send := func(w int, seq int64) testutil.Result {
		target := integrityTargets[(seq+int64(w))%int64(len(integrityTargets))]
		if w < len(socksClients) {
			fc := socksClients[w]
			fc.Target = target // worker-local client: never shared
			return fc.Send(socksAddr, seq)
		}
		return httpClients[w-len(socksClients)].Send(proxyAddr, target, seq)
	}
	workers := startLoadWorkers(nWorkers, connsTotal, 6*time.Millisecond, send, col.record)
	t.Cleanup(workers.stopAll)

	// ---- ramp check: the run must carry real load before chaos starts.
	if !waitUntil(5*time.Second, func() bool { return workers.completed.Load() >= 20 }) {
		t.Fatal("load too light: fewer than 20 connections completed in 5s")
	}

	// ---- chaos schedule: threshold events at fixed completion counts
	// (deterministic minimums) plus seeded random die/fault churn.
	var (
		died, faulted, drained, ejected, repaired, undrained int
		swapped                                              bool
	)
	type repairAt struct {
		due time.Time
		fn  func()
	}
	var repairs []repairAt
	scheduleRepair := func(d time.Duration, fn func()) {
		repairs = append(repairs, repairAt{due: time.Now().Add(d), fn: fn})
	}
	repairWAN := func(w *testutil.FakeWAN) func() {
		return func() { _ = w.SetMode(testutil.Good(0, paceBps)) }
	}
	randomFaultMode := func() testutil.Mode {
		switch rng.Intn(5) {
		case 0:
			return testutil.AcceptClose()
		case 1:
			return testutil.Refuse()
		case 2:
			return testutil.SlowTTFB(150 * time.Millisecond)
		case 3:
			return testutil.Flaky(0.5, rng.Int63())
		default:
			return testutil.RstAfter(8)
		}
	}

	// forceEject feeds SPEC-H.3 rule A (3 consecutive hard failures across
	// 3 distinct destination hosts) to one healthy path through the
	// production RecordOutcome API — a deterministic health ejection.
	forceEject := func() bool {
		var healthy []*path.Path
		for _, idx := range pool.GetSlotsByState(StateActive) {
			p := pool.Slots[idx].Current.Load()
			if p.GetState() == path.Active {
				healthy = append(healthy, p)
			}
		}
		if len(healthy) == 0 {
			return false
		}
		p := healthy[rng.Intn(len(healthy))]
		for k := 1; k <= 3; k++ {
			p.RecordOutcome(time.Now(), fmt.Sprintf("e%d.force.example:80", k), health.HardFail)
		}
		return p.Ejected()
	}

	// swap: pause the load, cover the ServicePort reads, cutover, resume.
	doSwap := func() {
		closeChan(workers.pause)
		for i := 0; i < nWorkers; i++ {
			select {
			case <-workers.paused:
			case <-time.After(8 * time.Second):
				t.Fatalf("worker %d never paused for the swap", i)
			}
		}
		// Let dial-stage handlers finish their dial + first relay step,
		// then acquire every metric a handler touches AFTER reading
		// slot.ServicePort: one acquire of each orders all those reads
		// before the swap's write (see the file header).
		time.Sleep(300 * time.Millisecond)
		coverServicePortReads(targetIdx)

		if slotInflight(pool, targetIdx) == 0 {
			t.Fatal("the long transfer finished before the swap: no session crosses the cutover, T-INT-01 cannot prove make-before-break")
		}
		if _, err := pool.DropAndReplace(targetIdx, opts); err != nil {
			t.Fatalf("DropAndReplace: %v", err)
		}
		// Publish the write's clock to the dial-entry metric BEFORE traffic
		// resumes: every future handler acquires it before reading
		// slot.ServicePort, ordering those reads after our write.
		metricProxyConnections.Value(strconv.Itoa(targetIdx), "socks5")
		metricProxyConnections.Value(strconv.Itoa(targetIdx), "connect")
		closeChan(workers.resume)
	}

	// ---- the chaos loop (this goroutine owns every chaos event)
	deadline := time.Now().Add(60 * time.Second)
	finished := false
	for !finished {
		if time.Now().After(deadline) {
			t.Fatal("chaos loop exceeded 60s")
		}
		c := workers.completed.Load()

		if c >= 10 && died == 0 {
			w := wans[rng.Intn(nWAN)]
			_ = w.Die()
			died++
			scheduleRepair(700*time.Millisecond, repairWAN(w))
		}
		if c >= 25 && faulted == 0 {
			w := wans[rng.Intn(nWAN)]
			_ = w.SetMode(randomFaultMode())
			faulted++
			scheduleRepair(900*time.Millisecond, repairWAN(w))
		}
		if c >= 40 && drained == 0 {
			if act := pool.GetSlotsByState(StateActive); len(act) > 0 {
				if err := pool.MarkDrainingHealth(act[rng.Intn(len(act))]); err == nil {
					drained++
				}
			}
		}
		if c >= 50 && ejected == 0 {
			if forceEject() {
				ejected++
			}
		}
		if c >= 60 && !swapped {
			doSwap()
			swapped = true
		}
		if c >= 75 && repaired == 0 {
			for _, w := range wans {
				_ = w.SetMode(testutil.Good(0, paceBps))
			}
			repaired++
		}
		if c >= 90 && undrained == 0 {
			for i := 0; i < nWAN; i++ {
				pool.UnDrainIfRecovered(i, true)
			}
			undrained++
		}
		// due repairs
		if len(repairs) > 0 {
			now := time.Now()
			kept := repairs[:0]
			for _, r := range repairs {
				if !now.Before(r.due) {
					r.fn()
				} else {
					kept = append(kept, r)
				}
			}
			repairs = kept
		}

		// seeded random churn: extra die/fault events, always self-repairing
		if rng.Float64() < 0.30 {
			w := wans[rng.Intn(nWAN)]
			if rng.Intn(2) == 0 {
				_ = w.Die()
			} else {
				_ = w.SetMode(randomFaultMode())
			}
			scheduleRepair(time.Duration(300+rng.Intn(700))*time.Millisecond, repairWAN(w))
		}

		select {
		case <-workers.done:
			finished = true
		default:
			time.Sleep(15 * time.Millisecond)
		}
	}

	if !swapped {
		t.Fatal("load finished before the swap fired: iteration did not exercise a cutover")
	}

	// ---- the long transfer must complete exactly as sent, on the retired
	// generation's WAN.
	var long transferResult
	select {
	case long = <-longDone:
	case <-time.After(45 * time.Second):
		t.Fatal("the long transfer never completed")
	}
	if long.Err != nil {
		t.Errorf("long transfer failed across the swap: %v", long.Err)
	} else {
		if long.Recv != longSize || long.Sent != longSize {
			t.Errorf("long transfer bytes: sent=%d recv=%d, want %d", long.Sent, long.Recv, longSize)
		}
		if long.SendHash != longExpected || long.RecvHash != longExpected {
			t.Errorf("long transfer SHA-256 mismatch across the swap")
		}
		if long.Tag != wans[targetIdx].ID {
			t.Errorf("long transfer echoed by tag %q, want the pre-swap WAN %q (mid-stream re-dial would show the new tag)", long.Tag, wans[targetIdx].ID)
		}
	}

	// ---- swap outcome: the slot cut over, the retired generation drains
	// with the transfer's reservation it held at cutover, and the
	// replacement actually received post-swap traffic.
	if got := pool.GetState(targetIdx); got != StateActive {
		t.Errorf("slot %d state = %s after the swap, want active", targetIdx, got)
	}
	if got := pool.Slots[targetIdx].ServicePort; got != newWAN.Port() {
		t.Errorf("slot %d service port = %d, want the replacement %d", targetIdx, got, newWAN.Port())
	}
	if cur := pool.Slots[targetIdx].Current.Load(); cur == targetPath {
		t.Error("slot still holds the pre-swap path after DropAndReplace")
	}
	if got := targetPath.GetState(); got != path.Draining {
		t.Errorf("retired path state = %s, want draining", got)
	}
	if newWAN.Hits() == 0 {
		t.Error("the replacement WAN received no traffic after the cutover")
	}

	// ---- accounting: every generation minted by this fixture must return
	// to a balanced zero — reserve and release are per-path, so a release
	// that hit the slot's CURRENT occupant instead of the held path shows
	// up here as a leak on the retired generation and a negative count on
	// the replacement.
	allPaths := append([]*path.Path{}, initialPaths...)
	if cur := pool.Slots[targetIdx].Current.Load(); cur != nil {
		allPaths = append(allPaths, cur)
	}
	if !waitUntil(10*time.Second, func() bool {
		for _, p := range allPaths {
			if p.Inflight.Load() != 0 {
				return false
			}
		}
		return true
	}) {
		var leaks []string
		for _, p := range allPaths {
			if n := p.Inflight.Load(); n != 0 {
				leaks = append(leaks, fmt.Sprintf("path %d (slot %d) inflight=%d", p.ID, p.Slot, n))
			}
		}
		t.Errorf("cross-generation accounting after quiesce: %s (reservations must balance per path generation)", strings.Join(leaks, ", "))
	}

	// ---- per-iteration verdict
	ok, clean, cross, tags, samples := col.endIter()
	if cross > 0 {
		t.Fatalf("iteration %d: CROSS-TALK detected: %d/%d exchanges carried bytes that were not the requesting client's own (samples: %s)",
			it, cross, cross+ok+clean, strings.Join(samples, "; "))
	}
	if ok < okFloor {
		t.Errorf("iteration %d: only %d/%d exchanges verified OK (floor %d) — chaos starved the proof", it, ok, connsTotal, okFloor)
	}
	if len(tags) < 2 {
		t.Errorf("iteration %d: OK exchanges landed on only %d WAN tag(s) %v — selection never mixed paths", it, len(tags), tags)
	}
	for _, chk := range []struct {
		name string
		got  int
	}{
		{"backend kill", died}, {"mode fault", faulted}, {"slot drain", drained},
		{"health ejection", ejected}, {"repair", repaired}, {"swap", boolToInt(swapped)},
	} {
		if chk.got < 1 {
			t.Errorf("iteration %d: chaos event %q never fired — the run was not under chaos", it, chk.name)
		}
	}
	t.Logf("iter %d: ok=%d clean=%d cross=0 tags=%d events[die=%d fault=%d drain=%d eject=%d repair=%d undrain=%d swap=%d]",
		it, ok, clean, len(tags), died, faulted, drained, ejected, repaired, undrained, boolToInt(swapped))
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// coverServicePortReads acquires every metric mutex a relay handler can
// touch AFTER it reads slot.ServicePort (byte callbacks during the relay,
// the retry/outcome events of a failed dial, the latency sample at relay
// end), so a subsequent swap write is ordered after all those reads.
func coverServicePortReads(slot int) {
	_ = metricProxyBytes.Value(strconv.Itoa(slot), "up")
	_ = metricProxyBytes.Value(strconv.Itoa(slot), "down")
	_ = metricProxyLatency.Value()
	_ = metricRetryTotal.Value("dial", "success")
	_ = metricRetryTotal.Value("dial", "retry")
	_ = metricRetryTotal.Value("dial", "failed")
	_ = metricRetryTotal.Value("dial", "no_candidate")
}

// metricSessionResetsMarker keeps the per-iteration session-reset check
// honest without re-reading a changing global mid-iteration; the real
// assertion is the suite-wide delta in TestIntegrity_TINT01.
const metricSessionResetsMarker = -1

// ---------------------------------------------------------------------------
// raw streamed transfer (T-INT-02, reused by T-CHAOS-09)
// ---------------------------------------------------------------------------

// transferResult reports one raw streamed transfer through a front-end.
type transferResult struct {
	Tag      string
	Sent     int64
	Recv     int64
	SendHash [sha256.Size]byte
	RecvHash [sha256.Size]byte
	Err      error
}

// fillTransferChunk builds the deterministic transfer pattern: every chunk
// carries its own 8-byte sequence number, so any shift, drop or splice of
// the stream changes the SHA-256 even when the length still matches.
func fillTransferChunk(buf []byte, seq uint64) {
	for i := range buf {
		buf[i] = byte(i*31) ^ 0x5a
	}
	binary.LittleEndian.PutUint64(buf[:8], seq)
}

// transferExpectedHash is the SHA-256 of exactly size pattern bytes.
func transferExpectedHash(size int64) [sha256.Size]byte {
	h := sha256.New()
	buf := make([]byte, 64<<10)
	var off int64
	var seq uint64
	for off < size {
		fillTransferChunk(buf, seq)
		n := int64(len(buf))
		if off+n > size {
			n = size - off
		}
		h.Write(buf[:n])
		off += n
		seq++
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

// streamTransfer pushes exactly size pattern bytes through a SOCKS5
// front-end and reads "[ID]" + exactly size bytes back, hashing both
// directions. Any writer or reader error lands in Err.
func streamTransfer(addr, target string, size int64, deadline time.Duration) transferResult {
	var res transferResult
	conn, err := integritySocks5Connect(addr, target, deadline)
	if err != nil {
		res.Err = err
		return res
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(deadline))

	type wstat struct {
		sent int64
		sum  [sha256.Size]byte
		err  error
	}
	wch := make(chan wstat, 1)
	go func() {
		h := sha256.New()
		buf := make([]byte, 64<<10)
		var sent int64
		var seq uint64
		for sent < size {
			fillTransferChunk(buf, seq)
			n := int64(len(buf))
			if sent+n > size {
				n = size - sent
			}
			h.Write(buf[:n])
			if _, err := conn.Write(buf[:n]); err != nil {
				wch <- wstat{sent: sent, err: err}
				return
			}
			sent += n
			seq++
		}
		var sum [sha256.Size]byte
		copy(sum[:], h.Sum(nil))
		wch <- wstat{sent: sent, sum: sum}
	}()

	// Read the "[ID]" tag, then exactly size bytes, hashing as they land.
	tagBuf := make([]byte, 1)
	var tb []byte
	for {
		if len(tb) > 512 {
			res.Err = errors.New("response tag exceeds 512 bytes or is missing ']'")
			<-wch
			return res
		}
		if _, err := io.ReadFull(conn, tagBuf); err != nil {
			res.Err = fmt.Errorf("read tag: %w", err)
			<-wch
			return res
		}
		tb = append(tb, tagBuf[0])
		if tagBuf[0] == ']' {
			break
		}
	}
	if tb[0] != '[' {
		res.Err = errors.New("response does not start with an [ID] tag")
		<-wch
		return res
	}
	res.Tag = string(tb[1 : len(tb)-1])

	rh := sha256.New()
	buf := make([]byte, 64<<10)
	for res.Recv < size {
		n, err := conn.Read(buf)
		if n > 0 {
			rh.Write(buf[:n])
			res.Recv += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			res.Err = fmt.Errorf("read body: %w (after %d bytes)", err, res.Recv)
			<-wch
			return res
		}
	}
	copy(res.RecvHash[:], rh.Sum(nil))

	w := <-wch
	res.Sent = w.sent
	res.SendHash = w.sum
	if w.err != nil && res.Err == nil {
		res.Err = fmt.Errorf("write body: %w (after %d bytes)", w.err, w.sent)
	}
	if res.Err == nil && res.Recv != size {
		res.Err = fmt.Errorf("short transfer: received %d of %d bytes", res.Recv, size)
	}
	return res
}

// integritySocks5Connect opens addr and performs a no-auth SOCKS5 CONNECT
// to target ("host:port"), returning the tunnelled connection.
func integritySocks5Connect(addr, target string, deadline time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, deadline)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (net.Conn, error) {
		conn.Close()
		return nil, err
	}
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return fail(fmt.Errorf("socks greeting: %w", err))
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fail(fmt.Errorf("socks greeting reply: %w", err))
	}
	if resp[0] != 0x05 || resp[1] != 0x00 {
		return fail(fmt.Errorf("socks method rejected: %v", resp))
	}
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return fail(fmt.Errorf("bad target %q: %w", target, err))
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil || port <= 0 || port > 65535 {
		return fail(fmt.Errorf("bad target port %q", portStr))
	}
	var req []byte
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			req = []byte{0x05, 0x01, 0x00, 0x01}
			req = append(req, ip4...)
		} else {
			req = []byte{0x05, 0x01, 0x00, 0x04}
			req = append(req, ip.To16()...)
		}
	} else {
		if len(host) == 0 || len(host) > 255 {
			return fail(fmt.Errorf("bad domain %q", host))
		}
		req = []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
		req = append(req, host...)
	}
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		return fail(fmt.Errorf("socks connect: %w", err))
	}
	head := make([]byte, 10)
	if _, err := io.ReadFull(conn, head); err != nil {
		return fail(fmt.Errorf("socks connect reply: %w", err))
	}
	if head[0] != 0x05 {
		return fail(fmt.Errorf("bad socks version %d in reply", head[0]))
	}
	if head[1] != 0 {
		return fail(fmt.Errorf("CONNECT failed, reply code %d", head[1]))
	}
	return conn, nil
}

// ---------------------------------------------------------------------------
// T-INT-02
// ---------------------------------------------------------------------------

// TestIntegrity_TINT02_LargeTransferSurvivesMakeBeforeBreakSwap pushes a
// 256 MiB (32 MiB under -short) SHA-256-verified stream through the SOCKS
// front-end and swaps the stream's OWN slot mid-transfer: the retired
// generation must keep serving the flow to completion — zero resets, no
// re-dial to the replacement, byte-identical delivery.
func TestIntegrity_TINT02_LargeTransferSurvivesMakeBeforeBreakSwap(t *testing.T) {
	setIntegrityEnv(t)
	size := int64(256 << 20)
	if testing.Short() {
		size = 32 << 20
	}
	expected := transferExpectedHash(size)

	pool := NewWANPool(1, 0) // single slot: the stream is pinned to slot 0
	oldWAN, err := testutil.NewFakeWAN("t0-old", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oldWAN.Close() })
	oldPath := swpMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Protocol: "ss", Server: "t0-old.example", Port: 443, Raw: "ss://t0-old"}, oldWAN.Port())
	socksAddr := startIntegritySocks(t, pool, 60*time.Second)

	newWAN, err := testutil.NewFakeWAN("t0-new", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newWAN.Close() })
	spare, err := ports.New(newWAN.Port(), 1)
	if err != nil {
		t.Fatal(err)
	}
	candidates := cands.NewPool(2)
	candidates.Update([]*cands.Entry{{
		Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "t0-new.example", Port: 8443, Raw: "ss://t0-new"},
		Speed:  42,
	}})

	sessionResetsBefore := metricSessionResetOnDrain.Value()
	baselineUp := metricProxyBytes.Value("0", "up")

	resCh := make(chan transferResult, 1)
	go func() {
		resCh <- streamTransfer(socksAddr, "1.2.3.4:80", size, 3*time.Minute)
	}()

	// Wait until a solid slice of the stream has actually flowed through
	// the transfer handler: its byte callbacks happen AFTER the handler
	// read slot 0's service port, so acquiring the metric here orders that
	// read before the swap's write (see the file header).
	swapAt := baselineUp + float64(size)/8
	if !waitUntil(60*time.Second, func() bool {
		return metricProxyBytes.Value("0", "up") >= swapAt
	}) {
		t.Fatal("transfer never started flowing; cannot swap mid-stream")
	}

	// Make-before-break cutover of the stream's OWN slot while the stream
	// is in flight.
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		SparePorts: spare,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			time.Sleep(50 * time.Millisecond)
			return &TestResult{Config: cfg, Speed: 42}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ ...bool) (*xrayproc.Handle, string, error) {
			return swpDummyHandle(), "", nil
		},
	}
	if _, err := pool.DropAndReplace(0, opts); err != nil {
		t.Fatalf("DropAndReplace mid-stream: %v", err)
	}
	// Publish the write's clock before anything dials the slot again.
	metricProxyConnections.Value("0", "socks5")

	// The retired generation must still hold the stream's reservation at
	// cutover — that IS make-before-break.
	if got := oldPath.Inflight.Load(); got < 1 {
		t.Errorf("retired path inflight = %d right after the swap, want >= 1 (the stream must still be held by the old generation)", got)
	}

	var res transferResult
	select {
	case res = <-resCh:
	case <-time.After(3 * time.Minute):
		t.Fatal("transfer never completed")
	}
	if res.Err != nil {
		t.Fatalf("transfer failed across the swap: %v", res.Err)
	}
	if res.Recv != size || res.Sent != size {
		t.Errorf("transfer bytes: sent=%d recv=%d, want %d", res.Sent, res.Recv, size)
	}
	if res.SendHash != expected || res.RecvHash != expected {
		t.Errorf("transfer SHA-256 mismatch after the swap")
	}
	if res.Tag != "t0-old" {
		t.Errorf("transfer echoed by tag %q, want \"t0-old\": a mid-transfer re-dial would have hit the replacement", res.Tag)
	}
	if got := newWAN.Hits(); got != 0 {
		t.Errorf("the replacement WAN accepted %d connection(s) mid-transfer, want 0 (no re-dial, no replay)", got)
	}
	if got := pool.GetState(0); got != StateActive {
		t.Errorf("slot 0 state = %s after the cutover, want active", got)
	}
	if got := pool.Slots[0].ServicePort; got != newWAN.Port() {
		t.Errorf("slot 0 service port = %d, want the replacement %d", got, newWAN.Port())
	}
	if got := metricSessionResetOnDrain.Value() - sessionResetsBefore; got != 0 {
		t.Errorf("viberoxy_session_reset_on_drain_total delta = %.0f, want 0", got)
	}

	// Accounting balances once the stream's handler releases.
	if !waitUntil(15*time.Second, func() bool {
		return oldPath.Inflight.Load() == 0 && pool.Slots[0].Current.Load().Inflight.Load() == 0
	}) {
		t.Errorf("reservation never released: old=%d new=%d",
			oldPath.Inflight.Load(), pool.Slots[0].Current.Load().Inflight.Load())
	}
	if got := oldPath.GetState(); got != path.Draining {
		t.Errorf("retired path state = %s, want draining", got)
	}
	t.Logf("T-INT-02: %d bytes SHA-256 verified across a make-before-break swap (tag=%q, replacement hits=%d)",
		size, res.Tag, newWAN.Hits())
}
