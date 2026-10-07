package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"viberoxy/internal/auth"
	"viberoxy/internal/cands"
	"viberoxy/internal/health"
	"viberoxy/internal/ports"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/subs"
)

var (
	cycleTimingMu sync.RWMutex
	lastCycle     time.Time
	nextCycle     time.Time
	triggerCycle  = make(chan struct{}, 1)
	candidatePool *cands.Pool
)

func resetCycleTiming() {
	cycleTimingMu.Lock()
	lastCycle = time.Time{}
	nextCycle = time.Time{}
	cycleTimingMu.Unlock()
}

func cycleTimingSnapshot() (time.Time, time.Time) {
	cycleTimingMu.RLock()
	defer cycleTimingMu.RUnlock()
	return lastCycle, nextCycle
}

func markCycleStarted(now time.Time) {
	cycleTimingMu.Lock()
	lastCycle = now
	nextCycle = time.Time{}
	cycleTimingMu.Unlock()
}

func markCycleComplete(now time.Time, interval time.Duration) {
	cycleTimingMu.Lock()
	nextCycle = now.Add(interval)
	cycleTimingMu.Unlock()
}

// fetchSubscriptionState holds the validators and configs of the last
// usable fetch, so a 304 (unchanged subscription) reuses them without
// re-parsing and a failed fetch never overwrites them (F-18).
type fetchSubscriptionState struct {
	url          string
	etag         string
	lastModified string
	configs      []*proxycfg.ProxyConfig
}

var (
	subStateMu sync.Mutex
	subState   fetchSubscriptionState
)

// fetchSubscription is a thin caller over internal/subs: it logs and
// returns the configs main.go needs. On any error it returns nil, so both
// call sites keep whatever configs they already have (startup retries, a
// running cycle skips); on a 304 it returns the previously parsed configs.
func fetchSubscription(url string) []*proxycfg.ProxyConfig {
	subStateMu.Lock()
	prev := subState
	subStateMu.Unlock()

	opts := subs.Options{
		AllowHTTP: subs.AllowHTTPFromEnv(),
	}
	if prev.url == url {
		// Only replay validators for the subscription they came from.
		opts.ETag = prev.etag
		opts.LastModified = prev.lastModified
	}

	res, err := subs.Fetch(url, opts)
	if err != nil {
		slog.Warn("fetch subscription failed", "url", url, "kind", string(subs.KindOf(err)), "error", err)
		return nil
	}
	if res.NotModified {
		if prev.url != url {
			slog.Warn("fetch subscription failed", "url", url, "kind", string(subs.KindNotModified), "error", "no cached configs for this url")
			return nil
		}
		slog.Info("subscription not modified", "url", url, "configs", len(prev.configs))
		return prev.configs
	}

	configs := proxycfg.ParseConfigs(string(res.Body))
	if len(configs) == 0 {
		// Garbage payload: report it as a failure so the caller keeps the
		// configs it already has, and leave subState (and its validators)
		// untouched so a later conditional GET still has a good fallback.
		slog.Warn("fetch subscription failed", "url", url, "kind", string(subs.KindEmptyBody), "error", "no parseable configs in body")
		return nil
	}

	subStateMu.Lock()
	subState = fetchSubscriptionState{
		url:          url,
		etag:         res.ETag,
		lastModified: res.LastModified,
		configs:      configs,
	}
	subStateMu.Unlock()

	slog.Info("fetched subscription", "url", url, "configs", len(configs))
	return configs
}

func buildDownloadURL(cfg *proxycfg.Config, downloadSize int64) string {
	if cfg.DownloadEndpoint != "" {
		return cfg.DownloadEndpoint + strconv.FormatInt(downloadSize, 10)
	}
	return cfg.DownloadFallback
}

func writeSortedTxt(results []*TestResult) {
	f, err := os.Create("sorted.txt")
	if err != nil {
		slog.Warn("failed to write sorted.txt", "error", err)
		return
	}
	defer f.Close()

	for _, r := range results {
		line := fmt.Sprintf("%s://%s:%d", r.Config.Protocol, r.Config.Server, r.Config.Port)
		if r.Error != nil {
			line += " error=" + r.Error.Error()
		} else {
			line += fmt.Sprintf(" speed=%.2f", r.Speed)
		}
		fmt.Fprintln(f, line)
	}
}

// logUnsupportedOnce logs a "skipping unsupported protocol" notice at most
// once per protocol per cycle, so leak-guarded configs (hysteria2/tuic/
// wireguard) don't spam the log. Each promotion loop passes a fresh set per
// cycle.
func logUnsupportedOnce(logged map[string]bool, proto string) {
	if logged[proto] {
		return
	}
	logged[proto] = true
	slog.Info("skipping unsupported protocol", "protocol", proto)
}

// gateMetrics requires an Authorization: Bearer header with the API token
// on the /metrics endpoint only (F-14): /healthz and /readyz stay open so
// liveness and
// readiness probes never need a credential.
func gateMetrics(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	gated := auth.RequireBearer(token, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			gated.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// listenHost returns the bind host for a listener: cfg.ListenAddr, or the
// loopback default when the config was built without it (test fixtures) —
// a zero-value Config must never widen the bind to all interfaces (F-14).
func listenHost(cfg *proxycfg.Config) string {
	if cfg.ListenAddr == "" {
		return auth.LoopbackHost
	}
	return cfg.ListenAddr
}

func startup(cfg *proxycfg.Config, ctx context.Context) {
	slog.Info("starting viberoxy...")

	pool := NewWANPool(cfg.WanCount, cfg.WanBasePort)
	candidatePool = cands.NewPool(50)

	// F-13 port discipline: one shared allocator for every speed-test port
	// (the cycle's tests and the drop/replace API draw from the same pool,
	// so they can never fight over one listener port) plus spare service
	// ports for make-before-break replacements (an occupied slot holds its
	// own port while its replacement boots on a spare). Sizing: the cycle
	// tests sequentially (MaxTestPerCycle) plus one in-flight test per
	// concurrent API replacement (WanCount); 2xWanCount spares because an
	// occupied slot still holds one while its replacement checks out the
	// next, so one full round of concurrent swaps needs two per slot.
	// An invalid range (leaving the valid port space) must not refuse to
	// boot: log loudly and fall back to the legacy fixed-port behavior.
	testPorts, err := ports.NewTestPorts(cfg.TestBasePort, cfg.MaxTestPerCycleVal()+cfg.WanCount)
	if err != nil {
		slog.Error("speed-test port range invalid; falling back to fixed test ports", "error", err)
		testPorts = nil
	}
	sparePorts, err := ports.NewSpareWANPorts(cfg.WanBasePort, cfg.WanCount, 2*cfg.WanCount)
	if err != nil {
		slog.Error("spare service port range invalid; replacements fall back to the slot's own port", "error", err)
		sparePorts = nil
	}
	pool.SetPortAllocators(testPorts, sparePorts)

	var (
		proxy        *ProxyServer
		obsSrv       *http.Server
		proxyStarted bool
	)

	// startServices boots the HTTPS proxy, the observability server and the
	// health canary loop exactly once. With degraded boot enabled this happens
	// as soon as the first WAN slot is active; otherwise only after the pool
	// is full. Idempotent, so later calls (e.g. after the pool reaches the
	// full WAN_COUNT) are no-ops.
	startServices := func() {
		if proxyStarted {
			return
		}
		proxyStarted = true

		proxy = NewProxyServer(cfg.ProxyPort, pool, cfg.Router)
		proxy.listenAddr = listenHost(cfg)
		proxy.AccessLog = cfg.AccessLog
		if cfg.WanFailThreshold > 0 {
			proxy.WanFailThreshold = cfg.WanFailThreshold
		}
		go func() {
			if err := proxy.Start(ctx); err != nil && err != http.ErrServerClosed {
				slog.Error("proxy server error", "error", err)
			}
		}()

		if cfg.SocksPort > 0 {
			socks := NewSocksServer(cfg.SocksPort, pool, cfg.Router)
			socks.listenAddr = listenHost(cfg)
			socks.AccessLog = cfg.AccessLog
			if cfg.WanFailThreshold > 0 {
				socks.WanFailThreshold = cfg.WanFailThreshold
			}
			go func() {
				if err := socks.Listen(ctx); err != nil {
					slog.Error("socks5 server error", "error", err)
				}
			}()
			slog.Info("socks5 server started", "port", cfg.SocksPort)
		}

		if cfg.MetricsPort > 0 {
			obsSrv = &http.Server{
				Addr: net.JoinHostPort(listenHost(cfg), strconv.Itoa(cfg.MetricsPort)),
				// F-14: /metrics carries operational detail; with an
				// API_TOKEN configured it requires a bearer token.
				Handler: gateMetrics(cfg.APIToken, NewObservabilityHandler(pool)),
			}
			go func() {
				if err := obsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					slog.Error("metrics server error", "error", err)
				}
			}()
			slog.Info("metrics server started", "port", cfg.MetricsPort)
		}

		// API server for per-slot state + exit IP exposure. Runs on its own
		// port (default 1980) alongside the proxy / observability listeners.
		apiPort := 1980
		if v := os.Getenv("API_PORT"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 65535 {
				apiPort = n
			}
		}
		apiSrv := &http.Server{
			Addr:    net.JoinHostPort(listenHost(cfg), strconv.Itoa(apiPort)),
			Handler: NewAPIHandler(pool, cfg),
		}
		go func() {
			if err := apiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("api server error", "error", err)
			}
		}()
		slog.Info("api server started", "port", apiPort)

		if cfg.KeepaliveInterval > 0 {
			go canaryLoop(cfg, pool, ctx)
		}
	}

	for {
		configs := fetchSubscription(cfg.SubscriberURL)
		if len(configs) == 0 {
			slog.Warn("no configs fetched, retrying...")
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				continue
			}
		}

		slog.Info("startup: testing configs...", "count", len(configs))
		skippedUnsupported := map[string]bool{}
		var tested int
		for _, c := range configs {
			if pool.ActiveCount() >= cfg.WanCount {
				break
			}
			// Leak guard: protocols that map to a freedom (direct) xray
			// outbound must never be promoted to a WAN slot.
			if !proxycfg.IsXraySupported(c) {
				logUnsupportedOnce(skippedUnsupported, c.Protocol)
				continue
			}
			// WAN dedupe: never promote the same server:port twice.
			if pool.HasServerPort(c.Server, c.Port) {
				slog.Info("skipping duplicate wan", "server", c.Server, "port", c.Port)
				continue
			}
			testPort, releaseTest, ok := acquireCycleTestPort(pool, cfg.TestBasePort, tested)
			if !ok {
				slog.Warn("startup: no free speed-test port, retrying...",
					"active", pool.ActiveCount(), "needed", cfg.WanCount)
				break
			}
			result := TestSpeedWithStability(c, testPort, time.Duration(cfg.TestTimeout)*time.Second, buildDownloadURL(cfg, cfg.DownloadSize), cfg.DownloadSize, cfg.StabilityProbes)
			releaseTest()
			tested++
			if result.Error != nil || result.Speed < cfg.MinimumSpeed {
				continue
			}
			emptySlots := pool.GetSlotsByState(StateEmpty)
			if len(emptySlots) == 0 {
				break
			}
			slotIdx := emptySlots[0]
			pool.StartTesting(slotIdx, c)
			cmd, path, err := StartXray(c, cfg.WanBasePort+slotIdx, cfg.XrayMux)
			if err != nil {
				slog.Error("startup: xray failed", "error", err)
				pool.ResetEmpty(slotIdx)
				continue
			}
			pool.SetActive(slotIdx, cmd, path)
			pool.SetSlotSpeedMbps(slotIdx, result.Speed)
			pool.SetSlotStability(slotIdx, result.StabilityScore)
			slog.Info("startup: wan active", "index", slotIdx, "server", c.Server, "speed", result.Speed, "stability", result.StabilityScore)

			// Degraded boot: serve traffic as soon as the first WAN is up,
			// then keep filling slots until the full WAN_COUNT is reached.
			if cfg.AllowDegradedBoot && !proxyStarted && pool.ActiveCount() >= 1 {
				startServices()
				slog.Info("viberoxy started (degraded)", "wans", pool.ActiveCount(), "needed", cfg.WanCount)
			}
		}

		if pool.ActiveCount() >= cfg.WanCount {
			break
		}

		// Preserve active slots across retries: only reset slots that are
		// still mid-test; any xray process already promoted stays up.
		for _, idx := range pool.GetSlotsByState(StateTesting) {
			pool.ResetEmpty(idx)
		}

		slog.Warn("startup: not enough configs passed minimum speed, retrying...",
			"active", pool.ActiveCount(), "needed", cfg.WanCount)
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}

	startServices()
	slog.Info("viberoxy started", "wans", pool.ActiveCount(), "proxy_port", cfg.ProxyPort)

	runLoop(cfg, pool, candidatePool, proxy, ctx)

	slog.Info("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proxy.Stop(shutdownCtx)
	if obsSrv != nil {
		if err := obsSrv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("metrics server shutdown", "error", err)
		}
	}
	pool.ShutdownAll()
	slog.Info("viberoxy stopped")
}

// DefaultDrainMax is the DRAIN_MAX default (F-02): a draining slot is
// reaped no later than this long after it started draining, even with
// in-flight connections. It replaces the old fixed
// max(60s, 2×FETCH_INTERVAL) timer; the usual completion rule is
// inflight == 0, DRAIN_MAX is the hard stop.
const DefaultDrainMax = 600 * time.Second

// drainMaxFromEnv reads DRAIN_MAX (seconds). Unset, invalid or
// non-positive values yield DefaultDrainMax (with a warning for the
// bad ones) — the dual-read pattern shared by the other env knobs.
func drainMaxFromEnv() time.Duration {
	v := os.Getenv("DRAIN_MAX")
	if v == "" {
		return DefaultDrainMax
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		slog.Warn("invalid DRAIN_MAX, using default", "DRAIN_MAX", v, "default_s", int(DefaultDrainMax/time.Second))
		return DefaultDrainMax
	}
	return time.Duration(n) * time.Second
}

func runLoop(cfg *proxycfg.Config, pool *WANPool, candidatePool *cands.Pool, proxy *ProxyServer, ctx context.Context) {
	interval := time.Duration(cfg.FetchInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	markCycleComplete(time.Now(), interval)

	// F-02: the drain clock is DRAIN_MAX (default 600s), not a function
	// of the fetch interval: runCycle reaps a draining slot when its
	// in-flight flows finish, or at DRAIN_MAX as the hard stop.
	drainMax := drainMaxFromEnv()

	run := func() {
		runCycle(cfg, pool, candidatePool, drainMax)
		// A manual cycle restarts the interval so the exposed next-cycle time
		// matches the ticker's actual schedule.
		ticker.Reset(interval)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		case <-triggerCycle:
			run()
		}
	}
}

// canaryLoop is the active health prober (SPEC-H.5), replacing the old
// keepaliveLoop: every jittered 15–30s interval it runs one canary pass
// over every active/draining WAN. The interval's base comes from
// KEEPALIVE_INTERVAL, clamped into the 15–30s band (default 300 → 30s);
// the endpoints come from HEALTH_ENDPOINTS (defaults: gstatic generate_204
// + cloudflare cdn-cgi/trace — ipify is NEVER used for health, F-07).
// It stops when ctx is cancelled.
func canaryLoop(cfg *proxycfg.Config, pool *WANPool, ctx context.Context) {
	base := canaryBaseInterval(cfg.KeepaliveInterval)
	endpoints := health.EndpointsFromEnv()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	timer := time.NewTimer(health.JitterInterval(base, rng))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			runCanaries(cfg, pool, endpoints)
			timer.Reset(health.JitterInterval(base, rng))
		}
	}
}

// canaryBaseInterval maps KEEPALIVE_INTERVAL (seconds) onto the canary
// band: the legacy env knob now drives the ACTIVE canary interval, clamped
// to SPEC-H.5's 15–30s window (values >= 30 → 30s, <= 15 → 15s; the
// configured default 300 therefore runs canaries every 30s ± 20% jitter).
func canaryBaseInterval(keepaliveSeconds int) time.Duration {
	d := time.Duration(keepaliveSeconds) * time.Second
	if d < health.CanaryIntervalMin {
		return health.CanaryIntervalMin
	}
	if d > health.CanaryIntervalMax {
		return health.CanaryIntervalMax
	}
	return d
}

// runCanaries executes one canary interval (SPEC-H.5/H.6): it GETs every
// health endpoint through each active/draining slot's SOCKS5 listener — a
// path's canary fails only if ALL endpoints failed — then applies the
// interval verdict through health.ApplyCanary:
//
//   - environmental failure (>=75% of paths failing in the SAME interval):
//     NOTHING is recorded, ejected or drained — only the env_degraded
//     gauge moves and a warning is logged (T-HLT-03 / T-CHAOS-05);
//   - otherwise a failing path records a failure exactly like the old
//     keepalive did (counting toward WAN_FAIL_THRESHOLD, which marks the
//     slot Draining for health reasons — MarkDrainingHealth, so the
//     drain is reversible), and a passing path resets the counter, feeds
//     its half-open/health streak, un-drains the slot when that streak
//     recovers it (F-02) and refreshes the exit IP through ipify
//     (exit-IP reporting only — F-07's SPOF fix).
func runCanaries(cfg *proxycfg.Config, pool *WANPool, endpoints []string) {
	now := time.Now()
	timeout := time.Duration(cfg.TestTimeout) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second // hand-built configs (tests) leave it zero
	}

	slots := pool.GetSlotsByState(StateActive, StateDraining)
	if len(slots) == 0 {
		return
	}
	results := make([]health.PathCanary, 0, len(slots))
	for _, idx := range slots {
		slot := pool.Slots[idx]
		slot.mu.Lock()
		port := slot.ServicePort
		slot.mu.Unlock()
		socksAddr := fmt.Sprintf("127.0.0.1:%d", port)
		ok, rtt := canaryProbe(socksAddr, endpoints, timeout)
		results = append(results, health.PathCanary{Index: idx, OK: ok, RTT: rtt})
	}

	degraded, failing := health.ApplyCanary(results)
	health.SetEnvDegraded(degraded)
	metricEnvDegraded.Set(bool01(degraded)) // SPEC-M: stamped at event time too
	if degraded {
		slog.Warn("env_degraded: canary endpoints failing on most paths; keeping everything serving",
			"paths", len(results), "interval", "environmental")
		return
	}

	failingSet := make(map[int]bool, len(failing))
	for _, idx := range failing {
		failingSet[idx] = true
	}
	for i, idx := range slots {
		slot := pool.Slots[idx]
		slot.mu.Lock()
		port := slot.ServicePort
		slot.mu.Unlock()
		socksAddr := fmt.Sprintf("127.0.0.1:%d", port)
		cur := pool.Slots[idx].Current.Load()
		r := results[i]

		if !failingSet[idx] {
			pool.RecordSuccess(idx)
			recovered := cur.NoteCanary(now, true)
			// F-02 un-drain: a HEALTH-drained slot whose canary streak
			// recovered returns to Active here instead of waiting to be
			// reaped. Replacement drains are never un-drained (the pool
			// tells them apart by drain reason).
			if pool.UnDrainIfRecovered(idx, recovered) {
				slog.Info("canary: draining wan recovered, back to active", "index", idx)
			}
			if ip, err := probeExitIP(socksAddr, timeout); err == nil {
				slot := pool.Slots[idx]
				slot.mu.Lock()
				slot.ExitIP = ip
				slot.LastProbe = time.Now()
				slot.mu.Unlock()
				slog.Info("canary: ok", "index", idx, "exit_ip", ip, "rtt_ms", r.RTT.Milliseconds())
			} else {
				slog.Info("canary: ok", "index", idx, "rtt_ms", r.RTT.Milliseconds())

			}
			continue
		}

		pool.RecordFailure(idx)
		cur.NoteCanary(now, false)
		fails := pool.SlotConsecutiveFails(idx)
		slog.Warn("canary: path failed all endpoints", "index", idx, "fails", fails)
		if cfg.WanFailThreshold > 0 && fails >= int64(cfg.WanFailThreshold) && pool.GetState(idx) == StateActive {
			// Health flavor (F-02): this drain reverses if the canary
			// streak recovers before the drain completes.
			if err := pool.MarkDrainingHealth(idx); err != nil {
				slog.Warn("canary: failed to mark draining", "index", idx, "error", err)
			} else {
				slog.Warn("canary: wan unhealthy, marked draining", "index", idx, "fails", fails)
			}
		}
	}
}

// canaryProbe GETs each health endpoint through socksAddr and reports
// whether ANY endpoint succeeded (a path's canary fails only if ALL fail)
// plus the round-trip time of the first successful endpoint. Non-2xx/3xx
// statuses count as endpoint failures.
func canaryProbe(socksAddr string, endpoints []string, timeout time.Duration) (bool, time.Duration) {
	for _, ep := range endpoints {
		start := time.Now()
		resp, conn, err := probeRoundTrip(socksAddr, ep, timeout)
		if err != nil {
			continue
		}
		rtt := time.Since(start)
		if resp.StatusCode < 200 || resp.StatusCode >= 400 {
			resp.Body.Close()
			conn.Close()
			continue
		}
		// Drain a small slice so the round trip really carried bytes.
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		conn.Close()
		return true, rtt
	}
	return false, 0
}

// toCands converts speed-test results into cands.Entry values for the
// candidate pool. The pool lives in internal/cands and cannot see main's
// TestResult, so the value fields are copied here and Config stays shared
// by pointer.
func toCands(results []*TestResult) []*cands.Entry {
	entries := make([]*cands.Entry, len(results))
	for i, r := range results {
		entries[i] = &cands.Entry{
			Config:         r.Config,
			Speed:          r.Speed,
			StabilityScore: r.StabilityScore,
			Error:          r.Error,
		}
	}
	return entries
}

// ---- F-03 / SPEC-R: continuous evaluation + hysteresis rotation ----

// maxTestPerCycleFull bounds how many NEW candidate configs one cycle
// speed-tests while the pool is already at WAN_COUNT (F-03): a full pool
// keeps evaluating candidates instead of going deaf, but stays bounded
// so a churny subscription cannot turn every cycle into a benchmarking
// marathon. New = not already serving (HasServerPort) and not tested
// within the candidate pool's TTL (cands.DefaultTTL ages the memory out).
const maxTestPerCycleFull = 2

const (
	// rotationHysteresis is SPEC-R's swap bar: a candidate only
	// displaces an incumbent when its score is at least
	// (1+rotationHysteresis) times the incumbent's — equal or
	// marginally-better configs never churn the pool.
	rotationHysteresis = 0.30
	// rotationMinDwell is how long an incumbent must have served
	// (since its activation) before it may be rotated out, so a
	// freshly-activated WAN is never immediately re-evaluated.
	rotationMinDwell = 10 * time.Minute
)

// candidateExitIP resolves a not-yet-running candidate's exit IP for
// SPEC-R's exit-IP dedupe. Until the scheduler task can carry observed
// exit IPs over to candidates, the best available answer is the
// candidate's own server address when it is an IP literal (a direct,
// non-relayed config egresses there); a hostname means the exit IP is
// UNKNOWN and the caller skips the gate. The served side of the
// comparison is the ExitIP the canary loop records per slot.
func candidateExitIP(cfg *proxycfg.ProxyConfig) string {
	if cfg == nil || net.ParseIP(cfg.Server) == nil {
		return ""
	}
	return cfg.Server
}

// rotationDecision is SPEC-R's verdict for one cycle: at most one swap
// (Victim >= 0 plus the chosen Candidate), else Victim == -1 and Reason
// names the gate that refused. Score = benchmark speed today; the live
// goodput/TTFB EWMA score lands with the scheduler task.
type rotationDecision struct {
	Victim    int
	Candidate *TestResult
	Reason    string
}

// rotator carries SPEC-R's knobs, its injected clock and the swap
// bookkeeping (cooldown). The process-wide instance is rotation; tests
// construct their own via newRotator and inject now/exitIP/test/start.
type rotator struct {
	mu         sync.Mutex
	hysteresis float64
	minDwell   time.Duration
	now        func() time.Time
	exitIP     func(*proxycfg.ProxyConfig) string
	// test/start override DropAndReplace's mandatory re-test and the
	// process start in tests; nil keeps the production behavior.
	test  ReplacementTester
	start ReplacementStarter
	// lastSwap is when the last successful rotation cut over (zero =
	// never), the cooldown reference for "now - lastSwap >= FETCH_INTERVAL".
	lastSwap time.Time
}

func newRotator() *rotator {
	return &rotator{
		hysteresis: rotationHysteresis,
		minDwell:   rotationMinDwell,
		now:        time.Now,
		exitIP:     candidateExitIP,
	}
}

// rotation is the process-wide SPEC-R rotator runCycle swaps through.
var rotation = newRotator()

// decide picks at most ONE SPEC-R swap for a cycle, or reports why
// none may happen. Gates, in order: FETCH_INTERVAL cooldown; worst
// incumbent (lowest score; PickReplacementSlot's instability preference
// breaks ties); MIN_DWELL on the incumbent's age; per-candidate gates
// (speed bar, no server:port already active/draining, no exit IP already
// served by an active path — unknown exit IPs skip that check); and
// finally hysteresis (candidate >= (1+hysteresis) x incumbent).
func (r *rotator) decide(pool *WANPool, results []*TestResult, minSpeed float64, cooldown time.Duration) rotationDecision {
	r.mu.Lock()
	nowFn, exitFn := r.now, r.exitIP
	hyst, dwell := r.hysteresis, r.minDwell
	lastSwap := r.lastSwap
	r.mu.Unlock()

	now := nowFn()
	if !lastSwap.IsZero() && now.Sub(lastSwap) < cooldown {
		return rotationDecision{Victim: -1, Reason: fmt.Sprintf("cooldown: last swap %s ago, need %s",
			now.Sub(lastSwap).Round(time.Second), cooldown)}
	}

	active := pool.GetSlotsByState(StateActive)
	if len(active) == 0 {
		return rotationDecision{Victim: -1, Reason: "no active wan"}
	}

	// Worst incumbent: lowest score, with today's instability
	// preference (PickReplacementSlot) breaking ties.
	worst := pool.SlotSpeedMbps(active[0])
	for _, idx := range active[1:] {
		if s := pool.SlotSpeedMbps(idx); s < worst {
			worst = s
		}
	}
	tied := make([]int, 0, len(active))
	for _, idx := range active {
		if pool.SlotSpeedMbps(idx) == worst {
			tied = append(tied, idx)
		}
	}
	victim := pool.PickReplacementSlot(tied)

	// MIN_DWELL: age = time since the incumbent's current occupant was
	// activated (one *path.Path is minted per activation).
	if age := now.Sub(pool.Slots[victim].Current.Load().CreatedAt); age < dwell {
		return rotationDecision{Victim: -1, Reason: fmt.Sprintf("dwell: incumbent served %s, need %s",
			age.Round(time.Second), dwell)}
	}

	// Exit IPs the canary loop recorded for the active paths ("" =
	// never probed = unknown).
	activeExit := make(map[string]bool, len(active))
	for _, idx := range active {
		slot := pool.Slots[idx]
		slot.mu.Lock()
		ip := slot.ExitIP
		slot.mu.Unlock()
		if ip != "" {
			activeExit[ip] = true
		}
	}
	blocked := func(c *proxycfg.ProxyConfig) bool {
		if c == nil {
			return true
		}
		if pool.HasServerPort(c.Server, c.Port) {
			return true // already serving another active/draining path
		}
		ip := exitFn(c)
		return ip != "" && activeExit[ip] // exit-IP duplicate
	}
	cand := bestNewCandidate(results, minSpeed, blocked)
	if cand == nil {
		return rotationDecision{Victim: -1, Reason: "no candidate cleared the speed bar, wan dedupe and exit-ip dedupe"}
	}

	// Hysteresis: only a clear win rotates an incumbent.
	incumbent := pool.SlotSpeedMbps(victim)
	if cand.Speed < (1+hyst)*incumbent {
		return rotationDecision{Victim: -1, Reason: fmt.Sprintf(
			"hysteresis: candidate %.2f < %.2f (1+%.2f) x incumbent %.2f",
			cand.Speed, (1+hyst)*incumbent, hyst, incumbent)}
	}
	return rotationDecision{Victim: victim, Candidate: cand, Reason: "swap"}
}

// swapResultOf maps a rotationDecision.Reason onto the closed set of
// viberoxy_swap_total result labels (SPEC-M): the reason strings are
// free-form log text, the metric labels are stable.
func swapResultOf(reason string) string {
	switch {
	case strings.HasPrefix(reason, "cooldown"):
		return "cooldown"
	case strings.HasPrefix(reason, "dwell"):
		return "dwell"
	case strings.HasPrefix(reason, "hysteresis"):
		return "hysteresis"
	default:
		// "no candidate cleared the speed bar..." and "no active wan":
		// no swap happened because no candidate cleared the gates.
		return "no_candidate"
	}
}

// maybeSwap runs SPEC-R's rotation for one cycle: decide, then perform
// the single chosen swap through the make-before-break DropAndReplace
// path (the old occupant keeps serving until the candidate is tested
// and started on a spare port). At most one DropAndReplace ever runs
// per call, so a cycle can never swap twice; a successful cutover
// stamps lastSwap to start the cooldown. Returns true iff a swap cut
// over. Every verdict increments viberoxy_swap_total{result} (SPEC-M).
func (r *rotator) maybeSwap(pool *WANPool, cfg *proxycfg.Config, results []*TestResult) bool {
	d := r.decide(pool, results, cfg.MinimumSpeed, time.Duration(cfg.FetchInterval)*time.Second)
	if d.Victim < 0 {
		metricSwapTotal.Inc(swapResultOf(d.Reason))
		slog.Info("cycle: rotation skipped", "reason", d.Reason)
		return false
	}

	// DropAndReplace picks its candidate through a cands pool: hand it
	// one holding exactly the candidate the gates above approved, so
	// the decision and the swap can never disagree (it still re-tests
	// and re-validates make-before-break on its own).
	only := cands.NewPool(1)
	only.Update(toCands([]*TestResult{d.Candidate}))

	opts := DropAndReplaceOptions{
		Candidates:      only,
		TestPort:        cfg.TestBasePort,
		Timeout:         time.Duration(cfg.TestTimeout) * time.Second,
		DownloadURL:     buildDownloadURL(cfg, cfg.DownloadSize),
		DownloadSize:    cfg.DownloadSize,
		StabilityProbes: cfg.StabilityProbes,
		XrayMux:         cfg.XrayMux,
		TestPorts:       pool.TestPorts(),
		SparePorts:      pool.SparePorts(),
	}
	r.mu.Lock()
	opts.TestCandidate, opts.StartCandidate = r.test, r.start
	r.mu.Unlock()

	slog.Info("cycle: rotating wan",
		"index", d.Victim,
		"server", d.Candidate.Config.Server,
		"speed", d.Candidate.Speed,
		"incumbent_speed", pool.SlotSpeedMbps(d.Victim))
	if _, err := pool.DropAndReplace(d.Victim, opts); err != nil {
		// Every failure path here is a candidate-class refusal (no
		// candidate, failed re-test, failed start, lost swap race):
		// the observable result is "no swap because of the candidate".
		metricSwapTotal.Inc("no_candidate")
		slog.Warn("cycle: rotation swap failed", "index", d.Victim, "error", err)
		return false
	}
	metricSwapTotal.Inc("swapped")
	r.mu.Lock()
	r.lastSwap = r.now()
	r.mu.Unlock()
	return true
}

// reapCompletedDrains performs the cycle's drain reap (F-02): every drain
// DrainExpired reports complete is recorded as a drain duration
// (SPEC-M viberoxy_drain_seconds) while DrainAt is still intact, logged,
// and reset. Returns the reaped indices.
func reapCompletedDrains(pool *WANPool, drainMax time.Duration) []int {
	expired := pool.DrainExpired(drainMax)
	for _, idx := range expired {
		pool.observeDrainDuration(idx)
		slog.Info("cycle: draining expired", "index", idx)
		pool.ResetEmpty(idx)
	}
	return expired
}

// runCycle runs one fetch/test/replace cycle. drainMax is DRAIN_MAX
// (default 600s): the hard-stop age at which a draining slot is reaped
// even with in-flight connections; normally the drain completes earlier
// (inflight == 0) — see WANPool.DrainExpired.
func runCycle(cfg *proxycfg.Config, pool *WANPool, candidatePool *cands.Pool, drainMax time.Duration) {
	markCycleStarted(time.Now())
	defer func() {
		markCycleComplete(time.Now(), time.Duration(cfg.FetchInterval)*time.Second)
	}()

	slog.Info("cycle: started")

	// Reap completed drains (F-02): inflight == 0, or DRAIN_MAX elapsed;
	// a health-drained slot whose canary streak recovered comes back to
	// Active inside DrainExpired instead of being killed here.
	reapCompletedDrains(pool, drainMax)

	for _, idx := range pool.HealthCheckAll() {
		slog.Warn("cycle: wan died, resetting", "index", idx)
		pool.ResetEmpty(idx)
	}

	configs := fetchSubscription(cfg.SubscriberURL)
	if len(configs) == 0 {
		slog.Warn("cycle: no configs fetched")
		return
	}

	// The subscription is latency-sorted (viberayd serves best configs first).
	// Test in that order and promote the first configs that clear the
	// MINIMUM_SPEED bar — no need to test the entire list. When the pool
	// is FULL the cycle does not go deaf (F-03): it keeps testing up to
	// maxTestPerCycleFull NEW candidates per cycle — deduped against the
	// active/draining WANs (HasServerPort) and against everything the
	// candidate pool still remembers within its TTL — which is exactly
	// what feeds SPEC-R's hysteresis rotation below.
	results := []*TestResult{}
	tested := 0
	evaluated := 0
	skippedUnsupported := map[string]bool{}
	recentlyTested := make(map[string]bool, len(results))
	if candidatePool != nil {
		for _, e := range candidatePool.List() {
			if e != nil && e.Config != nil {
				recentlyTested[e.Config.Raw] = true
			}
		}
	}
	for _, c := range configs {
		poolFull := pool.ActiveCount() >= cfg.WanCount && len(pool.GetSlotsByState(StateEmpty)) == 0
		if poolFull {
			// F-03: a full pool still evaluates NEW candidates, bounded
			// by MAX_TEST_PER_CYCLE_FULL instead of stopping dead.
			if evaluated >= maxTestPerCycleFull {
				break
			}
		} else if tested >= cfg.MaxTestPerCycleVal() {
			break
		}
		// Leak guard: never speed-test or promote protocols that map to a
		// freedom (direct) xray outbound.
		if !proxycfg.IsXraySupported(c) {
			logUnsupportedOnce(skippedUnsupported, c.Protocol)
			continue
		}
		// WAN dedupe: never promote the same server:port twice.
		if pool.HasServerPort(c.Server, c.Port) {
			slog.Info("skipping duplicate wan", "server", c.Server, "port", c.Port)
			continue
		}
		// F-03 full-pool dedupe: a config tested within the candidate
		// pool's TTL is not NEW — its result already lives there.
		if poolFull && recentlyTested[c.Raw] {
			continue
		}
		testPort, releaseTest, ok := acquireCycleTestPort(pool, cfg.TestBasePort, tested)
		if !ok {
			// Whole shared range checked out (an API replacement is mid-
			// test): skip the rest of this cycle's tests rather than reuse
			// a port we don't hold (F-13).
			slog.Warn("cycle: no free speed-test port, skipping the rest of this cycle's tests")
			break
		}
		result := TestSpeedWithStability(c, testPort, time.Duration(cfg.TestTimeout)*time.Second, buildDownloadURL(cfg, cfg.DownloadSize), cfg.DownloadSize, cfg.StabilityProbes)
		releaseTest()
		tested++
		if poolFull {
			evaluated++
		}
		if c.Raw != "" {
			recentlyTested[c.Raw] = true
		}
		results = append(results, result)

		// Fill empty slots immediately as soon as a config passes.
		if result.Error != nil || result.Speed < cfg.MinimumSpeed {
			continue
		}
		emptySlots := pool.GetSlotsByState(StateEmpty)
		if len(emptySlots) == 0 {
			if poolFull {
				// Full: keep spending the F-03 evaluation budget.
				continue
			}
			break
		}
		slotIdx := emptySlots[0]
		pool.StartTesting(slotIdx, result.Config)
		cmd, path, err := StartXray(result.Config, cfg.WanBasePort+slotIdx, cfg.XrayMux)
		if err != nil {
			slog.Warn("cycle: xray start failed", "error", err)
			pool.ResetEmpty(slotIdx)
			continue
		}
		pool.SetActive(slotIdx, cmd, path)
		pool.SetSlotSpeedMbps(slotIdx, result.Speed)
		pool.SetSlotStability(slotIdx, result.StabilityScore)
		slog.Info("cycle: filled empty slot", "index", slotIdx, "server", result.Config.Server, "speed", result.Speed, "stability", result.StabilityScore)
	}

	writeSortedTxt(results)

	// Populate the candidate pool with tested configs for use by the
	// drop-and-replace API and future cycles.
	if candidatePool != nil {
		candidatePool.Update(toCands(results))
	}

	// SPEC-R rotation (F-03's second half): the evaluations above feed
	// the candidate pool, and a FULL pool may rotate — at most ONE
	// make-before-break swap per cycle, only when a candidate is
	// clearly better (hysteresis + MIN_DWELL + cooldown + exit-IP
	// dedupe, all in rotator.decide). This used to be unreachable dead
	// code: the loop head above stopped all evaluation when the pool
	// was full, so len(results) was always 0 here.
	activeSlots := pool.GetSlotsByState(StateActive)
	if len(activeSlots) == cfg.WanCount && len(results) > 0 {
		rotation.maybeSwap(pool, cfg, results)
	}

	slog.Info("cycle: complete", "active", pool.ActiveCount(), "tested", tested)
}

func main() {
	cfg, err := proxycfg.ParseConfig()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startup(cfg, ctx)
}
