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
	for _, idx := range pool.DrainExpired(drainMax) {
		slog.Info("cycle: draining expired", "index", idx)
		pool.ResetEmpty(idx)
	}

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
	// MINIMUM_SPEED bar — no need to test the entire list. This bounds each
	// cycle to roughly WAN_COUNT tests plus the candidates we actually use.
	results := []*TestResult{}
	tested := 0
	skippedUnsupported := map[string]bool{}
	for _, c := range configs {
		if pool.ActiveCount() >= cfg.WanCount && len(pool.GetSlotsByState(StateEmpty)) == 0 {
			break
		}
		if tested >= cfg.MaxTestPerCycleVal() {
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
		results = append(results, result)

		// Fill empty slots immediately as soon as a config passes.
		if result.Error != nil || result.Speed < cfg.MinimumSpeed {
			continue
		}
		emptySlots := pool.GetSlotsByState(StateEmpty)
		if len(emptySlots) == 0 {
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

	// Replacement: only consider candidates tested this cycle, and only when
	// the pool is already full — replace a WAN with the best new config we
	// actually measured (fastest; speed ties broken by lower stability
	// score). The slot replaced is the least stable active WAN when scores
	// are known, else the first active slot (historical behavior).
	activeSlots := pool.GetSlotsByState(StateActive)
	if len(activeSlots) == cfg.WanCount && len(results) > 0 {
		alreadyActive := func(cfg *proxycfg.ProxyConfig) bool {
			for _, idx := range activeSlots {
				activeCfg := pool.Slots[idx].Config
				if activeCfg != nil && activeCfg.Server == cfg.Server && activeCfg.Port == cfg.Port {
					return true
				}
			}
			return false
		}
		best := bestNewCandidate(results, cfg.MinimumSpeed, alreadyActive)

		if best != nil {
			replaceIdx := pool.PickReplacementSlot(activeSlots)
			slog.Info("cycle: replacing wan",
				"index", replaceIdx,
				"new_server", best.Config.Server,
				"new_speed", best.Speed,
				"new_stability", best.StabilityScore)
			if err := pool.MarkDraining(replaceIdx); err != nil {
				slog.Warn("cycle: failed to mark draining", "index", replaceIdx, "error", err)
			}
		}
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
