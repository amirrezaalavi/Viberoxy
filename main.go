package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viberoxy/internal/auth"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/subs"
)

var (
	cycleTimingMu sync.RWMutex
	lastCycle     time.Time
	nextCycle     time.Time
	triggerCycle  = make(chan struct{}, 1)
	candidatePool *CandidatePool
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
	candidatePool = NewCandidatePool(50)

	var (
		proxy        *ProxyServer
		obsSrv       *http.Server
		proxyStarted bool
	)

	// startServices boots the HTTPS proxy, the observability server and the
	// keepalive loop exactly once. With degraded boot enabled this happens
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
			go keepaliveLoop(cfg, pool, ctx)
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
			result := TestSpeedWithStability(c, cfg.TestBasePort+tested, time.Duration(cfg.TestTimeout)*time.Second, buildDownloadURL(cfg, cfg.DownloadSize), cfg.DownloadSize, cfg.StabilityProbes)
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

func runLoop(cfg *proxycfg.Config, pool *WANPool, candidatePool *CandidatePool, proxy *ProxyServer, ctx context.Context) {
	interval := time.Duration(cfg.FetchInterval) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	markCycleComplete(time.Now(), interval)

	gracePeriod := 2 * interval
	if gracePeriod < 60*time.Second {
		gracePeriod = 60 * time.Second
	}

	run := func() {
		runCycle(cfg, pool, candidatePool, gracePeriod)
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

// keepaliveLoop probes every active/draining WAN through its SOCKS5 listener
// once per KEEPALIVE_INTERVAL. It stops when ctx is cancelled.
func keepaliveLoop(cfg *proxycfg.Config, pool *WANPool, ctx context.Context) {
	ticker := time.NewTicker(time.Duration(cfg.KeepaliveInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeAllWANs(cfg, pool)
		}
	}
}

// probeAllWANs runs one ProbeWAN per active/draining slot. A failed probe
// increments the slot's ConsecutiveFails (a successful one resets it); once
// the count reaches WAN_FAIL_THRESHOLD the slot is marked draining so the
// next runCycle replaces it.
func probeAllWANs(cfg *proxycfg.Config, pool *WANPool) {
	timeout := time.Duration(cfg.TestTimeout) * time.Second
	for _, idx := range pool.GetSlotsByState(StateActive, StateDraining) {
		socksAddr := fmt.Sprintf("127.0.0.1:%d", pool.Slots[idx].ServicePort)
		if err := ProbeWAN(socksAddr, timeout); err != nil {
			pool.RecordFailure(idx)
			fails := pool.SlotConsecutiveFails(idx)
			slog.Warn("keepalive: probe failed", "index", idx, "fails", fails, "error", err)
			if cfg.WanFailThreshold > 0 && fails >= int64(cfg.WanFailThreshold) && pool.GetState(idx) == StateActive {
				if err := pool.MarkDraining(idx); err != nil {
					slog.Warn("keepalive: failed to mark draining", "index", idx, "error", err)
				} else {
					slog.Warn("keepalive: wan unhealthy, marked draining", "index", idx, "fails", fails)
				}
			}
			continue
		}
		pool.RecordSuccess(idx)

		// Resolve exit IP after a successful probe: a lightweight HTTP GET
		// to api.ipify.org through the slot's SOCKS5 listener. Failures are
		// non-fatal — the slot is already proven healthy by ProbeWAN above.
		if ip, err := probeExitIP(socksAddr, timeout); err == nil {
			slot := pool.Slots[idx]
			slot.mu.Lock()
			slot.ExitIP = ip
			slot.LastProbe = time.Now()
			slot.mu.Unlock()
			slog.Info("keepalive: probe ok", "index", idx, "exit_ip", ip)
		} else {
			slog.Info("keepalive: probe ok", "index", idx)
		}
	}
}

func runCycle(cfg *proxycfg.Config, pool *WANPool, candidatePool *CandidatePool, gracePeriod time.Duration) {
	markCycleStarted(time.Now())
	defer func() {
		markCycleComplete(time.Now(), time.Duration(cfg.FetchInterval)*time.Second)
	}()

	slog.Info("cycle: started")

	for _, idx := range pool.DrainExpired(gracePeriod) {
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
		result := TestSpeedWithStability(c, cfg.TestBasePort+tested, time.Duration(cfg.TestTimeout)*time.Second, buildDownloadURL(cfg, cfg.DownloadSize), cfg.DownloadSize, cfg.StabilityProbes)
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
		candidatePool.Update(results)
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
