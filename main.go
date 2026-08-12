package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type Config struct {
	SubscriberURL     string
	FetchInterval     int
	TestTimeout       int
	DownloadSize      int64
	DownloadEndpoint  string
	DownloadFallback  string
	WanCount          int
	WanBasePort       int
	TestBasePort      int
	ProxyPort         int
	SocksPort         int
	MetricsPort       int
	MinimumSpeed      float64
	MaxTestPerCycle   int
	KeepaliveInterval int
	WanFailThreshold  int
	StabilityProbes   int
	AccessLog         bool
	AllowDegradedBoot bool
	Router            *Router
	XrayMux           bool
}

// MaxTestPerCycle bounds how many configs are speed-tested in one runCycle.
// The subscription is latency-sorted, so testing beyond this is wasted work.
func (c *Config) MaxTestPerCycleVal() int {
	if c.MaxTestPerCycle > 0 {
		return c.MaxTestPerCycle
	}
	return 20
}

func parseConfig() (*Config, error) {
	cfg := &Config{}

	cfg.SubscriberURL = os.Getenv("SUBSCRIBER_URL")
	if cfg.SubscriberURL == "" {
		return nil, fmt.Errorf("error: SUBSCRIBER_URL=%q: must be set to a valid HTTP/HTTPS URL", cfg.SubscriberURL)
	}
	u, err := url.Parse(cfg.SubscriberURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("error: SUBSCRIBER_URL=%q: must be a valid HTTP/HTTPS URL", cfg.SubscriberURL)
	}

	cfg.FetchInterval = 300
	if v := os.Getenv("FETCH_INTERVAL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: FETCH_INTERVAL=%q: must be a valid integer", v)
		}
		if n < 30 {
			return nil, fmt.Errorf("error: FETCH_INTERVAL=%q: must be >= 30", v)
		}
		cfg.FetchInterval = n
	}

	cfg.TestTimeout = 10
	if v := os.Getenv("TEST_TIMEOUT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: TEST_TIMEOUT=%q: must be a valid integer", v)
		}
		if n < 3 {
			return nil, fmt.Errorf("error: TEST_TIMEOUT=%q: must be >= 3", v)
		}
		cfg.TestTimeout = n
	}

	cfg.DownloadSize = 10000000
	if v := os.Getenv("DOWNLOAD_SIZE"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error: DOWNLOAD_SIZE=%q: must be a valid integer", v)
		}
		if n < 1_000_000 {
			return nil, fmt.Errorf("error: DOWNLOAD_SIZE=%q: must be >= 1000000", v)
		}
		cfg.DownloadSize = n
	}

	cfg.DownloadEndpoint = "https://speed.cloudflare.com/__down?bytes="
	if v, ok := os.LookupEnv("DOWNLOAD_ENDPOINT"); ok {
		if v != "" {
			u, err := url.Parse(v)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				return nil, fmt.Errorf("error: DOWNLOAD_ENDPOINT=%q: must be a valid HTTP/HTTPS URL", v)
			}
		}
		cfg.DownloadEndpoint = v
	}

	cfg.DownloadFallback = "https://proof.ovh.net/files/"
	if v, ok := os.LookupEnv("DOWNLOAD_FALLBACK"); ok {
		if v != "" {
			u, err := url.Parse(v)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				return nil, fmt.Errorf("error: DOWNLOAD_FALLBACK=%q: must be a valid HTTP/HTTPS URL", v)
			}
		}
		cfg.DownloadFallback = v
	}

	cfg.WanCount = 4
	if v := os.Getenv("WAN_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: WAN_COUNT=%q: must be a valid integer", v)
		}
		if n < 1 || n > 5 {
			return nil, fmt.Errorf("error: WAN_COUNT=%q: must be between 1 and 5", v)
		}
		cfg.WanCount = n
	}

	cfg.WanBasePort = 10700
	if v := os.Getenv("WAN_BASE_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: WAN_BASE_PORT=%q: must be a valid integer", v)
		}
		if n < 1 || n > 65535 {
			return nil, fmt.Errorf("error: WAN_BASE_PORT=%q: must be between 1 and 65535", v)
		}
		cfg.WanBasePort = n
	}

	cfg.TestBasePort = 10800
	if v := os.Getenv("TEST_BASE_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: TEST_BASE_PORT=%q: must be a valid integer", v)
		}
		if n < 1 || n > 65535 {
			return nil, fmt.Errorf("error: TEST_BASE_PORT=%q: must be between 1 and 65535", v)
		}
		cfg.TestBasePort = n
	}

	cfg.ProxyPort = 1080
	if v := os.Getenv("PROXY_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: PROXY_PORT=%q: must be a valid integer", v)
		}
		if n < 1 || n > 65535 {
			return nil, fmt.Errorf("error: PROXY_PORT=%q: must be between 1 and 65535", v)
		}
		cfg.ProxyPort = n
	}

	// SOCKS_PORT: 0 disables the SOCKS5 front-end listener. Any value in
	// 1..65535 starts it on that port.
	cfg.SocksPort = 0
	if v := os.Getenv("SOCKS_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: SOCKS_PORT=%q: must be a valid integer", v)
		}
		if n < 0 || n > 65535 {
			return nil, fmt.Errorf("error: SOCKS_PORT=%q: must be between 0 and 65535", v)
		}
		cfg.SocksPort = n
	}

	cfg.MinimumSpeed = 5.0
	if v := os.Getenv("MINIMUM_SPEED"); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("error: MINIMUM_SPEED=%q: must be a valid number", v)
		}
		if n < 0.1 {
			return nil, fmt.Errorf("error: MINIMUM_SPEED=%q: must be >= 0.1", v)
		}
		cfg.MinimumSpeed = n
	}

	cfg.MaxTestPerCycle = 20
	if v := os.Getenv("MAX_TEST_PER_CYCLE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: MAX_TEST_PER_CYCLE=%q: must be a valid integer", v)
		}
		if n < 1 || n > 500 {
			return nil, fmt.Errorf("error: MAX_TEST_PER_CYCLE=%q: must be between 1 and 500", v)
		}
		cfg.MaxTestPerCycle = n
	}

	// KEEPALIVE_INTERVAL: seconds between end-to-end WAN health probes
	// (HTTP GET through each slot's SOCKS5 listener).
	cfg.KeepaliveInterval = 300
	if v := os.Getenv("KEEPALIVE_INTERVAL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: KEEPALIVE_INTERVAL=%q: must be a valid integer", v)
		}
		if n < 10 {
			return nil, fmt.Errorf("error: KEEPALIVE_INTERVAL=%q: must be >= 10", v)
		}
		cfg.KeepaliveInterval = n
	}

	// WAN_FAIL_THRESHOLD: consecutive probe/dial failures before a WAN slot
	// is excluded from load balancing and marked draining for replacement.
	cfg.WanFailThreshold = 2
	if v := os.Getenv("WAN_FAIL_THRESHOLD"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: WAN_FAIL_THRESHOLD=%q: must be a valid integer", v)
		}
		if n < 1 {
			return nil, fmt.Errorf("error: WAN_FAIL_THRESHOLD=%q: must be >= 1", v)
		}
		cfg.WanFailThreshold = n
	}

	// STABILITY_PROBES: how many exit-IP probes to run per passed speed
	// test (0 disables stability ranking entirely). Each probe is a GET
	// through the same temp xray; the distinct exit IPs observed become
	// the slot's stability score, used only as a preference when choosing
	// which active WAN to replace — never to reject a config.
	cfg.StabilityProbes = 0
	if v := os.Getenv("STABILITY_PROBES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: STABILITY_PROBES=%q: must be a valid integer", v)
		}
		if n < 0 || n > 5 {
			return nil, fmt.Errorf("error: STABILITY_PROBES=%q: must be between 0 and 5", v)
		}
		cfg.StabilityProbes = n
	}

	// METRICS_PORT: 0 disables the observability server (/metrics, /healthz,
	// /readyz). Any value in 1..65535 starts it on that port.
	cfg.MetricsPort = 0
	if v := os.Getenv("METRICS_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("error: METRICS_PORT=%q: must be a valid integer", v)
		}
		if n < 0 || n > 65535 {
			return nil, fmt.Errorf("error: METRICS_PORT=%q: must be between 0 and 65535", v)
		}
		cfg.MetricsPort = n
	}

	// ACCESS_LOG: enable one structured log line per proxied connection.
	cfg.AccessLog = true
	if v := os.Getenv("ACCESS_LOG"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("error: ACCESS_LOG=%q: must be a boolean (true/false)", v)
		}
		cfg.AccessLog = b
	}

	// ALLOW_DEGRADED_BOOT: start the proxy as soon as the first WAN slot is
	// active instead of waiting for the full WAN_COUNT. When false, the
	// proxy (and observability server) only start after the pool is full.
	cfg.AllowDegradedBoot = true
	if v := os.Getenv("ALLOW_DEGRADED_BOOT"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("error: ALLOW_DEGRADED_BOOT=%q: must be a boolean (true/false)", v)
		}
		cfg.AllowDegradedBoot = b
	}

	// Split routing: ROUTE_MODE picks the policy, DIRECT_DOMAINS /
	// PROXY_DOMAINS / *_LIST_FILE provide suffix lists. Default all-proxy
	// preserves the historical behavior (everything via the WAN pool); a
	// Router is built only when a non-default mode or any list is set.
	mode := RouteAllProxy
	if v := os.Getenv("ROUTE_MODE"); v != "" {
		m, err := ParseRouteMode(v)
		if err != nil {
			return nil, fmt.Errorf("error: %w", err)
		}
		mode = m
	}

	direct := parseSuffixList(os.Getenv("DIRECT_DOMAINS"))
	proxy := parseSuffixList(os.Getenv("PROXY_DOMAINS"))

	if v := os.Getenv("DIRECT_LIST_FILE"); v != "" {
		fromFile, err := loadSuffixFile(v)
		if err != nil {
			return nil, fmt.Errorf("error: DIRECT_LIST_FILE=%q: %w", v, err)
		}
		direct = append(direct, fromFile...)
	}
	if v := os.Getenv("PROXY_LIST_FILE"); v != "" {
		fromFile, err := loadSuffixFile(v)
		if err != nil {
			return nil, fmt.Errorf("error: PROXY_LIST_FILE=%q: %w", v, err)
		}
		proxy = append(proxy, fromFile...)
	}

	if mode != RouteAllProxy || len(direct) > 0 || len(proxy) > 0 {
		cfg.Router = NewRouter(mode, direct, proxy)
	}

	// XRAY_MUX enables xray outbound connection multiplexing (mux). With mux
	// on (default), many client connections share one upstream connection to
	// the proxy server, amortizing the TLS/protocol handshake that otherwise
	// runs per connection — the single biggest lever on per-connection setup
	// latency. Turn it off for workloads dominated by very large transfers.
	cfg.XrayMux = true
	if v := os.Getenv("XRAY_MUX"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("error: XRAY_MUX=%q: must be a boolean (true/false)", v)
		}
		cfg.XrayMux = b
	}

	return cfg, nil
}

func fetchSubscription(url string) []*ProxyConfig {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		slog.Warn("fetch subscription failed", "url", url, "error", err)
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Warn("read subscription body failed", "error", err)
		return nil
	}

	configs := ParseConfigs(string(body))
	slog.Info("fetched subscription", "url", url, "configs", len(configs))
	return configs
}

func buildDownloadURL(cfg *Config, downloadSize int64) string {
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

func startup(cfg *Config, ctx context.Context) {
	slog.Info("starting viberoxy...")

	pool := NewWANPool(cfg.WanCount, cfg.WanBasePort)

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
				Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
				Handler: NewObservabilityHandler(pool),
			}
			go func() {
				if err := obsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					slog.Error("metrics server error", "error", err)
				}
			}()
			slog.Info("metrics server started", "port", cfg.MetricsPort)
		}

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
			if !IsXraySupported(c) {
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

	runLoop(cfg, pool, proxy, ctx)

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

func runLoop(cfg *Config, pool *WANPool, proxy *ProxyServer, ctx context.Context) {
	ticker := time.NewTicker(time.Duration(cfg.FetchInterval) * time.Second)
	defer ticker.Stop()

	gracePeriod := 2 * time.Duration(cfg.FetchInterval) * time.Second
	if gracePeriod < 60*time.Second {
		gracePeriod = 60 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runCycle(cfg, pool, gracePeriod)
		}
	}
}

// keepaliveLoop probes every active/draining WAN through its SOCKS5 listener
// once per KEEPALIVE_INTERVAL. It stops when ctx is cancelled.
func keepaliveLoop(cfg *Config, pool *WANPool, ctx context.Context) {
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
func probeAllWANs(cfg *Config, pool *WANPool) {
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
		slog.Info("keepalive: probe ok", "index", idx)
	}
}

func runCycle(cfg *Config, pool *WANPool, gracePeriod time.Duration) {
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
		if !IsXraySupported(c) {
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

	// Replacement: only consider candidates tested this cycle, and only when
	// the pool is already full — replace a WAN with the best new config we
	// actually measured (fastest; speed ties broken by lower stability
	// score). The slot replaced is the least stable active WAN when scores
	// are known, else the first active slot (historical behavior).
	activeSlots := pool.GetSlotsByState(StateActive)
	if len(activeSlots) == cfg.WanCount && len(results) > 0 {
		alreadyActive := func(cfg *ProxyConfig) bool {
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
	cfg, err := parseConfig()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startup(cfg, ctx)
}
