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
	MetricsPort       int
	MinimumSpeed      float64
	MaxTestPerCycle   int
	KeepaliveInterval int
	WanFailThreshold  int
	AccessLog         bool
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

func startup(cfg *Config, ctx context.Context) {
	slog.Info("starting viberoxy...")

	pool := NewWANPool(cfg.WanCount, cfg.WanBasePort)

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
		var tested int
		for _, c := range configs {
			if pool.ActiveCount() >= cfg.WanCount {
				break
			}
			result := TestSpeed(c, cfg.TestBasePort+tested, time.Duration(cfg.TestTimeout)*time.Second, buildDownloadURL(cfg, cfg.DownloadSize), cfg.DownloadSize)
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
			cmd, path, err := StartXray(c, cfg.WanBasePort+slotIdx)
			if err != nil {
				slog.Error("startup: xray failed", "error", err)
				pool.ResetEmpty(slotIdx)
				continue
			}
			pool.SetActive(slotIdx, cmd, path)
			pool.SetSlotSpeedMbps(slotIdx, result.Speed)
			slog.Info("startup: wan active", "index", slotIdx, "server", c.Server, "speed", result.Speed)
		}

		if pool.ActiveCount() >= cfg.WanCount {
			break
		}

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

	proxy := NewProxyServer(cfg.ProxyPort, pool)
	proxy.AccessLog = cfg.AccessLog
	if cfg.WanFailThreshold > 0 {
		proxy.WanFailThreshold = cfg.WanFailThreshold
	}
	go func() {
		if err := proxy.Start(ctx); err != nil && err != http.ErrServerClosed {
			slog.Error("proxy server error", "error", err)
		}
	}()

	var obsSrv *http.Server
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

	slog.Info("viberoxy started", "wans", pool.ActiveCount(), "proxy_port", cfg.ProxyPort)

	if cfg.KeepaliveInterval > 0 {
		go keepaliveLoop(cfg, pool, ctx)
	}

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
	for _, c := range configs {
		if pool.ActiveCount() >= cfg.WanCount && len(pool.GetSlotsByState(StateEmpty)) == 0 {
			break
		}
		if tested >= cfg.MaxTestPerCycleVal() {
			break
		}
		result := TestSpeed(c, cfg.TestBasePort+tested, time.Duration(cfg.TestTimeout)*time.Second, buildDownloadURL(cfg, cfg.DownloadSize), cfg.DownloadSize)
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
		cmd, path, err := StartXray(result.Config, cfg.WanBasePort+slotIdx)
		if err != nil {
			slog.Warn("cycle: xray start failed", "error", err)
			pool.ResetEmpty(slotIdx)
			continue
		}
		pool.SetActive(slotIdx, cmd, path)
		pool.SetSlotSpeedMbps(slotIdx, result.Speed)
		slog.Info("cycle: filled empty slot", "index", slotIdx, "server", result.Config.Server, "speed", result.Speed)
	}

	writeSortedTxt(results)

	// Replacement: only consider candidates tested this cycle, and only when
	// the pool is already full — replace the first active WAN with the best
	// new config we actually measured.
	activeSlots := pool.GetSlotsByState(StateActive)
	if len(activeSlots) == cfg.WanCount && len(results) > 0 {
		var bestNewConfig *ProxyConfig
		var bestNewSpeed float64
		for _, r := range results {
			if r.Error != nil || r.Speed < cfg.MinimumSpeed {
				continue
			}
			alreadyActive := false
			for _, idx := range activeSlots {
				activeCfg := pool.Slots[idx].Config
				if activeCfg != nil && activeCfg.Server == r.Config.Server && activeCfg.Port == r.Config.Port {
					alreadyActive = true
					break
				}
			}
			if alreadyActive {
				continue
			}
			bestNewConfig = r.Config
			bestNewSpeed = r.Speed
			break
		}

		if bestNewConfig != nil {
			replaceIdx := activeSlots[0]
			slog.Info("cycle: replacing wan",
				"index", replaceIdx,
				"new_server", bestNewConfig.Server,
				"new_speed", bestNewSpeed)
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
