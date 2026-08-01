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
	SubscriberURL    string
	FetchInterval    int
	TestTimeout      int
	DownloadSize     int64
	DownloadEndpoint string
	DownloadFallback string
	WanCount         int
	WanBasePort      int
	TestBasePort     int
	ProxyPort        int
	MinimumSpeed     float64
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
	go func() {
		if err := proxy.Start(ctx); err != nil && err != http.ErrServerClosed {
			slog.Error("proxy server error", "error", err)
		}
	}()

	slog.Info("viberoxy started", "wans", pool.ActiveCount(), "proxy_port", cfg.ProxyPort)

	runLoop(cfg, pool, proxy, ctx)

	slog.Info("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proxy.Stop(shutdownCtx)
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

	results := TestAll(configs, cfg.TestBasePort, time.Duration(cfg.TestTimeout)*time.Second, buildDownloadURL(cfg, cfg.DownloadSize), cfg.DownloadSize)
	results = SortResults(results)

	writeSortedTxt(results)

	for _, r := range results {
		emptySlots := pool.GetSlotsByState(StateEmpty)
		if len(emptySlots) == 0 {
			break
		}
		if r.Error != nil || r.Speed < cfg.MinimumSpeed {
			continue
		}
		slotIdx := emptySlots[0]
		pool.StartTesting(slotIdx, r.Config)
		cmd, path, err := StartXray(r.Config, cfg.WanBasePort+slotIdx)
		if err != nil {
			slog.Warn("cycle: xray start failed", "error", err)
			pool.ResetEmpty(slotIdx)
			continue
		}
		pool.SetActive(slotIdx, cmd, path)
		slog.Info("cycle: filled empty slot", "index", slotIdx, "server", r.Config.Server, "speed", r.Speed)
	}

	activeSlots := pool.GetSlotsByState(StateActive)
	if len(activeSlots) > 0 {
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

	slog.Info("cycle: complete", "active", pool.ActiveCount())
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
