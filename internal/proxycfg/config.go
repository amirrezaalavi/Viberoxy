package proxycfg

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
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
	// AllowHTTPSubscription opts in to plain http:// SUBSCRIBER_URL values
	// (ALLOW_HTTP_SUBSCRIPTION). https-only by default; false unless the
	// env var explicitly says otherwise.
	AllowHTTPSubscription bool
	Router                *Router
	XrayMux               bool
}

// MaxTestPerCycle bounds how many configs are speed-tested in one runCycle.
// The subscription is latency-sorted, so testing beyond this is wasted work.
func (c *Config) MaxTestPerCycleVal() int {
	if c.MaxTestPerCycle > 0 {
		return c.MaxTestPerCycle
	}
	return 20
}

func ParseConfig() (*Config, error) {
	cfg := &Config{}

	// ALLOW_HTTP_SUBSCRIPTION: opt in to a plain http:// SUBSCRIBER_URL.
	// Subscriptions are fetched over https only unless this is true
	// (F-18). Parsed before SUBSCRIBER_URL so the scheme policy below can
	// enforce it; unparsable values hard-exit like every other knob.
	cfg.AllowHTTPSubscription = false
	if v := os.Getenv("ALLOW_HTTP_SUBSCRIPTION"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("error: ALLOW_HTTP_SUBSCRIPTION=%q: must be a boolean (true/false)", v)
		}
		cfg.AllowHTTPSubscription = b
	}

	cfg.SubscriberURL = os.Getenv("SUBSCRIBER_URL")
	if cfg.SubscriberURL == "" {
		return nil, fmt.Errorf("error: SUBSCRIBER_URL=%q: must be set to a valid HTTP/HTTPS URL", cfg.SubscriberURL)
	}
	u, err := url.Parse(cfg.SubscriberURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("error: SUBSCRIBER_URL=%q: must be a valid HTTP/HTTPS URL", cfg.SubscriberURL)
	}
	if u.Scheme == "http" && !cfg.AllowHTTPSubscription {
		return nil, fmt.Errorf("error: SUBSCRIBER_URL=%q: plain http:// is refused by default, set ALLOW_HTTP_SUBSCRIPTION=true to allow it", cfg.SubscriberURL)
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
