package proxycfg

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"viberoxy/internal/auth"
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

	// F-14 security configuration (all from env, hard-exit validated).
	// ListenAddr is the bind host for the proxy, SOCKS5, API and metrics
	// listeners: loopback (127.0.0.1) unless LISTEN_ADDR overrides it, and
	// a non-loopback value is refused at startup unless ProxyUsers or
	// APIToken authenticates the service or AllowPublic opts in.
	ListenAddr string
	// ProxyUsers are the PROXY_USERS user:password pairs enabling proxy
	// authentication (CONNECT 407 / SOCKS5 RFC 1929). The front-ends re-read
	// the env at request time (dual-read, like subs.AllowHTTPFromEnv); the
	// parsed value here exists for startup validation and the bind gate.
	ProxyUsers []auth.UserCred
	// APIToken gates /api/* and the metrics endpoint (Bearer).
	APIToken string
	// AllowPublic accepts a non-loopback LISTEN_ADDR without authentication.
	AllowPublic bool
	// AllowPrivateTargets opts in to proxying loopback/link-local/private
	// destinations (SSRF guard). Blocked by default.
	AllowPrivateTargets bool
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

	// XRAY_MUX enables xray outbound connection multiplexing (mux). D-04:
	// mux is OFF by default; set XRAY_MUX=true to opt in. With mux on, many
	// client connections share one upstream connection to the proxy server,
	// amortizing the TLS/protocol handshake that otherwise runs per
	// connection — the single biggest lever on per-connection setup latency.
	// Keep it off for workloads dominated by very large single transfers
	// (mux adds a framing hop that costs throughput there).
	cfg.XrayMux = false
	if v := os.Getenv("XRAY_MUX"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("error: XRAY_MUX=%q: must be a boolean (true/false)", v)
		}
		cfg.XrayMux = b
	}

	// ---- F-14 security hardening ----

	// LISTEN_ADDR: bind host for the proxy, SOCKS5, API and metrics
	// listeners. Defaults to loopback so a fresh install is never exposed;
	// must be an IP literal (hard-exit on anything else).
	cfg.ListenAddr = auth.LoopbackHost
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		if net.ParseIP(v) == nil {
			return nil, fmt.Errorf("error: LISTEN_ADDR=%q: must be a valid IP address", v)
		}
		cfg.ListenAddr = v
	}

	// PROXY_USERS: comma-separated user:password entries enabling proxy
	// authentication (CONNECT 407, SOCKS5 RFC 1929). The error never echoes
	// the value: it contains passwords and is logged on hard exit.
	cfg.ProxyUsers = nil
	if v := os.Getenv("PROXY_USERS"); v != "" {
		users, err := auth.ParseUsers(v)
		if err != nil {
			return nil, fmt.Errorf("error: PROXY_USERS: %w", err)
		}
		cfg.ProxyUsers = users
	}

	// API_TOKEN: bearer token gating /api/* and the metrics endpoint.
	// Whitespace would make the Authorization header unparseable; the error
	// must not echo the token itself.
	cfg.APIToken = ""
	if v := os.Getenv("API_TOKEN"); v != "" {
		if strings.ContainsAny(v, " 	\r\n") {
			return nil, fmt.Errorf("error: API_TOKEN must not contain whitespace")
		}
		cfg.APIToken = v
	}

	// ALLOW_PUBLIC: explicit opt-in to a non-loopback LISTEN_ADDR without
	// authentication (useful behind another access-control layer).
	cfg.AllowPublic = false
	if v := os.Getenv("ALLOW_PUBLIC"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("error: ALLOW_PUBLIC=%q: must be a boolean (true/false)", v)
		}
		cfg.AllowPublic = b
	}

	// ALLOW_PRIVATE_TARGETS: opt-in to proxying connections to loopback/
	// link-local/private destinations. Blocked by default (SSRF guard).
	cfg.AllowPrivateTargets = false
	if v := os.Getenv("ALLOW_PRIVATE_TARGETS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("error: ALLOW_PRIVATE_TARGETS=%q: must be a boolean (true/false)", v)
		}
		cfg.AllowPrivateTargets = b
	}

	// The non-loopback gate runs after every opt-in above is parsed: a
	// public LISTEN_ADDR needs authentication (PROXY_USERS / API_TOKEN) or
	// an explicit ALLOW_PUBLIC=true. The listeners re-check the same policy
	// at bind time (auth.EnsureBindAllowed) as defense in depth.
	if !auth.IsLoopbackHost(cfg.ListenAddr) &&
		!auth.BindAllowed(len(cfg.ProxyUsers) > 0, cfg.APIToken != "", cfg.AllowPublic) {
		return nil, fmt.Errorf("error: LISTEN_ADDR=%q: non-loopback bind refused without authentication: set PROXY_USERS or API_TOKEN, or ALLOW_PUBLIC=true to accept public exposure", cfg.ListenAddr)
	}

	return cfg, nil
}
