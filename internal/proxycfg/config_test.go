package proxycfg

import (
	"os"
	"testing"
)

func setenv(t *testing.T, key, value string) {
	t.Helper()
	orig, ok := os.LookupEnv(key)
	os.Setenv(key, value)
	t.Cleanup(func() {
		if ok {
			os.Setenv(key, orig)
		} else {
			os.Unsetenv(key)
		}
	})
}

func unsetenv(t *testing.T, key string) {
	t.Helper()
	orig, ok := os.LookupEnv(key)
	os.Unsetenv(key)
	t.Cleanup(func() {
		if ok {
			os.Setenv(key, orig)
		}
	})
}

func TestParseConfig_Defaults(t *testing.T) {
	for _, key := range []string{
		"SUBSCRIBER_URL",
		"FETCH_INTERVAL",
		"TEST_TIMEOUT",
		"DOWNLOAD_SIZE",
		"DOWNLOAD_ENDPOINT",
		"DOWNLOAD_FALLBACK",
		"WAN_COUNT",
		"WAN_BASE_PORT",
		"TEST_BASE_PORT",
		"PROXY_PORT",
		"SOCKS_PORT",
		"MINIMUM_SPEED",
		"METRICS_PORT",
		"ACCESS_LOG",
		"KEEPALIVE_INTERVAL",
		"WAN_FAIL_THRESHOLD",
		"STABILITY_PROBES",
		"ALLOW_DEGRADED_BOOT",
		"XRAY_MUX",
	} {
		unsetenv(t, key)
	}
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}

	if cfg.SubscriberURL != "https://example.com/sub" {
		t.Errorf("SubscriberURL = %q, want %q", cfg.SubscriberURL, "https://example.com/sub")
	}
	if cfg.FetchInterval != 300 {
		t.Errorf("FetchInterval = %d, want %d", cfg.FetchInterval, 300)
	}
	if cfg.TestTimeout != 10 {
		t.Errorf("TestTimeout = %d, want %d", cfg.TestTimeout, 10)
	}
	if cfg.DownloadSize != 10000000 {
		t.Errorf("DownloadSize = %d, want %d", cfg.DownloadSize, 10000000)
	}
	if cfg.DownloadEndpoint != "https://speed.cloudflare.com/__down?bytes=" {
		t.Errorf("DownloadEndpoint = %q, want %q", cfg.DownloadEndpoint, "https://speed.cloudflare.com/__down?bytes=")
	}
	if cfg.DownloadFallback != "https://proof.ovh.net/files/" {
		t.Errorf("DownloadFallback = %q, want %q", cfg.DownloadFallback, "https://proof.ovh.net/files/")
	}
	if cfg.WanCount != 4 {
		t.Errorf("WanCount = %d, want %d", cfg.WanCount, 4)
	}
	if cfg.WanBasePort != 10700 {
		t.Errorf("WanBasePort = %d, want %d", cfg.WanBasePort, 10700)
	}
	if cfg.TestBasePort != 10800 {
		t.Errorf("TestBasePort = %d, want %d", cfg.TestBasePort, 10800)
	}
	if cfg.ProxyPort != 1080 {
		t.Errorf("ProxyPort = %d, want %d", cfg.ProxyPort, 1080)
	}
	if cfg.SocksPort != 0 {
		t.Errorf("SocksPort = %d, want 0 (off)", cfg.SocksPort)
	}
	if cfg.MinimumSpeed != 5.0 {
		t.Errorf("MinimumSpeed = %f, want %f", cfg.MinimumSpeed, 5.0)
	}
	if cfg.MetricsPort != 0 {
		t.Errorf("MetricsPort = %d, want 0 (off)", cfg.MetricsPort)
	}
	if cfg.KeepaliveInterval != 300 {
		t.Errorf("KeepaliveInterval = %d, want %d", cfg.KeepaliveInterval, 300)
	}
	if cfg.WanFailThreshold != 2 {
		t.Errorf("WanFailThreshold = %d, want %d", cfg.WanFailThreshold, 2)
	}
	if cfg.StabilityProbes != 0 {
		t.Errorf("StabilityProbes = %d, want 0 (disabled by default)", cfg.StabilityProbes)
	}
	if !cfg.AccessLog {
		t.Error("AccessLog = false, want true (default)")
	}
	if !cfg.AllowDegradedBoot {
		t.Error("AllowDegradedBoot = false, want true (default)")
	}
	if !cfg.XrayMux {
		t.Error("XrayMux = false, want true (default)")
	}
}

func TestParseConfig_MuxOverrides(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")

	setenv(t, "XRAY_MUX", "false")
	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.XrayMux {
		t.Error("XrayMux = true, want false (XRAY_MUX=false)")
	}

	setenv(t, "XRAY_MUX", "true")
	cfg, err = ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if !cfg.XrayMux {
		t.Error("XrayMux = false, want true (XRAY_MUX=true)")
	}
}

func TestParseConfig_InvalidMux(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "XRAY_MUX", "not-a-bool")

	if _, err := ParseConfig(); err == nil {
		t.Error("expected error for invalid XRAY_MUX value")
	}
}

func TestParseConfig_ValidOverrides(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "FETCH_INTERVAL", "60")
	setenv(t, "TEST_TIMEOUT", "15")
	setenv(t, "DOWNLOAD_SIZE", "5000000")
	setenv(t, "WAN_COUNT", "2")
	setenv(t, "PROXY_PORT", "8080")
	setenv(t, "MINIMUM_SPEED", "10.5")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}

	if cfg.SubscriberURL != "https://example.com/sub" {
		t.Errorf("SubscriberURL = %q, want %q", cfg.SubscriberURL, "https://example.com/sub")
	}
	if cfg.FetchInterval != 60 {
		t.Errorf("FetchInterval = %d, want %d", cfg.FetchInterval, 60)
	}
	if cfg.TestTimeout != 15 {
		t.Errorf("TestTimeout = %d, want %d", cfg.TestTimeout, 15)
	}
	if cfg.DownloadSize != 5000000 {
		t.Errorf("DownloadSize = %d, want %d", cfg.DownloadSize, 5000000)
	}
	if cfg.WanCount != 2 {
		t.Errorf("WanCount = %d, want %d", cfg.WanCount, 2)
	}
	if cfg.ProxyPort != 8080 {
		t.Errorf("ProxyPort = %d, want %d", cfg.ProxyPort, 8080)
	}
	if cfg.MinimumSpeed != 10.5 {
		t.Errorf("MinimumSpeed = %f, want %f", cfg.MinimumSpeed, 10.5)
	}
}

func TestParseConfig_KeepaliveOverrides(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "KEEPALIVE_INTERVAL", "60")
	setenv(t, "WAN_FAIL_THRESHOLD", "5")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.KeepaliveInterval != 60 {
		t.Errorf("KeepaliveInterval = %d, want 60", cfg.KeepaliveInterval)
	}
	if cfg.WanFailThreshold != 5 {
		t.Errorf("WanFailThreshold = %d, want 5", cfg.WanFailThreshold)
	}
}

func TestParseConfig_InvalidKeepaliveInterval(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "KEEPALIVE_INTERVAL", "5")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error for KEEPALIVE_INTERVAL < 10, got nil")
	}
}

func TestParseConfig_InvalidWanFailThreshold(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "WAN_FAIL_THRESHOLD", "0")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error for WAN_FAIL_THRESHOLD < 1, got nil")
	}
}

func TestParseConfig_StabilityProbesValid(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "STABILITY_PROBES", "3")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.StabilityProbes != 3 {
		t.Errorf("StabilityProbes = %d, want 3", cfg.StabilityProbes)
	}
}

func TestParseConfig_StabilityProbesZero(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "STABILITY_PROBES", "0")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.StabilityProbes != 0 {
		t.Errorf("StabilityProbes = %d, want 0", cfg.StabilityProbes)
	}
}

func TestParseConfig_InvalidStabilityProbes(t *testing.T) {
	for _, v := range []string{"abc", "6", "-1", "2.5"} {
		t.Run(v, func(t *testing.T) {
			setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
			setenv(t, "STABILITY_PROBES", v)

			if _, err := ParseConfig(); err == nil {
				t.Fatalf("expected error for STABILITY_PROBES=%q, got nil", v)
			}
		})
	}
}

func TestParseConfig_InvalidTimeout(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "TEST_TIMEOUT", "1")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_InvalidWanCount(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "WAN_COUNT", "10")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_SocksPortValid(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "SOCKS_PORT", "1081")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.SocksPort != 1081 {
		t.Errorf("SocksPort = %d, want 1081", cfg.SocksPort)
	}
}

func TestParseConfig_SocksPortZero(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "SOCKS_PORT", "0")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.SocksPort != 0 {
		t.Errorf("SocksPort = %d, want 0 (disabled)", cfg.SocksPort)
	}
}

func TestParseConfig_InvalidSocksPort(t *testing.T) {
	for _, v := range []string{"abc", "65536", "-1", "1.5"} {
		t.Run(v, func(t *testing.T) {
			setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
			setenv(t, "SOCKS_PORT", v)

			if _, err := ParseConfig(); err == nil {
				t.Fatalf("expected error for SOCKS_PORT=%q, got nil", v)
			}
		})
	}
}

func TestParseConfig_MissingSubUrl(t *testing.T) {
	unsetenv(t, "SUBSCRIBER_URL")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_BadSubUrl(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "ftp://bad")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_ZeroDownloadSize(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "DOWNLOAD_SIZE", "0")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_NegativeMinimumSpeed(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "MINIMUM_SPEED", "-1")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_EmptyDownloadEndpoint(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "DOWNLOAD_ENDPOINT", "")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.DownloadEndpoint != "" {
		t.Errorf("DownloadEndpoint = %q, want empty string", cfg.DownloadEndpoint)
	}
}

func TestParseConfig_AllowDegradedBootFalse(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ALLOW_DEGRADED_BOOT", "false")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.AllowDegradedBoot {
		t.Error("AllowDegradedBoot = true, want false")
	}
}

func TestParseConfig_AllowDegradedBootTrue(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ALLOW_DEGRADED_BOOT", "true")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if !cfg.AllowDegradedBoot {
		t.Error("AllowDegradedBoot = false, want true")
	}
}

func TestParseConfig_InvalidAllowDegradedBoot(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ALLOW_DEGRADED_BOOT", "maybe")

	_, err := ParseConfig()
	if err == nil {
		t.Fatal("expected error for ALLOW_DEGRADED_BOOT=maybe, got nil")
	}
}

func TestParseConfig_RouterDefaultNil(t *testing.T) {
	for _, key := range []string{"ROUTE_MODE", "DIRECT_DOMAINS", "PROXY_DOMAINS", "DIRECT_LIST_FILE", "PROXY_LIST_FILE"} {
		unsetenv(t, key)
	}
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.Router != nil {
		t.Error("Router = non-nil, want nil (all-proxy default)")
	}
}

func TestParseConfig_RouterProxyDefault(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ROUTE_MODE", "proxy-default")
	setenv(t, "DIRECT_DOMAINS", ".ir, example.com")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.Router == nil {
		t.Fatal("Router = nil, want non-nil")
	}
	if cfg.Router.Mode != RouteProxyDefault {
		t.Errorf("Mode = %q, want proxy-default", cfg.Router.Mode)
	}
	if got := cfg.Router.Decide("somedomain.ir"); got != RouteDirect {
		t.Errorf("Decide(.ir) = %v, want direct", got)
	}
	if got := cfg.Router.Decide("google.com"); got != RouteWAN {
		t.Errorf("Decide(google.com) = %v, want wan", got)
	}
}

func TestParseConfig_RouterDirectDefault(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ROUTE_MODE", "direct-default")
	setenv(t, "PROXY_DOMAINS", ".google.com")

	cfg, err := ParseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.Router == nil {
		t.Fatal("Router = nil, want non-nil")
	}
	if cfg.Router.Mode != RouteDirectDefault {
		t.Errorf("Mode = %q, want direct-default", cfg.Router.Mode)
	}
	if got := cfg.Router.Decide("www.google.com"); got != RouteWAN {
		t.Errorf("Decide(www.google.com) = %v, want wan", got)
	}
	if got := cfg.Router.Decide("github.com"); got != RouteDirect {
		t.Errorf("Decide(github.com) = %v, want direct", got)
	}
}

func TestParseConfig_RouterInvalidMode(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ROUTE_MODE", "bogus-mode")

	if _, err := ParseConfig(); err == nil {
		t.Error("expected error for invalid ROUTE_MODE")
	}
}

func TestParseConfig_RouterMissingListFile(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ROUTE_MODE", "proxy-default")
	setenv(t, "DIRECT_LIST_FILE", "/nonexistent/direct.txt")

	if _, err := ParseConfig(); err == nil {
		t.Error("expected error for missing DIRECT_LIST_FILE")
	}
}
