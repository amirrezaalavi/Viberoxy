package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

	cfg, err := parseConfig()
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
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.XrayMux {
		t.Error("XrayMux = true, want false (XRAY_MUX=false)")
	}

	setenv(t, "XRAY_MUX", "true")
	cfg, err = parseConfig()
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

	if _, err := parseConfig(); err == nil {
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

	cfg, err := parseConfig()
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

	cfg, err := parseConfig()
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

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error for KEEPALIVE_INTERVAL < 10, got nil")
	}
}

func TestParseConfig_InvalidWanFailThreshold(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "WAN_FAIL_THRESHOLD", "0")

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error for WAN_FAIL_THRESHOLD < 1, got nil")
	}
}

func TestParseConfig_StabilityProbesValid(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "STABILITY_PROBES", "3")

	cfg, err := parseConfig()
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

	cfg, err := parseConfig()
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

			if _, err := parseConfig(); err == nil {
				t.Fatalf("expected error for STABILITY_PROBES=%q, got nil", v)
			}
		})
	}
}

func TestParseConfig_InvalidTimeout(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "TEST_TIMEOUT", "1")

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_InvalidWanCount(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "WAN_COUNT", "10")

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_SocksPortValid(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "SOCKS_PORT", "1081")

	cfg, err := parseConfig()
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

	cfg, err := parseConfig()
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

			if _, err := parseConfig(); err == nil {
				t.Fatalf("expected error for SOCKS_PORT=%q, got nil", v)
			}
		})
	}
}

func TestParseConfig_MissingSubUrl(t *testing.T) {
	unsetenv(t, "SUBSCRIBER_URL")

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_BadSubUrl(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "ftp://bad")

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_ZeroDownloadSize(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "DOWNLOAD_SIZE", "0")

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_NegativeMinimumSpeed(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "MINIMUM_SPEED", "-1")

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseConfig_EmptyDownloadEndpoint(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "DOWNLOAD_ENDPOINT", "")

	cfg, err := parseConfig()
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

	cfg, err := parseConfig()
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

	cfg, err := parseConfig()
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

	_, err := parseConfig()
	if err == nil {
		t.Fatal("expected error for ALLOW_DEGRADED_BOOT=maybe, got nil")
	}
}

// consecutiveFreePorts returns a port whose next n-1 ports are also free, so
// callers can hand it to configs that bind base+index.
func consecutiveFreePorts(t *testing.T, n int) int {
	t.Helper()
	for {
		base := freePort(t)
		ok := true
		for i := 1; i < n; i++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				ok = false
				break
			}
			l.Close()
		}
		if ok {
			return base
		}
	}
}

// TestStartup_DegradedBoot verifies that with ALLOW_DEGRADED_BOOT the proxy
// starts as soon as the first WAN slot is active, before the pool reaches the
// full WAN_COUNT. The second speed test is gated behind a channel: while it
// is blocked, the pool has exactly one active WAN, so the proxy port becoming
// reachable proves degraded boot. Requires a real xray binary (like
// TestTestSpeed); skipped otherwise.
func TestStartup_DegradedBoot(t *testing.T) {
	if _, err := exec.LookPath("xray"); err != nil {
		t.Skip("xray not found in PATH, skipping degraded boot integration test")
	}

	// Two distinct upstreams so both WAN slots fill without tripping the
	// server:port dedupe.
	socksA, addrA := startTestSocksServer(t)
	defer socksA.Close()
	socksB, addrB := startTestSocksServer(t)
	defer socksB.Close()
	hostA, portStrA, err := net.SplitHostPort(addrA)
	if err != nil {
		t.Fatalf("split socksA addr: %v", err)
	}
	portA, _ := strconv.Atoi(portStrA)
	hostB, portStrB, err := net.SplitHostPort(addrB)
	if err != nil {
		t.Fatalf("split socksB addr: %v", err)
	}
	portB, _ := strconv.Atoi(portStrB)

	// The download server blocks the SECOND speed test until the gate is
	// closed. While blocked, only WAN slot 0 is active.
	var mu sync.Mutex
	requests := 0
	gate := make(chan struct{})
	downloadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		first := requests == 1
		mu.Unlock()
		if !first {
			<-gate
		}
		w.Header().Set("Content-Length", "50000")
		w.Write(testDownloadData[:50000])
	}))
	defer downloadServer.Close()

	sub := fmt.Sprintf("socks5://%s:%d#wan-a\nsocks5://%s:%d#wan-b\n", hostA, portA, hostB, portB)
	encoded := base64.StdEncoding.EncodeToString([]byte(sub))
	subServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer subServer.Close()

	cfg := &Config{
		SubscriberURL:     subServer.URL,
		FetchInterval:     300,
		TestTimeout:       10,
		DownloadSize:      50000,
		DownloadEndpoint:  downloadServer.URL,
		DownloadFallback:  downloadServer.URL,
		WanCount:          2,
		WanBasePort:       consecutiveFreePorts(t, 2),
		TestBasePort:      consecutiveFreePorts(t, 2),
		ProxyPort:         freePort(t),
		MinimumSpeed:      0.1,
		AllowDegradedBoot: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		startup(cfg, ctx)
	}()

	// The proxy must come up while only 1 of 2 WANs is active (the second
	// speed test is still blocked on the gate).
	proxyAddr := fmt.Sprintf("127.0.0.1:%d", cfg.ProxyPort)
	if err := waitForPort(proxyAddr, 15*time.Second); err != nil {
		close(gate)
		cancel()
		<-done
		t.Fatalf("proxy did not start with 1 of %d WANs active (degraded boot): %v", cfg.WanCount, err)
	}

	// Release the second speed test; the pool fills to WAN_COUNT and startup
	// enters the run loop.
	close(gate)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not return within 5s after context cancel")
	}
}

// TestStartup_NoDegradedBoot_PoolMustFill verifies that with
// ALLOW_DEGRADED_BOOT=false the proxy does NOT start with a single active
// WAN: while the second speed test is gated, the proxy port must stay closed.
// Requires a real xray binary; skipped otherwise.
func TestStartup_NoDegradedBoot_PoolMustFill(t *testing.T) {
	if _, err := exec.LookPath("xray"); err != nil {
		t.Skip("xray not found in PATH, skipping degraded boot integration test")
	}

	socksA, addrA := startTestSocksServer(t)
	defer socksA.Close()
	socksB, addrB := startTestSocksServer(t)
	defer socksB.Close()
	hostA, portStrA, err := net.SplitHostPort(addrA)
	if err != nil {
		t.Fatalf("split socksA addr: %v", err)
	}
	portA, _ := strconv.Atoi(portStrA)
	hostB, portStrB, err := net.SplitHostPort(addrB)
	if err != nil {
		t.Fatalf("split socksB addr: %v", err)
	}
	portB, _ := strconv.Atoi(portStrB)

	var mu sync.Mutex
	requests := 0
	gate := make(chan struct{})
	downloadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		first := requests == 1
		mu.Unlock()
		if !first {
			<-gate
		}
		w.Header().Set("Content-Length", "50000")
		w.Write(testDownloadData[:50000])
	}))
	defer downloadServer.Close()

	sub := fmt.Sprintf("socks5://%s:%d#wan-a\nsocks5://%s:%d#wan-b\n", hostA, portA, hostB, portB)
	encoded := base64.StdEncoding.EncodeToString([]byte(sub))
	subServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer subServer.Close()

	cfg := &Config{
		SubscriberURL:     subServer.URL,
		FetchInterval:     300,
		TestTimeout:       10,
		DownloadSize:      50000,
		DownloadEndpoint:  downloadServer.URL,
		DownloadFallback:  downloadServer.URL,
		WanCount:          2,
		WanBasePort:       consecutiveFreePorts(t, 2),
		TestBasePort:      consecutiveFreePorts(t, 2),
		ProxyPort:         freePort(t),
		MinimumSpeed:      0.1,
		AllowDegradedBoot: false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		startup(cfg, ctx)
	}()

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", cfg.ProxyPort)
	// Give the first test + activation plenty of time; the proxy must NOT
	// be listening while the second WAN is still gated.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", proxyAddr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			close(gate)
			cancel()
			<-done
			t.Fatalf("proxy started before pool was full (degraded boot disabled)")
		}
		time.Sleep(100 * time.Millisecond)
	}

	close(gate)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not return within 5s after context cancel")
	}
}

func TestFetchSubscription(t *testing.T) {
	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS",
		"trojan://password123@5.6.7.8:443#TestTrojan",
	}
	rawData := strings.Join(lines, "\n")
	encoded := base64.StdEncoding.EncodeToString([]byte(rawData))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer server.Close()

	configs := fetchSubscription(server.URL)
	if len(configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(configs))
	}
	if configs[0].Server != "1.2.3.4" || configs[0].Port != 12345 {
		t.Errorf("config[0] = %s:%d, want 1.2.3.4:12345", configs[0].Server, configs[0].Port)
	}
	if configs[1].Server != "5.6.7.8" || configs[1].Port != 443 {
		t.Errorf("config[1] = %s:%d, want 5.6.7.8:443", configs[1].Server, configs[1].Port)
	}
}

func TestFetchSubscription_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	configs := fetchSubscription(server.URL)
	if configs != nil {
		t.Errorf("expected nil on server error, got %d configs", len(configs))
	}
}

func TestFetchSubscription_InvalidURL(t *testing.T) {
	configs := fetchSubscription("http://127.0.0.1:1")
	if configs != nil {
		t.Errorf("expected nil on connection error, got %d configs", len(configs))
	}
}

func TestWriteSortedTxt(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	results := []*TestResult{
		{Config: &ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 12345}, Speed: 50.0},
		{Config: &ProxyConfig{Protocol: "trojan", Server: "5.6.7.8", Port: 443}, Speed: 30.0, Error: fmt.Errorf("timeout")},
		{Config: &ProxyConfig{Protocol: "vmess", Server: "9.10.11.12", Port: 8080}, Speed: 10.0},
	}

	writeSortedTxt(results)

	f, err := os.Open("sorted.txt")
	if err != nil {
		t.Fatalf("open sorted.txt: %v", err)
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}

	if lines[0] != "ss://1.2.3.4:12345 speed=50.00" {
		t.Errorf("line[0] = %q, want %q", lines[0], "ss://1.2.3.4:12345 speed=50.00")
	}
	if !strings.Contains(lines[1], "trojan://5.6.7.8:443 error=timeout") {
		t.Errorf("line[1] = %q, want containing 'trojan://5.6.7.8:443 error=timeout'", lines[1])
	}
	if lines[2] != "vmess://9.10.11.12:8080 speed=10.00" {
		t.Errorf("line[2] = %q, want %q", lines[2], "vmess://9.10.11.12:8080 speed=10.00")
	}
}

func TestWriteSortedTxt_Empty(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	writeSortedTxt(nil)

	if _, err := os.Stat("sorted.txt"); err != nil {
		t.Fatalf("sorted.txt should exist: %v", err)
	}
}

func TestBuildDownloadURL_WithEndpoint(t *testing.T) {
	cfg := &Config{
		DownloadEndpoint: "https://speed.cloudflare.com/__down?bytes=",
		DownloadFallback: "https://proof.ovh.net/files/",
	}
	url := buildDownloadURL(cfg, 5000000)
	expected := "https://speed.cloudflare.com/__down?bytes=5000000"
	if url != expected {
		t.Errorf("buildDownloadURL = %q, want %q", url, expected)
	}
}

func TestBuildDownloadURL_Fallback(t *testing.T) {
	cfg := &Config{
		DownloadEndpoint: "",
		DownloadFallback: "https://proof.ovh.net/files/",
	}
	url := buildDownloadURL(cfg, 5000000)
	if url != "https://proof.ovh.net/files/" {
		t.Errorf("buildDownloadURL = %q, want %q", url, "https://proof.ovh.net/files/")
	}
}

func TestBuildDownloadURL_WithCustomEndpoint(t *testing.T) {
	cfg := &Config{
		DownloadEndpoint: "https://custom.example.com/dl?size=",
		DownloadFallback: "https://proof.ovh.net/files/",
	}
	url := buildDownloadURL(cfg, 10000000)
	expected := "https://custom.example.com/dl?size=10000000"
	if url != expected {
		t.Errorf("buildDownloadURL = %q, want %q", url, expected)
	}
}

func TestStartup_Shutdown(t *testing.T) {
	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS",
	}
	rawData := strings.Join(lines, "\n")
	encoded := base64.StdEncoding.EncodeToString([]byte(rawData))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())

	cfg := &Config{
		SubscriberURL:    server.URL,
		FetchInterval:    300,
		TestTimeout:      3,
		DownloadSize:     10000000,
		DownloadEndpoint: server.URL + "?bytes=",
		DownloadFallback: server.URL,
		WanCount:         1,
		WanBasePort:      20700,
		TestBasePort:     20800,
		ProxyPort:        0,
		MinimumSpeed:     1e9,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		startup(cfg, ctx)
	}()

	time.Sleep(200 * time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not return within 5s after context cancel")
	}
}

func TestRunCycle_PopulatesCandidatePool(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS",
	}
	rawData := strings.Join(lines, "\n")
	encoded := base64.StdEncoding.EncodeToString([]byte(rawData))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer server.Close()

	cfg := &Config{
		SubscriberURL:    server.URL,
		FetchInterval:    300,
		TestTimeout:      3,
		DownloadSize:     10000000,
		DownloadEndpoint: server.URL + "?bytes=",
		DownloadFallback: server.URL,
		WanCount:         1,
		WanBasePort:      20700,
		TestBasePort:     20800,
		ProxyPort:        0,
		MinimumSpeed:     1e9,
	}

	pool := NewWANPool(1, 20700)
	candidatePool := NewCandidatePool(50)

	runCycle(cfg, pool, candidatePool, 60*time.Second)

	if candidatePool.Len() == 0 {
		t.Error("candidate pool should be populated after runCycle, but is empty")
	}
}

func TestRunCycle(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS",
	}
	rawData := strings.Join(lines, "\n")
	encoded := base64.StdEncoding.EncodeToString([]byte(rawData))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer server.Close()

	cfg := &Config{
		SubscriberURL:    server.URL,
		FetchInterval:    300,
		TestTimeout:      3,
		DownloadSize:     10000000,
		DownloadEndpoint: server.URL + "?bytes=",
		DownloadFallback: server.URL,
		WanCount:         1,
		WanBasePort:      20700,
		TestBasePort:     20800,
		ProxyPort:        0,
		MinimumSpeed:     1e9,
	}

	pool := NewWANPool(1, 20700)

	runCycle(cfg, pool, NewCandidatePool(50), 60*time.Second)

	if _, err := os.Stat("sorted.txt"); err != nil {
		t.Errorf("sorted.txt should exist: %v", err)
	}
}

func TestRunCycle_WithActiveSlot(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS",
	}
	rawData := strings.Join(lines, "\n")
	encoded := base64.StdEncoding.EncodeToString([]byte(rawData))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer server.Close()

	cfg := &Config{
		SubscriberURL:    server.URL,
		FetchInterval:    300,
		TestTimeout:      3,
		DownloadSize:     10000000,
		DownloadEndpoint: server.URL + "?bytes=",
		DownloadFallback: server.URL,
		WanCount:         1,
		WanBasePort:      20700,
		TestBasePort:     20800,
		ProxyPort:        10,
		MinimumSpeed:     0.1,
	}

	pool := NewWANPool(1, 20700)
	pool.Slots[0].State = StateActive
	pool.Slots[0].Config = &ProxyConfig{
		Protocol: "socks5",
		Server:   "127.0.0.1",
		Port:     1080,
		Raw:      "socks5://127.0.0.1:1080",
	}

	runCycle(cfg, pool, NewCandidatePool(50), 60*time.Second)

	if _, err := os.Stat("sorted.txt"); err != nil {
		t.Errorf("sorted.txt should exist: %v", err)
	}
}

func TestRunCycle_UpdatesCycleTiming(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &Config{
		SubscriberURL: server.URL,
		FetchInterval: 300,
		WanCount:      1,
	}
	pool := NewWANPool(1, 20700)
	resetCycleTiming()

	before := time.Now()
	runCycle(cfg, pool, NewCandidatePool(50), 60*time.Second)
	after := time.Now()

	last, next := cycleTimingSnapshot()
	if last.Before(before) || last.After(after) {
		t.Errorf("last cycle = %v, want between %v and %v", last, before, after)
	}
	wantNext := after.Add(time.Duration(cfg.FetchInterval) * time.Second)
	if next.Before(wantNext.Add(-20*time.Millisecond)) || next.After(wantNext.Add(time.Second)) {
		t.Errorf("next cycle = %v, want near %v", next, wantNext)
	}
}

func TestHandleGetCycleTiming(t *testing.T) {
	last := time.Now().Add(-time.Minute).Round(0)
	nextBase := time.Now().Round(0)
	markCycleStarted(last)
	markCycleComplete(nextBase, 90*time.Second)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/viberoxy/cycle", nil)
	handleGetCycleTiming().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}

	var info CycleTimingInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if info.LastCycle != last.Format(time.RFC3339Nano) {
		t.Errorf("last_cycle = %q, want %q", info.LastCycle, last.Format(time.RFC3339Nano))
	}
	wantNext := nextBase.Add(90 * time.Second).Format(time.RFC3339Nano)
	if info.NextCycle != wantNext {
		t.Errorf("next_cycle = %q, want %q", info.NextCycle, wantNext)
	}
	if info.SecondsUntilNext < 89 || info.SecondsUntilNext > 90 {
		t.Errorf("seconds_until_next = %d, want 89 or 90", info.SecondsUntilNext)
	}
}

func TestHandleTriggerCycle_NonBlocking(t *testing.T) {
	triggerCycle = make(chan struct{}, 1)
	handler := handleTriggerCycle()

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/viberoxy/cycle/trigger", nil)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("request %d status = %d, want %d", i+1, rec.Code, http.StatusAccepted)
		}
	}

	if got := len(triggerCycle); got != 1 {
		t.Errorf("queued triggers = %d, want 1", got)
	}
}

func TestHandleTriggerCycle_RejectsOtherMethods(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/viberoxy/cycle/trigger", nil)
	handleTriggerCycle().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("Allow = %q, want %q", got, http.MethodPost)
	}
}

func TestHandleGetCandidates_EmptyPool(t *testing.T) {
	candidatePool = nil

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/viberoxy/candidates", nil)
	handleGetCandidates().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if rec.Body.String() != "[]" {
		t.Errorf("body = %q, want \"[]\"", rec.Body.String())
	}
}

func TestHandleGetCandidates_ReturnsPool(t *testing.T) {
	p := NewCandidatePool(10)
	p.Update([]*TestResult{
		{Config: &ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 12345, Raw: "ss://test"}, Speed: 50.0},
	})
	candidatePool = p
	defer func() { candidatePool = nil }()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/viberoxy/candidates", nil)
	handleGetCandidates().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var list []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(list))
	}
	if list[0]["server"] != "1.2.3.4" {
		t.Errorf("server = %v, want 1.2.3.4", list[0]["server"])
	}
}

func TestHandleGetCandidates_RejectsNonGet(t *testing.T) {
	candidatePool = NewCandidatePool(5)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/viberoxy/candidates", nil)
	handleGetCandidates().ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestAPIHandler_RegistersCycleEndpoints(t *testing.T) {
	triggerCycle = make(chan struct{}, 1)
	handler := NewAPIHandler(NewWANPool(0, 10700))

	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/api/viberoxy/cycle", nil))
	if getRec.Code != http.StatusOK {
		t.Errorf("GET cycle status = %d, want %d", getRec.Code, http.StatusOK)
	}

	postRec := httptest.NewRecorder()
	handler.ServeHTTP(postRec, httptest.NewRequest(http.MethodPost, "/api/viberoxy/cycle/trigger", nil))
	if postRec.Code != http.StatusAccepted {
		t.Errorf("POST trigger status = %d, want %d", postRec.Code, http.StatusAccepted)
	}
}

func TestRunLoop_ManualTriggerRunsCycle(t *testing.T) {
	fetched := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case fetched <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &Config{
		SubscriberURL: server.URL,
		FetchInterval: 3600,
		WanCount:      1,
	}
	pool := NewWANPool(1, 20700)
	triggerCycle = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runLoop(cfg, pool, NewCandidatePool(50), nil, ctx)
		close(done)
	}()

	triggerCycle <- struct{}{}
	select {
	case <-fetched:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("manual trigger did not run a cycle")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runLoop did not stop after cancellation")
	}
}

func TestRunCycle_EmptyPool(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &Config{
		SubscriberURL:    server.URL,
		FetchInterval:    300,
		TestTimeout:      3,
		DownloadSize:     10000000,
		DownloadEndpoint: server.URL + "?bytes=",
		DownloadFallback: server.URL,
		WanCount:         1,
		WanBasePort:      20700,
		TestBasePort:     20800,
		ProxyPort:        10,
		MinimumSpeed:     0.1,
	}

	pool := NewWANPool(1, 20700)

	runCycle(cfg, pool, NewCandidatePool(50), 60*time.Second)
}

func TestRunCycle_DrainExpired(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(base64.StdEncoding.EncodeToString([]byte("ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS"))))
	}))
	defer server.Close()

	cfg := &Config{
		SubscriberURL:    server.URL,
		FetchInterval:    300,
		TestTimeout:      3,
		DownloadSize:     10000000,
		DownloadEndpoint: server.URL + "?bytes=",
		DownloadFallback: server.URL,
		WanCount:         1,
		WanBasePort:      20700,
		TestBasePort:     20800,
		ProxyPort:        10,
		MinimumSpeed:     0.1,
	}

	pool := NewWANPool(1, 20700)
	pool.Slots[0].State = StateDraining
	pool.Slots[0].DrainAt = time.Now().Add(-2 * time.Minute)

	runCycle(cfg, pool, NewCandidatePool(50), 10*time.Second)
}

func TestStartup_Shutdown_WithProxy(t *testing.T) {
	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS",
	}
	rawData := strings.Join(lines, "\n")
	encoded := base64.StdEncoding.EncodeToString([]byte(rawData))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(encoded))
	}))
	defer server.Close()

	proxyPort := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())

	cfg := &Config{
		SubscriberURL:    server.URL,
		FetchInterval:    300,
		TestTimeout:      3,
		DownloadSize:     10000000,
		DownloadEndpoint: server.URL + "?bytes=",
		DownloadFallback: server.URL,
		WanCount:         1,
		WanBasePort:      20700,
		TestBasePort:     20800,
		ProxyPort:        proxyPort,
		MinimumSpeed:     1e9,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		startup(cfg, ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("startup did not return within 5s after context cancel")
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestWriteSortedTxt_ErrorHandling(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	results := []*TestResult{
		{Config: &ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 12345}, Speed: 100.5},
	}

	writeSortedTxt(results)

	data, err := os.ReadFile("sorted.txt")
	if err != nil {
		t.Fatalf("read sorted.txt: %v", err)
	}
	if !strings.Contains(string(data), "speed=100.50") {
		t.Errorf("expected speed=100.50 in output, got %s", string(data))
	}
}

func TestBuildDownloadURL_EmptyEndpoint(t *testing.T) {
	cfg := &Config{
		DownloadEndpoint: "",
		DownloadFallback: "",
	}
	url := buildDownloadURL(cfg, 1000)
	if url != "" {
		t.Errorf("buildDownloadURL = %q, want empty", url)
	}
}

func TestParseConfig_RouterDefaultNil(t *testing.T) {
	for _, key := range []string{"ROUTE_MODE", "DIRECT_DOMAINS", "PROXY_DOMAINS", "DIRECT_LIST_FILE", "PROXY_LIST_FILE"} {
		unsetenv(t, key)
	}
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")

	cfg, err := parseConfig()
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

	cfg, err := parseConfig()
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

	cfg, err := parseConfig()
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

	if _, err := parseConfig(); err == nil {
		t.Error("expected error for invalid ROUTE_MODE")
	}
}

func TestParseConfig_RouterMissingListFile(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ROUTE_MODE", "proxy-default")
	setenv(t, "DIRECT_LIST_FILE", "/nonexistent/direct.txt")

	if _, err := parseConfig(); err == nil {
		t.Error("expected error for missing DIRECT_LIST_FILE")
	}
}
