package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
		"MINIMUM_SPEED",
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
	if cfg.MinimumSpeed != 5.0 {
		t.Errorf("MinimumSpeed = %f, want %f", cfg.MinimumSpeed, 5.0)
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

	runCycle(cfg, pool, 60*time.Second)

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

	runCycle(cfg, pool, 60*time.Second)

	if _, err := os.Stat("sorted.txt"); err != nil {
		t.Errorf("sorted.txt should exist: %v", err)
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

	runCycle(cfg, pool, 60*time.Second)
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

	runCycle(cfg, pool, 10*time.Second)
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
