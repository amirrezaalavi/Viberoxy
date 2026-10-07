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
	"viberoxy/internal/proxycfg"
)

func init() {
	// Integration tests drive fetchSubscription and startup() against
	// plain-http httptest servers, so opt in to the subscription scheme
	// policy that ParseConfig enforces by default (https-only unless
	// ALLOW_HTTP_SUBSCRIPTION=true). internal/subs tests cover both
	// sides of the policy with the variable explicitly set/unset.
	os.Setenv("ALLOW_HTTP_SUBSCRIPTION", "true")
}

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

// consecutiveFreePorts returns a port whose next n-1 ports are also free, so
// callers can hand it to configs that bind base+index. The whole run is
// claimed via claimPorts so no later allocation can alias any port of it.
func consecutiveFreePorts(t *testing.T, n int) int {
	t.Helper()
	for attempt := 0; attempt < 1000; attempt++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("consecutiveFreePorts: %v", err)
		}
		_, portStr, _ := net.SplitHostPort(l.Addr().String())
		base, _ := strconv.Atoi(portStr)
		l.Close()

		ok := true
		var held []net.Listener
		for i := 1; i < n; i++ {
			hl, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
			if err != nil {
				ok = false
				break
			}
			held = append(held, hl)
		}
		for _, hl := range held {
			hl.Close()
		}

		ports := make([]int, n)
		for i := range ports {
			ports[i] = base + i
		}
		if ok && claimPorts(ports...) {
			return base
		}
	}
	t.Fatal("consecutiveFreePorts: no unissued consecutive run found")
	return 0
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

	cfg := &proxycfg.Config{
		SubscriberURL:     subServer.URL,
		FetchInterval:     300,
		TestTimeout:       10,
		DownloadSize:      50000,
		DownloadEndpoint:  downloadServer.URL + "?bytes=",
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
	secondBlocked := make(chan struct{})
	var blockOnce sync.Once
	downloadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		first := requests == 1
		mu.Unlock()
		if !first {
			blockOnce.Do(func() { close(secondBlocked) })
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

	cfg := &proxycfg.Config{
		SubscriberURL:     subServer.URL,
		FetchInterval:     300,
		TestTimeout:       10,
		DownloadSize:      50000,
		DownloadEndpoint:  downloadServer.URL + "?bytes=",
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
	// Deterministic observation window: secondBlocked fires when the second
	// speed test is parked on the gate inside the download handler. Only two
	// configs exist and the gated test cannot finish, so at that moment the
	// pool is provably short of WanCount — exactly the state in which the
	// proxy must NOT be serving yet.
	select {
	case <-secondBlocked:
	case <-time.After(15 * time.Second):
		close(gate)
		cancel()
		<-done
		t.Fatal("second speed test never reached the download server")
	}

	conn, err := net.DialTimeout("tcp", proxyAddr, 500*time.Millisecond)
	if err == nil {
		conn.Close()
		close(gate)
		cancel()
		<-done
		t.Fatalf("proxy started before pool was full (degraded boot disabled)")
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

// TestFetchSubscription_ConditionalGetKeepsConfigs: the second fetch
// replays the stored ETag, and the server's 304 is answered with the
// configs already parsed — no re-parse, no empty result.
func TestFetchSubscription_ConditionalGetKeepsConfigs(t *testing.T) {
	const etag = `"sub-v2"`
	body := base64.StdEncoding.EncodeToString([]byte(
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#TestSS\n" +
			"trojan://password123@5.6.7.8:443#TestTrojan"))

	var mu sync.Mutex
	sawConditional := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Header.Get("If-None-Match") == etag {
			sawConditional = true
			mu.Unlock()
			w.WriteHeader(http.StatusNotModified)
			return
		}
		mu.Unlock()
		w.Header().Set("ETag", etag)
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	first := fetchSubscription(srv.URL)
	if len(first) != 2 {
		t.Fatalf("first fetch: %d configs, want 2", len(first))
	}

	second := fetchSubscription(srv.URL)
	if len(second) != 2 {
		t.Fatalf("304 fetch: %d configs, want the 2 previously parsed", len(second))
	}
	if second[0].Server != "1.2.3.4" || second[1].Server != "5.6.7.8" {
		t.Errorf("304 configs = %s:%d, %s:%d; want 1.2.3.4, 5.6.7.8",
			second[0].Server, second[0].Port, second[1].Server, second[1].Port)
	}

	mu.Lock()
	defer mu.Unlock()
	if !sawConditional {
		t.Error("second request did not carry If-None-Match")
	}
}

// TestFetchSubscription_GarbageBodyKeepsPrevious: a body that parses to
// zero configs is reported as a failure (nil), so callers keep the configs
// they already have.
func TestFetchSubscription_GarbageBodyKeepsPrevious(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "definitely not a proxy config line")
	}))
	defer srv.Close()

	configs := fetchSubscription(srv.URL)
	if len(configs) != 0 {
		t.Errorf("expected no configs for garbage body, got %d", len(configs))
	}
}

func TestWriteSortedTxt(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origDir)

	results := []*TestResult{
		{Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 12345}, Speed: 50.0},
		{Config: &proxycfg.ProxyConfig{Protocol: "trojan", Server: "5.6.7.8", Port: 443}, Speed: 30.0, Error: fmt.Errorf("timeout")},
		{Config: &proxycfg.ProxyConfig{Protocol: "vmess", Server: "9.10.11.12", Port: 8080}, Speed: 10.0},
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
	cfg := &proxycfg.Config{
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
	cfg := &proxycfg.Config{
		DownloadEndpoint: "",
		DownloadFallback: "https://proof.ovh.net/files/",
	}
	url := buildDownloadURL(cfg, 5000000)
	if url != "https://proof.ovh.net/files/" {
		t.Errorf("buildDownloadURL = %q, want %q", url, "https://proof.ovh.net/files/")
	}
}

func TestBuildDownloadURL_WithCustomEndpoint(t *testing.T) {
	cfg := &proxycfg.Config{
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

	cfg := &proxycfg.Config{
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

	cfg := &proxycfg.Config{
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

	cfg := &proxycfg.Config{
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

	cfg := &proxycfg.Config{
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
	pool.Slots[0].Config = &proxycfg.ProxyConfig{
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

	cfg := &proxycfg.Config{
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
		{Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 12345, Raw: "ss://test"}, Speed: 50.0},
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

	cfg := &proxycfg.Config{
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

	cfg := &proxycfg.Config{
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

	cfg := &proxycfg.Config{
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

	cfg := &proxycfg.Config{
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
		{Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 12345}, Speed: 100.5},
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
	cfg := &proxycfg.Config{
		DownloadEndpoint: "",
		DownloadFallback: "",
	}
	url := buildDownloadURL(cfg, 1000)
	if url != "" {
		t.Errorf("buildDownloadURL = %q, want empty", url)
	}
}
