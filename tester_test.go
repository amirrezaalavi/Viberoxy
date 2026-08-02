package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func startTestSocksServer(t *testing.T) (net.Listener, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go handleTestSocksConn(conn)
		}
	}()

	return l, l.Addr().String()
}

func handleTestSocksConn(client net.Conn) {
	defer client.Close()

	header := make([]byte, 2)
	if _, err := io.ReadFull(client, header); err != nil {
		return
	}
	if header[0] != 0x05 {
		return
	}

	nmethods := int(header[1])
	methods := make([]byte, nmethods)
	if _, err := io.ReadFull(client, methods); err != nil {
		return
	}

	if _, err := client.Write([]byte{0x05, 0x00}); err != nil {
		return
	}

	req := make([]byte, 4)
	if _, err := io.ReadFull(client, req); err != nil {
		return
	}

	var host string
	switch req[3] {
	case 0x01:
		ip := make([]byte, 4)
		if _, err := io.ReadFull(client, ip); err != nil {
			return
		}
		host = net.IP(ip).String()
	case 0x03:
		lenByte := make([]byte, 1)
		if _, err := io.ReadFull(client, lenByte); err != nil {
			return
		}
		domain := make([]byte, lenByte[0])
		if _, err := io.ReadFull(client, domain); err != nil {
			return
		}
		host = string(domain)
	case 0x04:
		ip := make([]byte, 16)
		if _, err := io.ReadFull(client, ip); err != nil {
			return
		}
		host = net.IP(ip).String()
	default:
		return
	}

	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(client, portBytes); err != nil {
		return
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])

	target, err := net.Dial("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		client.Write([]byte{0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
		return
	}
	defer target.Close()

	client.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	go io.Copy(target, client)
	io.Copy(client, target)
}

var testDownloadData []byte

func init() {
	testDownloadData = make([]byte, 100000)
	for i := range testDownloadData {
		testDownloadData[i] = byte(i)
	}
}

func startTestDownloadServer(t *testing.T, downloadSize int64) *httptest.Server {
	t.Helper()
	data := testDownloadData
	if int64(len(data)) > downloadSize {
		data = data[:downloadSize]
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
		w.Write(data)
	}))
	return server
}

func TestDownloadMeasurer(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	downloadServer := startTestDownloadServer(t, 50000)
	defer downloadServer.Close()

	speed, err := DownloadMeasurer(socksAddr, downloadServer.URL, 50000, 5*time.Second)
	if err != nil {
		t.Fatalf("DownloadMeasurer error: %v", err)
	}
	if speed <= 0 {
		t.Errorf("expected speed > 0, got %f", speed)
	}
}

func TestDownloadMeasurer_Timeout(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.Write([]byte("done"))
	}))
	defer slowServer.Close()

	_, err := DownloadMeasurer(socksAddr, slowServer.URL, 1000, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestSOCKS5Dial(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoListener.Close()

	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		conn, err := echoListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 5)
		io.ReadFull(conn, buf)
		conn.Write(buf)
	}()

	ctx := context.Background()
	conn, err := socks5Dial(ctx, socksAddr, echoListener.Addr().String())
	if err != nil {
		t.Fatalf("socks5Dial error: %v", err)
	}
	defer conn.Close()

	msg := []byte("hello")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}

	response := make([]byte, 5)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(response) != "hello" {
		t.Errorf("got %q, want %q", string(response), "hello")
	}

	<-echoDone
}

func TestSOCKS5Dial_IPv4(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoListener.Close()

	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		conn, err := echoListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 3)
		io.ReadFull(conn, buf)
		conn.Write(buf)
	}()

	addr := echoListener.Addr().String()
	if !strings.Contains(addr, "127.0.0.1") {
		t.Skip("echo listener not on IPv4")
	}

	ctx := context.Background()
	conn, err := socks5Dial(ctx, socksAddr, addr)
	if err != nil {
		t.Fatalf("socks5Dial error: %v", err)
	}
	defer conn.Close()

	msg := []byte("ipv")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}

	response := make([]byte, 3)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(response) != "ipv" {
		t.Errorf("got %q, want %q", string(response), "ipv")
	}

	<-echoDone
}

func TestSOCKS5Dial_InvalidAddr(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	ctx := context.Background()
	_, err := socks5Dial(ctx, socksAddr, "127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error connecting to unreachable address, got nil")
	}
}

func TestProbeWAN(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("1.2.3.4"))
	}))
	defer probeServer.Close()

	orig := ProbeWANURL
	ProbeWANURL = probeServer.URL
	t.Cleanup(func() { ProbeWANURL = orig })

	if err := ProbeWAN(socksAddr, 5*time.Second); err != nil {
		t.Fatalf("ProbeWAN error: %v", err)
	}
}

// TestProbeWAN_PortlessURL is a regression test for the keepalive probe
// failing on URLs without an explicit port (e.g. https://api.ipify.org/):
// socks5Dial requires host:port, so the dial target must get the default
// port appended.
func TestProbeWAN_PortlessURL(t *testing.T) {
	orig := ProbeWANURL
	defer func() { ProbeWANURL = orig }()

	ProbeWANURL = "https://api.ipify.org/"
	u, err := url.Parse(ProbeWANURL)
	if err != nil {
		t.Fatalf("parse probe url: %v", err)
	}
	if got := probeDialTarget(u); got != "api.ipify.org:443" {
		t.Errorf("probeDialTarget(https portless) = %q, want %q", got, "api.ipify.org:443")
	}

	ProbeWANURL = "http://example.com/speed"
	u, _ = url.Parse(ProbeWANURL)
	if got := probeDialTarget(u); got != "example.com:80" {
		t.Errorf("probeDialTarget(http portless) = %q, want %q", got, "example.com:80")
	}

	ProbeWANURL = "https://127.0.0.1:8443/"
	u, _ = url.Parse(ProbeWANURL)
	if got := probeDialTarget(u); got != "127.0.0.1:8443" {
		t.Errorf("probeDialTarget(explicit port) = %q, want %q", got, "127.0.0.1:8443")
	}
}

func TestProbeWAN_Timeout(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.Write([]byte("done"))
	}))
	defer slowServer.Close()

	orig := ProbeWANURL
	ProbeWANURL = slowServer.URL
	t.Cleanup(func() { ProbeWANURL = orig })

	if err := ProbeWAN(socksAddr, 100*time.Millisecond); err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestProbeWAN_SocksUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	if err := ProbeWAN(addr, 500*time.Millisecond); err == nil {
		t.Fatal("expected error for unreachable socks listener, got nil")
	}
}

func TestProbeWAN_BadStatus(t *testing.T) {
	socksListener, socksAddr := startTestSocksServer(t)
	defer socksListener.Close()

	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer errServer.Close()

	orig := ProbeWANURL
	ProbeWANURL = errServer.URL
	t.Cleanup(func() { ProbeWANURL = orig })

	if err := ProbeWAN(socksAddr, 5*time.Second); err == nil {
		t.Fatal("expected error for non-2xx probe status, got nil")
	}
}

func waitForPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("port %s not reachable within %v", addr, timeout)
}

func TestTestSpeed(t *testing.T) {
	if _, err := exec.LookPath("xray"); err != nil {
		t.Skip("xray not found in PATH, skipping integration test")
	}

	downloadServer := startTestDownloadServer(t, 50000)
	defer downloadServer.Close()

	mockSocksListener, mockSocksAddr := startTestSocksServer(t)
	defer mockSocksListener.Close()

	mockHost, mockPortStr, err := net.SplitHostPort(mockSocksAddr)
	if err != nil {
		t.Fatalf("split mock socks addr: %v", err)
	}
	mockPort, _ := strconv.Atoi(mockPortStr)

	port := 20800
	rawURL := fmt.Sprintf("socks5://%s:%d", mockHost, mockPort)
	cfg := &ProxyConfig{
		Protocol: "socks5",
		Server:   mockHost,
		Port:     mockPort,
		Name:     "test",
		Raw:      rawURL,
	}

	cmd, configPath, err := StartXray(cfg, port)
	if err != nil {
		t.Fatalf("StartXray error: %v", err)
	}
	defer StopXray(cmd, configPath)

	if err := waitForPort(fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second); err != nil {
		t.Fatalf("xray did not start: %v", err)
	}

	socksAddr := fmt.Sprintf("127.0.0.1:%d", port)
	speed, err := DownloadMeasurer(socksAddr, downloadServer.URL, 50000, 10*time.Second)
	if err != nil {
		t.Fatalf("DownloadMeasurer error: %v", err)
	}
	if speed <= 0 {
		t.Errorf("expected speed > 0, got %f", speed)
	}
}

func TestTestSpeed_XrayFailure(t *testing.T) {
	cfg := &ProxyConfig{
		Protocol: "socks5",
		Server:   "0.0.0.1",
		Port:     1,
		Raw:      "socks5://127.0.0.1:1080",
	}

	result := TestSpeed(cfg, 20901, 5*time.Second, "http://127.0.0.1:1/", 10000)
	if result.Error == nil {
		t.Error("expected error, got nil")
	}
	if result.Speed != 0 {
		t.Errorf("expected speed 0 on failure, got %f", result.Speed)
	}
	if result.Config != cfg {
		t.Error("result config does not match input")
	}
}

func TestSortResults(t *testing.T) {
	results := []*TestResult{
		{Speed: 10.0},
		{Speed: 50.0},
		{Speed: 30.0},
		{Speed: 1.0},
	}

	sorted := SortResults(results)
	if len(sorted) != 4 {
		t.Fatalf("expected 4 results, got %d", len(sorted))
	}

	expected := []float64{50.0, 30.0, 10.0, 1.0}
	for i, s := range sorted {
		if s.Speed != expected[i] {
			t.Errorf("sorted[%d].Speed = %f, want %f", i, s.Speed, expected[i])
		}
	}
}

func TestSortResults_WithErrors(t *testing.T) {
	results := []*TestResult{
		{Speed: 10.0},
		{Speed: 50.0, Error: fmt.Errorf("fail")},
		{Speed: 30.0},
		{Speed: 0, Error: fmt.Errorf("fail2")},
	}

	sorted := SortResults(results)
	if len(sorted) != 4 {
		t.Fatalf("expected 4 results, got %d", len(sorted))
	}

	if sorted[0].Speed != 30.0 {
		t.Errorf("sorted[0].Speed = %f, want 30.0", sorted[0].Speed)
	}
	if sorted[1].Speed != 10.0 {
		t.Errorf("sorted[1].Speed = %f, want 10.0", sorted[1].Speed)
	}
	if sorted[2].Error == nil {
		t.Error("expected sorted[2] to have error")
	}
	if sorted[3].Error == nil {
		t.Error("expected sorted[3] to have error")
	}
}

func TestTestAll(t *testing.T) {
	downloadServer := startTestDownloadServer(t, 10000)
	defer downloadServer.Close()

	configs := []*ProxyConfig{
		{Protocol: "socks5", Server: "127.0.0.1", Port: 1080, Raw: "socks5://127.0.0.1:1080", Name: "A"},
		{Protocol: "socks5", Server: "127.0.0.1", Port: 1080, Raw: "socks5://127.0.0.1:1080", Name: "B"},
	}

	results := TestAll(configs, 20810, 5*time.Second, downloadServer.URL, 10000)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	for i, r := range results {
		if r.Config != configs[i] {
			t.Errorf("results[%d].Config mismatch", i)
		}
	}
}

func TestTestAll_SortsResults(t *testing.T) {
	configs := []*ProxyConfig{
		{Protocol: "socks5", Server: "127.0.0.1", Port: 1080, Raw: "socks5://127.0.0.1:1080", Name: "A"},
		{Protocol: "socks5", Server: "127.0.0.1", Port: 1080, Raw: "socks5://127.0.0.1:1080", Name: "B"},
		{Protocol: "socks5", Server: "127.0.0.1", Port: 1080, Raw: "socks5://127.0.0.1:1080", Name: "C"},
	}

	downloadServer := startTestDownloadServer(t, 10000)
	defer downloadServer.Close()

	results := TestAll(configs, 20820, 5*time.Second, downloadServer.URL, 10000)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	var prevSpeed float64
	for i, r := range results {
		if r.Error == nil && r.Speed > 0 && i > 0 && prevSpeed > 0 && r.Speed > prevSpeed {
			t.Errorf("results not sorted descending: results[%d].Speed=%f > results[%d].Speed=%f", i-1, prevSpeed, i, r.Speed)
		}
		if r.Error == nil && r.Speed > 0 {
			prevSpeed = r.Speed
		}
	}
}
