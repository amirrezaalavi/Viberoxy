package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.Atoi(portStr)
	l.Close()
	return port
}

func TestNewProxyServer(t *testing.T) {
	pool := NewWANPool(4, 10700)
	proxy := NewProxyServer(8080, pool)

	if proxy.port != 8080 {
		t.Errorf("port = %d, want 8080", proxy.port)
	}
	if proxy.pool != pool {
		t.Error("pool mismatch")
	}
	if proxy.server != nil {
		t.Error("expected nil server")
	}
	if proxy.WanFailThreshold != DefaultFailThreshold {
		t.Errorf("WanFailThreshold = %d, want default %d", proxy.WanFailThreshold, DefaultFailThreshold)
	}
}

func TestHandleConnect_MethodNotAllowed(t *testing.T) {
	pool := NewWANPool(1, 0)
	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", proxyPort))
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 405 {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestHandleConnect_NoWAN(t *testing.T) {
	pool := NewWANPool(1, 0)
	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 503 {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}
}

func TestHandleConnect_Success(t *testing.T) {
	socksLn, socksAddr := startTestSocksServer(t)
	defer socksLn.Close()

	_, socksPortStr, err := net.SplitHostPort(socksAddr)
	if err != nil {
		t.Fatalf("split socks addr: %v", err)
	}
	socksPort, _ := strconv.Atoi(socksPortStr)

	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoLn.Close()

	go func() {
		conn, err := echoLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(conn, conn)
	}()

	pool := NewWANPool(1, 0)
	pool.Slots[0].ServicePort = socksPort
	pool.Slots[0].State = StateActive

	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	echoAddr := echoLn.Addr().String()
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", echoAddr, echoAddr)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	testMsg := []byte("hello proxy tunnel!")
	if _, err := conn.Write(testMsg); err != nil {
		t.Fatalf("write: %v", err)
	}

	response := make([]byte, len(testMsg))
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(response) != string(testMsg) {
		t.Errorf("got %q, want %q", string(response), string(testMsg))
	}
}

func TestHandleConnect_ConnectionCount(t *testing.T) {
	socksLn, socksAddr := startTestSocksServer(t)
	defer socksLn.Close()

	_, socksPortStr, _ := net.SplitHostPort(socksAddr)
	socksPort, _ := strconv.Atoi(socksPortStr)

	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoLn.Close()
	go func() {
		conn, _ := echoLn.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	pool := NewWANPool(1, 0)
	pool.Slots[0].ServicePort = socksPort
	pool.Slots[0].State = StateActive

	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	echoAddr := echoLn.Addr().String()
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", echoAddr, echoAddr)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c != 1 {
		t.Errorf("expected ConnCount=1 during connection, got %d", c)
	}

	conn.Close()

	for i := 0; i < 50; i++ {
		if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if c := atomic.LoadInt64(&pool.Slots[0].ConnCount); c != 0 {
		t.Errorf("expected ConnCount=0 after close, got %d", c)
	}
}

func TestHandleConnect_ByteCounting(t *testing.T) {
	socksLn, socksAddr := startTestSocksServer(t)
	defer socksLn.Close()

	_, socksPortStr, err := net.SplitHostPort(socksAddr)
	if err != nil {
		t.Fatalf("split socks addr: %v", err)
	}
	socksPort, _ := strconv.Atoi(socksPortStr)

	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoLn.Close()

	go func() {
		conn, err := echoLn.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		io.Copy(conn, conn)
	}()

	pool := NewWANPool(1, 0)
	pool.Slots[0].ServicePort = socksPort
	pool.Slots[0].State = StateActive

	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	upBefore := metricProxyBytes.Value("0", "up")
	downBefore := metricProxyBytes.Value("0", "down")
	connsBefore := metricProxyConnections.Value("0", "connect")

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	echoAddr := echoLn.Addr().String()
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", echoAddr, echoAddr)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	payload := bytes.Repeat([]byte("x"), 64*1024)
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("echo payload mismatch")
	}

	conn.Close()

	// Metrics are recorded after both pipes close (handleConnect returns),
	// which is signalled by ConnCount returning to 0.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&pool.Slots[0].ConnCount) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	upDelta := metricProxyBytes.Value("0", "up") - upBefore
	downDelta := metricProxyBytes.Value("0", "down") - downBefore
	if upDelta < float64(len(payload)) {
		t.Errorf("bytes up delta = %v, want >= %d", upDelta, len(payload))
	}
	if downDelta < float64(len(payload)) {
		t.Errorf("bytes down delta = %v, want >= %d", downDelta, len(payload))
	}
	if conns := metricProxyConnections.Value("0", "connect") - connsBefore; conns != 1 {
		t.Errorf("connections delta = %v, want 1", conns)
	}
}

func TestHandleConnect_DialFailureCounted(t *testing.T) {
	pool := NewWANPool(1, 0)
	pool.Slots[0].ServicePort = freePort(t) // nothing listening there
	pool.Slots[0].State = StateActive

	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	connsBefore := metricProxyConnections.Value("0", "connect")
	failsBefore := atomic.LoadInt64(&pool.Slots[0].ConsecutiveFails)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 502 {
		t.Errorf("expected 502, got %d", resp.StatusCode)
	}

	// The failed attempt must still be counted, and the slot's
	// ConsecutiveFails must have been incremented.
	if conns := metricProxyConnections.Value("0", "connect") - connsBefore; conns != 1 {
		t.Errorf("connections delta = %v, want 1 (failed attempt counted)", conns)
	}
	if fails := atomic.LoadInt64(&pool.Slots[0].ConsecutiveFails); fails != failsBefore+1 {
		t.Errorf("ConsecutiveFails = %d, want %d", fails, failsBefore+1)
	}
}

func TestHandleConnect_SkipsUnhealthyWAN(t *testing.T) {
	pool := NewWANPool(1, 0)
	pool.Slots[0].ServicePort = freePort(t) // nothing listening there
	pool.Slots[0].State = StateActive
	// 2 fails >= default threshold 2 → slot excluded from load balancing.
	pool.RecordFailure(0)
	pool.RecordFailure(0)

	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	// With the only slot unhealthy, the proxy must answer 503 instead of
	// attempting (and failing) the SOCKS5 dial with 502.
	if resp.StatusCode != 503 {
		t.Errorf("expected 503 (no healthy WAN), got %d", resp.StatusCode)
	}
}

func TestStartStop(t *testing.T) {
	pool := NewWANPool(1, 0)
	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- proxy.Start(ctx)
	}()

	waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial before stop: %v", err)
	}
	conn.Close()

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Start to return")
	}

	conn, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err == nil {
		conn.Close()
		t.Error("expected connection to fail after shutdown")
	}
}
