package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// startSocksServer runs a SocksServer on an ephemeral port and returns its
// address. The server is torn down when the test finishes.
func startSocksServer(t *testing.T, pool *WANPool) string {
	t.Helper()
	port := freePort(t)
	srv := NewSocksServer(port, pool)
	srv.AccessLog = false
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := srv.Listen(ctx); err != nil {
			t.Errorf("socks server error: %v", err)
		}
	}()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForPort(addr, 2*time.Second); err != nil {
		t.Fatalf("socks server did not start: %v", err)
	}
	return addr
}

// socksTestWAN starts a mock upstream SOCKS5 server and returns a pool whose
// single active slot routes to it.
func socksTestWAN(t *testing.T) *WANPool {
	t.Helper()
	socksLn, socksAddr := startTestSocksServer(t)
	t.Cleanup(func() { socksLn.Close() })
	_, portStr, err := net.SplitHostPort(socksAddr)
	if err != nil {
		t.Fatalf("split socks addr: %v", err)
	}
	port, _ := strconv.Atoi(portStr)
	pool := NewWANPool(1, 0)
	pool.Slots[0].ServicePort = port
	pool.Slots[0].State = StateActive
	return pool
}

// startEchoServer returns the address of a TCP server that echoes bytes back.
func startEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// socksGreet writes a no-auth SOCKS5 greeting and returns the server reply.
func socksGreet(t *testing.T, conn net.Conn) [2]byte {
	t.Helper()
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	var reply [2]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatalf("read greeting reply: %v", err)
	}
	return reply
}

// socksSendRequest writes a SOCKS5 request with the given CMD/ATYP/addr/port
// and returns the 10-byte server reply.
func socksSendRequest(t *testing.T, conn net.Conn, cmd, atyp byte, addr string, port int) [10]byte {
	t.Helper()
	var req []byte
	switch atyp {
	case 0x01:
		ip := net.ParseIP(addr).To4()
		if ip == nil {
			t.Fatalf("invalid IPv4 address %q", addr)
		}
		req = []byte{0x05, cmd, 0x00, 0x01, ip[0], ip[1], ip[2], ip[3]}
	case 0x03:
		if len(addr) == 0 || len(addr) > 255 {
			t.Fatalf("invalid domain %q", addr)
		}
		req = append([]byte{0x05, cmd, 0x00, 0x03, byte(len(addr))}, addr...)
	case 0x04:
		ip := net.ParseIP(addr).To16()
		if ip == nil {
			t.Fatalf("invalid IPv6 address %q", addr)
		}
		req = append([]byte{0x05, cmd, 0x00, 0x04}, ip...)
	default:
		t.Fatalf("invalid ATYP %d", atyp)
	}
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	var reply [10]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatalf("read request reply: %v", err)
	}
	return reply
}

func TestSocksHandshake(t *testing.T) {
	addr := startSocksServer(t, socksTestWAN(t))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	reply := socksGreet(t, conn)
	if reply != [2]byte{0x05, 0x00} {
		t.Fatalf("greeting reply = %v, want [5 0]", reply)
	}
}

func TestSocksConnect_IPv4(t *testing.T) {
	pool := socksTestWAN(t)
	addr := startSocksServer(t, pool)
	echoAddr := startEchoServer(t)
	echoHost, echoPortStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo addr: %v", err)
	}
	echoPort, _ := strconv.Atoi(echoPortStr)

	upBefore := metricProxyBytes.Value("0", "up")
	downBefore := metricProxyBytes.Value("0", "down")
	connsBefore := metricProxyConnections.Value("0", "socks5")

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}

	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x01, 0x01, echoHost, echoPort)
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("connect reply = %v, want REP 0", reply)
	}

	payload := []byte("hello over socks5!")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo = %q, want %q", got, payload)
	}

	// The connection metric is recorded at CONNECT time; bytes only after
	// the relay ends (handleSocksConn returns), signalled by the slot's
	// connection count dropping back to 0.
	if delta := metricProxyConnections.Value("0", "socks5") - connsBefore; delta != 1 {
		t.Errorf("socks5 connections delta = %v, want 1", delta)
	}
	conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if metricProxyBytes.Value("0", "up")-upBefore >= float64(len(payload)) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if upDelta := metricProxyBytes.Value("0", "up") - upBefore; upDelta < float64(len(payload)) {
		t.Errorf("bytes up delta = %v, want >= %d", upDelta, len(payload))
	}
	if downDelta := metricProxyBytes.Value("0", "down") - downBefore; downDelta < float64(len(payload)) {
		t.Errorf("bytes down delta = %v, want >= %d", downDelta, len(payload))
	}
}

func TestSocksConnect_Domain(t *testing.T) {
	addr := startSocksServer(t, socksTestWAN(t))
	echoAddr := startEchoServer(t)
	_, echoPortStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo addr: %v", err)
	}
	echoPort, _ := strconv.Atoi(echoPortStr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x01, 0x03, "localhost", echoPort)
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("connect reply = %v, want REP 0", reply)
	}

	payload := []byte("domain round trip!")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo = %q, want %q", got, payload)
	}
}

func TestSocks_UDPAssociateRejected(t *testing.T) {
	addr := startSocksServer(t, socksTestWAN(t))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x03, 0x01, "127.0.0.1", 80)
	if reply[0] != 0x05 || reply[1] != 0x07 {
		t.Fatalf("UDP ASSOCIATE reply = %v, want REP 0x07", reply)
	}
}

func TestSocks_BindRejected(t *testing.T) {
	addr := startSocksServer(t, socksTestWAN(t))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x02, 0x01, "127.0.0.1", 80)
	if reply[0] != 0x05 || reply[1] != 0x07 {
		t.Fatalf("BIND reply = %v, want REP 0x07", reply)
	}
}

func TestSocks_BadVersionClosed(t *testing.T) {
	addr := startSocksServer(t, socksTestWAN(t))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	// Wrong version byte: the server must close without any reply.
	if _, err := conn.Write([]byte{0x04, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if n, err := conn.Read(buf); err == nil || n > 0 {
		t.Fatalf("expected closed connection, got n=%d err=%v", n, err)
	}
}

func TestSocks_NoWANAvailable(t *testing.T) {
	// Empty pool: no active slot, so GetLeastLoaded returns -1.
	addr := startSocksServer(t, NewWANPool(1, 0))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x01, 0x01, "127.0.0.1", 80)
	if reply[0] != 0x05 || reply[1] != 0x01 {
		t.Fatalf("no-WAN reply = %v, want REP 0x01", reply)
	}
}

func TestSocks_DialFailureCounted(t *testing.T) {
	pool := NewWANPool(1, 0)
	pool.Slots[0].ServicePort = freePort(t) // nothing listening there
	pool.Slots[0].State = StateActive
	addr := startSocksServer(t, pool)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x01, 0x01, "127.0.0.1", 80)
	if reply[0] != 0x05 || reply[1] != 0x01 {
		t.Fatalf("dial-failure reply = %v, want REP 0x01", reply)
	}
	// The failed attempt must have incremented the slot's failure counter.
	if fails := pool.SlotConsecutiveFails(0); fails != 1 {
		t.Errorf("ConsecutiveFails = %d, want 1", fails)
	}
}

func TestSocksConnect_DirectRoute(t *testing.T) {
	// WAN pool with NO active slots: only the direct route can succeed.
	pool := NewWANPool(1, 0)
	router := NewRouter(RouteProxyDefault, []string{".127.0.0.1"}, nil)

	port := freePort(t)
	srv := NewSocksServer(port, pool, router)
	srv.AccessLog = false
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := srv.Listen(ctx); err != nil {
			t.Errorf("socks server error: %v", err)
		}
	}()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForPort(addr, 2*time.Second); err != nil {
		t.Fatalf("socks server did not start: %v", err)
	}

	echoAddr := startEchoServer(t)
	echoHost, echoPortStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo addr: %v", err)
	}
	echoPort, _ := strconv.Atoi(echoPortStr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x01, 0x01, echoHost, echoPort)
	if reply[1] != 0x00 {
		t.Fatalf("expected success reply, got rep=%d", reply[1])
	}

	testMsg := []byte("hello socks direct!")
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
