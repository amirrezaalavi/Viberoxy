package main

// F-14 wiring tests: loopback bind by default, proxy/SOCKS authentication,
// bearer-gated API/metrics, candidate credential redaction and the
// private-target SSRF guard.
//
// Coexistence note: the pre-existing front-end tests (proxy_test.go,
// socks_test.go) legitimately target 127.0.0.1/localhost echo servers, so
// this file's init() opts in to ALLOW_PRIVATE_TARGETS for the whole test
// binary — the same pattern main_test.go's init() uses for
// ALLOW_HTTP_SUBSCRIPTION. The guard itself is pinned both ways here (each
// guard test sets the env explicitly in both directions) and at the unit
// level in internal/auth.

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"viberoxy/internal/proxycfg"
)

func init() {
	os.Setenv("ALLOW_PRIVATE_TARGETS", "true")
}

// proxyConnect sends one CONNECT through the proxy on port, optionally with
// a Proxy-Authorization header value, and returns the parsed response.
func proxyConnect(t *testing.T, port int, target, proxyAuth string) *http.Response {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	var req strings.Builder
	fmt.Fprintf(&req, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", target, target)
	if proxyAuth != "" {
		fmt.Fprintf(&req, "Proxy-Authorization: %s\r\n", proxyAuth)
	}
	req.WriteString("\r\n")
	if _, err := io.WriteString(conn, req.String()); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	return resp
}

func basicAuthValue(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// socksUserPass runs the RFC 1929 subnegotiation and returns the 2-byte
// reply ([VER, STATUS]).
func socksUserPass(t *testing.T, conn net.Conn, user, pass string) [2]byte {
	t.Helper()
	req := []byte{0x01, byte(len(user))}
	req = append(req, user...)
	req = append(req, byte(len(pass)))
	req = append(req, pass...)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write user/pass subnegotiation: %v", err)
	}
	var reply [2]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatalf("read user/pass reply: %v", err)
	}
	return reply
}

// socksGreetWith writes a greeting offering exactly the given methods and
// returns the server's method selection.
func socksGreetWith(t *testing.T, conn net.Conn, methods ...byte) [2]byte {
	t.Helper()
	greeting := append([]byte{0x05, byte(len(methods))}, methods...)
	if _, err := conn.Write(greeting); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	var reply [2]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatalf("read greeting reply: %v", err)
	}
	return reply
}

// firstNonLoopbackIPv4 returns a local non-loopback IPv4 address, or "" when
// the machine has none (the external-bind assertion is then skipped).
func firstNonLoopbackIPv4() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}
			return ip.String()
		}
	}
	return ""
}

// TestBindDefaultIsLoopback pins the F-14 default: every front-end binds
// loopback unless LISTEN_ADDR overrides it. If someone mutates the
// constructor default (or the bind address) back to ":%d"/0.0.0.0 this
// test fails.
func TestBindDefaultIsLoopback(t *testing.T) {
	pool := NewWANPool(1, 0)

	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)
	if proxy.listenAddr != "127.0.0.1" {
		t.Errorf("ProxyServer.listenAddr = %q, want 127.0.0.1 (loopback by default)", proxy.listenAddr)
	}

	socksPort := freePort(t)
	socks := NewSocksServer(socksPort, pool)
	if socks.listenAddr != "127.0.0.1" {
		t.Errorf("SocksServer.listenAddr = %q, want 127.0.0.1 (loopback by default)", socks.listenAddr)
	}

	// Behavioral: start the proxy and check the bound address plus actual
	// reachability from a non-loopback local address.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan error, 1)
	go func() { started <- proxy.Start(ctx) }()
	if err := waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second); err != nil {
		t.Fatalf("proxy did not start: %v", err)
	}

	proxy.mu.Lock()
	boundAddr := proxy.server.Addr
	proxy.mu.Unlock()
	if want := fmt.Sprintf("127.0.0.1:%d", proxyPort); boundAddr != want {
		t.Errorf("proxy bound %q, want %q", boundAddr, want)
	}

	if ip := firstNonLoopbackIPv4(); ip != "" {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, proxyPort), time.Second)
		if err == nil {
			conn.Close()
			t.Errorf("proxy accepted a connection on non-loopback address %s (bound %q)", ip, boundAddr)
		}
		conn, err = net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, socksPort), time.Second)
		if err == nil {
			conn.Close()
			t.Error("socks5 accepted a connection on a non-loopback address")
		}
	} else {
		t.Log("no non-loopback IPv4 address on this machine; external-bind dial skipped")
	}

	cancel()
	select {
	case err := <-started:
		if err != nil {
			t.Errorf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

// TestFrontendsRefusePublicBindWithoutAuth: a non-loopback listen address
// with no auth and no ALLOW_PUBLIC is refused by the listeners themselves
// (defense in depth behind ParseConfig's hard exit).
func TestFrontendsRefusePublicBindWithoutAuth(t *testing.T) {
	unsetenv(t, "PROXY_USERS")
	unsetenv(t, "API_TOKEN")
	unsetenv(t, "ALLOW_PUBLIC")

	pool := NewWANPool(1, 0)

	proxy := NewProxyServer(freePort(t), pool)
	proxy.listenAddr = "0.0.0.0"
	err := proxy.Start(context.Background())
	if err == nil {
		t.Fatal("proxy Start on 0.0.0.0 without auth returned nil, want refusal")
	}
	if !strings.Contains(err.Error(), "ALLOW_PUBLIC") {
		t.Errorf("proxy refusal %q does not explain the opt-ins", err)
	}

	socks := NewSocksServer(freePort(t), pool)
	socks.listenAddr = "0.0.0.0"
	if err := socks.Listen(context.Background()); err == nil {
		t.Fatal("socks Listen on 0.0.0.0 without auth returned nil, want refusal")
	}
}

// TestFrontendsAllowPublicBindWithOptIn: ALLOW_PUBLIC=true (or configured
// credentials) opens the non-loopback bind.
func TestFrontendsAllowPublicBindWithOptIn(t *testing.T) {
	pool := NewWANPool(1, 0)

	t.Run("ALLOW_PUBLIC", func(t *testing.T) {
		unsetenv(t, "PROXY_USERS")
		unsetenv(t, "API_TOKEN")
		setenv(t, "ALLOW_PUBLIC", "true")

		proxyPort := freePort(t)
		proxy := NewProxyServer(proxyPort, pool)
		proxy.listenAddr = "0.0.0.0"
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan error, 1)
		go func() { started <- proxy.Start(ctx) }()
		if err := waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second); err != nil {
			cancel()
			t.Fatalf("proxy did not start: %v", err)
		}
		cancel()
		select {
		case err := <-started:
			if err != nil {
				t.Errorf("Start returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Start did not return after cancel")
		}
	})

	t.Run("PROXY_USERS", func(t *testing.T) {
		unsetenv(t, "ALLOW_PUBLIC")
		unsetenv(t, "API_TOKEN")
		setenv(t, "PROXY_USERS", "alice:s3cret")

		socksPort := freePort(t)
		socks := NewSocksServer(socksPort, pool)
		socks.listenAddr = "0.0.0.0"
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- socks.Listen(ctx) }()
		if err := waitForPort(fmt.Sprintf("127.0.0.1:%d", socksPort), 2*time.Second); err != nil {
			cancel()
			t.Fatalf("socks did not start: %v", err)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Listen returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Listen did not return after cancel")
		}
	})
}

// TestProxyConnect_AuthRequired: with PROXY_USERS set, CONNECT without
// credentials (or with wrong ones) is answered 407; valid credentials pass
// through to normal routing (503 here: no WAN, i.e. not a 407).
func TestProxyConnect_AuthRequired(t *testing.T) {
	unsetenv(t, "ALLOW_PUBLIC")
	setenv(t, "PROXY_USERS", "alice:s3cret")

	pool := NewWANPool(1, 0)
	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)
	if err := waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second); err != nil {
		t.Fatalf("proxy did not start: %v", err)
	}

	resp := proxyConnect(t, proxyPort, "example.com:443", "")
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Errorf("no credentials: status = %d, want 407", resp.StatusCode)
	}
	if sa := resp.Header.Get("Proxy-Authenticate"); !strings.Contains(sa, "Basic") {
		t.Errorf("Proxy-Authenticate = %q, want a Basic challenge", sa)
	}

	resp = proxyConnect(t, proxyPort, "example.com:443", basicAuthValue("alice", "wrong"))
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Errorf("wrong password: status = %d, want 407", resp.StatusCode)
	}

	resp = proxyConnect(t, proxyPort, "example.com:443", basicAuthValue("mallory", "s3cret"))
	if resp.StatusCode != http.StatusProxyAuthRequired {
		t.Errorf("wrong user: status = %d, want 407", resp.StatusCode)
	}

	resp = proxyConnect(t, proxyPort, "example.com:443", basicAuthValue("alice", "s3cret"))
	if resp.StatusCode == http.StatusProxyAuthRequired {
		t.Fatalf("valid credentials: status = 407, want the request to pass auth")
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("valid credentials: status = %d, want 503 (auth passed, no WAN)", resp.StatusCode)
	}
}

// TestProxyConnect_AuthSuccess: a full tunnel through the proxy while
// PROXY_USERS is set, using valid Proxy-Authorization.
func TestProxyConnect_AuthSuccess(t *testing.T) {
	setenv(t, "PROXY_USERS", "alice:s3cret")

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
		for {
			c, err := echoLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(c)
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
	if err := waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second); err != nil {
		t.Fatalf("proxy did not start: %v", err)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", proxyPort))
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	echoAddr := echoLn.Addr().String()
	fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n",
		echoAddr, echoAddr, basicAuthValue("alice", "s3cret"))

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (authenticated tunnel established)", resp.StatusCode)
	}

	payload := []byte("authenticated hello")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("echo = %q, want %q", got, payload)
	}
}

// TestProxyTargetBlocklist: the SSRF guard is tested both ways on the real
// front-end — blocked unless ALLOW_PRIVATE_TARGETS=true, public targets
// always allowed.
func TestProxyTargetBlocklist(t *testing.T) {
	unsetenv(t, "PROXY_USERS")

	pool := NewWANPool(1, 0) // empty: allowed targets fall through to 503
	proxyPort := freePort(t)
	proxy := NewProxyServer(proxyPort, pool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go proxy.Start(ctx)
	if err := waitForPort(fmt.Sprintf("127.0.0.1:%d", proxyPort), 2*time.Second); err != nil {
		t.Fatalf("proxy did not start: %v", err)
	}

	setenv(t, "ALLOW_PRIVATE_TARGETS", "false")
	for _, target := range []string{"127.0.0.1:8080", "169.254.169.254:80", "localhost:80", "[::1]:443"} {
		resp := proxyConnect(t, proxyPort, target, "")
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("target %s (guard on): status = %d, want 403", target, resp.StatusCode)
		}
	}
	// Public targets are never blocked: they fall through to routing
	// (empty pool → 503, not 403).
	resp := proxyConnect(t, proxyPort, "example.com:443", "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("public target (guard on): status = %d, want 503", resp.StatusCode)
	}

	// Opt-in: private targets fall through to routing instead of 403.
	setenv(t, "ALLOW_PRIVATE_TARGETS", "true")
	resp = proxyConnect(t, proxyPort, "127.0.0.1:8080", "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("target 127.0.0.1:8080 (guard off): status = %d, want 503", resp.StatusCode)
	}
}

// TestSocksTargetBlocklist: same both-ways pin on the SOCKS5 front-end
// (REP 0x02 = connection not allowed by ruleset).
func TestSocksTargetBlocklist(t *testing.T) {
	unsetenv(t, "PROXY_USERS")
	addr := startSocksServer(t, socksTestWAN(t))
	echoAddr := startEchoServer(t)
	_, echoPortStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo addr: %v", err)
	}
	echoPort, _ := strconv.Atoi(echoPortStr)

	setenv(t, "ALLOW_PRIVATE_TARGETS", "false")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	socksGreet(t, conn)
	reply := socksSendRequest(t, conn, 0x01, 0x01, "127.0.0.1", echoPort)
	conn.Close()
	if reply[1] != 0x02 {
		t.Errorf("private target (guard on): REP = 0x%02x, want 0x02", reply[1])
	}

	setenv(t, "ALLOW_PRIVATE_TARGETS", "true")
	conn, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()
	socksGreet(t, conn)
	reply = socksSendRequest(t, conn, 0x01, 0x01, "127.0.0.1", echoPort)
	if reply[1] != 0x00 {
		t.Errorf("private target (guard off): REP = 0x%02x, want 0x00", reply[1])
	}
	payload := []byte("guard off")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("echo = %q, want %q", got, payload)
	}
}

// TestSocksRFC1929: with PROXY_USERS set the server negotiates RFC 1929
// user/password, rejects wrong credentials with STATUS 0x01, refuses
// no-auth-only greetings with 0xFF, and admits valid credentials.
func TestSocksRFC1929(t *testing.T) {
	setenv(t, "PROXY_USERS", "alice:s3cret")

	pool := socksTestWAN(t)
	addr := startSocksServer(t, pool)
	echoAddr := startEchoServer(t)
	echoHost, echoPortStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo addr: %v", err)
	}
	echoPort, _ := strconv.Atoi(echoPortStr)

	// Greeting offering no-auth + user/pass: server must select 0x02.
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	if reply := socksGreetWith(t, conn, 0x00, 0x02); reply != [2]byte{0x05, 0x02} {
		t.Errorf("greeting reply = %v, want [5 2] (RFC 1929 selected)", reply)
	}
	// Wrong password → STATUS 0x01.
	if reply := socksUserPass(t, conn, "alice", "wrong"); reply != [2]byte{0x01, 0x01} {
		t.Errorf("wrong-password subnegotiation reply = %v, want [1 1]", reply)
	}
	conn.Close()

	// A client offering only no-auth cannot skip the required auth.
	conn, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	if reply := socksGreetWith(t, conn, 0x00); reply != [2]byte{0x05, 0xFF} {
		t.Errorf("no-auth-only greeting reply = %v, want [5 FF]", reply)
	}
	conn.Close()

	// A client offering only user/pass (no no-auth) is accepted and may
	// proceed after valid credentials.
	conn, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()
	if reply := socksGreetWith(t, conn, 0x02); reply != [2]byte{0x05, 0x02} {
		t.Fatalf("user/pass-only greeting reply = %v, want [5 2]", reply)
	}
	if reply := socksUserPass(t, conn, "alice", "s3cret"); reply != [2]byte{0x01, 0x00} {
		t.Fatalf("valid subnegotiation reply = %v, want [1 0]", reply)
	}
	reply := socksSendRequest(t, conn, 0x01, 0x01, echoHost, echoPort)
	if reply[1] != 0x00 {
		t.Fatalf("connect reply = 0x%02x, want 0x00", reply[1])
	}
	payload := []byte("rfc1929 ok")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("echo = %q, want %q", got, payload)
	}
}

// TestAPIBearerAuth: /api/* requires an Authorization: Bearer header when
// a token is configured; without a token the API stays open (default).
func TestAPIBearerAuth(t *testing.T) {
	triggerCycle = make(chan struct{}, 1)
	pool := NewWANPool(0, 10700)

	// No token configured → open (unchanged default behavior).
	open := NewAPIHandler(pool)
	rec := httptest.NewRecorder()
	open.ServeHTTP(rec, httptest.NewRequest("GET", "/api/viberoxy/cycle", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("no token configured: status = %d, want 200", rec.Code)
	}

	gated := NewAPIHandler(pool, &proxycfg.Config{APIToken: "sekret"})

	// No header → 403.
	rec = httptest.NewRecorder()
	gated.ServeHTTP(rec, httptest.NewRequest("GET", "/api/viberoxy/cycle", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("no header: status = %d, want 403", rec.Code)
	}

	// Wrong token → 403.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/viberoxy/cycle", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	gated.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("wrong token: status = %d, want 403", rec.Code)
	}

	// Correct token → 200.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/viberoxy/cycle", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	gated.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("valid token: status = %d, want 200", rec.Code)
	}

	// Gating covers every /api/* route, including POST endpoints.
	rec = httptest.NewRecorder()
	gated.ServeHTTP(rec, httptest.NewRequest("POST", "/api/viberoxy/cycle/trigger", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("POST without token: status = %d, want 403", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/viberoxy/cycle/trigger", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	gated.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Errorf("POST with token: status = %d, want 202", rec.Code)
	}
}

// TestMetricsBearerAuth: the metrics endpoint is bearer-gated the same way
// (main.go composes gateMetrics over NewObservabilityHandler); health
// probes stay open.
func TestMetricsBearerAuth(t *testing.T) {
	pool := NewWANPool(1, 10700)
	h := gateMetrics("sekret", NewObservabilityHandler(pool))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("/metrics without token: status = %d, want 403", rec.Code)
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("/metrics with token: status = %d, want 200", rec.Code)
	}

	// Probes are not token-gated.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz without token: status = %d, want 200", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code == http.StatusForbidden {
		t.Errorf("/readyz without token: status = 403, probes must stay open")
	}
}

// TestCandidatesRedactCredentials: /api/viberoxy/candidates must never
// return Raw share links — they carry UUIDs, passwords and keys. The
// response exposes only identity fields plus a sha256 of the Raw value.
func TestCandidatesRedactCredentials(t *testing.T) {
	fixtures := []struct {
		server   string
		protocol string
		port     int
		raw      string
	}{
		{
			server:   "1.2.3.4",
			protocol: "vless",
			port:     443,
			raw:      "vless://11111111-2222-3333-4444-555555555555@1.2.3.4:443?encryption=none&type=tcp#leaky",
		},
		{
			server:   "5.6.7.8",
			protocol: "trojan",
			port:     443,
			raw:      "trojan://s3cr3t-trojan-pass@5.6.7.8:443?security=tls#t",
		},
		{
			server:   "9.9.9.8",
			protocol: "ss",
			port:     8388,
			raw:      "ss://YWVzLTI1Ni1nY206czNjcmV0cGFzcw==@9.9.9.8:8388#s",
		},
	}

	p := NewCandidatePool(10)
	results := make([]*TestResult, 0, len(fixtures))
	for i, f := range fixtures {
		results = append(results, &TestResult{
			Config: &proxycfg.ProxyConfig{
				Name:     fmt.Sprintf("cfg-%d", i),
				Protocol: f.protocol,
				Server:   f.server,
				Port:     f.port,
				Raw:      f.raw,
			},
			Speed: float64(100 - i),
		})
	}
	p.Update(results)
	candidatePool = p
	defer func() { candidatePool = nil }()

	rec := httptest.NewRecorder()
	handleGetCandidates().ServeHTTP(rec, httptest.NewRequest("GET", "/api/viberoxy/candidates", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	// No raw share links, no credential substrings, no URI scheme at all.
	for _, f := range fixtures {
		if strings.Contains(body, f.raw) {
			t.Errorf("candidates body leaks a Raw share link: %s", f.raw)
		}
	}
	for _, secret := range []string{
		"11111111-2222-3333-4444-555555555555", // vless UUID
		"s3cr3t-trojan-pass",                   // trojan password
		"YWVzLTI1Ni1nY206czNjcmV0cGFzcw",       // ss base64 userinfo (method:password)
		`"raw"`,
		"://",
	} {
		if strings.Contains(body, secret) {
			t.Errorf("candidates body contains credential material %q: %s", secret, body)
		}
	}

	// The required identity fields are present, raw_sha256 matches sha256(Raw).
	var list []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(list) != len(fixtures) {
		t.Fatalf("got %d candidates, want %d", len(list), len(fixtures))
	}
	wantSHA := map[string]string{}
	for _, f := range fixtures {
		sum := sha256.Sum256([]byte(f.raw))
		wantSHA[f.server] = hex.EncodeToString(sum[:])
	}
	for i, entry := range list {
		for _, key := range []string{"name", "server", "protocol", "port", "raw_sha256"} {
			if _, ok := entry[key]; !ok {
				t.Errorf("entry %d missing field %q: %v", i, key, entry)
			}
		}
		if _, ok := entry["raw"]; ok {
			t.Errorf("entry %d still carries a raw field: %v", i, entry)
		}
		server, _ := entry["server"].(string)
		sha, _ := entry["raw_sha256"].(string)
		if want := wantSHA[server]; sha != want {
			t.Errorf("entry %d raw_sha256 = %q, want %q (server %q)", i, sha, want, server)
		}
	}
}
