package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TestResult struct {
	Config *ProxyConfig
	Speed  float64
	// StabilityScore is the number of distinct exit IPs observed across
	// STABILITY_PROBES probes minus one: 0 means a single stable exit IP
	// (or no probes run), higher means more upstream exit churn. It is
	// used only for ranking/preference — never to reject a config.
	StabilityScore int
	Error          error
}

// TestSpeed speed-tests one config through a temp xray. It never runs
// stability probes (see TestSpeedWithStability).
func TestSpeed(cfg *ProxyConfig, testPort int, timeout time.Duration, downloadURL string, downloadSize int64) *TestResult {
	return TestSpeedWithStability(cfg, testPort, timeout, downloadURL, downloadSize, 0)
}

// TestSpeedWithStability is TestSpeed plus optional exit-IP stability
// probing: when stabilityProbes > 0 and the download succeeds, the exit IP
// is probed stabilityProbes times through the SAME temp xray (before it is
// torn down) and the distinct IP count becomes the result's
// StabilityScore. With stabilityProbes == 0 the behavior is identical to
// TestSpeed.
func TestSpeedWithStability(cfg *ProxyConfig, testPort int, timeout time.Duration, downloadURL string, downloadSize int64, stabilityProbes int) *TestResult {
	start := time.Now()
	defer func() { metricTestDuration.Observe(time.Since(start).Seconds()) }()

	cmd, configPath, err := StartXray(cfg, testPort)
	if err != nil {
		return &TestResult{Config: cfg, Error: fmt.Errorf("start xray: %w", err)}
	}
	defer StopXray(cmd, configPath)

	socksAddr := fmt.Sprintf("127.0.0.1:%d", testPort)
	// xray binds its SOCKS port asynchronously after spawn; dialing before
	// the port is open fails every test with connection refused.
	if err := waitPortReady(socksAddr, 5*time.Second); err != nil {
		return &TestResult{Config: cfg, Error: fmt.Errorf("xray not ready: %w", err)}
	}
	speed, err := DownloadMeasurer(socksAddr, downloadURL, downloadSize, timeout)
	if err != nil {
		return &TestResult{Config: cfg, Error: err}
	}
	result := &TestResult{Config: cfg, Speed: speed}
	if stabilityProbes > 0 {
		result.StabilityScore = measureStability(socksAddr, stabilityProbes, timeout)
	}
	return result
}

func TestAll(configs []*ProxyConfig, testPortBase int, timeout time.Duration, downloadURL string, downloadSize int64) []*TestResult {
	results := make([]*TestResult, len(configs))
	for i, cfg := range configs {
		results[i] = TestSpeed(cfg, testPortBase+i, timeout, downloadURL, downloadSize)
	}
	return SortResults(results)
}

func SortResults(results []*TestResult) []*TestResult {
	sorted := make([]*TestResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Error != nil && sorted[j].Error == nil {
			return false
		}
		if sorted[i].Error == nil && sorted[j].Error != nil {
			return true
		}
		return sorted[i].Speed > sorted[j].Speed
	})
	return sorted
}

func DownloadMeasurer(socksAddr string, downloadURL string, downloadSize int64, timeout time.Duration) (float64, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return socks5Dial(ctx, socksAddr, addr)
		},
	}
	client := &http.Client{
		Transport: transport,
	}
	defer client.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if downloadSize > 0 {
		reader = io.LimitReader(resp.Body, downloadSize)
	}

	n, err := io.Copy(io.Discard, reader)
	if err != nil {
		return 0, fmt.Errorf("read body: %w", err)
	}

	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	return float64(n*8) / elapsed / 1_000_000, nil
}

// ProbeWANURL is the endpoint used by keepalive probes. It is a variable so
// tests can point it at a local server instead of the real ipify service.
var ProbeWANURL = "https://api.ipify.org/"

// probeDialTarget returns the host:port pair socks5Dial should be given for a
// probe URL. socks5Dial requires an explicit port, but u.Host omits it for
// default schemes (https://api.ipify.org/ -> "api.ipify.org"), which made
// every probe of a healthy WAN fail with "missing port in address".
func probeDialTarget(u *url.URL) string {
	if _, _, err := net.SplitHostPort(u.Host); err == nil {
		return u.Host
	}
	port := "443"
	if u.Scheme == "http" {
		port = "80"
	}
	return net.JoinHostPort(u.Host, port)
}

// probeTLSConfigFn builds the TLS client config used when a probe URL uses
// https. It is a variable so tests can inject the root CA pool of a local
// httptest TLS server (the default config verifies against the system
// roots, which is correct for real endpoints like api.ipify.org).
var probeTLSConfigFn = func(hostname string) *tls.Config {
	return &tls.Config{
		ServerName: hostname,
		MinVersion: tls.VersionTLS12,
	}
}

// probeRoundTrip dials through the SOCKS5 listener at socksAddr and speaks
// one HTTP GET to probeURL. For https probe URLs the raw SOCKS connection is
// wrapped in a TLS client before any HTTP bytes are written — the dial
// target stays the same (the endpoint's 443), only the transport changes.
// The caller owns both the returned response and connection and must close
// resp.Body and conn. On error the connection is closed here.
func probeRoundTrip(socksAddr, probeURL string, timeout time.Duration) (*http.Response, net.Conn, error) {
	u, err := url.Parse(probeURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse probe url: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := socks5Dial(ctx, socksAddr, probeDialTarget(u))
	if err != nil {
		return nil, nil, fmt.Errorf("socks dial: %w", err)
	}

	// The raw conn does not honor the request context, so bound the whole
	// probe with an explicit deadline. Set it BEFORE the TLS wrap so the
	// handshake is bounded too.
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("set probe deadline: %w", err)
	}

	if u.Scheme == "https" {
		tlsConn := tls.Client(conn, probeTLSConfigFn(u.Hostname()))
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("tls handshake: %w", err)
		}
		conn = tlsConn
	}

	// The request is written with the ORIGINAL URL so the request line and
	// Host header are correct; only the transport (TLS wrap) changed.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("create probe request: %w", err)
	}
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("write probe request: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("read probe response: %w", err)
	}
	return resp, conn, nil
}

// ProbeWAN checks a WAN slot end-to-end with a lightweight HTTP GET through
// its SOCKS5 listener. Unlike DownloadMeasurer it transfers no meaningful
// payload — just enough to prove the dial + HTTP round trip both work. Any
// failure (dial, TLS handshake, write, response parse, non-2xx status, body
// read) is returned as an error.
func ProbeWAN(socksAddr string, timeout time.Duration) error {
	resp, conn, err := probeRoundTrip(socksAddr, ProbeWANURL, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("probe status: %s", resp.Status)
	}

	// Drain a small slice of the body to confirm data actually flows.
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096)); err != nil {
		return fmt.Errorf("read probe body: %w", err)
	}
	return nil
}

// probeExitIP returns the exit IP observed by an HTTP GET through the given
// SOCKS5 listener: the probe endpoint's response body (expected to be a
// bare IP, e.g. api.ipify.org). It reuses probeRoundTrip (including
// probeDialTarget for portless URLs and TLS for https) but returns the body
// instead of discarding it.
func probeExitIP(socksAddr string, timeout time.Duration) (string, error) {
	resp, conn, err := probeRoundTrip(socksAddr, ProbeWANURL, timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("probe status: %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("read probe body: %w", err)
	}
	ip := strings.TrimSpace(string(body))
	if ip == "" {
		return "", fmt.Errorf("probe body empty")
	}
	return ip, nil
}

// stabilityProbeInterval is the pause between consecutive exit-IP probes
// during a stability measurement. A variable so tests can shorten it.
var stabilityProbeInterval = 200 * time.Millisecond

// stabilityScoreFromIPs scores a set of observed exit IPs: distinct count
// minus one, so 0 = single stable exit IP and higher = more upstream churn.
// No (usable) observations yield 0.
func stabilityScoreFromIPs(ips []string) int {
	seen := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		if ip != "" {
			seen[ip] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return 0
	}
	return len(seen) - 1
}

// measureStability probes the exit IP through the given SOCKS5 listener
// `probes` times (stabilityProbeInterval apart) and scores the distinct
// IPs observed. Failed probes are skipped — all failures score 0. All
// probes go through the same tunnel config, so distinct IPs reflect
// upstream exit churn.
func measureStability(socksAddr string, probes int, timeout time.Duration) int {
	ips := make([]string, 0, probes)
	for i := 0; i < probes; i++ {
		if ip, err := probeExitIP(socksAddr, timeout); err == nil {
			ips = append(ips, ip)
		}
		if i < probes-1 {
			time.Sleep(stabilityProbeInterval)
		}
	}
	return stabilityScoreFromIPs(ips)
}

// bestNewCandidate picks the best replacement candidate among tested
// results: the fastest config that cleared minimumSpeed and is not already
// active. Speed ties (within a small epsilon) are broken by the lower
// StabilityScore (more stable), then earlier test order. Returns nil when
// no candidate qualifies. Speed-first: a slower but more stable config is
// never preferred over a faster one.
func bestNewCandidate(results []*TestResult, minimumSpeed float64, alreadyActive func(*ProxyConfig) bool) *TestResult {
	var best *TestResult
	for _, r := range results {
		if r.Error != nil || r.Speed < minimumSpeed {
			continue
		}
		if alreadyActive != nil && alreadyActive(r.Config) {
			continue
		}
		if best == nil {
			best = r
			continue
		}
		if r.Speed > best.Speed+0.001 {
			best = r // strictly faster wins
			continue
		}
		if r.Speed > best.Speed-0.001 && r.StabilityScore < best.StabilityScore {
			best = r // speed tie within epsilon: more stable wins
		}
	}
	return best
}

func waitPortReady(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("port %s not reachable within %v", addr, timeout)
}

func socks5Dial(ctx context.Context, socksAddr, targetAddr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", socksAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to socks: %w", err)
	}
	tuneTCPConn(conn)

	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("auth write: %w", err)
	}

	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		conn.Close()
		return nil, fmt.Errorf("auth read: %w", err)
	}
	if buf[0] != 0x05 || buf[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("auth failed: ver=%d status=%d", buf[0], buf[1])
	}

	host, portStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("split host port: %w", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		conn.Close()
		return nil, fmt.Errorf("invalid port: %s", portStr)
	}

	ip := net.ParseIP(host)
	var req []byte
	if ip != nil && ip.To4() != nil {
		ip4 := ip.To4()
		req = []byte{0x05, 0x01, 0x00, 0x01, ip4[0], ip4[1], ip4[2], ip4[3], byte(port >> 8), byte(port)}
	} else if ip != nil {
		ip16 := ip.To16()
		req = append([]byte{0x05, 0x01, 0x00, 0x04}, ip16...)
		req = append(req, byte(port>>8), byte(port))
	} else {
		if len(host) > 255 {
			conn.Close()
			return nil, fmt.Errorf("hostname too long: %d", len(host))
		}
		req = append([]byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}, []byte(host)...)
		req = append(req, byte(port>>8), byte(port))
	}

	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect write: %w", err)
	}

	resp := make([]byte, 4)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect response: %w", err)
	}
	if resp[0] != 0x05 || resp[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("connect failed: ver=%d rep=%d", resp[0], resp[1])
	}

	switch resp[3] {
	case 0x01:
		rest := make([]byte, 6)
		if _, err := io.ReadFull(conn, rest); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind addr ipv4: %w", err)
		}
	case 0x03:
		lenByte := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenByte); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind domain len: %w", err)
		}
		rest := make([]byte, int(lenByte[0])+2)
		if _, err := io.ReadFull(conn, rest); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind domain: %w", err)
		}
	case 0x04:
		rest := make([]byte, 18)
		if _, err := io.ReadFull(conn, rest); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind addr ipv6: %w", err)
		}
	default:
		conn.Close()
		return nil, fmt.Errorf("unknown atyp: %d", resp[3])
	}

	return conn, nil
}
