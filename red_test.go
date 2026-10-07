//go:build red

package main

// Red-test suite for the Viberoxy review (baseline commit 158e85c).
//
// Every test asserts the CORRECT behavior, so on the baseline each one FAILS.
// A fix is done when its RT-xx test passes and the full race-enabled test run stays green.
// If a fix changes an internal API (e.g. slot index -> *Path), keep the assertion
// and adapt the plumbing; do not weaken the assertion.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"viberoxy/internal/proxycfg"
)

// ---------- helpers ----------

func rtFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return p
}

// rtUpstream starts a fake xray-style SOCKS5 listener. Like real xray it
// answers CONNECT with success BEFORE any "upstream dial", then hands the
// connection to behave(). Only IPv4 targets are supported.
func rtUpstream(t *testing.T, hits *int64, behave func(c net.Conn)) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if hits != nil {
					atomic.AddInt64(hits, 1)
				}
				g := make([]byte, 3)
				if _, err := io.ReadFull(c, g); err != nil {
					return
				}
				c.Write([]byte{5, 0})
				r := make([]byte, 10)
				if _, err := io.ReadFull(c, r); err != nil {
					return
				}
				c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				behave(c)
			}(c)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func rtMarkRoutable(p *WANPool, i, port int) {
	s := p.Slots[i]
	s.mu.Lock()
	s.State = StateActive
	s.ServicePort = port
	s.Cmd = &exec.Cmd{} // non-nil => routable; no real process needed
	s.mu.Unlock()
}

func rtStartSocksFront(t *testing.T, pool *WANPool) int {
	t.Helper()
	port := rtFreePort(t)
	s := NewSocksServer(port, pool)
	s.AccessLog = false
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Listen(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return port
}

// rtSocksConnect performs a SOCKS5 CONNECT to 1.2.3.4:80 via the front-end.
func rtSocksConnect(port int) (net.Conn, byte, error) {
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, 0xFF, err
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	c.Write([]byte{5, 1, 0})
	if _, err := io.ReadFull(c, make([]byte, 2)); err != nil {
		c.Close()
		return nil, 0xFF, err
	}
	c.Write([]byte{5, 1, 0, 1, 1, 2, 3, 4, 0, 80})
	rep := make([]byte, 10)
	if _, err := io.ReadFull(c, rep); err != nil {
		c.Close()
		return nil, 0xFF, err
	}
	return c, rep[1], nil
}

// ---------- RT-01  (F-02) draining slots must not receive new connections ----------
func TestRed_RT01_DrainingSlotNotSelected(t *testing.T) {
	p := NewWANPool(2, 20000)
	rtMarkRoutable(p, 0, 20000)
	rtMarkRoutable(p, 1, 20001)
	if err := p.MarkDraining(0); err != nil {
		t.Fatal(err)
	}
	p.IncConnCount(1)
	p.IncConnCount(1) // slot 1 busier, slot 0 idle-but-draining
	if got := p.GetLeastLoaded(2); got != 1 {
		t.Fatalf("GetLeastLoaded picked draining slot %d; want active slot 1", got)
	}
}

// ---------- RT-02  (F-01) a black-holing WAN must not become the attractor ----------
func TestRed_RT02_DeadWANDoesNotAttractTraffic(t *testing.T) {
	var goodHits, deadHits int64
	good := rtUpstream(t, &goodHits, func(c net.Conn) {
		one := make([]byte, 1)
		if _, err := io.ReadFull(c, one); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
		c.Write(one)
		io.Copy(io.Discard, c)
	})
	dead := rtUpstream(t, &deadHits, func(c net.Conn) { /* accept, say OK, close: 0 bytes down */ })

	pool := NewWANPool(2, 0)
	rtMarkRoutable(pool, 0, good)
	rtMarkRoutable(pool, 1, dead)
	front := rtStartSocksFront(t, pool)

	var wg sync.WaitGroup
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				c, rep, err := rtSocksConnect(front)
				if err != nil || rep != 0 {
					continue
				}
				c.Write([]byte{'x'})
				c.Read(make([]byte, 1))
				c.Close()
			}
		}()
	}
	wg.Wait()
	t.Logf("good=%d dead=%d dead.ConsecutiveFails=%d", goodHits, deadHits, pool.SlotConsecutiveFails(1))
	if deadHits > 40 { // <=20% of 200 attempts, allowing for detection + half-open probes
		t.Fatalf("dead WAN received %d/200 connections; passive health must eject it quickly", deadHits)
	}
}

// ---------- RT-03  (F-04) connection counters never go negative ----------
func TestRed_RT03_ConnCountNeverNegative(t *testing.T) {
	p := NewWANPool(1, 20000)
	rtMarkRoutable(p, 0, 20000)
	p.Slots[0].Cmd = nil
	for i := 0; i < 3; i++ {
		p.IncConnCount(0)
	}
	p.ResetEmpty(0) // replacement / reap while handlers are still in flight
	for i := 0; i < 3; i++ {
		p.DecConnCount(0) // the old handlers finish afterwards
	}
	if n := atomic.LoadInt64(&p.Slots[0].ConnCount); n < 0 {
		t.Fatalf("ConnCount = %d; late decrements from old handlers leaked into the new WAN", n)
	}
}

// ---------- RT-04  (F-06) crashed xray must be detected as dead ----------
func TestRed_RT04_HealthCheckDetectsExitedProcess(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 1")
	if err := cmd.Start(); err != nil {
		t.Skip("sh unavailable")
	}
	time.Sleep(300 * time.Millisecond)
	if HealthCheckXray(cmd) {
		t.Fatal("HealthCheckXray reports an exited (zombie) process as healthy")
	}
}

// ---------- RT-05  (F-11) mux must not be combined with xtls-rprx-vision ----------
func TestRed_RT05_NoMuxWithVisionFlow(t *testing.T) {
	raw := "vless://11111111-1111-1111-1111-111111111111@1.2.3.4:443?encryption=none&flow=xtls-rprx-vision&security=reality&sni=example.com&fp=chrome&pbk=abc&sid=01&type=tcp#n"
	cfg := &proxycfg.ProxyConfig{Protocol: "vless", Server: "1.2.3.4", Port: 443, Raw: raw}
	b, err := BuildXrayConfig(cfg, 10700, true)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "\"mux\"") {
		t.Fatalf("mux enabled together with flow=xtls-rprx-vision:\n%s", b)
	}
}

// ---------- RT-06  (F-09) bytes pipelined after CONNECT must reach upstream ----------
func TestRed_RT06_ConnectPipelinedBytesPreserved(t *testing.T) {
	got := make(chan string, 1)
	up := rtUpstream(t, nil, func(c net.Conn) {
		c.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
		b := make([]byte, 4)
		n, _ := io.ReadFull(c, b)
		got <- string(b[:n])
	})
	pool := NewWANPool(1, 0)
	rtMarkRoutable(pool, 0, up)

	port := rtFreePort(t)
	ps := NewProxyServer(port, pool)
	ps.AccessLog = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ps.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Optimistic client: CONNECT and first payload bytes in ONE write.
	c.Write([]byte("CONNECT 1.2.3.4:80 HTTP/1.1\r\nHost: 1.2.3.4:80\r\n\r\nPING"))
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(c)
	if line, _ := br.ReadString('\n'); !strings.Contains(line, "200") {
		t.Fatalf("unexpected CONNECT reply %q", line)
	}
	select {
	case s := <-got:
		if s != "PING" {
			t.Fatalf("upstream received %q; want %q (hijack discarded buffered bytes)", s, "PING")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("upstream never received the pipelined bytes")
	}
}

// ---------- RT-07  (F-10) half-close must be preserved ----------
func TestRed_RT07_HalfCloseKeepsReverseDirectionOpen(t *testing.T) {
	up := rtUpstream(t, nil, func(c net.Conn) {
		io.Copy(io.Discard, c) // until client's FIN
		c.Write([]byte("BYE"))
	})
	pool := NewWANPool(1, 0)
	rtMarkRoutable(pool, 0, up)
	front := rtStartSocksFront(t, pool)

	c, rep, err := rtSocksConnect(front)
	if err != nil || rep != 0 {
		t.Fatalf("connect: rep=%d err=%v", rep, err)
	}
	defer c.Close()
	c.Write([]byte("x"))
	c.(*net.TCPConn).CloseWrite()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	b, _ := io.ReadAll(c)
	if string(b) != "BYE" {
		t.Fatalf("got %q after client half-close; want %q", b, "BYE")
	}
}

// ---------- RT-08  (F-08) dial failure on one WAN must fail over to another ----------
func TestRed_RT08_FailoverOnDialFailure(t *testing.T) {
	closed := rtFreePort(t) // nothing listens => connection refused
	good := rtUpstream(t, nil, func(c net.Conn) {
		b := make([]byte, 1)
		if _, err := io.ReadFull(c, b); err == nil {
			c.Write(b)
		}
		io.Copy(io.Discard, c)
	})
	pool := NewWANPool(2, 0)
	rtMarkRoutable(pool, 0, closed) // lowest index wins the tie, so it is tried first
	rtMarkRoutable(pool, 1, good)
	front := rtStartSocksFront(t, pool)

	c, rep, err := rtSocksConnect(front)
	if err != nil || rep != 0 {
		t.Fatalf("first connection failed (rep=%d err=%v); expected transparent retry on the healthy WAN", rep, err)
	}
	defer c.Close()
	c.Write([]byte("z"))
	b := make([]byte, 1)
	if _, err := io.ReadFull(c, b); err != nil || b[0] != 'z' {
		t.Fatalf("echo through failover path failed: %v %q", err, b)
	}
}

// ---------- RT-09  (F-12) candidate pool must not accumulate duplicates ----------
func TestRed_RT09_CandidatePoolDedupes(t *testing.T) {
	cp := NewCandidatePool(10)
	r := func() *TestResult {
		return &TestResult{Config: &proxycfg.ProxyConfig{Raw: "ss://same", Server: "1.1.1.1", Port: 1}, Speed: 10}
	}
	cp.Update([]*TestResult{r()})
	cp.Update([]*TestResult{r()})
	cp.Update([]*TestResult{r()})
	if n := len(cp.List()); n != 1 {
		t.Fatalf("pool holds %d entries for one config; want 1 (merge by Raw, keep newest)", n)
	}
}

// ---------- RT-10  (F-13) drop-and-replace must be make-before-break ----------
func TestRed_RT10_DropAndReplaceKeepsOldWANUntilCandidateValidated(t *testing.T) {
	old := exec.Command("sleep", "30")
	if err := old.Start(); err != nil {
		t.Skip("sleep unavailable")
	}
	pool := NewWANPool(1, 20000)
	pool.Slots[0].State = StateActive
	pool.Slots[0].Cmd = old
	pool.Slots[0].Config = &proxycfg.ProxyConfig{Raw: "a", Server: "1.1.1.1", Port: 1}
	defer pool.ShutdownAll()

	cp := NewCandidatePool(5)
	cp.Update([]*TestResult{{Config: &proxycfg.ProxyConfig{Raw: "b", Server: "2.2.2.2", Port: 2}, Speed: 20}})

	var stateDuringTest WANState = -1
	opts := DropAndReplaceOptions{
		Candidates: cp,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			stateDuringTest = pool.GetState(0)
			return &TestResult{Config: cfg, Speed: 20}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ ...bool) (*exec.Cmd, string, error) {
			c := exec.Command("sleep", "30")
			return c, "", c.Start()
		},
	}
	if _, err := pool.DropAndReplace(0, opts); err != nil {
		t.Fatal(err)
	}
	if stateDuringTest != StateActive {
		t.Fatalf("slot was %s while the replacement was being tested; traffic is dropped for the whole test duration", stateDuringTest)
	}
}

// ---------- RT-11  (F-03) a full pool must still evaluate candidates / rotate ----------
func TestRed_RT11_FullPoolStillEvaluatesCandidates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ss://YWVzLTI1Ni1nY206cGFzcw@9.9.9.9:8388#new\n")
	}))
	defer srv.Close()

	pool := NewWANPool(1, 20000)
	cmd := exec.Command("sleep", "30") // resolved+started before PATH is scrubbed below
	cmd.Start()
	// Hermetic "start xray" error path: xray.go spawns exec.Command("xray", ...)
	// resolved via PATH, so hide xray from this test's own environment instead
	// of skipping when xray happens to be installed.
	t.Setenv("PATH", t.TempDir())
	defer cmd.Process.Kill()
	pool.Slots[0].State = StateActive
	pool.Slots[0].Cmd = cmd
	pool.Slots[0].Config = &proxycfg.ProxyConfig{Protocol: "ss", Server: "1.1.1.1", Port: 1}
	pool.Slots[0].SpeedMbps = 5.1

	dir := t.TempDir()
	wd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(wd)
	cfg := &proxycfg.Config{SubscriberURL: srv.URL, WanCount: 1, MinimumSpeed: 5, FetchInterval: 30, TestTimeout: 3,
		TestBasePort: rtFreePort(t), WanBasePort: 20000, DownloadSize: 1000000}
	runCycle(cfg, pool, NewCandidatePool(10), time.Minute)

	b, _ := os.ReadFile("sorted.txt")
	if len(b) == 0 {
		t.Fatal("runCycle evaluated 0 candidates with a full pool: rotation/upgrade can never happen")
	}
}
