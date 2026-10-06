package testutil_test

// Self-tests for the fault-injection harness: at least one test per fault mode
// proving the documented behavior. The AcceptClose/Blackhole/RstAfter tests
// specifically pin the critical fidelity property: the SOCKS5 handshake
// SUCCEEDS and only the data phase faults (like real xray, which replies with
// SUCCESS before dialing the upstream target).

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"viberoxy/internal/testutil"
)

// newSOCKSClient returns a FakeClient that negotiates SOCKS5 first (FakeWAN is
// a SOCKS5 server, and the campaign's system under test is a SOCKS5 front-end).
func newSOCKSClient(id string, timeout time.Duration) *testutil.FakeClient {
	c := testutil.NewFakeClient(id)
	c.Socks5 = true
	c.Timeout = timeout
	return c
}

func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// closedWithoutResponse reports errors consistent with "peer closed or reset
// the connection without sending response bytes".
func closedWithoutResponse(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

// corruptRandomByte flips one byte inside the payload's random field (between
// the second and third '|') and returns the corrupted copy.
func corruptRandomByte(p []byte) []byte {
	out := append([]byte(nil), p...)
	first := bytes.IndexByte(out, '|')
	second := bytes.IndexByte(out[first+1:], '|') + first + 1
	out[second+1] ^= 0x01
	return out
}

// ---------- Good ----------

func TestGoodModeEchoesTagPrefixedPayload(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-A", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	c := newSOCKSClient("cli", 3*time.Second)
	for seq := int64(1); seq <= 2; seq++ {
		r := c.Send(w.Addr(), seq)
		if !r.OK {
			t.Fatalf("send %d: OK=false err=%v", seq, r.Err)
		}
		if r.Tag != "wan-A" {
			t.Fatalf("send %d: tag %q, want %q (the tag must identify the serving fake)", seq, r.Tag, "wan-A")
		}
		if r.Bytes <= 0 {
			t.Fatalf("send %d: Bytes=%d, want > 0", seq, r.Bytes)
		}
	}
	if got := w.Hits(); got != 2 {
		t.Fatalf("Hits=%d, want 2", got)
	}
}

func TestGoodModeLatencyDelaysFirstResponseByte(t *testing.T) {
	const latency = 150 * time.Millisecond
	w, err := testutil.NewFakeWAN("wan-L", testutil.Good(latency, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	start := time.Now()
	r := newSOCKSClient("cli", 3*time.Second).Send(w.Addr(), 1)
	elapsed := time.Since(start)
	if !r.OK {
		t.Fatalf("send failed: %v", r.Err)
	}
	if elapsed < latency {
		t.Fatalf("first response after %v, want >= %v of configured latency", elapsed, latency)
	}
}

func TestGoodModeBandwidthPacingSlowsResponse(t *testing.T) {
	const bps = 1000
	w, err := testutil.NewFakeWAN("wan-P", testutil.Good(0, bps))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// A long client ID makes the payload ~200 bytes; at 1000 B/s the echo
	// must take roughly 200ms. The lower bound is deliberately conservative.
	longID := strings.Repeat("p", 180)
	start := time.Now()
	r := newSOCKSClient(longID, 5*time.Second).Send(w.Addr(), 1)
	elapsed := time.Since(start)
	if !r.OK {
		t.Fatalf("send failed: %v", r.Err)
	}
	if min := 140 * time.Millisecond; elapsed < min {
		t.Fatalf("echo of %d bytes at %d B/s finished in %v, want >= %v (bandwidth pacing ignored)",
			r.Bytes, bps, elapsed, min)
	}
}

// ---------- AcceptClose ----------

func TestAcceptCloseModeHandshakeSucceedsThenCloses(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-C", testutil.AcceptClose())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	r := newSOCKSClient("cli", 3*time.Second).Send(w.Addr(), 1)
	if r.OK {
		t.Fatal("connection verified OK against a dead WAN")
	}
	// The fault fidelity this harness exists for: the SOCKS5 handshake must
	// have SUCCEEDED (otherwise the error would wrap ErrHandshake) and the
	// failure must surface in the data phase with zero response bytes.
	if errors.Is(r.Err, testutil.ErrHandshake) {
		t.Fatalf("SOCKS5 handshake failed (%v); the fake must accept the CONNECT and fail only in the data phase", r.Err)
	}
	if r.Bytes != 0 {
		t.Fatalf("Bytes=%d, want 0 (AcceptClose sends nothing after the handshake)", r.Bytes)
	}
	if !closedWithoutResponse(r.Err) {
		t.Fatalf("err=%v, want EOF/reset/broken-pipe from the immediate close", r.Err)
	}
}

// ---------- Blackhole ----------

func TestBlackholeModeHandshakeSucceedsThenStalls(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-K", testutil.Blackhole())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	r := newSOCKSClient("cli", 250*time.Millisecond).Send(w.Addr(), 1)
	if r.OK {
		t.Fatal("connection verified OK against a black-holing WAN")
	}
	if errors.Is(r.Err, testutil.ErrHandshake) {
		t.Fatalf("SOCKS5 handshake failed (%v); the fake must accept the CONNECT and only then stall", r.Err)
	}
	if !isNetTimeout(r.Err) {
		t.Fatalf("err=%v, want a deadline timeout (the flow must be observably stuck, not closed)", r.Err)
	}
	if r.Bytes != 0 {
		t.Fatalf("Bytes=%d, want 0 (Blackhole neither reads nor writes)", r.Bytes)
	}
}

// ---------- Refuse ----------

func TestRefuseModeDialsFailWithConnectionRefused(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-R", testutil.Refuse())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	r := newSOCKSClient("cli", 3*time.Second).Send(w.Addr(), 1)
	if r.OK {
		t.Fatal("connection verified OK against a refused WAN")
	}
	if !errors.Is(r.Err, syscall.ECONNREFUSED) {
		t.Fatalf("err=%v, want connection refused", r.Err)
	}
	// Double-check at the socket level: nothing may be listening.
	if c, err := net.DialTimeout("tcp", w.Addr(), 200*time.Millisecond); err == nil {
		c.Close()
		t.Fatal("something accepted a connection in Refuse mode")
	}
}

// ---------- RstAfter ----------

func TestRstAfterModeAbortsMidStreamWithRST(t *testing.T) {
	const (
		tag          = "wan-S"
		payloadBytes = 16
	)
	w, err := testutil.NewFakeWAN(tag, testutil.RstAfter(payloadBytes))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// clientID "rst-client" makes the payload ~30 bytes, well past the cap.
	r := newSOCKSClient("rst-client", 3*time.Second).Send(w.Addr(), 1)
	if r.OK {
		t.Fatal("connection verified OK although the WAN resets mid-stream")
	}
	if errors.Is(r.Err, testutil.ErrHandshake) {
		t.Fatalf("SOCKS5 handshake failed (%v); the fake must accept the CONNECT and only then reset", r.Err)
	}
	if !errors.Is(r.Err, syscall.ECONNRESET) {
		t.Fatalf("err=%v, want ECONNRESET (a plain FIN/EOF would hide the RST semantics)", r.Err)
	}
	want := int64(len("["+tag+"]") + payloadBytes)
	if r.Bytes != want {
		t.Fatalf("Bytes=%d, want %d (tag prefix + exactly %d echoed payload bytes)", r.Bytes, want, payloadBytes)
	}
}

// ---------- SlowTTFB ----------

func TestSlowTTFBModeDelaysFirstResponseByte(t *testing.T) {
	const ttfb = 150 * time.Millisecond
	w, err := testutil.NewFakeWAN("wan-T", testutil.SlowTTFB(ttfb))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	start := time.Now()
	r := newSOCKSClient("cli", 3*time.Second).Send(w.Addr(), 1)
	elapsed := time.Since(start)
	if !r.OK {
		t.Fatalf("send failed: %v", r.Err)
	}
	if elapsed < ttfb {
		t.Fatalf("first response after %v, want >= %v of configured TTFB delay", elapsed, ttfb)
	}
}

// ---------- Flaky ----------

func TestFlakyModeIsDeterministicPerSeed(t *testing.T) {
	const (
		n      = 24
		p      = 0.5
		seed   = int64(42)
		prefix = "wan-F"
	)
	run := func() []bool {
		w, err := testutil.NewFakeWAN(prefix, testutil.Flaky(p, seed))
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		out := make([]bool, n)
		for i := range out {
			c := newSOCKSClient("cli", 3*time.Second)
			r := c.Send(w.Addr(), int64(i))
			if r.OK {
				out[i] = false // echoed: the good outcome
				if r.Tag != prefix {
					t.Fatalf("conn %d: tag %q, want %q", i, r.Tag, prefix)
				}
			} else {
				out[i] = true // AcceptClose outcome
				if errors.Is(r.Err, testutil.ErrHandshake) || !closedWithoutResponse(r.Err) {
					t.Fatalf("conn %d: flaky dead outcome must be handshake-OK-then-close, got err=%v", i, r.Err)
				}
			}
		}
		return out
	}

	first := run()
	second := run()
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("conn %d differs across runs with seed %d: %v vs %v (flaky draws are not deterministic)", i, seed, first, second)
		}
	}

	// The pattern must be exactly the seeded draw sequence: one Float64 draw
	// per accepted connection, in accept order, dead when draw < p.
	rng := rand.New(rand.NewSource(seed))
	var dead int
	for i := 0; i < n; i++ {
		want := rng.Float64() < p
		if first[i] != want {
			t.Fatalf("conn %d: dead=%v, want %v (seed %d must reproduce the draw sequence)", i, first[i], want, seed)
		}
		if want {
			dead++
		}
	}
	if dead == 0 || dead == n {
		t.Fatalf("seed %d produced %d/%d dead conns; need both outcomes to prove per-connection probability", seed, dead, n)
	}

	// Edge probabilities: p=0 never fails, p=1 always fails.
	w0, err := testutil.NewFakeWAN("wan-F0", testutil.Flaky(0, 7))
	if err != nil {
		t.Fatal(err)
	}
	defer w0.Close()
	for i := 0; i < 6; i++ {
		if r := newSOCKSClient("cli", 3*time.Second).Send(w0.Addr(), int64(i)); !r.OK {
			t.Fatalf("p=0 conn %d failed: %v", i, r.Err)
		}
	}
	w1, err := testutil.NewFakeWAN("wan-F1", testutil.Flaky(1, 7))
	if err != nil {
		t.Fatal(err)
	}
	defer w1.Close()
	for i := 0; i < 6; i++ {
		if r := newSOCKSClient("cli", 3*time.Second).Send(w1.Addr(), int64(i)); r.OK {
			t.Fatalf("p=1 conn %d unexpectedly succeeded", i)
		}
	}
}

// ---------- HalfClose ----------

func TestHalfCloseModeRespondsOnlyAfterClientFIN(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-H", testutil.HalfClose())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Without a client half-close the response must never arrive: the flow is
	// observably stuck at the client deadline.
	stuck := newSOCKSClient("cli", 250*time.Millisecond).Send(w.Addr(), 1)
	if stuck.OK {
		t.Fatal("response arrived although the client never half-closed")
	}
	if !isNetTimeout(stuck.Err) {
		t.Fatalf("err=%v, want a deadline timeout while waiting for the withheld response", stuck.Err)
	}

	// With CloseWrite after the payload, the response must arrive intact.
	r := newSOCKSClient("cli", 3*time.Second).SendHalfClose(w.Addr(), 2)
	if !r.OK {
		t.Fatalf("send with half-close failed: %v", r.Err)
	}
	if r.Tag != "wan-H" {
		t.Fatalf("tag %q, want %q", r.Tag, "wan-H")
	}
}

// ---------- DieAt / ReviveAt ----------

func TestDieAtReviveAtCyclesTheListener(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-D", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	start := time.Now()
	w.DieAt(150 * time.Millisecond)
	w.ReviveAt(700 * time.Millisecond)

	if r := newSOCKSClient("cli", 3*time.Second).Send(w.Addr(), 1); !r.OK {
		t.Fatalf("send before the scheduled death failed: %v", r.Err)
	}

	// Well past DieAt, before ReviveAt: the listener must be gone.
	if wait := start.Add(400 * time.Millisecond); time.Now().Before(wait) {
		time.Sleep(time.Until(wait))
	}
	r := newSOCKSClient("cli", 3*time.Second).Send(w.Addr(), 2)
	if r.OK {
		t.Fatal("connection succeeded after DieAt fired; the listener must be closed")
	}
	if !errors.Is(r.Err, syscall.ECONNREFUSED) {
		t.Fatalf("err=%v, want connection refused after DieAt", r.Err)
	}

	// Past ReviveAt: the same address must serve again.
	deadline := time.Now().Add(3 * time.Second)
	for {
		r := newSOCKSClient("cli", 3*time.Second).Send(w.Addr(), 3)
		if r.OK {
			if r.Tag != "wan-D" {
				t.Fatalf("revived fake served with tag %q, want %q", r.Tag, "wan-D")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fake did not revive on %s after ReviveAt: last err=%v", w.Addr(), r.Err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------- tag prefix on the wire ----------

func TestTagPrefixOnTheWire(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-B", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	c, err := net.Dial("tcp", w.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))

	// Minimal SOCKS5 client (IPv4 CONNECT to 1.2.3.4:80).
	if _, err := c.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(c, method); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(method, []byte{5, 0}) {
		t.Fatalf("method reply %v, want [5 0]", method)
	}
	if _, err := c.Write([]byte{5, 1, 0, 1, 1, 2, 3, 4, 0, 80}); err != nil {
		t.Fatal(err)
	}
	rep := make([]byte, 10)
	if _, err := io.ReadFull(c, rep); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rep, []byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("CONNECT reply %v, want xray-style success [5 0 0 1 0 0 0 0 0 0]", rep)
	}
	if _, err := c.Write([]byte("PING")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("[wan-B]")+4)
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "[wan-B]PING" {
		t.Fatalf("echo %q, want %q (every echoed byte stream is prefixed with [ID])", got, "[wan-B]PING")
	}
}

// ---------- crc32 corruption ----------

func TestVerifyPayloadCatchesCorruptedByte(t *testing.T) {
	payload, err := testutil.MakePayload("cli", 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := testutil.VerifyPayload(payload); err != nil {
		t.Fatalf("intact payload rejected: %v", err)
	}
	if err := testutil.VerifyPayload(corruptRandomByte(payload)); err == nil {
		t.Fatal("corrupted payload verified OK; the crc32 must catch it")
	} else if !strings.Contains(err.Error(), "crc32") {
		t.Fatalf("err=%v, want a crc32 mismatch report", err)
	}
}

func TestCorruptedEchoIsRejectedEndToEnd(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		payload, _ := io.ReadAll(c) // until the client's half-close
		c.Write(append([]byte("[X]"), corruptRandomByte(payload)...))
	}()

	c := testutil.NewFakeClient("cli")
	c.Timeout = 3 * time.Second
	r := c.SendHalfClose(ln.Addr().String(), 1)
	if r.OK {
		t.Fatal("corrupted echo verified OK")
	}
	if !strings.Contains(r.Err.Error(), "crc32") {
		t.Fatalf("err=%v, want the crc32 check to be what catches the corruption", r.Err)
	}
}

// ---------- hot-swap and lifecycle ----------

func TestSetModeHotSwapsAndRefuseTransitionsKeepAddress(t *testing.T) {
	addr := ""
	w, err := testutil.NewFakeWAN("wan-M", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	addr = w.Addr()

	c := newSOCKSClient("cli", 3*time.Second)
	if r := c.Send(addr, 1); !r.OK {
		t.Fatalf("initial Good exchange failed: %v", r.Err)
	}

	if err := w.SetMode(testutil.AcceptClose()); err != nil {
		t.Fatal(err)
	}
	if r := c.Send(addr, 2); r.OK || errors.Is(r.Err, testutil.ErrHandshake) {
		t.Fatalf("after SetMode(AcceptClose): OK=%v err=%v; want data-phase close", r.OK, r.Err)
	}

	if err := w.SetMode(testutil.Refuse()); err != nil {
		t.Fatal(err)
	}
	if r := c.Send(addr, 3); !errors.Is(r.Err, syscall.ECONNREFUSED) {
		t.Fatalf("after SetMode(Refuse): err=%v, want connection refused", r.Err)
	}

	if err := w.SetMode(testutil.Good(0, 0)); err != nil {
		t.Fatal(err)
	}
	if got := w.Addr(); got != addr {
		t.Fatalf("address changed across mode swaps: %s -> %s", addr, got)
	}
	if r := c.Send(addr, 4); !r.OK {
		t.Fatalf("after SetMode(Good) on the same address: err=%v", r.Err)
	}
}

func TestCloseIsIdempotentAndRestartReusesAddress(t *testing.T) {
	w, err := testutil.NewFakeWAN("wan-X", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	c := newSOCKSClient("cli", 3*time.Second)
	if r := c.Send(w.Addr(), 1); !r.OK {
		t.Fatalf("initial exchange failed: %v", r.Err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close must be idempotent, got %v", err)
	}
	if r := c.Send(w.Addr(), 2); !errors.Is(r.Err, syscall.ECONNREFUSED) {
		t.Fatalf("after Close: err=%v, want connection refused", r.Err)
	}
	if err := w.SetMode(testutil.Good(0, 0)); err == nil {
		t.Fatal("SetMode on a closed FakeWAN must fail")
	}

	if err := w.Restart(); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if r := c.Send(w.Addr(), 3); !r.OK {
		t.Fatalf("after Restart on the same address: err=%v", r.Err)
	}
	if got := w.Hits(); got != 2 {
		t.Fatalf("Hits=%d, want 2 (cumulative across Restart; refused dials never hit)", got)
	}
}
