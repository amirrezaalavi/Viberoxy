// Package testutil provides a hermetic fault-injection harness for tests of
// TCP relays and SOCKS5 front-ends.
//
// FakeWAN is a scriptable stand-in for a per-slot xray SOCKS5 inbound. Like
// real xray it replies to a CONNECT request with SUCCESS *before* dialing the
// upstream target, so a dead or failing "WAN" still completes the SOCKS5
// handshake and only misbehaves during the data phase. A fake that failed the
// handshake would hide the exact bug class this harness exists to catch.
//
// FakeClient dials a system under test, sends a unique self-verifying payload
// of the form "clientID|seq|random|crc32", and checks that the response is
// some fake's "[ID]" tag followed by exactly the payload it sent.
//
// The package uses only the standard library and is safe for concurrent use.
package testutil

import (
	"bytes"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// modeKind identifies the fault mode of a Mode.
type modeKind int

const (
	kindGood modeKind = iota
	kindSlowTTFB
	kindAcceptClose
	kindBlackhole
	kindRefuse
	kindRstAfter
	kindFlaky
	kindHalfClose
)

// Mode describes how a FakeWAN behaves after (or instead of) completing the
// SOCKS5 handshake. Build one with the constructor functions in this package;
// the zero value is not a valid mode.
type Mode struct {
	kind      modeKind
	delay     time.Duration // Good: data-phase latency; SlowTTFB: first-response-byte delay
	bandwidth int64         // Good: rough echo pacing in bytes per second (0 = unlimited)
	rstAfter  int64         // RstAfter: payload bytes to echo before the RST abort
	flakyP    float64       // Flaky: probability of an AcceptClose outcome per connection
	flakySeed int64         // Flaky: seed for the deterministic per-connection draws
}

// String names the mode for debugging output.
func (m Mode) String() string {
	switch m.kind {
	case kindGood:
		return fmt.Sprintf("Good(%v,%d)", m.delay, m.bandwidth)
	case kindSlowTTFB:
		return fmt.Sprintf("SlowTTFB(%v)", m.delay)
	case kindAcceptClose:
		return "AcceptClose"
	case kindBlackhole:
		return "Blackhole"
	case kindRefuse:
		return "Refuse"
	case kindRstAfter:
		return fmt.Sprintf("RstAfter(%d)", m.rstAfter)
	case kindFlaky:
		return fmt.Sprintf("Flaky(%v,%d)", m.flakyP, m.flakySeed)
	case kindHalfClose:
		return "HalfClose"
	}
	return "Mode(?)"
}

// Good is the healthy WAN: the handshake succeeds and client payload bytes are
// echoed back prefixed with the fake's "[ID]" tag. The first response byte is
// delayed by latency, and when bytesPerSec > 0 the echo is paced at roughly
// that bandwidth. The latency models round-trip slowness: the data phase (in
// both directions) starts only after it has elapsed.
func Good(latency time.Duration, bytesPerSec int64) Mode {
	return Mode{kind: kindGood, delay: latency, bandwidth: bytesPerSec}
}

// AcceptClose is the classic dead-WAN traffic attractor: the SOCKS5 handshake
// succeeds, then the connection is closed immediately — zero bytes down.
func AcceptClose() Mode { return Mode{kind: kindAcceptClose} }

// Blackhole completes the handshake and then neither reads nor writes, so
// flows through it are observably stuck until a deadline or shutdown.
func Blackhole() Mode { return Mode{kind: kindBlackhole} }

// Refuse runs no listener at all (the port is reserved at construction and the
// socket left closed), so dials fail with connection refused — the shape of an
// xray process that is not listening.
func Refuse() Mode { return Mode{kind: kindRefuse} }

// RstAfter echoes at most n payload bytes (after the "[ID]" tag) and then
// aborts the connection with a TCP RST (SetLinger(0) + Close). With n <= 0 the
// abort happens immediately after the handshake. A short drain delay before
// the abort lets the already-echoed bytes reach the client first.
func RstAfter(n int64) Mode { return Mode{kind: kindRstAfter, rstAfter: n} }

// SlowTTFB is like Good(0, 0) but the first response byte is delayed by d:
// client bytes are consumed normally while the response is withheld, which
// keeps the upstream direction fast and only the time-to-first-byte slow.
func SlowTTFB(d time.Duration) Mode { return Mode{kind: kindSlowTTFB, delay: d} }

// Flaky resolves per connection: with probability p the connection behaves
// like AcceptClose, otherwise like Good(0, 0). Outcomes are drawn from a
// math/rand source seeded with seed, one draw per accepted connection in
// accept order, so a fixed seed reproduces an exact outcome sequence for any
// given connection order. A fresh sequence starts on construction and on every
// SetMode(Flaky(...)).
func Flaky(p float64, seed int64) Mode {
	return Mode{kind: kindFlaky, flakyP: p, flakySeed: seed}
}

// HalfClose withholds its response — the "[ID]" tag plus the payload it
// received — until the client performs a TCP half-close (CloseWrite), and then
// sends it and closes. It proves relay half-close plumbing: if a relay never
// forwards the FIN, the response never arrives.
func HalfClose() Mode { return Mode{kind: kindHalfClose} }

// rstDrainDelay is how long a RstAfter mode lets already-echoed bytes drain to
// the client before issuing the RST. The abort itself is immediate once the
// cap is reached; the delay only keeps delivered bytes and the RST from racing
// in the kernel (an RST can discard bytes still sitting unread).
const rstDrainDelay = 25 * time.Millisecond

// FakeWAN is a fake xray-style SOCKS5 upstream with a scriptable fault mode
// (see Mode). Every byte stream it echoes back is prefixed with its "[ID]" tag
// so tests can assert which fake served a connection.
//
// The address returned by Addr is fixed at construction and survives
// Die/Revive/Restart, so a system under test may keep dialing it across a
// simulated crash. All methods are safe for concurrent use.
type FakeWAN struct {
	// ID tags every echoed byte stream: responses are "[ID]" + payload.
	// IDs must not contain '[' or ']'. Treat ID as immutable once the fake
	// has been constructed.
	ID string

	addr string // stable "127.0.0.1:port" for the fake's whole lifetime

	mu     sync.Mutex
	mode   Mode
	rng    *rand.Rand // Flaky draws; guarded by mu
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	timers []*time.Timer
	done   chan struct{} // closed by Close; replaces on Restart

	hits atomic.Int64
}

// NewFakeWAN starts a fake WAN with the given tag ID and fault mode, listening
// on an ephemeral 127.0.0.1 port. In Refuse mode the port is reserved and the
// listener left closed so dials fail with connection refused while the address
// stays stable.
func NewFakeWAN(id string, mode Mode) (*FakeWAN, error) {
	if id == "" {
		return nil, errors.New("testutil: FakeWAN ID must not be empty")
	}
	if strings.ContainsAny(id, "[]") {
		return nil, fmt.Errorf("testutil: FakeWAN ID %q must not contain '[' or ']'", id)
	}
	w := &FakeWAN{
		ID:    id,
		mode:  mode,
		conns: make(map[net.Conn]struct{}),
		done:  make(chan struct{}),
	}
	if mode.kind == kindFlaky {
		w.rng = rand.New(rand.NewSource(mode.flakySeed))
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	w.addr = ln.Addr().String()
	if mode.kind == kindRefuse {
		// Reserve the address, refuse every dial.
		ln.Close()
		return w, nil
	}
	w.startLocked(ln)
	return w, nil
}

// Addr returns the fake's stable "127.0.0.1:port" address.
func (w *FakeWAN) Addr() string { return w.addr }

// Port returns the fake's port.
func (w *FakeWAN) Port() int {
	_, p, _ := net.SplitHostPort(w.addr)
	port, _ := strconv.Atoi(p)
	return port
}

// Hits reports how many TCP connections the fake has accepted. The counter is
// cumulative across Die/Revive/Restart.
func (w *FakeWAN) Hits() int64 { return w.hits.Load() }

// SetMode hot-swaps the fault mode: connections accepted from now on use m.
// Switching to or from Refuse closes/reopens the listener while the address
// stays the same. A Flaky mode restarts its seeded draw sequence. Connections
// already in flight keep the mode they were accepted under. SetMode fails on a
// closed fake (see Close).
func (w *FakeWAN) SetMode(m Mode) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("testutil: SetMode on a closed FakeWAN (use Restart)")
	}
	w.mode = m
	if m.kind == kindFlaky {
		w.rng = rand.New(rand.NewSource(m.flakySeed))
	}
	wantListener := m.kind != kindRefuse
	switch {
	case wantListener && w.ln == nil:
		return w.listenLocked()
	case !wantListener && w.ln != nil:
		ln := w.ln
		w.ln = nil
		ln.Close()
	}
	return nil
}

// Die simulates an xray process crash: the listener closes so dials fail with
// connection refused, while the address stays reserved for Revive. Connections
// already in flight are left alone. Die is idempotent.
func (w *FakeWAN) Die() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("testutil: Die on a closed FakeWAN")
	}
	if w.ln != nil {
		ln := w.ln
		w.ln = nil
		ln.Close()
	}
	return nil
}

// DieAt schedules Die after d.
func (w *FakeWAN) DieAt(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.timers = append(w.timers, time.AfterFunc(d, func() { _ = w.Die() }))
}

// Revive reopens the listener on the same address after Die (idempotent while
// listening). It fails after Close, and in Refuse mode — a refused fake has no
// listener; SetMode first. Errors from Revive are swallowed by ReviveAt.
func (w *FakeWAN) Revive() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("testutil: Revive on a closed FakeWAN (use Restart)")
	}
	if w.mode.kind == kindRefuse {
		return errors.New("testutil: Revive with Refuse mode has no listener (use SetMode first)")
	}
	if w.ln != nil {
		return nil
	}
	return w.listenLocked()
}

// ReviveAt schedules Revive after d.
func (w *FakeWAN) ReviveAt(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.timers = append(w.timers, time.AfterFunc(d, func() { _ = w.Revive() }))
}

// Close shuts the fake down: the listener closes, scheduled DieAt/ReviveAt
// timers are cancelled and in-flight connections (including stuck Blackhole
// flows) are closed. Close is idempotent; use Restart to bring the fake back.
func (w *FakeWAN) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	for _, t := range w.timers {
		t.Stop()
	}
	w.timers = nil
	if w.ln != nil {
		w.ln.Close()
		w.ln = nil
	}
	close(w.done)
	for c := range w.conns {
		c.Close()
	}
	w.conns = make(map[net.Conn]struct{})
	return nil
}

// Restart brings a closed fake back on its original address, keeping the
// current mode and the cumulative hit counter. In Refuse mode the fake stays
// listenerless and dials keep failing with connection refused. Restart fails
// while the fake is still running.
func (w *FakeWAN) Restart() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		return errors.New("testutil: Restart on a running FakeWAN")
	}
	w.closed = false
	w.done = make(chan struct{})
	if w.mode.kind == kindRefuse {
		return nil
	}
	return w.listenLocked()
}

// listenLocked starts an accept loop on w.addr. Callers hold w.mu.
func (w *FakeWAN) listenLocked() error {
	ln, err := net.Listen("tcp", w.addr)
	if err != nil {
		return fmt.Errorf("testutil: re-listen on %s: %w", w.addr, err)
	}
	w.startLocked(ln)
	return nil
}

// startLocked adopts a fresh listener. Callers hold w.mu.
func (w *FakeWAN) startLocked(ln net.Listener) {
	w.ln = ln
	go w.acceptLoop(ln)
}

// acceptLoop accepts connections and hands each one its resolved behavior.
// Flaky draws happen here, in strict accept order, so a seeded run is
// deterministic even with concurrent connections.
func (w *FakeWAN) acceptLoop(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return // listener closed: Die, SetMode(Refuse), or Close
		}
		w.handleConn(c)
	}
}

// handleConn snapshots the mode (resolving Flaky), registers the connection
// and serves it. The snapshot pins the connection to the mode it was accepted
// under; later SetMode calls only affect new connections.
func (w *FakeWAN) handleConn(c net.Conn) {
	w.hits.Add(1)
	w.mu.Lock()
	m := w.mode
	if m.kind == kindFlaky && w.rng != nil {
		if w.rng.Float64() < m.flakyP {
			m = Mode{kind: kindAcceptClose}
		} else {
			m = Mode{kind: kindGood}
		}
	}
	done := w.done
	closed := w.closed
	if !closed {
		w.conns[c] = struct{}{}
	}
	w.mu.Unlock()
	if closed {
		c.Close()
		return
	}
	go w.serve(c, m, done)
}

// serve runs one connection: the SOCKS5 handshake first (replying SUCCESS
// before any upstream dial, exactly like real xray's socks inbound), then the
// mode's data-phase behavior. Every fault therefore surfaces after a completed
// handshake — never as a handshake failure.
func (w *FakeWAN) serve(c net.Conn, m Mode, done chan struct{}) {
	defer func() {
		w.mu.Lock()
		delete(w.conns, c)
		w.mu.Unlock()
		c.Close()
	}()
	if err := socks5ServerHandshake(c); err != nil {
		return
	}
	switch m.kind {
	case kindAcceptClose:
		return // the handshake succeeded; the close below is the fault
	case kindBlackhole:
		<-done // neither read nor write; released when the fake closes
	case kindHalfClose:
		w.respondAfterFIN(c)
	case kindRstAfter:
		w.echo(c, 0, 0, m.rstAfter, true)
	case kindSlowTTFB:
		w.echo(c, m.delay, 0, 0, false)
	case kindGood:
		if m.delay > 0 {
			time.Sleep(m.delay) // data-phase latency: no reads either
		}
		w.echo(c, 0, m.bandwidth, 0, false)
	}
}

// echo relays payload bytes from c back to c as "[ID]" + payload: the tag is
// written once, immediately before the first echoed byte (so a mode that
// echoes nothing sends no tag).
//
// firstWriteDelay sleeps before that first response byte (SlowTTFB).
// bandwidth > 0 paces the write roughly at that many bytes per second.
// maxPayload > 0 caps how many payload bytes are echoed (RstAfter); with rst
// set, the connection is then aborted with a TCP RST — after a short drain
// delay so the echoed bytes are delivered first — instead of closed with a FIN.
func (w *FakeWAN) echo(c net.Conn, firstWriteDelay time.Duration, bandwidth, maxPayload int64, rst bool) {
	tag := []byte("[" + w.ID + "]")
	buf := make([]byte, 32*1024)
	var echoed int64
	untagged := true
	defer func() {
		if rst {
			if tc, ok := c.(*net.TCPConn); ok {
				// The deferred Close now sends RST instead of FIN.
				tc.SetLinger(0)
				time.Sleep(rstDrainDelay)
			}
		}
	}()
	for {
		n, err := c.Read(buf)
		if n > 0 {
			data := buf[:n]
			if maxPayload > 0 && echoed+int64(len(data)) > maxPayload {
				data = data[:maxPayload-echoed]
			}
			if len(data) > 0 {
				if untagged {
					if firstWriteDelay > 0 {
						time.Sleep(firstWriteDelay)
					}
					if _, werr := c.Write(tag); werr != nil {
						return
					}
					untagged = false
				}
				if werr := pacedWrite(c, data, bandwidth); werr != nil {
					return
				}
				echoed += int64(len(data))
			}
		}
		if err != nil {
			return
		}
		if maxPayload > 0 && echoed >= maxPayload {
			return
		}
	}
}

// pacedWrite writes data to c in small chunks, sleeping proportionally to the
// bytes sent when bandwidth > 0 (rough bandwidth pacing; 0 writes at once).
func pacedWrite(c net.Conn, data []byte, bandwidth int64) error {
	const chunk = 64
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		if _, err := c.Write(data[off:end]); err != nil {
			return err
		}
		if bandwidth > 0 {
			time.Sleep(time.Duration(float64(end-off) / float64(bandwidth) * float64(time.Second)))
		}
	}
	return nil
}

// respondAfterFIN reads the client's payload until its TCP half-close (EOF)
// and only then writes "[ID]" + payload and closes. If a relay in between
// swallows the FIN, the response never arrives.
func (w *FakeWAN) respondAfterFIN(c net.Conn) {
	payload, err := io.ReadAll(c)
	if err != nil {
		return
	}
	pacedWrite(c, append([]byte("["+w.ID+"]"), payload...), 0)
}

// socks5ServerHandshake performs the server side of a no-auth SOCKS5 session
// the way xray's socks inbound does: it reads the greeting and a CONNECT
// request (IPv4, IPv6 or domain target) and replies with SUCCESS before any
// upstream dial. Only the CONNECT command is supported.
func socks5ServerHandshake(c net.Conn) error {
	// Greeting: VER NMETHODS METHODS...
	head := make([]byte, 2)
	if _, err := io.ReadFull(c, head); err != nil {
		return err
	}
	if head[0] != 5 {
		return fmt.Errorf("testutil: unexpected SOCKS version %d", head[0])
	}
	methods := make([]byte, int(head[1]))
	if _, err := io.ReadFull(c, methods); err != nil {
		return err
	}
	if !bytes.Contains(methods, []byte{0}) {
		c.Write([]byte{5, 0xFF})
		return errors.New("testutil: client offered no no-auth method")
	}
	if _, err := c.Write([]byte{5, 0}); err != nil {
		return err
	}

	// Request: VER CMD RSV ATYP ADDR PORT
	req := make([]byte, 4)
	if _, err := io.ReadFull(c, req); err != nil {
		return err
	}
	if req[0] != 5 {
		return fmt.Errorf("testutil: unexpected SOCKS version %d in request", req[0])
	}
	if req[1] != 1 { // CONNECT only
		c.Write([]byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0})
		return fmt.Errorf("testutil: unsupported SOCKS command %d", req[1])
	}
	addrLen := 0
	switch req[3] {
	case 1: // IPv4
		addrLen = 4
	case 3: // domain
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return err
		}
		addrLen = int(l[0])
	case 4: // IPv6
		addrLen = 16
	default:
		c.Write([]byte{5, 8, 0, 1, 0, 0, 0, 0, 0, 0})
		return fmt.Errorf("testutil: unsupported address type %d", req[3])
	}
	if _, err := io.ReadFull(c, make([]byte, addrLen+2)); err != nil {
		return err
	}

	// The critical xray semantic: SUCCESS is replied BEFORE any upstream
	// dial, so a dead or failing WAN still completes the handshake.
	_, err := c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
	return err
}

// ErrHandshake is wrapped into Result.Err when the SOCKS5 negotiation with the
// system under test fails, so tests can tell "handshake failed" apart from
// "handshake succeeded and the data phase faulted" — the fault fidelity this
// harness exists to prove.
var ErrHandshake = errors.New("socks5 handshake failed")

// Result reports one FakeClient exchange.
type Result struct {
	// OK is true when the response carried some fake's "[ID]" tag followed
	// by exactly the payload that was sent, with a matching crc32.
	OK bool
	// Tag is the fake's ID extracted from the response's "[ID]" prefix.
	Tag string
	// Bytes counts the response bytes received (on success: tag + payload).
	Bytes int64
	// Err is nil on success. Handshake failures wrap ErrHandshake.
	Err error
}

// FakeClient sends self-verifying payloads through a system under test and
// verifies the responses. Zero values are ready to use; the zero Timeout means
// DefaultTimeout.
type FakeClient struct {
	// ID is the client identity embedded in every payload.
	ID string
	// Timeout bounds each connection (dial, exchange); 0 means
	// DefaultTimeout.
	Timeout time.Duration
	// Socks5 negotiates a SOCKS5 CONNECT to Target at the dialed address
	// before sending the payload, for SOCKS5 front-ends (or FakeWAN
	// directly). When false the payload is sent as raw TCP.
	Socks5 bool
	// Target is the CONNECT target "host:port" for Socks5 mode
	// (default DefaultTarget; the target only shapes the handshake — a
	// FakeWAN echoes regardless, like xray).
	Target string
}

// DefaultTimeout is the per-connection deadline used when FakeClient.Timeout
// is zero.
const DefaultTimeout = 3 * time.Second

// DefaultTarget is the CONNECT target used when FakeClient.Target is empty.
const DefaultTarget = "1.2.3.4:80"

// NewFakeClient returns a FakeClient with the given payload identity and the
// default timeout.
func NewFakeClient(id string) *FakeClient {
	return &FakeClient{ID: id}
}

// Send dials addr, sends one self-verifying payload and reads back the
// response. See SendHalfClose for flows that need a client half-close.
func (c *FakeClient) Send(addr string, seq int64) Result {
	return c.send(addr, seq, false)
}

// SendHalfClose is Send with a TCP half-close (CloseWrite) after the payload,
// before reading the response — for modes like HalfClose that key on the
// client's FIN.
func (c *FakeClient) SendHalfClose(addr string, seq int64) Result {
	return c.send(addr, seq, true)
}

func (c *FakeClient) send(addr string, seq int64, halfClose bool) Result {
	payload, err := MakePayload(c.ID, seq)
	if err != nil {
		return Result{Err: err}
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return Result{Err: err}
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	if c.Socks5 {
		target := c.Target
		if target == "" {
			target = DefaultTarget
		}
		if err := socks5ClientConnect(conn, target); err != nil {
			return Result{Err: fmt.Errorf("%w: %v", ErrHandshake, err)}
		}
	}
	if _, err := conn.Write(payload); err != nil {
		return Result{Err: fmt.Errorf("send payload: %w", err)}
	}
	if halfClose {
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}

	tag, body, n, err := readTaggedResponse(conn, len(payload))
	if err != nil {
		return Result{Tag: tag, Bytes: n, Err: fmt.Errorf("read response: %w", err)}
	}
	// Verify the crc first: it is what catches a corrupted byte. Only then
	// require the echo to match this client's exact payload.
	if err := VerifyPayload(body); err != nil {
		return Result{Tag: tag, Bytes: n, Err: err}
	}
	if !bytes.Equal(body, payload) {
		return Result{Tag: tag, Bytes: n, Err: errors.New("testutil: echoed payload differs from the one sent")}
	}
	return Result{OK: true, Tag: tag, Bytes: n}
}

// readTaggedResponse reads one framed response: an "[ID]" tag up to and
// including ']', followed by exactly want body bytes. It returns the ID
// without brackets, the body, and the total response bytes consumed.
func readTaggedResponse(r io.Reader, want int) (string, []byte, int64, error) {
	var tb []byte
	one := make([]byte, 1)
	for {
		if len(tb) > 512 {
			return "", nil, int64(len(tb)), errors.New("testutil: response tag exceeds 512 bytes or is missing ']'")
		}
		if _, err := io.ReadFull(r, one); err != nil {
			return "", nil, int64(len(tb)), err
		}
		tb = append(tb, one[0])
		if one[0] == ']' {
			break
		}
	}
	if tb[0] != '[' {
		return "", nil, int64(len(tb)), errors.New("testutil: response does not start with an [ID] tag")
	}
	body := make([]byte, want)
	n, err := io.ReadFull(r, body)
	return string(tb[1 : len(tb)-1]), body[:n], int64(len(tb)) + int64(n), err
}

// MakePayload builds a self-verifying payload "clientID|seq|random|crc32":
// random is 8 lowercase hex chars, and crc32 is the IEEE CRC of the ASCII
// prefix "clientID|seq|random" as 8 hex chars. The clientID must not contain
// '|'.
func MakePayload(clientID string, seq int64) ([]byte, error) {
	if clientID == "" {
		return nil, errors.New("testutil: payload clientID must not be empty")
	}
	if strings.ContainsRune(clientID, '|') {
		return nil, fmt.Errorf("testutil: payload clientID %q must not contain '|'", clientID)
	}
	var rnd [4]byte
	if _, err := crand.Read(rnd[:]); err != nil {
		return nil, err
	}
	prefix := clientID + "|" + strconv.FormatInt(seq, 10) + "|" + hex.EncodeToString(rnd[:])
	crc := crc32.ChecksumIEEE([]byte(prefix))
	return []byte(prefix + "|" + fmt.Sprintf("%08x", crc)), nil
}

// VerifyPayload checks that payload has the MakePayload shape and that its
// trailing crc32 field matches the CRC of the "clientID|seq|random" prefix. A
// single corrupted byte anywhere in the payload makes verification fail.
func VerifyPayload(payload []byte) error {
	s := string(payload)
	i := strings.LastIndexByte(s, '|')
	if i <= 0 {
		return errors.New("testutil: payload has no crc32 field")
	}
	prefix, crcField := s[:i], s[i+1:]
	got, err := strconv.ParseUint(crcField, 16, 32)
	if err != nil {
		return fmt.Errorf("testutil: bad crc32 field %q: %w", crcField, err)
	}
	if want := crc32.ChecksumIEEE([]byte(prefix)); uint32(got) != want {
		return fmt.Errorf("testutil: crc32 mismatch: got %08x want %08x", got, want)
	}
	return nil
}

// socks5ClientConnect performs the client side of a no-auth SOCKS5 CONNECT to
// target ("host:port").
func socks5ClientConnect(c net.Conn, target string) error {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("bad target %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("bad port in target %q", target)
	}
	if _, err := c.Write([]byte{5, 1, 0}); err != nil {
		return err
	}
	method := make([]byte, 2)
	if _, err := io.ReadFull(c, method); err != nil {
		return err
	}
	if method[0] != 5 || method[1] != 0 {
		return fmt.Errorf("method negotiation rejected: %v", method)
	}

	req := []byte{5, 1, 0}
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			req = append(req, 1)
			req = append(req, ip4...)
		} else {
			req = append(req, 4)
			req = append(req, ip.To16()...)
		}
	} else {
		if len(host) == 0 || len(host) > 255 {
			return fmt.Errorf("bad domain in target %q", target)
		}
		req = append(req, 3, byte(len(host)))
		req = append(req, host...)
	}
	req = append(req, byte(port>>8), byte(port))
	if _, err := c.Write(req); err != nil {
		return err
	}

	head := make([]byte, 4)
	if _, err := io.ReadFull(c, head); err != nil {
		return err
	}
	if head[0] != 5 {
		return fmt.Errorf("unexpected SOCKS version %d in reply", head[0])
	}
	if head[1] != 0 {
		return fmt.Errorf("CONNECT failed, reply code %d", head[1])
	}
	addrLen := 0
	switch head[3] {
	case 1:
		addrLen = 4
	case 3:
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return err
		}
		addrLen = int(l[0])
	case 4:
		addrLen = 16
	default:
		return fmt.Errorf("unsupported address type %d in reply", head[3])
	}
	_, err = io.ReadFull(c, make([]byte, addrLen+2))
	return err
}
