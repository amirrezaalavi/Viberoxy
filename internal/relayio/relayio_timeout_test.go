package relayio

import (
	"bufio"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// TestSplice_IdleTimeoutClosesExactlyOnce: no bytes in EITHER direction for
// IdleTimeout closes the connection and releases both sockets exactly once.
func TestSplice_IdleTimeoutClosesExactlyOnce(t *testing.T) {
	cr, clientPeer := tcpPair(t)
	ur, upPeer := tcpPair(t)
	defer clientPeer.Close()
	defer upPeer.Close()
	client := &closeCounter{Conn: cr}
	upstream := &closeCounter{Conn: ur}

	const idle = 200 * time.Millisecond
	done := make(chan Stats, 1)
	start := time.Now()
	go func() {
		done <- Splice(client, upstream, nil,
			Options{IdleTimeout: idle, HandshakeTimeout: time.Hour})
	}()

	// Complete the first-byte phase: one byte each way, then total silence.
	if _, err := clientPeer.Write([]byte("a")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := upPeer.Write([]byte("b")); err != nil {
		t.Fatalf("write: %v", err)
	}

	var st Stats
	select {
	case st = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("splice did not return after idle timeout")
	}
	elapsed := time.Since(start)
	if !errors.Is(st.Err, ErrIdleTimeout) {
		t.Fatalf("Err = %v, want ErrIdleTimeout", st.Err)
	}
	if elapsed < idle-50*time.Millisecond {
		t.Fatalf("splice returned after %v, earlier than idle timeout %v", elapsed, idle)
	}
	if client.closes.Load() != 1 || upstream.closes.Load() != 1 {
		t.Fatalf("full closes: client %d, upstream %d; want 1 each (released exactly once)",
			client.closes.Load(), upstream.closes.Load())
	}

	// The client socket is genuinely closed (EOF/reset, never a hung read).
	clientPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(clientPeer); err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("client peer still open after idle close")
		}
	}
}

// TestSplice_HandshakeTimeoutBoundsSilentClient: with no bytes anywhere, the
// client's request phase is bounded by HandshakeTimeout, not the idle budget.
func TestSplice_HandshakeTimeoutBoundsSilentClient(t *testing.T) {
	cr, clientPeer := tcpPair(t)
	ur, upPeer := tcpPair(t)
	defer clientPeer.Close()
	defer upPeer.Close()
	client := &closeCounter{Conn: cr}
	upstream := &closeCounter{Conn: ur}

	const hs = 150 * time.Millisecond
	done := make(chan Stats, 1)
	start := time.Now()
	go func() {
		done <- Splice(client, upstream, nil,
			Options{IdleTimeout: time.Hour, HandshakeTimeout: hs})
	}()

	var st Stats
	select {
	case st = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("splice did not return after handshake timeout")
	}
	elapsed := time.Since(start)
	if !errors.Is(st.Err, ErrHandshakeTimeout) {
		t.Fatalf("Err = %v, want ErrHandshakeTimeout", st.Err)
	}
	if elapsed < hs-50*time.Millisecond {
		t.Fatalf("splice returned after %v, before handshake timeout %v", elapsed, hs)
	}
	if client.closes.Load() != 1 || upstream.closes.Load() != 1 {
		t.Fatalf("full closes: client %d, upstream %d; want 1 each (released exactly once)",
			client.closes.Load(), upstream.closes.Load())
	}
}

// TestSplice_HandshakeYieldsToUpstreamTraffic: if the upstream speaks first,
// the handshake deadline must not kill a live tunnel — the idle timeout takes
// over instead (first byte from EITHER direction establishes the tunnel).
func TestSplice_HandshakeYieldsToUpstreamTraffic(t *testing.T) {
	cr, clientPeer := tcpPair(t)
	ur, upPeer := tcpPair(t)
	defer clientPeer.Close()
	defer upPeer.Close()
	client := &closeCounter{Conn: cr}
	upstream := &closeCounter{Conn: ur}

	const (
		hs   = 500 * time.Millisecond
		idle = 1500 * time.Millisecond
	)
	done := make(chan Stats, 1)
	go func() {
		done <- Splice(client, upstream, nil,
			Options{IdleTimeout: idle, HandshakeTimeout: hs})
	}()

	// Upstream banner first; the client stays silent past HandshakeTimeout.
	if _, err := upPeer.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	clientPeer.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 5)
	if _, err := io.ReadFull(clientPeer, got); err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("client received %q, want hello", got)
	}

	select {
	case st := <-done:
		if !errors.Is(st.Err, ErrIdleTimeout) {
			t.Fatalf("Err = %v, want ErrIdleTimeout (handshake bound must yield to traffic)", st.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("splice did not return after idle timeout")
	}
}

// TestPeekReader_NoBufferedBytes: nothing buffered means the raw conn is read
// directly (nil).
func TestPeekReader_NoBufferedBytes(t *testing.T) {
	srvSide, cliSide := tcpPair(t)
	defer cliSide.Close()
	defer srvSide.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(srvSide), bufio.NewWriter(srvSide))
	if r := PeekReader(rw, srvSide); r != nil {
		t.Fatalf("PeekReader with empty buffer = %v, want nil", r)
	}
}
