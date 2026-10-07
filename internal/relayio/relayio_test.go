package relayio

import (
	"bufio"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// tcpPair returns both ends of a connected loopback TCP connection.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ch := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			ch <- nil
			return
		}
		ch <- c
	}()
	peer, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		ln.Close()
		t.Fatalf("dial: %v", err)
	}
	accepted := <-ch
	ln.Close()
	if accepted == nil {
		peer.Close()
		t.Fatal("accept failed")
	}
	return peer, accepted
}

// closeCounter counts full Close() calls on a conn and forwards CloseWrite,
// so tests can assert each socket is fully released exactly once while the
// splice's half-close still reaches the underlying TCP conn.
type closeCounter struct {
	net.Conn
	closes atomic.Int32
}

func (c *closeCounter) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

func (c *closeCounter) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// TestSplice_PipelinedBytesFromHijackBufferPreserved proves bytes the HTTP
// server buffered past the CONNECT request (rw.Reader.Buffered() after
// Hijack) reach the upstream BEFORE anything read from the raw conn (RT-06).
func TestSplice_PipelinedBytesFromHijackBufferPreserved(t *testing.T) {
	srvSide, cliSide := tcpPair(t) // srvSide plays the hijacked server conn
	upRelay, upPeer := tcpPair(t)  // upPeer plays the upstream
	defer cliSide.Close()
	defer upPeer.Close()

	// One optimistic write: CONNECT request + pipelined payload.
	if _, err := cliSide.Write([]byte("CONNECT 1.2.3.4:80 HTTP/1.1\r\nHost: 1.2.3.4:80\r\n\r\nPING")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Simulate net/http: parse the request through a bufio.ReadWriter, which
	// buffers the payload bytes that follow the header terminator.
	srvSide.SetReadDeadline(time.Now().Add(3 * time.Second))
	rw := bufio.NewReadWriter(bufio.NewReader(srvSide), bufio.NewWriter(srvSide))
	for {
		line, err := rw.Reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read request: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	if _, err := rw.Reader.Peek(4); err != nil {
		t.Fatalf("payload never buffered: %v", err)
	}
	if got := rw.Reader.Buffered(); got != 4 {
		t.Fatalf("precondition: Buffered() = %d, want 4", got)
	}
	srvSide.SetReadDeadline(time.Time{})

	done := make(chan Stats, 1)
	go func() {
		done <- Splice(srvSide, upRelay, PeekReader(rw, srvSide),
			Options{IdleTimeout: 5 * time.Second, HandshakeTimeout: 5 * time.Second})
	}()

	upPeer.SetReadDeadline(time.Now().Add(3 * time.Second))
	got := make([]byte, 4)
	if _, err := io.ReadFull(upPeer, got); err != nil {
		t.Fatalf("upstream read: %v", err)
	}
	if string(got) != "PING" {
		t.Fatalf("upstream received %q; want PING (hijack buffered bytes dropped)", got)
	}

	// Bytes sent after the request flow from the raw conn behind the buffer.
	if _, err := cliSide.Write([]byte("NEXT")); err != nil {
		t.Fatalf("write: %v", err)
	}
	got2 := make([]byte, 4)
	if _, err := io.ReadFull(upPeer, got2); err != nil {
		t.Fatalf("upstream read: %v", err)
	}
	if string(got2) != "NEXT" {
		t.Fatalf("upstream received %q; want NEXT", got2)
	}

	cliSide.Close()
	upPeer.Close()
	st := <-done
	if st.Err != nil {
		t.Fatalf("splice err: %v", st.Err)
	}
	if st.Up != 8 {
		t.Fatalf("Up = %d, want 8 (4 pipelined + 4 follow-up)", st.Up)
	}
}

// TestSplice_HalfCloseKeepsReverseDirectionOpen covers both half-close
// directions (RT-07): a client FIN must reach the upstream without tearing
// the connection down, and the upstream's reply + FIN must still reach the
// client that already half-closed. Both sockets are fully closed exactly once.
func TestSplice_HalfCloseKeepsReverseDirectionOpen(t *testing.T) {
	cr, clientPeer := tcpPair(t)
	ur, upPeer := tcpPair(t)
	client := &closeCounter{Conn: cr}
	upstream := &closeCounter{Conn: ur}

	done := make(chan Stats, 1)
	go func() {
		done <- Splice(client, upstream, nil,
			Options{IdleTimeout: 10 * time.Second, HandshakeTimeout: 10 * time.Second})
	}()

	// Client sends a payload then half-closes (FIN only, still readable).
	if _, err := clientPeer.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := clientPeer.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}

	// The upstream must observe the FIN while its conn is still fully open...
	upPeer.SetReadDeadline(time.Now().Add(3 * time.Second))
	got, err := io.ReadAll(upPeer)
	if err != nil {
		t.Fatalf("upstream read: %v", err)
	}
	if string(got) != "x" {
		t.Fatalf("upstream read %q before replying; want %q (relay tore down on FIN)", got, "x")
	}

	// ...reply, then half-close its own direction.
	if _, err := upPeer.Write([]byte("BYE")); err != nil {
		t.Fatalf("upstream write: %v", err)
	}
	if err := upPeer.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("upstream CloseWrite: %v", err)
	}

	// The client still receives the reply despite having half-closed.
	clientPeer.SetReadDeadline(time.Now().Add(3 * time.Second))
	reply, err := io.ReadAll(clientPeer)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if string(reply) != "BYE" {
		t.Fatalf("client received %q after its half-close; want BYE", reply)
	}

	st := <-done
	if st.Err != nil {
		t.Fatalf("splice err: %v", st.Err)
	}
	if st.Up != 1 || st.Down != 3 {
		t.Fatalf("Up/Down = %d/%d, want 1/3", st.Up, st.Down)
	}
	if client.closes.Load() != 1 || upstream.closes.Load() != 1 {
		t.Fatalf("full closes: client %d, upstream %d; want 1 each (released exactly once)",
			client.closes.Load(), upstream.closes.Load())
	}
}
