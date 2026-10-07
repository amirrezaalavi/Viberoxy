package relayio

import (
	"crypto/sha256"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

const transferSize = 32 << 20 // 32 MiB

// TestSplice_LargeTransferIntegrity pipes 32 MiB through the splice to an
// echo upstream and verifies the echoed stream byte-for-byte (SHA-256), plus
// the per-direction counts.
func TestSplice_LargeTransferIntegrity(t *testing.T) {
	clientRelay, clientPeer := tcpPair(t)
	upRelay, upPeer := tcpPair(t)

	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		io.Copy(upPeer, upPeer)
		upPeer.Close()
	}()

	done := make(chan Stats, 1)
	go func() {
		done <- Splice(clientRelay, upRelay, nil,
			Options{IdleTimeout: 30 * time.Second, HandshakeTimeout: 30 * time.Second})
	}()

	data := make([]byte, transferSize)
	for i := range data {
		data[i] = byte(i)
	}
	want := sha256.Sum256(data)

	clientPeer.SetDeadline(time.Now().Add(60 * time.Second))
	writeErr := make(chan error, 1)
	go func() {
		if _, err := clientPeer.Write(data); err != nil {
			writeErr <- err
			return
		}
		writeErr <- clientPeer.(*net.TCPConn).CloseWrite()
	}()

	got, err := io.ReadAll(clientPeer)
	if err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if len(got) != transferSize {
		t.Fatalf("echoed %d bytes, want %d", len(got), transferSize)
	}
	if sum := sha256.Sum256(got); sum != want {
		t.Fatalf("echoed %d bytes but sha256 mismatch (corrupted transfer)", len(got))
	}

	st := <-done
	if st.Err != nil {
		t.Fatalf("splice err: %v", st.Err)
	}
	if st.Up != transferSize || st.Down != transferSize {
		t.Fatalf("Up/Down = %d/%d, want %d/%d", st.Up, st.Down, transferSize, transferSize)
	}
	<-echoDone
}

// TestSplice_ShortConnectionsDoNotLeak runs 2000 short spliced connections
// and requires the goroutine count to return to (near) the pre-loop baseline:
// every direction goroutine, echo handler and accept helper must exit.
func TestSplice_ShortConnectionsDoNotLeak(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	pair := func() (net.Conn, net.Conn) {
		t.Helper()
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
			t.Fatalf("dial: %v", err)
		}
		accepted := <-ch
		if accepted == nil {
			peer.Close()
			t.Fatal("accept failed")
		}
		return peer, accepted
	}

	baseline := runtime.NumGoroutine()

	const iterations = 2000
	for i := 0; i < iterations; i++ {
		clientRelay, clientPeer := pair()
		upRelay, upPeer := pair()

		echoDone := make(chan struct{})
		go func() {
			defer close(echoDone)
			io.Copy(upPeer, upPeer)
			upPeer.Close()
		}()

		spliceDone := make(chan Stats, 1)
		go func() {
			spliceDone <- Splice(clientRelay, upRelay, nil,
				Options{IdleTimeout: 10 * time.Second, HandshakeTimeout: 10 * time.Second})
		}()

		if _, err := clientPeer.Write([]byte("p")); err != nil {
			t.Fatalf("iter %d: write: %v", i, err)
		}
		if _, err := io.ReadFull(clientPeer, make([]byte, 1)); err != nil {
			t.Fatalf("iter %d: read: %v", i, err)
		}
		clientPeer.Close()
		<-spliceDone
		<-echoDone
	}

	deadline := time.Now().Add(5 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		n = runtime.NumGoroutine()
		if n <= baseline+20 {
			t.Logf("goroutines: baseline=%d, after %d splices=%d", baseline, iterations, n)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines = %d after %d short connections, baseline = %d (leak)", n, iterations, baseline)
}
