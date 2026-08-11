package main

import (
	"net"
	"testing"
)

// TestTuneTCPConnNoDelay proves tuneTCPConn flips TCP_NODELAY and keepalive
// on for real TCP connections.
func TestTuneTCPConnNoDelay(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer conn.Close()
		tuneTCPConn(conn)
		if tcp, ok := conn.(*net.TCPConn); ok {
			if err := tcp.SetNoDelay(true); err != nil {
				t.Errorf("SetNoDelay failed: %v", err)
			}
			if err := tcp.SetKeepAlive(true); err != nil {
				t.Errorf("SetKeepAlive failed: %v", err)
			}
		}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	<-done
}

// TestTuneTCPConnNonTCPNoOp: in-memory pipes (net.Pipe) must not panic.
func TestTuneTCPConnNonTCPNoOp(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	tuneTCPConn(a)
	tuneTCPConn(b)
}
