package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"
	"viberoxy/internal/auth"
	"viberoxy/internal/path"
	"viberoxy/internal/xrayproc"
)

// affinityFront runs a SOCKS5 front-end on an ephemeral port with a SHORT
// idle timeout: the mock upstream does not propagate half-close, so the
// relay only tears down (and releases its reservation) once the idle
// bound fires after the client closes. That keeps the reservation-drain
// waits in these tests bounded at ~1.5s instead of the 300s default.
func affinityFront(t *testing.T, pool *WANPool) string {
	t.Helper()
	port := freePort(t)
	srv := NewSocksServer(port, pool)
	srv.AccessLog = false
	srv.IdleTimeout = 1500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Listen(ctx) }()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForPort(addr, 2*time.Second); err != nil {
		t.Fatalf("socks front-end did not start: %v", err)
	}
	return addr
}

// TestClientIdentity pins the affinity client key (F-05): authenticated
// username wins; otherwise the client IP with the ephemeral port stripped
// (so two connections from one host share one key); empty input stays
// empty (affinity off).
func TestClientIdentity(t *testing.T) {
	cases := []struct{ remote, user, want string }{
		{"10.0.0.7:51234", "", "10.0.0.7"},
		{"10.0.0.7:51234", "alice", "alice"},
		{"[2001:db8::9]:40000", "", "2001:db8::9"},
		{"", "", ""},
		{"no-port-form", "", "no-port-form"},
	}
	for _, c := range cases {
		if got := clientIdentity(c.remote, c.user); got != c.want {
			t.Errorf("clientIdentity(%q, %q) = %q, want %q", c.remote, c.user, got, c.want)
		}
	}
}

// TestSocksAuthenticate_ReturnsUsername pins that the RFC 1929 exchange
// hands back the authenticated username (it IS the affinity client
// identity when PROXY_USERS is configured) and refuses bad credentials.
func TestSocksAuthenticate_ReturnsUsername(t *testing.T) {
	users, err := auth.ParseUsers("alice:s3cret,bob:hunter2")
	if err != nil {
		t.Fatalf("ParseUsers: %v", err)
	}

	subneg := func(user, pass string) []byte {
		return append(append([]byte{0x01, byte(len(user))}, user...),
			append([]byte{byte(len(pass))}, pass...)...)
	}

	t.Run("valid credentials return the username", func(t *testing.T) {
		srv, cli := net.Pipe()
		defer srv.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			cli.Write(subneg("alice", "s3cret"))
			reply := make([]byte, 2)
			io.ReadFull(cli, reply)
			if reply[0] != 0x01 || reply[1] != 0x00 {
				t.Errorf("subnegotiation reply = %v, want [1 0]", reply)
			}
			cli.Close()
		}()
		user, ok := socksAuthenticate(srv, users)
		if !ok || user != "alice" {
			t.Errorf("socksAuthenticate = (%q, %v), want (\"alice\", true)", user, ok)
		}
		<-done
	})

	t.Run("wrong password is refused with empty user", func(t *testing.T) {
		srv, cli := net.Pipe()
		defer srv.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			cli.Write(subneg("alice", "wrong"))
			reply := make([]byte, 2)
			io.ReadFull(cli, reply)
			if reply[0] != 0x01 || reply[1] != 0x01 {
				t.Errorf("subnegotiation reply = %v, want [1 1]", reply)
			}
			cli.Close()
		}()
		user, ok := socksAuthenticate(srv, users)
		if ok || user != "" {
			t.Errorf("socksAuthenticate = (%q, %v), want (\"\", false)", user, ok)
		}
		<-done
	})
}

// affinityTestPool puts n Active slots on their own mock upstream SOCKS5
// listeners (production slot shape: Active, routable, fresh Active path
// generation).
func affinityTestPool(t *testing.T, n int) *WANPool {
	t.Helper()
	pool := NewWANPool(n, 0)
	for i := 0; i < n; i++ {
		ln, addr := startTestSocksServer(t)
		t.Cleanup(func() { ln.Close() })
		_, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("split mock upstream addr: %v", err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			t.Fatalf("parse mock upstream port: %v", err)
		}
		slot := pool.Slots[i]
		slot.State = StateActive
		slot.ServicePort = port
		slot.Cmd = xrayproc.Wrap(&exec.Cmd{}, "")
		slot.Current.Store(path.New(nil, slot.Cmd, i, port))
	}
	return pool
}

// socksRoundTrip opens a SOCKS5 connection through the front-end to the
// echo target, returns the slot that holds the reservation while the
// connection is open, then closes and waits for the reservation to drain.
func socksRoundTrip(t *testing.T, front string, pool *WANPool, echoHost string, echoPort int) int {
	t.Helper()
	conn, err := net.Dial("tcp", front)
	if err != nil {
		t.Fatalf("dial front-end: %v", err)
	}
	defer conn.Close()
	socksGreet(t, conn)
	atyp := byte(0x03) // domain
	if ip := net.ParseIP(echoHost); ip != nil && ip.To4() != nil {
		atyp = 0x01
	}
	reply := socksSendRequest(t, conn, 0x01, atyp, echoHost, echoPort)
	if reply[0] != 0x05 || reply[1] != 0x00 {
		t.Fatalf("CONNECT reply = %v, want REP 0", reply)
	}

	// Select reserved atomically before the dial, and the relay holds
	// the reservation until this connection closes: exactly one slot
	// must carry it now.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var holders []int
		for i := range pool.Slots {
			if slotInflight(pool, i) > 0 {
				holders = append(holders, i)
			}
		}
		if len(holders) == 1 {
			slot := holders[0]
			conn.Close()
			waitAllIdle(t, pool)
			return slot
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected exactly one slot holding the reservation, got %v", holders)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitAllIdle(t *testing.T, pool *WANPool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		idle := true
		for i := range pool.Slots {
			if slotInflight(pool, i) > 0 {
				idle = false
				break
			}
		}
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("reservations never drained after closing the connection")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// T-AFF wiring (F-05, end to end through the real SOCKS5 front-end): two
// SEQUENTIAL connections from one client to one target must land on the
// same slot — the (client, site) key was built by the front-end (client
// IP, port stripped) and honored by pool.Select. Without that key the
// picks would be independent coins and this would fail about half the
// time; with it, it is deterministic.
func TestAffinityWiring_SocksSequentialRequestsStick(t *testing.T) {
	pool := affinityTestPool(t, 2)
	front := affinityFront(t, pool)
	echoAddr := startEchoServer(t)
	echoHost, echoPortStr, err := net.SplitHostPort(echoAddr)
	if err != nil {
		t.Fatalf("split echo addr: %v", err)
	}
	echoPort, err := strconv.Atoi(echoPortStr)
	if err != nil {
		t.Fatalf("parse echo port: %v", err)
	}

	slotA := socksRoundTrip(t, front, pool, echoHost, echoPort)
	slotB := socksRoundTrip(t, front, pool, echoHost, echoPort)
	if slotA != slotB {
		t.Errorf("sequential same-(client,site) connections landed on slots %d then %d: affinity key not honored by Select", slotA, slotB)
	}
}
