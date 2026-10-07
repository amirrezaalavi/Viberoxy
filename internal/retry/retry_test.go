package retry

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"testing"
	"time"
)

// ---------- fixtures ----------

// refusedAddr returns a loopback address nothing listens on, so a dial to it
// fails with connection refused (the canonical dial-stage failure).
func refusedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// echoListener starts a local TCP echo server and returns its address.
func echoListener(t *testing.T) string {
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
				io.Copy(c, c)
			}(c)
		}
	}()
	return ln.Addr().String()
}

// sliceNext hands out candidates in order (the "excluded paths" iterator a
// front-end provides: each candidate is returned at most once per connection).
func sliceNext[T any](cands []T, calls *int) func() (T, bool) {
	var i int
	return func() (T, bool) {
		*calls++
		if i >= len(cands) {
			var zero T
			return zero, false
		}
		c := cands[i]
		i++
		return c, true
	}
}

// tcpDial is a plain dial-stage attempt: a connection or a dial error.
func tcpDial(ctx context.Context, addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
}

// ---------- T-RETRY-01 ----------
// A refused dial on the first path must fail over transparently: the caller
// gets a working connection through the second path without knowing the
// first one was tried.
func TestRetry_TRETRY01_RefusedFailsOverToHealthyPath(t *testing.T) {
	bad := refusedAddr(t)
	good := echoListener(t)

	dialCount := 0
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		dialCount++
		return tcpDial(ctx, addr)
	}

	conn, cand, err := Run(context.Background(), New(Options{}),
		sliceNext([]string{bad, good}, new(int)), dial)
	if err != nil {
		t.Fatalf("Run: %v; expected transparent failover to the healthy path", err)
	}
	defer conn.Close()
	if cand != good {
		t.Fatalf("winning candidate = %q, want the healthy path %q", cand, good)
	}
	if dialCount != 2 {
		t.Fatalf("dial attempts = %d, want 2 (refused, then healthy)", dialCount)
	}
	// The returned connection is a live pipe through the second path.
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo through failover path: %q %v", buf, err)
	}
}

// ---------- T-RETRY-02 ----------
// The two hard bounds: at most DefaultMaxAttempts (3 = 1 + 2 extra) dials per
// connection, and one shared wall-clock budget (DefaultBudget = 5s) across all
// attempts — enforced both by the control flow and by the deadline carried on
// the context every attempt receives.
func TestRetry_TRETRY02_MaxAttemptsAndBudgetHonored(t *testing.T) {
	t.Run("max attempts", func(t *testing.T) {
		p := New(Options{}) // defaults: 3 attempts, 5s budget
		dialCount := 0
		dial := func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			return nil, errors.New("refused")
		}
		cands := []string{"a", "b", "c", "d", "e"} // more paths than attempts
		_, _, err := Run(context.Background(), p, sliceNext(cands, new(int)), dial)
		if err == nil {
			t.Fatal("Run succeeded although every dial failed")
		}
		if dialCount != DefaultMaxAttempts {
			t.Fatalf("dial attempts = %d, want %d (initial + at most 2 retries)",
				dialCount, DefaultMaxAttempts)
		}
	})

	t.Run("budget", func(t *testing.T) {
		now := time.Now()
		p := New(Options{
			MaxAttempts: 5, // plenty of attempts: the budget must be the binding constraint
			Budget:      5 * time.Second,
			Now:         func() time.Time { return now },
		})
		dialCount := 0
		var sawDeadline bool
		dial := func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= 5*time.Second+time.Second {
				sawDeadline = true
			}
			now = now.Add(3 * time.Second) // each attempt "takes" 3s
			return nil, errors.New("refused")
		}
		_, _, err := Run(context.Background(), p, sliceNext([]string{"a", "b", "c", "d"}, new(int)), dial)
		if err == nil {
			t.Fatal("Run succeeded although every dial failed")
		}
		// attempt 1 runs at t=0, attempt 2 starts at t=3s (< 5s budget);
		// attempt 3 would start at t=6s — past the deadline, so denied.
		if dialCount != 2 {
			t.Fatalf("dial attempts = %d, want 2 (second attempt fits the 5s budget, third does not)", dialCount)
		}
		if !sawDeadline {
			t.Fatal("attempts did not receive a context bounded by the dial budget")
		}
	})
}

// ---------- T-RETRY-05 ----------
// Correlated-failure storm guard: every request deposits RetryRatio tokens,
// every retry debits one. Over N connections the total stays inside
// N + burst + N*ratio, and once the bucket is below one token a failing
// connection fails fast on its FIRST dial error instead of storming the pool.
func TestRetry_TRETRY05_StormBudgetFailsFast(t *testing.T) {
	const (
		n     = 100
		ratio = 0.1
		burst = 2.0
	)
	p := New(Options{RetryRatio: ratio, Burst: burst})

	// One "connection": three eligible paths, every dial fails.
	runConn := func(dialCount *int) {
		dial := func(ctx context.Context, addr string) (net.Conn, error) {
			*dialCount++
			return nil, errors.New("refused")
		}
		cands := []string{"p1", "p2", "p3"}
		_, _, err := Run(context.Background(), p, sliceNext(cands, new(int)), dial)
		if err == nil {
			t.Fatal("Run succeeded although every dial failed")
		}
	}

	total := 0
	for i := 0; i < n; i++ {
		runConn(&total)
	}

	// Upper bound: at most burst + N*ratio retry tokens ever existed.
	maxAllowed := n + int(burst) + int(math.Ceil(n*ratio))
	if total > maxAllowed {
		t.Fatalf("correlated-failure storm: %d dials over %d connections, budget allows at most %d",
			total, n, maxAllowed)
	}
	// Lower bound: the bucket starts full, so the first connection must have
	// used its retries (otherwise the policy is not retrying at all).
	if minExpected := n + 2; total < minExpected {
		t.Fatalf("only %d dials over %d connections (< %d): the retry budget was never spent",
			total, n, minExpected)
	}

	// Bucket exhausted -> fail fast on the first dial error: one connection,
	// three eligible paths, exactly one attempt.
	for p.Bucket.Take() {
	}
	after := 0
	runConn(&after)
	if after != 1 {
		t.Fatalf("exhausted bucket: %d dial attempts, want 1 (fail fast on the first dial error)", after)
	}
}

// ---------- T-RETRY-03 / T-RETRY-04 pin ----------
// Retries are only legal before any application byte moves. Run's contract is
// dial-stage only: it sees (candidate, dial result) pairs, and a non-nil conn
// is TERMINAL — returned to the caller untouched, never re-dialed, never
// inspected, with no second candidate even requested. There is deliberately no
// post-dial / replay API in this package; this test pins that shape so a
// future "retry after the relay started" cannot be smuggled in unnoticed.
func TestRetry_TRETRY0304_NeverSeesPostDialConnections(t *testing.T) {
	good := echoListener(t)

	nextCalls, dialCalls := 0, 0
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		dialCalls++
		return tcpDial(ctx, addr)
	}
	conn, _, err := Run(context.Background(), New(Options{}),
		sliceNext([]string{good, good, good}, &nextCalls), dial)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	defer conn.Close()

	if dialCalls != 1 {
		t.Fatalf("dial calls = %d, want 1 (a successful dial ends the sequence)", dialCalls)
	}
	if nextCalls != 1 {
		t.Fatalf("candidate lookups = %d, want 1 (a connection in hand means no further selection)", nextCalls)
	}
	// The connection comes back untouched: bytes flow on the very first use.
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("returned conn not intact: %q %v", buf, err)
	}
}

// No candidate at all (empty/exhausted pool) is reported distinctly from "all
// dials failed", so the front-ends keep 503 vs 502 / REP 0x01 semantics.
func TestRetry_NoCandidateIsDistinctFromDialFailure(t *testing.T) {
	p := New(Options{})
	_, _, err := Run(context.Background(), p, sliceNext([]string{}, new(int)),
		func(ctx context.Context, addr string) (net.Conn, error) {
			t.Fatal("dial must not run without a candidate")
			return nil, nil
		})
	if !errors.Is(err, ErrNoCandidate) {
		t.Fatalf("err = %v, want ErrNoCandidate", err)
	}
}
