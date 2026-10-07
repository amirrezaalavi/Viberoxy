package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"
	"viberoxy/internal/health"
	"viberoxy/internal/path"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/relayio"
	"viberoxy/internal/retry"
)

// wanRelay carries the WAN pool and per-connection relay settings shared by
// the HTTPS CONNECT proxy (proxy.go) and the SOCKS5 front-end listener
// (socks.go). Both embed it so CONNECT and SOCKS5 traffic flows through
// identical plumbing: pick WAN -> count connection -> SOCKS5 dial ->
// bidirectional pipe -> metrics -> access log.
type wanRelay struct {
	pool             *WANPool
	router           *proxycfg.Router
	AccessLog        bool
	WanFailThreshold int
	// IdleTimeout and HandshakeTimeout bound the relayio splice (IDLE_TIMEOUT
	// and the client's first-byte request phase). Zero means the relayio
	// defaults (300s / 30s); fields exist so tests can inject short values.
	IdleTimeout      time.Duration
	HandshakeTimeout time.Duration
	// Retry is the bounded dial-stage failover policy (F-08 stage 1): how
	// many WAN paths one connection may attempt (max 2 extra), the shared
	// dial budget and the token-bucket storm guard. Zero means
	// defaultDialRetry; tests inject their own policy.
	Retry *retry.Policy
}

// defaultDialRetry backs hand-built wanRelays (tests do &wanRelay{pool: p})
// that never went through NewProxyServer/NewSocksServer. It is shared so the
// token-bucket storm guard still sees aggregate traffic from those callers.
var defaultDialRetry = retry.New(retry.Options{})

// relayOptions builds the splice options for one proxied connection on the
// given byte-metric label ("direct" or the WAN index). Byte metrics are fed
// through OnBytes as bytes flow, so they are visible while a half-closed
// splice is still legitimately waiting on the reverse direction; the
// per-direction totals from the returned Stats feed the access log.
func (r *wanRelay) relayOptions(byteLabel string) relayio.Options {
	return relayio.Options{
		IdleTimeout:      r.IdleTimeout,
		HandshakeTimeout: r.HandshakeTimeout,
		OnBytes: func(d relayio.Direction, n int64) {
			if d == relayio.ClientToUpstream {
				metricProxyBytes.Add(float64(n), byteLabel, "up")
			} else {
				metricProxyBytes.Add(float64(n), byteLabel, "down")
			}
		},
	}
}

// beginWAN reserves a connection on the path selected for this connection:
// it bumps that path's inflight counter and records the per-protocol
// connection metric. The path — not the slot index — is the accounting
// identity, and the caller must defer endWAN(wanPath) immediately after a
// successful beginWAN, passing the very same pointer.
func (r *wanRelay) beginWAN(wanPath *path.Path, proto string) {
	wanPath.Reserve()
	metricProxyConnections.Inc(strconv.Itoa(wanPath.Slot), proto)
}

// endWAN releases a connection previously reserved with beginWAN. It always
// releases the path the handler holds, never the slot's current occupant: a
// handler that outlives a reset/replacement can only ever touch its own
// generation's counters (the F-04 ABA guard).
func (r *wanRelay) endWAN(wanPath *path.Path) {
	wanPath.Release()
}

// dialWAN connects to the slot's local SOCKS5 listener and completes the
// SOCKS5 handshake to targetHost. On failure it records the failure on the
// held path, writes the access-log line and returns the error; the caller
// maps the error to its protocol-specific reply (HTTP 502 vs SOCKS5 REP 0x01)
// or, through dialWANFailover, retries it on another path first.
func (r *wanRelay) dialWAN(ctx context.Context, wanPath *path.Path, targetHost string, start time.Time, proto string) (net.Conn, error) {
	// The service port is read from the slot: it is fixed for the slot's
	// lifetime, so the held path and the occupant behind it always agree.
	socksAddr := fmt.Sprintf("127.0.0.1:%d", r.pool.Slots[wanPath.Slot].ServicePort)
	conn, err := socks5Dial(ctx, socksAddr, targetHost)
	if err != nil {
		// A dial failure is a HARD_FAIL outcome (SPEC-H.1): it feeds the
		// consecutive-failure counter AND the sliding window, so a path
		// whose SOCKS layer answers but cannot be dialed ejects like any
		// other hard failure.
		wanPath.RecordOutcome(time.Now(), targetHost, health.HardFail)
		// Dial-stage failures are connection outcomes too (SPEC-M):
		// the same HardFail the window records, reason dial_error.
		recordConnOutcome(wanPath, health.HardFail, "dial_error")
		slog.Warn("socks5 dial failed", "wan", wanPath.Slot, "target", targetHost, "error", err)
		r.logAccess(targetHost, wanPath.Slot, 0, 0, start, "err", proto, "wan")
		return nil, err
	}
	return conn, nil
}

// dialWANFailover runs the bounded dial stage for one connection (F-08 stage
// 1): select the best WAN path, dial it, and on a DIAL-STAGE failure retry on
// the next-best path that has not been tried yet on this connection — at most
// r.Retry.MaxAttempts (3 total) attempts inside one shared dial budget,
// guarded by the policy's token bucket. On success it returns the live
// connection and the path that produced it; the caller must defer
// endWAN(wanPath) with that very pointer and owns the conn. On failure it
// returns nil, nil and an error: retry.ErrNoCandidate when no path was ever
// eligible (front-ends answer 503 / their no-WAN reply), the last dial error
// otherwise (502 / REP 0x01 — the pre-existing final-failure semantics).
//
// SAFETY (T-RETRY-03/04): this is a DIAL-stage retry only. Every attempt
// happens before the relay splices a single application byte, and the retry
// helper (retry.Run) terminates on the first successful dial — there is no
// code here, deliberately, that could re-dial or replay once the relay has
// started. Direct-route dials never come through here: a direct failure is a
// direct failure.
func (r *wanRelay) dialWANFailover(ctx context.Context, targetHost string, start time.Time, proto string) (net.Conn, *path.Path, error) {
	pol := r.Retry
	if pol == nil {
		pol = defaultDialRetry
	}

	// Paths already attempted on THIS connection. next() hands out the
	// best path not in here and marks it, so every retry lands on a path
	// not yet tried while the selection rule stays GetLeastLoaded's.
	tried := make(map[*path.Path]bool)
	next := func() (*path.Path, bool) {
		cand := r.pool.GetLeastLoadedExcluding(tried, r.WanFailThreshold)
		if cand == nil {
			return nil, false
		}
		tried[cand] = true
		return cand, true
	}

	// One dial-stage attempt on one path: reserve around the dial exactly
	// like the pre-failover code did, and give the reservation back when
	// the dial fails (the connection never used that path). On success the
	// reservation stays held — the caller releases it via endWAN.
	dial := func(actx context.Context, cand *path.Path) (net.Conn, error) {
		r.beginWAN(cand, proto)
		conn, err := r.dialWAN(actx, cand, targetHost, start, proto)
		if err != nil {
			r.endWAN(cand)
			return nil, err
		}
		return conn, nil
	}

	conn, wanPath, err := retry.Run(ctx, pol, next, dial)
	if err != nil {
		return nil, nil, err
	}
	return conn, wanPath, nil
}

// decideRoute returns the egress route for a target host. With no router
// configured (or an all-proxy router) everything goes through the WAN pool.
func (r *wanRelay) decideRoute(targetHost string) proxycfg.Route {
	if r.router == nil {
		return proxycfg.RouteWAN
	}
	return r.router.Decide(targetHost)
}

// tuneTCPConn disables Nagle and enables keepalive on a TCP connection so
// small interactive writes (TLS handshakes, HTTP requests, chat messages)
// are not artificially delayed. Non-TCP conns are left untouched.
func tuneTCPConn(conn net.Conn) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	tcp.SetNoDelay(true)
	tcp.SetKeepAlive(true)
	tcp.SetKeepAlivePeriod(30 * time.Second)
}

// directDial connects straight to targetHost from the local machine,
// bypassing the WAN pool (split-routing direct route). The connection is
// tuned (TCP_NODELAY + keepalive) before returning.
func (r *wanRelay) directDial(ctx context.Context, targetHost string, start time.Time, proto string) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, socksDialTimeout)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", targetHost)
	if err != nil {
		slog.Warn("direct dial failed", "target", targetHost, "error", err)
		r.logAccess(targetHost, -1, 0, 0, start, "err", proto, "direct")
		return nil, err
	}
	tuneTCPConn(conn)
	return conn, nil
}

// directRelay pipes clientConn and the direct upstream connection through
// relayio.Splice (same shape as relayThroughWAN, without WAN slot metrics)
// and writes a direct-route access-log line. Blocking; caller owns conns.
func (r *wanRelay) directRelay(targetHost string, start time.Time, proto string, clientConn, upstream net.Conn, clientSrc io.Reader) {
	tuneTCPConn(clientConn)
	tuneTCPConn(upstream)

	stats := relayio.Splice(clientConn, upstream, clientSrc, r.relayOptions("direct"))
	metricProxyLatency.Observe(time.Since(start).Seconds())
	r.logAccess(targetHost, -1, stats.Up, stats.Down, start, "ok", proto, "direct")
}

// logAccess emits one structured access-log line per proxied connection.
//
// F-16 truth: the logged status is DERIVED from what actually happened,
// not from what the caller hoped — "ok" only when bytes reached the client
// (down > 0), "err" otherwise, the same rule health.Classify scores the
// connection with. The status argument is deliberately kept (call sites
// unchanged) and always overridden here, so no call site can log a relay
// that delivered nothing as "ok".
func (r *wanRelay) logAccess(target string, wan int, up, down int64, start time.Time, status, proto, route string) {
	if !r.AccessLog {
		return
	}
	if down > 0 {
		status = "ok"
	} else {
		status = "err"
	}
	slog.Info("proxy access",
		"target", target,
		"wan", wan,
		"proto", proto,
		"route", route,
		"bytes_up", up,
		"bytes_down", down,
		"latency_ms", time.Since(start).Milliseconds(),
		"status", status,
	)
}

// relayThroughWAN pipes clientConn and the upstream connection through
// relayio.Splice until both directions finish (half-close preserved, idle and
// handshake timeouts enforced), then classifies the outcome (SPEC-H.1),
// credits it to the held path (success only when down > 0 — F-01), feeds the
// path's sliding health window (ejection on the window's terms), records
// latency metrics and the access-log line. Byte metrics are recorded
// incrementally via relayOptions' OnBytes callback. Blocking; the caller
// owns both conns.
func (r *wanRelay) relayThroughWAN(wanPath *path.Path, targetHost string, start time.Time, proto string, clientConn, upstream net.Conn, clientSrc io.Reader) {
	opts := r.relayOptions(strconv.Itoa(wanPath.Slot))
	// Capture the first down byte for the path's TTFB EWMA. Only the
	// upstream->client goroutine writes firstDown, and Splice's WaitGroup
	// establishes the happens-before edge with the read below.
	var firstDown int64
	onBytes := opts.OnBytes
	opts.OnBytes = func(d relayio.Direction, n int64) {
		if d == relayio.UpstreamToClient && firstDown == 0 {
			firstDown = time.Now().UnixNano()
		}
		onBytes(d, n)
	}
	stats := relayio.Splice(clientConn, upstream, clientSrc, opts)
	duration := time.Since(start)

	// SPEC-H.1 outcome classification. The success rule CHANGED with
	// F-01: only down > 0 credits success; up > 0 with nothing delivered
	// is a hard failure that feeds the window (a black-holing WAN stops
	// resetting its failure counter and can be ejected).
	//
	// ClassifyReason is Classify plus the clause that decided it, so the
	// same call feeds the window below AND viberoxy_conn_outcome_total
	// (SPEC-M / F-16) — metric and health can never disagree.
	outcome, outcomeReason := health.ClassifyReason(health.Result{
		Up:       stats.Up,
		Down:     stats.Down,
		Err:      stats.Err,
		Duration: duration,
	})
	recordConnOutcome(wanPath, outcome, outcomeReason)
	if ejected := wanPath.RecordOutcome(time.Now(), targetHost, outcome); ejected {
		slog.Warn("wan ejected by passive health",
			"wan", wanPath.Slot, "target", targetHost, "outcome", outcome)
	}
	if outcome == health.OK {
		var ttfb time.Duration
		if firstDown > 0 {
			ttfb = time.Unix(0, firstDown).Sub(start)
		}
		goodput := 0.0
		if duration > 0 {
			goodput = float64(stats.Down) * 8 / duration.Seconds()
		}
		wanPath.RecordHealth(ttfb, goodput)
	}

	metricProxyLatency.Observe(time.Since(start).Seconds())
	r.logAccess(targetHost, wanPath.Slot, stats.Up, stats.Down, start, "ok", proto, "wan")
}
