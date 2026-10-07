package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"
	"viberoxy/internal/path"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/relayio"
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
}

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
// held path, writes the access-log line and returns the error; the caller maps
// the error to its protocol-specific reply (HTTP 502 vs SOCKS5 REP 0x01).
func (r *wanRelay) dialWAN(ctx context.Context, wanPath *path.Path, targetHost string, start time.Time, proto string) (net.Conn, error) {
	// The service port is read from the slot: it is fixed for the slot's
	// lifetime, so the held path and the occupant behind it always agree.
	socksAddr := fmt.Sprintf("127.0.0.1:%d", r.pool.Slots[wanPath.Slot].ServicePort)
	conn, err := socks5Dial(ctx, socksAddr, targetHost)
	if err != nil {
		wanPath.RecordFailure()
		slog.Warn("socks5 dial failed", "wan", wanPath.Slot, "target", targetHost, "error", err)
		r.logAccess(targetHost, wanPath.Slot, 0, 0, start, "err", proto, "wan")
		return nil, err
	}
	return conn, nil
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
func (r *wanRelay) logAccess(target string, wan int, up, down int64, start time.Time, status, proto, route string) {
	if !r.AccessLog {
		return
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
// handshake timeouts enforced), then records latency metrics, credits success
// to the held path and writes the access-log line. Byte metrics are recorded
// incrementally via relayOptions' OnBytes callback. Blocking; the caller owns
// both conns.
func (r *wanRelay) relayThroughWAN(wanPath *path.Path, targetHost string, start time.Time, proto string, clientConn, upstream net.Conn, clientSrc io.Reader) {
	stats := relayio.Splice(clientConn, upstream, clientSrc, r.relayOptions(strconv.Itoa(wanPath.Slot)))
	metricProxyLatency.Observe(time.Since(start).Seconds())
	// Success is credited to the path this connection ran on — the same
	// generation it reserved on — never to whatever occupies the slot now.
	wanPath.RecordSuccess()
	r.logAccess(targetHost, wanPath.Slot, stats.Up, stats.Down, start, "ok", proto, "wan")
}
