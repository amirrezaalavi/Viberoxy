package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// wanRelay carries the WAN pool and per-connection relay settings shared by
// the HTTPS CONNECT proxy (proxy.go) and the SOCKS5 front-end listener
// (socks.go). Both embed it so CONNECT and SOCKS5 traffic flows through
// identical plumbing: pick WAN -> count connection -> SOCKS5 dial ->
// bidirectional pipe -> metrics -> access log.
type wanRelay struct {
	pool             *WANPool
	AccessLog        bool
	WanFailThreshold int
}

// beginWAN reserves a connection on the chosen slot: it bumps the slot's
// connection counter and records the per-protocol connection metric. The
// caller must defer endWAN immediately after a successful beginWAN.
func (r *wanRelay) beginWAN(wanIndex int, proto string) {
	r.pool.IncConnCount(wanIndex)
	metricProxyConnections.Inc(strconv.Itoa(wanIndex), proto)
}

// endWAN releases a connection previously reserved with beginWAN.
func (r *wanRelay) endWAN(wanIndex int) {
	r.pool.DecConnCount(wanIndex)
}

// dialWAN connects to the slot's local SOCKS5 listener and completes the
// SOCKS5 handshake to targetHost. On failure it records the slot failure,
// writes the access-log line and returns the error; the caller maps the
// error to its protocol-specific reply (HTTP 502 vs SOCKS5 REP 0x01).
func (r *wanRelay) dialWAN(ctx context.Context, wanIndex int, targetHost string, start time.Time, proto string) (net.Conn, error) {
	socksAddr := fmt.Sprintf("127.0.0.1:%d", r.pool.Slots[wanIndex].ServicePort)
	conn, err := socks5Dial(ctx, socksAddr, targetHost)
	if err != nil {
		r.pool.RecordFailure(wanIndex)
		slog.Warn("socks5 dial failed", "wan", wanIndex, "target", targetHost, "error", err)
		r.logAccess(targetHost, wanIndex, 0, 0, start, "err", proto)
		return nil, err
	}
	return conn, nil
}

// relayThroughWAN pipes clientConn and the upstream connection
// bidirectionally until both directions close, then records byte/latency
// metrics, resets the slot's failure counter and writes the access-log line.
// Blocking; the caller owns both conns.
func (r *wanRelay) relayThroughWAN(wanIndex int, targetHost string, start time.Time, proto string, clientConn, upstream net.Conn) {
	var wg sync.WaitGroup
	var upBytes, downBytes int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(upstream, clientConn)
		atomic.AddInt64(&upBytes, n)
		upstream.Close()
	}()
	go func() {
		defer wg.Done()
		n, _ := io.Copy(clientConn, upstream)
		atomic.AddInt64(&downBytes, n)
		clientConn.Close()
	}()
	wg.Wait()

	up := atomic.LoadInt64(&upBytes)
	down := atomic.LoadInt64(&downBytes)
	metricProxyBytes.Add(float64(up), strconv.Itoa(wanIndex), "up")
	metricProxyBytes.Add(float64(down), strconv.Itoa(wanIndex), "down")
	metricProxyLatency.Observe(time.Since(start).Seconds())
	r.pool.RecordSuccess(wanIndex)
	r.logAccess(targetHost, wanIndex, up, down, start, "ok", proto)
}

// logAccess emits one structured access-log line per proxied connection.
func (r *wanRelay) logAccess(target string, wan int, up, down int64, start time.Time, status, proto string) {
	if !r.AccessLog {
		return
	}
	slog.Info("proxy access",
		"target", target,
		"wan", wan,
		"proto", proto,
		"bytes_up", up,
		"bytes_down", down,
		"latency_ms", time.Since(start).Milliseconds(),
		"status", status,
	)
}
