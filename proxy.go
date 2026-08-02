package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ProxyServer struct {
	port             int
	pool             *WANPool
	server           *http.Server
	AccessLog        bool
	WanFailThreshold int
}

func NewProxyServer(port int, pool *WANPool) *ProxyServer {
	return &ProxyServer{
		port:             port,
		pool:             pool,
		AccessLog:        true,
		WanFailThreshold: DefaultFailThreshold,
	}
}

func (p *ProxyServer) Start(ctx context.Context) error {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			p.handleConnect(w, r)
		} else {
			p.handleDefault(w, r)
		}
	})

	p.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", p.port),
		Handler: handler,
	}

	errCh := make(chan error, 1)
	go func() {
		err := p.server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		p.server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}

func (p *ProxyServer) Stop(ctx context.Context) error {
	if p.server == nil {
		return nil
	}
	return p.server.Shutdown(ctx)
}

func (p *ProxyServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	targetHost := r.Host
	if targetHost == "" {
		http.Error(w, "Bad Request", 400)
		return
	}

	wanIndex := p.pool.GetLeastLoaded(p.WanFailThreshold)
	if wanIndex < 0 {
		http.Error(w, "No WAN Available", 503)
		return
	}

	p.pool.IncConnCount(wanIndex)
	defer p.pool.DecConnCount(wanIndex)

	metricProxyConnections.Inc(strconv.Itoa(wanIndex), "connect")

	servicePort := p.pool.Slots[wanIndex].ServicePort
	socksAddr := fmt.Sprintf("127.0.0.1:%d", servicePort)

	conn, err := socks5Dial(r.Context(), socksAddr, targetHost)
	if err != nil {
		p.pool.RecordFailure(wanIndex)
		slog.Warn("socks5 dial failed", "wan", wanIndex, "target", targetHost, "error", err)
		p.logAccess(targetHost, wanIndex, 0, 0, start, "err")
		http.Error(w, "Bad Gateway", 502)
		return
	}
	defer conn.Close()

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", 500)
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		slog.Warn("hijack failed", "error", err)
		http.Error(w, "Internal Server Error", 500)
		return
	}
	defer clientConn.Close()

	clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

	var wg sync.WaitGroup
	var upBytes, downBytes int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(conn, clientConn)
		atomic.AddInt64(&upBytes, n)
		conn.Close()
	}()
	go func() {
		defer wg.Done()
		n, _ := io.Copy(clientConn, conn)
		atomic.AddInt64(&downBytes, n)
		clientConn.Close()
	}()
	wg.Wait()

	up := atomic.LoadInt64(&upBytes)
	down := atomic.LoadInt64(&downBytes)
	metricProxyBytes.Add(float64(up), strconv.Itoa(wanIndex), "up")
	metricProxyBytes.Add(float64(down), strconv.Itoa(wanIndex), "down")
	metricProxyLatency.Observe(time.Since(start).Seconds())
	p.pool.RecordSuccess(wanIndex)
	p.logAccess(targetHost, wanIndex, up, down, start, "ok")
}

// logAccess emits one structured access-log line per proxied connection.
func (p *ProxyServer) logAccess(target string, wan int, up, down int64, start time.Time, status string) {
	if !p.AccessLog {
		return
	}
	slog.Info("proxy access",
		"target", target,
		"wan", wan,
		"bytes_up", up,
		"bytes_down", down,
		"latency_ms", time.Since(start).Milliseconds(),
		"status", status,
	)
}

func (p *ProxyServer) handleDefault(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Method Not Allowed (CONNECT only)", 405)
}
