package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/relayio"
)

type ProxyServer struct {
	port int
	// mu guards server: Start publishes it from its own goroutine while
	// startup()'s shutdown path may call Stop concurrently.
	mu     sync.Mutex
	server *http.Server
	wanRelay
}

func NewProxyServer(port int, pool *WANPool, router ...*proxycfg.Router) *ProxyServer {
	p := &ProxyServer{
		port: port,
		wanRelay: wanRelay{
			pool:             pool,
			AccessLog:        true,
			WanFailThreshold: DefaultFailThreshold,
		},
	}
	if len(router) > 0 {
		p.router = router[0]
	}
	return p
}

func (p *ProxyServer) Start(ctx context.Context) error {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			p.handleConnect(w, r)
		} else {
			p.handleDefault(w, r)
		}
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", p.port),
		Handler: handler,
	}
	p.mu.Lock()
	p.server = srv
	p.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		err := srv.ListenAndServe()
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
		srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}

func (p *ProxyServer) Stop(ctx context.Context) error {
	p.mu.Lock()
	srv := p.server
	p.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

func (p *ProxyServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	targetHost := r.Host
	if targetHost == "" {
		http.Error(w, "Bad Request", 400)
		return
	}

	// Split routing: a direct-route target bypasses the WAN pool entirely.
	if p.decideRoute(targetHost) == proxycfg.RouteDirect {
		conn, err := p.directDial(r.Context(), targetHost, start, "connect")
		if err != nil {
			http.Error(w, "Bad Gateway", 502)
			return
		}
		defer conn.Close()

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Hijacking not supported", 500)
			return
		}
		clientConn, rw, err := hijacker.Hijack()
		if err != nil {
			slog.Warn("hijack failed", "error", err)
			http.Error(w, "Internal Server Error", 500)
			return
		}
		defer clientConn.Close()

		clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		p.directRelay(targetHost, start, "connect", clientConn, conn, relayio.PeekReader(rw, clientConn))
		return
	}

	wanIndex := p.pool.GetLeastLoaded(p.WanFailThreshold)
	if wanIndex < 0 {
		http.Error(w, "No WAN Available", 503)
		return
	}

	p.beginWAN(wanIndex, "connect")
	defer p.endWAN(wanIndex)

	conn, err := p.dialWAN(r.Context(), wanIndex, targetHost, start, "connect")
	if err != nil {
		http.Error(w, "Bad Gateway", 502)
		return
	}
	defer conn.Close()

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking not supported", 500)
		return
	}

	clientConn, rw, err := hijacker.Hijack()
	if err != nil {
		slog.Warn("hijack failed", "error", err)
		http.Error(w, "Internal Server Error", 500)
		return
	}
	defer clientConn.Close()

	clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

	p.relayThroughWAN(wanIndex, targetHost, start, "connect", clientConn, conn, relayio.PeekReader(rw, clientConn))
}

func (p *ProxyServer) handleDefault(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Method Not Allowed (CONNECT only)", 405)
}
