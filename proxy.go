package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type ProxyServer struct {
	port   int
	server *http.Server
	wanRelay
}

func NewProxyServer(port int, pool *WANPool, router ...*Router) *ProxyServer {
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

	// Split routing: a direct-route target bypasses the WAN pool entirely.
	if p.decideRoute(targetHost) == RouteDirect {
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
		clientConn, _, err := hijacker.Hijack()
		if err != nil {
			slog.Warn("hijack failed", "error", err)
			http.Error(w, "Internal Server Error", 500)
			return
		}
		defer clientConn.Close()

		clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		p.directRelay(targetHost, start, "connect", clientConn, conn)
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

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		slog.Warn("hijack failed", "error", err)
		http.Error(w, "Internal Server Error", 500)
		return
	}
	defer clientConn.Close()

	clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

	p.relayThroughWAN(wanIndex, targetHost, start, "connect", clientConn, conn)
}

func (p *ProxyServer) handleDefault(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Method Not Allowed (CONNECT only)", 405)
}
