package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type ProxyServer struct {
	port   int
	pool   *WANPool
	server *http.Server
}

func NewProxyServer(port int, pool *WANPool) *ProxyServer {
	return &ProxyServer{
		port: port,
		pool: pool,
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
	targetHost := r.Host
	if targetHost == "" {
		http.Error(w, "Bad Request", 400)
		return
	}

	wanIndex := p.pool.GetLeastLoaded()
	if wanIndex < 0 {
		http.Error(w, "No WAN Available", 503)
		return
	}

	p.pool.IncConnCount(wanIndex)
	defer p.pool.DecConnCount(wanIndex)

	servicePort := p.pool.Slots[wanIndex].ServicePort
	socksAddr := fmt.Sprintf("127.0.0.1:%d", servicePort)

	conn, err := socks5Dial(r.Context(), socksAddr, targetHost)
	if err != nil {
		slog.Warn("socks5 dial failed", "wan", wanIndex, "target", targetHost, "error", err)
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
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(conn, clientConn)
		conn.Close()
	}()
	go func() {
		defer wg.Done()
		io.Copy(clientConn, conn)
		clientConn.Close()
	}()
	wg.Wait()
}

func (p *ProxyServer) handleDefault(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Method Not Allowed (CONNECT only)", 405)
}
