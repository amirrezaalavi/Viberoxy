package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
	"viberoxy/internal/auth"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/relayio"
	"viberoxy/internal/retry"
)

type ProxyServer struct {
	port int
	// listenAddr is the bind host. Constructors default it to loopback
	// (auth.LoopbackHost); main overrides it from cfg.ListenAddr. A
	// non-loopback value is refused by Start unless auth is configured or
	// ALLOW_PUBLIC=true (F-14).
	listenAddr string
	// mu guards server: Start publishes it from its own goroutine while
	// startup()'s shutdown path may call Stop concurrently.
	mu     sync.Mutex
	server *http.Server
	wanRelay
}

func NewProxyServer(port int, pool *WANPool, router ...*proxycfg.Router) *ProxyServer {
	p := &ProxyServer{
		port:       port,
		listenAddr: auth.LoopbackHost,
		wanRelay: wanRelay{
			pool:             pool,
			AccessLog:        true,
			WanFailThreshold: DefaultFailThreshold,
			Retry:            retry.New(retry.Options{}),
		},
	}
	if len(router) > 0 {
		p.router = router[0]
	}
	return p
}

func (p *ProxyServer) Start(ctx context.Context) error {
	// Defense in depth (F-14): proxycfg.ParseConfig refuses a non-loopback
	// LISTEN_ADDR at startup, but direct constructors bypass it — re-check
	// the same policy here before binding.
	if err := auth.EnsureBindAllowed(p.listenAddr); err != nil {
		return err
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			p.handleConnect(w, r)
		} else {
			p.handleDefault(w, r)
		}
	})

	srv := &http.Server{
		Addr:    net.JoinHostPort(p.listenAddr, strconv.Itoa(p.port)),
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

	// Proxy authentication (F-14): while PROXY_USERS is set, every CONNECT
	// must present matching Proxy-Authorization Basic credentials.
	users, required, err := auth.UsersFromEnv()
	if err != nil {
		// Malformed PROXY_USERS (ParseConfig would have hard-exited at
		// startup): fail closed — no usable users, so every request 407s.
		slog.Error("invalid PROXY_USERS, rejecting proxy connections", "error", err)
	}
	if required {
		user, pass, ok := auth.ParseBasic(r.Header.Get("Proxy-Authorization"))
		if !ok || !auth.CheckUsers(users, user, pass) {
			w.Header().Set("Proxy-Authenticate", `Basic realm="viberoxy"`)
			http.Error(w, "Proxy Authentication Required", http.StatusProxyAuthRequired)
			return
		}
	}

	targetHost := r.Host
	if targetHost == "" {
		http.Error(w, "Bad Request", 400)
		return
	}

	// SSRF guard (F-14): never dial loopback/link-local/private targets
	// unless ALLOW_PRIVATE_TARGETS=true.
	if !auth.TargetAllowed(targetHost, auth.AllowPrivateTargetsFromEnv()) {
		http.Error(w, "Forbidden", http.StatusForbidden)
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

	// Bounded dial-stage failover (F-08 stage 1): the dial phase may touch
	// several distinct WAN paths within the retry policy's attempt cap and
	// budget before this connection gives up. Final-failure semantics are
	// unchanged: 503 when no WAN is eligible at all, 502 when every
	// attempt failed.
	conn, wanPath, err := p.dialWANFailover(r.Context(), targetHost, start, "connect")
	if err != nil {
		if errors.Is(err, retry.ErrNoCandidate) {
			http.Error(w, "No WAN Available", 503)
		} else {
			http.Error(w, "Bad Gateway", 502)
		}
		return
	}
	defer p.endWAN(wanPath)
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

	p.relayThroughWAN(wanPath, targetHost, start, "connect", clientConn, conn, relayio.PeekReader(rw, clientConn))
}

func (p *ProxyServer) handleDefault(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Method Not Allowed (CONNECT only)", 405)
}
