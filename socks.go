package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"
)

// socksHandshakeTimeout bounds the SOCKS5 greeting/request phase so a client
// that connects and stalls cannot pin a goroutine forever.
const socksHandshakeTimeout = 30 * time.Second

// socksDialTimeout bounds the upstream SOCKS5 dial (TCP connect phase) for
// SOCKS front-end connections, which have no request context to cancel.
const socksDialTimeout = 30 * time.Second

// SocksServer is a SOCKS5 front-end listener (RFC 1928). It accepts TCP
// CONNECT commands only: BIND (0x02) and UDP ASSOCIATE (0x03) are rejected
// with REP 0x07 (command not supported). No authentication methods are
// offered (no-auth only; RFC 1929 user/password is not implemented).
type SocksServer struct {
	port int
	wanRelay
}

// NewSocksServer creates a SOCKS5 front-end listener on the given port.
func NewSocksServer(port int, pool *WANPool) *SocksServer {
	return &SocksServer{
		port: port,
		wanRelay: wanRelay{
			pool:             pool,
			AccessLog:        true,
			WanFailThreshold: DefaultFailThreshold,
		},
	}
}

// Listen binds the SOCKS5 listener and serves one goroutine per accepted
// connection until ctx is cancelled, then closes the listener and returns
// nil. It returns an error only if the listener cannot be bound or accept
// fails while the context is still active.
func (s *SocksServer) Listen(ctx context.Context) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}
	defer ln.Close()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("socks5 accept error", "error", err)
			return err
		}
		go s.handleSocksConn(conn)
	}
}

// handleSocksConn runs the SOCKS5 protocol for one client connection:
// greeting (no-auth), request (CONNECT only), WAN selection, upstream SOCKS5
// dial, success reply and bidirectional relay.
func (s *SocksServer) handleSocksConn(clientConn net.Conn) {
	defer clientConn.Close()
	start := time.Now()

	// Bound the handshake: the client must complete greeting + request
	// within socksHandshakeTimeout or the connection is dropped.
	clientConn.SetReadDeadline(time.Now().Add(socksHandshakeTimeout))

	// Greeting: [VER=0x05, NMETHODS, METHODS...]. Only no-auth (0x00) is
	// offered; any other version is rejected by closing the connection.
	header := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, header); err != nil {
		return
	}
	if header[0] != 0x05 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(clientConn, methods); err != nil {
		return
	}
	if _, err := clientConn.Write([]byte{0x05, 0x00}); err != nil {
		return
	}

	// Request: [VER, CMD, RSV, ATYP, DST.ADDR, DST.PORT].
	req := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, req); err != nil {
		return
	}
	if req[0] != 0x05 {
		return
	}
	if req[1] != 0x01 { // CONNECT only; BIND (0x02) and UDP ASSOCIATE (0x03) unsupported
		writeSocksReply(clientConn, 0x07) // command not supported
		return
	}

	targetHost, err := parseSocksTarget(clientConn, req[3])
	if err != nil {
		writeSocksReply(clientConn, 0x08) // address type not supported
		return
	}

	wanIndex := s.pool.GetLeastLoaded(s.WanFailThreshold)
	if wanIndex < 0 {
		writeSocksReply(clientConn, 0x01) // general failure
		return
	}

	s.beginWAN(wanIndex, "socks5")
	defer s.endWAN(wanIndex)

	dialCtx, cancel := context.WithTimeout(context.Background(), socksDialTimeout)
	defer cancel()
	upstream, err := s.dialWAN(dialCtx, wanIndex, targetHost, start, "socks5")
	if err != nil {
		writeSocksReply(clientConn, 0x01) // general failure
		return
	}
	defer upstream.Close()

	// Success: REP 0x00, RSV 0x00, ATYP 0x01 (IPv4), BND.ADDR 0.0.0.0,
	// BND.PORT 0.
	if _, err := clientConn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); err != nil {
		return
	}

	// Handshake complete: drop the deadline before relaying.
	clientConn.SetReadDeadline(time.Time{})

	s.relayThroughWAN(wanIndex, targetHost, start, "socks5", clientConn, upstream)
}

// writeSocksReply sends a fixed-shape SOCKS5 reply: VER 0x05, REP, RSV 0x00,
// ATYP 0x01 (IPv4), BND.ADDR 0.0.0.0, BND.PORT 0.
func writeSocksReply(conn net.Conn, rep byte) {
	conn.Write([]byte{0x05, rep, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
}

// parseSocksTarget reads the DST.ADDR + DST.PORT portion of a SOCKS5 request
// for the given ATYP (0x01 IPv4, 0x03 domain, 0x04 IPv6) and returns the
// target as host:port.
func parseSocksTarget(conn net.Conn, atyp byte) (string, error) {
	var host string
	switch atyp {
	case 0x01: // IPv4
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return "", err
		}
		host = net.IP(buf).String()
	case 0x03: // domain name
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return "", err
		}
		if lenBuf[0] == 0 {
			return "", fmt.Errorf("socks: empty domain name")
		}
		buf := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(conn, buf); err != nil {
			return "", err
		}
		host = string(buf)
	case 0x04: // IPv6
		buf := make([]byte, 16)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return "", err
		}
		host = net.IP(buf).String()
	default:
		return "", fmt.Errorf("socks: unsupported address type 0x%02x", atyp)
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return "", err
	}
	port := int(portBuf[0])<<8 | int(portBuf[1])
	if port == 0 {
		return "", fmt.Errorf("socks: invalid port 0")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}
