// Package relayio provides the shared bidirectional byte splice used by both
// relay front-ends (the HTTPS CONNECT proxy in proxy.go and the SOCKS5
// listener in socks.go), so both flow through identical byte plumbing.
//
// Semantics (review findings F-09 / F-10, requirements RT-06 / RT-07):
//
//   - Half-close: when one direction reaches EOF, the write side of the
//     socket it was copying into is half-closed (CloseWrite when the conn
//     supports it, i.e. *net.TCPConn) so the peer sees a FIN but the reverse
//     direction keeps delivering data. Neither socket is torn down early.
//   - Full close: both sockets are closed exactly once (sync.Once) only when
//     BOTH directions are done, or immediately on a fatal error/timeout.
//   - Idle timeout (IDLE_TIMEOUT, default 300s): when no bytes flow in
//     EITHER direction for IdleTimeout, the connection is closed and its
//     resources released exactly once.
//   - Handshake/first-byte timeout: the client side must produce its first
//     byte within HandshakeTimeout (the bounded client request phase behind
//     an established CONNECT/SOCKS tunnel); if the upstream speaks first the
//     bound yields to the idle timeout instead.
//   - Hijack peek (PeekReader): bytes the HTTP server already buffered past
//     the CONNECT request (bufio.Reader.Buffered() > 0 after Hijack) are
//     replayed to the upstream before any bytes read from the raw conn, so
//     pipelined client data is never dropped.
//
// Both timeouts are injectable through Options (durations as fields); zero
// means the package default. All durations are parameters, so tests can drive
// sub-second values.
package relayio

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultIdleTimeout is IDLE_TIMEOUT: no bytes in either direction for
	// this long closes the connection and releases its resources.
	DefaultIdleTimeout = 300 * time.Second

	// DefaultHandshakeTimeout bounds the client's first byte after the
	// splice starts (the client's request phase).
	DefaultHandshakeTimeout = 30 * time.Second

	// copyBufferSize is the per-direction copy buffer size.
	copyBufferSize = 32 * 1024
)

// ErrIdleTimeout is returned in Stats.Err when the splice closed the
// connection because neither direction carried any bytes for
// Options.IdleTimeout.
var ErrIdleTimeout = errors.New("relayio: idle timeout: no bytes in either direction")

// ErrHandshakeTimeout is returned in Stats.Err when the client produced no
// first byte within Options.HandshakeTimeout and the upstream stayed silent
// too — the bounded client request phase expired.
var ErrHandshakeTimeout = errors.New("relayio: handshake timeout: no first byte")

// Direction identifies which way bytes are flowing.
type Direction uint8

const (
	// ClientToUpstream is the "up" direction: client bytes relayed to upstream.
	ClientToUpstream Direction = iota
	// UpstreamToClient is the "down" direction: upstream bytes relayed to client.
	UpstreamToClient
)

// Options configures one Splice. The zero value means "defaults": 300s idle
// timeout, 30s handshake timeout, no byte callback.
type Options struct {
	// IdleTimeout closes the connection when NEITHER direction has carried
	// bytes for this long. <= 0 means DefaultIdleTimeout.
	IdleTimeout time.Duration

	// HandshakeTimeout bounds the client's first byte after the splice
	// starts. If the upstream sends first, the bound yields to IdleTimeout.
	// <= 0 means DefaultHandshakeTimeout.
	HandshakeTimeout time.Duration

	// OnBytes, when non-nil, is called from the splice goroutines with the
	// number of bytes just WRITTEN for a direction. It lets callers record
	// byte metrics while the splice is still running (a half-closed splice
	// may legitimately stay open waiting on the reverse direction long after
	// one side is gone). Implementations must be safe for concurrent calls.
	OnBytes func(Direction, int64)
}

func (o Options) withDefaults() Options {
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = DefaultIdleTimeout
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = DefaultHandshakeTimeout
	}
	return o
}

// Stats is the outcome of a Splice. Up/Down are the per-direction byte counts
// (bytes actually written); Err is the fatal error that tore the splice down,
// or nil for a clean close after both directions finished.
type Stats struct {
	Up   int64
	Down int64
	Err  error
}

// splice is the per-connection state shared by the two direction goroutines.
type splice struct {
	client, upstream net.Conn
	opts             Options

	// start is the splice start time; startNano doubles as the "no activity
	// yet" sentinel for lastNano.
	start     time.Time
	startNano int64

	// lastNano is the unix nano timestamp of the last byte read or written
	// by EITHER direction; the idle timeout is measured from it.
	lastNano atomic.Int64

	closeOnce sync.Once // both sockets closed exactly once
	errOnce   sync.Once // first fatal error wins
	err       error
}

func (s *splice) touch()                  { s.lastNano.Store(time.Now().UnixNano()) }
func (s *splice) lastActivity() time.Time { return time.Unix(0, s.lastNano.Load()) }

// closeBoth tears down both sockets exactly once. It runs only when both
// directions are done or when a fatal error/timeout fired.
func (s *splice) closeBoth() {
	s.closeOnce.Do(func() {
		s.client.Close()
		s.upstream.Close()
	})
}

// fail records the first fatal error and tears both sockets down, unblocking
// the other direction goroutine.
func (s *splice) fail(err error) {
	s.errOnce.Do(func() { s.err = err })
	s.closeBoth()
}

// closeWrite half-closes the write side of c (peer sees FIN, reads keep
// working) when c supports it — *net.TCPConn and friends. Non-close-writer
// conns are left untouched; their direction is closed only by closeBoth once
// both directions are done.
func closeWrite(c net.Conn) {
	type closeWriter interface{ CloseWrite() error }
	if cw, ok := c.(closeWriter); ok {
		cw.CloseWrite()
	}
}

// isTimeout reports whether err is a network deadline expiration.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// copyDir copies src into wr (reading through rd for deadline control) until
// EOF, a fatal timeout, or an error. On clean EOF the write side of wr is
// half-closed so the reverse direction stays usable. handshake (client-side
// first byte only) switches the read deadline to the handshake timeout until
// the first byte arrives.
func (s *splice) copyDir(rd net.Conn, src io.Reader, wr net.Conn, dir Direction, handshake bool) (int64, error) {
	buf := make([]byte, copyBufferSize)
	var n int64
	for {
		deadline := s.lastActivity().Add(s.opts.IdleTimeout)
		if handshake {
			deadline = s.start.Add(s.opts.HandshakeTimeout)
		}
		rd.SetReadDeadline(deadline)

		nr, rerr := src.Read(buf)
		if nr > 0 {
			handshake = false
			s.touch()
			nw, werr := wr.Write(buf[:nr])
			if nw > 0 {
				s.touch()
				n += int64(nw)
				if s.opts.OnBytes != nil {
					s.opts.OnBytes(dir, int64(nw))
				}
			}
			if werr != nil {
				return n, werr
			}
			if nw != nr {
				return n, io.ErrShortWrite
			}
		}
		if rerr == nil {
			continue
		}
		if rerr == io.EOF {
			return n, nil
		}
		if isTimeout(rerr) {
			if handshake {
				// Client request phase expired with no first byte from
				// anyone: if the upstream had spoken, activity would have
				// moved the deadline to the idle bound already.
				if s.lastNano.Load() == s.startNano {
					return n, ErrHandshakeTimeout
				}
				handshake = false
				continue
			}
			// Idle deadline: only fatal when neither direction moved since
			// the deadline was computed; otherwise re-arm and keep copying.
			if !time.Now().Before(s.lastActivity().Add(s.opts.IdleTimeout)) {
				return n, ErrIdleTimeout
			}
			continue
		}
		return n, rerr
	}
}

// Splice relays client <-> upstream bidirectionally until both directions are
// done or a fatal error/timeout fires, then closes both sockets exactly once
// and returns the per-direction byte counts.
//
// clientSrc, when non-nil, is read instead of client for the
// client-to-upstream direction (pass PeekReader's result after an HTTP Hijack
// so buffered pipelined bytes reach the upstream first); nil reads client
// directly.
//
// On EOF in one direction the peer's write side is half-closed rather than
// closed, so e.g. a client that half-closes still receives the upstream's
// final response (RT-07), and the reverse direction is torn down only when it
// finishes on its own, errors, or the idle/handshake timeout fires.
func Splice(client, upstream net.Conn, clientSrc io.Reader, opts Options) Stats {
	o := opts.withDefaults()
	if clientSrc == nil {
		clientSrc = client
	}
	s := &splice{
		client:   client,
		upstream: upstream,
		opts:     o,
	}
	s.start = time.Now()
	s.startNano = s.start.UnixNano()
	s.lastNano.Store(s.startNano)

	var up, down int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, err := s.copyDir(client, clientSrc, upstream, ClientToUpstream, true)
		up = n
		if err != nil {
			s.fail(err)
		} else {
			closeWrite(upstream)
		}
	}()
	go func() {
		defer wg.Done()
		n, err := s.copyDir(upstream, upstream, client, UpstreamToClient, false)
		down = n
		if err != nil {
			s.fail(err)
		} else {
			closeWrite(client)
		}
	}()
	wg.Wait()
	s.closeBoth()
	return Stats{Up: up, Down: down, Err: s.err}
}

// PeekReader returns the reader to use for the client side of a hijacked HTTP
// connection. The server's bufio.Reader may have buffered bytes past the
// CONNECT request (rw.Reader.Buffered() > 0); those were already read off the
// wire and would be lost if the relay read the raw conn, so they are replayed
// first through an io.MultiReader. Returns nil when nothing is buffered, in
// which case Splice reads the raw conn directly.
func PeekReader(rw *bufio.ReadWriter, conn net.Conn) io.Reader {
	if rw == nil || rw.Reader == nil || rw.Reader.Buffered() == 0 {
		return nil
	}
	return io.MultiReader(rw.Reader, conn)
}
