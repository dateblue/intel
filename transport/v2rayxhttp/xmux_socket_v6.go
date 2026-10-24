package v2rayxhttp

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
)

// xmuxTrackedSocket belongs to one logical XMUX resource. Go's HTTP/2
// Transport.CloseIdleConnections deliberately ignores active sockets; a
// definitive loss needs a way to wake their blocked readers and writers.
// The tracking layer is transparent to TLS/REALITY and to bytes on the wire.
type xmuxTrackedSocket struct {
	net.Conn
	owner    *http2XmuxConn
	once     sync.Once
	closeErr error
}

// Preserve the project's Upstream-based socket unwrapping for diagnostics
// and platform-specific TCP options through this transparent layer.
func (c *xmuxTrackedSocket) Upstream() any { return c.Conn }

func (c *xmuxTrackedSocket) Close() error {
	c.once.Do(func() {
		c.closeErr = c.Conn.Close()
		c.owner.socketMu.Lock()
		delete(c.owner.sockets, c)
		c.owner.socketMu.Unlock()
	})
	return c.closeErr
}

func (c *http2XmuxConn) trackSocket(conn net.Conn) (net.Conn, error) {
	tracked := &xmuxTrackedSocket{Conn: conn, owner: c}
	c.socketMu.Lock()
	if c.fatal.Load() {
		c.socketMu.Unlock()
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	if c.sockets == nil {
		c.sockets = make(map[*xmuxTrackedSocket]struct{})
	}
	c.sockets[tracked] = struct{}{}
	c.socketMu.Unlock()
	return tracked, nil
}

// abortDead operates only on a definitive physical-transport loss, not on a
// per-stream reset or caller timeout. Scope the abort to the affected pool so
// healthy siblings continue forwarding; the manager can create a replacement.
func (c *http2XmuxConn) abortDead() {
	c.socketMu.Lock()
	if c.fatal.Swap(true) {
		c.socketMu.Unlock()
		return
	}
	c.closed.Store(true)
	active := make([]*xmuxTrackedSocket, 0, len(c.sockets))
	for sock := range c.sockets {
		active = append(active, sock)
	}
	c.socketMu.Unlock()
	for _, sock := range active {
		_ = sock.Close()
	}
	c.transport.CloseIdleConnections()
}

// An http2 response-body error does not necessarily mean that the underlying
// TCP connection died. Only definitive connection-level failures may tear down
// other live streams in the same pool; all other errors use deferred eviction.
func xhttpDefinitiveTransportLoss(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) && (op.Op == "read" || op.Op == "write") && op.Timeout() {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "http2: client connection lost") || strings.Contains(msg, "http2: client conn is closed")
}
