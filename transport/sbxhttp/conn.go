package sbxhttp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func cleanPath(value string) (string, error) {
	if value == "" {
		return "/sbxhttp", nil
	}
	if !strings.HasPrefix(value, "/") || strings.ContainsAny(value, "?#%") || strings.Contains(value, "..") {
		return "", errors.New("sbxhttp: path must be an absolute clean URL path without query, fragment or '..'")
	}
	return value, nil
}

// clientConn is an early-write net.Conn backed by a bidirectional H2 request.
// Close never waits for a blocked Write/Flush, preventing a second deadlock.
type clientConn struct {
	write     *io.PipeWriter
	cancel    context.CancelFunc
	remove    func(*clientConn)
	ready     chan struct{}
	once      sync.Once
	closeOnce sync.Once
	mu        sync.Mutex
	reader    io.ReadCloser
	physical  net.Conn
	err       error
	closed    atomic.Bool
}

func newClientConn(pw *io.PipeWriter, cancel context.CancelFunc, remove func(*clientConn)) *clientConn {
	return &clientConn{write: pw, cancel: cancel, remove: remove, ready: make(chan struct{})}
}
func (c *clientConn) setPhysical(raw net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return false
	}
	c.physical = raw
	return true
}
func (c *clientConn) publish(reader io.ReadCloser, err error) bool {
	accepted := false
	c.once.Do(func() {
		c.mu.Lock()
		c.reader = reader
		c.err = err
		c.mu.Unlock()
		accepted = true
		close(c.ready)
	})
	if !accepted && reader != nil {
		_ = reader.Close()
	}
	return accepted
}
func (c *clientConn) Read(p []byte) (int, error) {
	<-c.ready
	c.mu.Lock()
	reader, err := c.reader, c.err
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if reader == nil {
		return 0, net.ErrClosed
	}
	n, err := reader.Read(p)
	if err != nil {
		_ = c.Close()
	}
	return n, err
}
func (c *clientConn) Write(p []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	return c.write.Write(p)
}
func (c *clientConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.publish(nil, net.ErrClosed)
		c.cancel()
		_ = c.write.CloseWithError(net.ErrClosed)
		c.mu.Lock()
		reader, physical := c.reader, c.physical
		c.mu.Unlock()
		if reader != nil {
			_ = reader.Close()
		}
		if physical != nil {
			_ = physical.Close()
		}
		if c.remove != nil {
			c.remove(c)
		}
	})
	return nil
}
func (c *clientConn) LocalAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.physical != nil {
		return c.physical.LocalAddr()
	}
	return &net.TCPAddr{}
}
func (c *clientConn) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.physical != nil {
		return c.physical.RemoteAddr()
	}
	return &net.TCPAddr{}
}
func (c *clientConn) SetDeadline(t time.Time) error      { return os.ErrInvalid }
func (c *clientConn) SetReadDeadline(t time.Time) error  { return os.ErrInvalid }
func (c *clientConn) SetWriteDeadline(t time.Time) error { return os.ErrInvalid }

// The upstream reader may be a streaming HTTP response without a native
// deadline. sing-box should use its additional read-deadline mechanism.
func (c *clientConn) NeedAdditionalReadDeadline() bool { return true }

// serverConn passes request bytes to VLESS and VLESS writes back to H2.
// Serializing only writes keeps http.ResponseWriter safe. Close is atomic:
// it NEVER blocks behind a stuck response writer / HTTP2 window.
type serverConn struct {
	reader   io.ReadCloser
	writer   io.Writer
	flush    func() error
	closeFn  context.CancelFunc
	writeMu  sync.Mutex
	closed   atomic.Bool
	bytesIn  atomic.Int64
	bytesOut atomic.Int64
	report   func(stage string, duration time.Duration, transferred int)
}

func (c *serverConn) Read(b []byte) (int, error) {
	n, err := c.reader.Read(b)
	c.bytesIn.Add(int64(n))
	return n, err
}
func (c *serverConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	start := time.Now()
	n, err := c.writer.Write(b)
	writeDuration := time.Since(start)
	c.bytesOut.Add(int64(n))
	if c.report != nil && writeDuration >= 2*time.Second {
		c.report("write", writeDuration, n)
	}
	if err != nil {
		return n, err
	}
	start = time.Now()
	err = c.flush()
	flushDuration := time.Since(start)
	if c.report != nil && flushDuration >= 2*time.Second {
		c.report("flush", flushDuration, n)
	}
	if err != nil {
		return n, err
	}
	return n, nil
}
func (c *serverConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.closeFn()
	}
	return nil
}
func (c *serverConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *serverConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *serverConn) SetDeadline(time.Time) error      { return os.ErrInvalid }
func (c *serverConn) SetReadDeadline(time.Time) error  { return os.ErrInvalid }
func (c *serverConn) SetWriteDeadline(time.Time) error { return os.ErrInvalid }
func (c *serverConn) NeedAdditionalReadDeadline() bool { return true }

var _ net.Conn = (*clientConn)(nil)
var _ net.Conn = (*serverConn)(nil)
