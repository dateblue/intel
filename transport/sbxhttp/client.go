// Package sbxhttp implements a deliberately small sing-box-to-sing-box HTTP/2
// VLESS transport. It is NOT wire-compatible with Xray XHTTP.
//
// Each logical tunnel uses its own HTTP/2 connection. This intentionally
// eliminates cross-stream head-of-line amplification from XMUX at the cost
// of additional TCP/TLS handshakes and connection overhead.
package sbxhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/net/http2"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

// Client is a small factory: it intentionally owns no shared HTTP/2 pool.
// One call to DialContext == one independent TLS/TCP + HTTP/2 connection.
type Client struct {
	serverAddr M.Socksaddr
	tlsDialer  tls.Dialer
	config     tls.Config
	target     url.URL
	closed     atomic.Bool
	mu         sync.Mutex
	sessions   map[*clientConn]struct{}
}

func NewClient(_ context.Context, dialer N.Dialer, serverAddr M.Socksaddr, opts option.V2RaySBXHTTPOptions, config tls.Config) (adapter.V2RayClientTransport, error) {
	if config == nil {
		return nil, errors.New("sbxhttp: Reality TLS is required")
	}
	path, err := cleanPath(opts.Path)
	if err != nil {
		return nil, err
	}
	// Explicit h2-only ALPN. Reality/uTLS is negotiated by sing-box's TLS stack.
	config.SetNextProtos([]string{http2.NextProtoTLS})
	c := &Client{serverAddr: serverAddr, tlsDialer: tls.NewDialer(dialer, config), config: config, sessions: make(map[*clientConn]struct{})}
	c.target = url.URL{Scheme: "https", Host: serverAddr.String(), Path: path}
	return c, nil
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	if c.closed.Load() {
		return nil, net.ErrClosed
	}
	// VLESS uses early writes; the request must be full-duplex and must live
	// longer than a transient per-dial context. It is explicitly cancelled on
	// Close, handshake failure, interface update, or transport shutdown.
	requestCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	pr, pw := io.Pipe()
	conn := newClientConn(pw, cancel, c.removeSession)
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		cancel()
		pr.Close()
		pw.Close()
		return nil, net.ErrClosed
	}
	c.sessions[conn] = struct{}{}
	c.mu.Unlock()

	req := (&http.Request{Method: http.MethodPost, URL: &c.target, Header: make(http.Header), Body: pr, ContentLength: -1}).WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Cache-Control", "no-store")
	// For consistency with Reality disguise, the HTTP/2 :authority is the SNI.
	if name := c.config.ServerName(); name != "" {
		req.Host = name
	}

	// Distinct transport per logical VLESS session: no cross-stream sharing.
	tr := &http2.Transport{
		DialTLSContext: func(dialCtx context.Context, _, _ string, _ *tls.STDConfig) (net.Conn, error) {
			raw, err := c.tlsDialer.DialTLSContext(dialCtx, c.serverAddr)
			if err != nil {
				return nil, err
			}
			negotiated := raw.ConnectionState().NegotiatedProtocol
			if !privateH2ALPNAllowed(negotiated) {
				raw.Close()
				return nil, fmt.Errorf("sbxhttp: Reality negotiated ALPN %q instead of h2", negotiated)
			}
			if !conn.setPhysical(raw) {
				raw.Close()
				return nil, net.ErrClosed
			}
			return raw, nil
		},
		// No ReadIdleTimeout/Ping timeout: idle != stalled. We do not abort
		// active IPTV streams due to an unproven synthetic health threshold.
	}
	go func() {
		defer tr.CloseIdleConnections()
		defer pr.Close()
		timer := time.AfterFunc(C.TCPTimeout, func() {
			if conn.publish(nil, context.DeadlineExceeded) {
				_ = conn.Close() // release only if timeout won the response race
			}
		})
		resp, err := tr.RoundTrip(req)
		timer.Stop()
		if err != nil {
			conn.publish(nil, fmt.Errorf("sbxhttp: HTTP/2 request: %w", err))
			_ = conn.Close()
			return
		}
		if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			conn.publish(nil, fmt.Errorf("sbxhttp: expected HTTP/2 200, got %s %s", resp.Proto, resp.Status))
			_ = conn.Close()
			return
		}
		conn.publish(resp.Body, nil)
		// RoundTrip returns as soon as response headers arrive; the response
		// body must remain open until the caller closes its net.Conn.
		<-requestCtx.Done()
	}()
	return conn, nil
}

func (c *Client) removeSession(conn *clientConn) {
	c.mu.Lock()
	delete(c.sessions, conn)
	c.mu.Unlock()
}

func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.mu.Lock()
	sessions := make([]*clientConn, 0, len(c.sessions))
	for s := range c.sessions {
		sessions = append(sessions, s)
	}
	c.mu.Unlock()
	for _, s := range sessions {
		_ = s.Close()
	}
	return nil
}
