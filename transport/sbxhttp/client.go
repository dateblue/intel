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
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/net/http2"
)

var _ adapter.V2RayClientTransport = (*Client)(nil)

// Client is a small factory: it intentionally owns no shared HTTP/2 pool.
// One call to DialContext == one independent TLS/TCP + HTTP/2 connection.
type Client struct {
	serverAddr       M.Socksaddr
	tlsDialer        tls.Dialer
	config           tls.Config
	target           url.URL
	closed           atomic.Bool
	mu               sync.Mutex
	sessions         map[*clientConn]struct{}
	reliable         bool
	logger           logger.ContextLogger
	visionInnerTLS   bool
	reliableSessions map[*reliableSession]struct{}
	cdnMode          bool
	cdnSessions      map[*cdnClientConn]struct{}
}

func NewClient(_ context.Context, dialer N.Dialer, serverAddr M.Socksaddr, opts option.V2RaySBXHTTPOptions, config tls.Config) (adapter.V2RayClientTransport, error) {
	if config == nil {
		return nil, errors.New("sbxhttp: TLS is required")
	}
	if opts.Mode != "" && opts.Mode != "cdn" {
		return nil, fmt.Errorf("sbxhttp: unknown transport mode %q", opts.Mode)
	}
	if err := validatePrivateVisionMode(opts.VisionInnerTLS, opts.Reliable); err != nil {
		return nil, err
	}
	path, err := cleanPath(opts.Path)
	if err != nil {
		return nil, err
	}
	// Explicit h2-only ALPN. Reality/uTLS is negotiated by sing-box's TLS stack.
	config.SetNextProtos([]string{http2.NextProtoTLS})
	c := &Client{serverAddr: serverAddr, tlsDialer: tls.NewDialer(dialer, config), config: config, sessions: make(map[*clientConn]struct{}), reliable: opts.Reliable, visionInnerTLS: opts.VisionInnerTLS, reliableSessions: make(map[*reliableSession]struct{}), cdnMode: opts.Mode == "cdn", cdnSessions: make(map[*cdnClientConn]struct{})}
	c.target = url.URL{Scheme: "https", Host: serverAddr.String(), Path: path}
	return c, nil
}

// SetLogger attaches the owning VLESS outbound logger without modifying the
// generic V2Ray transport constructor API used by other protocols.
func (c *Client) SetLogger(log logger.ContextLogger) {
	c.logger = log
	if log == nil {
		return
	}
	if c.cdnMode {
		log.Info("sbxhttp: client initialized mode=CDN-TLS-splitH2-v1 path=", c.target.Path)
	} else if c.reliable {
		log.Info("sbxhttp: client initialized mode=Replay-v2 path=", c.target.Path, " version=", reliableVersion)
	} else {
		log.Warn("sbxhttp: client initialized mode=legacy-v1 path=", c.target.Path,
			" vision_inner_tls=", c.visionInnerTLS,
			" (does NOT send X-SBXHTTP-Version; incompatible with Replay-v2-only server)")
	}
}

func (c *Client) DialContext(ctx context.Context) (net.Conn, error) {
	if c.cdnMode {
		return c.dialCDN(ctx)
	}
	if c.reliable {
		stream, err := c.dialReliable(ctx)
		if err != nil || !c.visionInnerTLS {
			return stream, err
		}
		return privateVisionClientTLS(ctx, stream)
	}
	if c.closed.Load() {
		return nil, net.ErrClosed
	}
	// Respect a shorter caller deadline during the initial HTTP/2 handshake.
	// The connection lifetime remains independent of the transient dial ctx.
	handshakeTimeout, err := boundedHandshakeTimeout(ctx)
	if err != nil {
		return nil, err
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
	cancelPendingDial(ctx, conn)

	req := (&http.Request{Method: http.MethodPost, URL: &c.target, Header: make(http.Header), Body: pr, ContentLength: -1}).WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Cache-Control", "no-store")
	if c.visionInnerTLS {
		req.Header.Set(privateVisionModeHeader, privateVisionModeTLS13)
	}
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
		// PING runs only when no H2 frames are received. It probes a dead TCP
		// path, without applying an idle timeout to valid VLESS streams.
		ReadIdleTimeout:  h2ReadIdleTimeout,
		PingTimeout:      h2PingTimeout,
		WriteByteTimeout: h2WriteByteTimeout,
	}
	go func() {
		defer tr.CloseIdleConnections()
		defer pr.Close()
		timer := time.AfterFunc(handshakeTimeout, func() {
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
	if c.visionInnerTLS {
		wrapper, err := privateVisionClientTLS(ctx, conn)
		if err != nil {
			if c.logger != nil {
				c.logger.Debug("sbxhttp: private Vision TLS 1.3 handshake failed: ", err)
			}
			return nil, err
		}
		if c.logger != nil {
			c.logger.Debug("sbxhttp: private Vision TLS 1.3 ready (HTTP/2 logical carrier)")
		}
		return wrapper, nil
	}
	return conn, nil
}

func (c *Client) removeSession(conn *clientConn) {
	c.mu.Lock()
	delete(c.sessions, conn)
	c.mu.Unlock()
}

// Reset closes active sessions after a network/interface change, while keeping
// this transport available for subsequent DialContext calls. Unlike Close,
// it must not mark the Client permanently closed.
func (c *Client) Reset() {
	c.mu.Lock()
	sessions := make([]*clientConn, 0, len(c.sessions))
	for s := range c.sessions {
		sessions = append(sessions, s)
	}
	reliable := make([]*reliableSession, 0, len(c.reliableSessions))
	for s := range c.reliableSessions {
		reliable = append(reliable, s)
	}
	c.mu.Unlock()
	for _, s := range sessions {
		_ = s.Close()
	}
	c.cdnReset()
	for _, s := range reliable {
		if c.reliable && !c.closed.Load() {
			s.resetPhysical()
		} else {
			s.Close()
		}
	}
}

// Close is terminal and is called when the outbound is actually destroyed.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.Reset()
	return nil
}
