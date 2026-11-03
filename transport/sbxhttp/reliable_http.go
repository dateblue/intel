package sbxhttp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sagernet/sing-box/common/tls"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/net/http2"
)

const (
	reliableSessionLimit = 128
	reliableResumeGrace  = 30 * time.Second
)

func randomSessionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func validSessionID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// runReliableClient holds the *logical* VLESS stream while swapping physical
// H2+Reality sockets. It is started once per VLESS dial.
func (c *Client) runReliableClient(sess *reliableSession, id string, ready chan<- error) {
	defer sess.Close()
	established := false
	var disconnectedSince time.Time
	backoff := 250 * time.Millisecond
	for {
		if sess.ctx.Err() != nil {
			return
		}
		attemptStart := time.Now()
		connected := false
		_ = c.reliableAttempt(sess, id, func() {
			connected = true
			if !established {
				established = true
				ready <- nil
			}
		})
		if sess.ctx.Err() != nil {
			return
		}
		// Recovery grace starts only AFTER an actually working physical socket
		// disconnects; a healthy multi-hour socket must still get 30s to resume.
		if connected && time.Since(attemptStart) >= 2*time.Second {
			disconnectedSince = time.Now()
			backoff = 250 * time.Millisecond
		}
		if disconnectedSince.IsZero() {
			disconnectedSince = time.Now()
		}
		if established && time.Since(disconnectedSince) > reliableResumeGrace {
			return
		}
		select {
		case <-sess.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) reliableAttempt(sess *reliableSession, id string, established func()) error {
	attemptCtx, cancel := context.WithCancel(sess.ctx)
	sess.setPhysicalCancel(cancel)
	defer func() { cancel(); sess.clearPhysicalCancel() }()
	bodyReader, bodyWriter := io.Pipe()
	defer bodyReader.Close()
	defer bodyWriter.Close()
	target := url.URL{Scheme: "https", Host: c.serverAddr.String(), Path: c.target.Path}
	req := (&http.Request{Method: http.MethodPost, URL: &target, Header: make(http.Header), Body: bodyReader, ContentLength: -1}).WithContext(attemptCtx)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Cache-Control", "no-store")
	req.Header.Set("X-SBXHTTP-Version", reliableVersion)
	req.Header.Set("X-SBXHTTP-Session", id)
	if name := c.config.ServerName(); name != "" {
		req.Host = name
	}
	var mu sync.Mutex
	var physical net.Conn
	tr := &http2.Transport{
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.STDConfig) (net.Conn, error) {
			raw, err := c.tlsDialer.DialTLSContext(ctx, c.serverAddr)
			if err != nil {
				return nil, err
			}
			if !privateH2ALPNAllowed(raw.ConnectionState().NegotiatedProtocol) {
				raw.Close()
				return nil, fmt.Errorf("sbxhttp: wrong Reality ALPN: %q", raw.ConnectionState().NegotiatedProtocol)
			}
			mu.Lock()
			physical = raw
			mu.Unlock()
			return raw, nil
		},
		ReadIdleTimeout: h2ReadIdleTimeout, PingTimeout: h2PingTimeout, WriteByteTimeout: h2WriteByteTimeout,
	}
	defer func() {
		tr.CloseIdleConnections()
		mu.Lock()
		conn := physical
		mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	}()
	go func() {
		defer bodyWriter.Close()
		_ = sess.sendFrames(attemptCtx, bodyWriter, func() error { return nil })
		cancel()
	}()
	// A deadline on initial session establishment must not limit the lifetime
	// of a successfully established VLESS session.
	resp, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 || resp.Header.Get("X-SBXHTTP-Version") != reliableVersion {
		return fmt.Errorf("sbxhttp: incompatible reliable peer: %s version=%q", resp.Status, resp.Header.Get("X-SBXHTTP-Version"))
	}
	established()
	// Receiving DATA (and its confirmation after delivery into the logical
	// stream) is serialized per attempt. After failure the next socket replays.
	return sess.recvFrames(resp.Body)
}

func (c *Client) dialReliable(ctx context.Context) (net.Conn, error) {
	if c.closed.Load() {
		return nil, net.ErrClosed
	}
	id, err := randomSessionID()
	if err != nil {
		return nil, err
	}
	wait, err := boundedHandshakeTimeout(ctx)
	if err != nil {
		return nil, err
	}
	sess := newReliableSession(context.WithoutCancel(ctx))
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		sess.Close()
		return nil, net.ErrClosed
	}
	c.reliableSessions[sess] = struct{}{}
	c.mu.Unlock()
	ready := make(chan error, 1)
	go func() {
		c.runReliableClient(sess, id, ready)
		c.mu.Lock()
		delete(c.reliableSessions, sess)
		c.mu.Unlock()
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ready:
		return sess.conn(), nil
	case <-timer.C:
		sess.Close()
		return nil, context.DeadlineExceeded
	case <-ctx.Done():
		sess.Close()
		return nil, ctx.Err()
	case <-sess.ctx.Done():
		return nil, errors.New("sbxhttp: session ended before handshake")
	}
}

func (s *Server) reliableHandlerDone(id string, sess *reliableSession) {
	sess.finishApp()
	go func() {
		<-sess.ctx.Done()
		s.mu.Lock()
		if s.reliableSessions[id] == sess {
			delete(s.reliableSessions, id)
		}
		s.mu.Unlock()
	}()
}
func (s *Server) reliableAttach(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-SBXHTTP-Version") != reliableVersion {
		http.Error(w, "SB-XHTTP v2 required", http.StatusUpgradeRequired)
		return
	}
	id := r.Header.Get("X-SBXHTTP-Session")
	if !validSessionID(id) {
		http.Error(w, "invalid session ID", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		http.Error(w, "closed", http.StatusServiceUnavailable)
		return
	}
	sess, ok := s.reliableSessions[id]
	if !ok {
		if len(s.reliableSessions) >= reliableSessionLimit {
			s.mu.Unlock()
			http.Error(w, "session limit", http.StatusServiceUnavailable)
			return
		}
		// Preserve request context metadata but not its short H2 lifetime.
		sess = newReliableSession(context.WithoutCancel(r.Context()))
		s.reliableSessions[id] = sess
	}
	s.mu.Unlock()
	if !ok {
		source := M.ParseSocksaddr(r.RemoteAddr)
		app := sess.conn()
		go func() {
			if s.visionInnerTLS {
				wrapped, err := privateVisionServerTLS(sess.ctx, app, s.privateVisionTLS)
				if err != nil {
					s.logger.Debug("sbxhttp: private Vision reliable handshake: ", err)
					sess.Close()
					return
				}
				app = wrapped
			}
			s.handler.NewConnectionEx(sess.ctx, app, source, M.Socksaddr{}, N.OnceClose(func(error) { s.reliableHandlerDone(id, sess) }))
		}()
	}
	if sess.closed.Load() {
		http.Error(w, "session closed", http.StatusGone)
		return
	}
	// Only one physical H2 attachment can deliver bytes into a logical stream.
	// A reconnect first cancels the previous attachment and waits for it to end.
	sess.transitionMu.Lock()
	sess.attachMu.Lock()
	oldCancel, oldDone := sess.activeCancel, sess.activeDone
	sess.attachMu.Unlock()
	if oldCancel != nil {
		oldCancel()
	}
	if oldDone != nil {
		select {
		case <-oldDone:
		case <-time.After(3 * time.Second):
			sess.transitionMu.Unlock()
			http.Error(w, "previous attachment busy", http.StatusConflict)
			return
		}
	}
	attemptCtx, cancel := context.WithCancel(r.Context())
	done := make(chan struct{})
	sess.attachMu.Lock()
	sess.activeCancel = cancel
	sess.activeDone = done
	sess.lastDetached.Store(0)
	sess.attachMu.Unlock()
	sess.transitionMu.Unlock()
	defer func() {
		cancel()
		sess.attachMu.Lock()
		if sess.activeDone == done {
			sess.activeCancel = nil
			sess.activeDone = nil
			sess.lastDetached.Store(time.Now().UnixNano())
		}
		sess.attachMu.Unlock()
		close(done)
	}()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-SBXHTTP-Version", reliableVersion)
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		return
	}
	receiveDone := make(chan error, 1)
	go func() { receiveDone <- sess.recvFrames(r.Body); cancel() }()
	_ = sess.sendFrames(attemptCtx, w, rc.Flush)
	cancel()
	// r.Body read observes request cancellation; don't block response cleanup
	// indefinitely if a local VLESS reader is legitimately backpressured.
	select {
	case <-receiveDone:
	case <-time.After(3 * time.Second):
	}
}

func (s *Server) sweepReliable() {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.reliableStop:
			return
		case <-tick.C:
			now := time.Now()
			s.mu.Lock()
			for id, sess := range s.reliableSessions {
				last := sess.lastDetached.Load()
				if sess.closed.Load() || (last > 0 && now.Sub(time.Unix(0, last)) > reliableResumeGrace) {
					delete(s.reliableSessions, id)
					sess.Close()
				}
			}
			s.mu.Unlock()
		}
	}
}
