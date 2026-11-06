package sbxhttp

// CDN mode is a private SB-to-SB HTTP/2 transport, NOT Xray's XHTTP wire format.
// It separates packetized uplink POSTs from a streaming downlink GET.
// Unlike a single indefinitely streaming POST, finite upstream requests
// work with CDN request-body buffering. The downstream must remain streaming.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/common/tls"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/net/http2"
)

const (
	cdnWireVersion    = "1"
	cdnPreamble       = "SBX1"
	cdnMaxFrame       = 32 * 1024
	cdnSessionLimit   = 128
	cdnHeartbeat      = 20 * time.Second
	cdnUplinkWait     = 30 * time.Second
	cdnPacketMax      = 128 * 1024
	cdnPacketWindow   = 32
	cdnInflight       = 8
	cdnPacketInterval = 10 * time.Millisecond
)

// Downstream is framed: 4-byte BE length; zero length is a keepalive. The
// framing is outside VLESS Encryption and never substitutes for authenticated
// VLESS payload encryption.
func cdnReadFrame(r io.Reader) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n > cdnMaxFrame {
		return nil, fmt.Errorf("sbxhttp CDN: frame too large: %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

func cdnWriteFrame(w io.Writer, b []byte) error {
	if len(b) > cdnMaxFrame {
		return errors.New("sbxhttp CDN: frame too large")
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(b)))
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if len(b) > 0 {
		_, err := w.Write(b)
		return err
	}
	return nil
}

func cdnNewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func cdnValidID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// A separate H2 transport is deliberately owned by each logical VLESS
// session. It can multiplex GET/POST without sharing fate with other sessions.
type cdnClientConn struct {
	parent       *Client
	ctx          context.Context
	cancel       context.CancelFunc
	tr           *http2.Transport
	uploadReader *io.PipeReader
	uploadWriter *io.PipeWriter
	download     io.ReadCloser
	mu           sync.Mutex
	sockets      []net.Conn
	closed       atomic.Bool
	closeOnce    sync.Once
	readMu       sync.Mutex
	pending      []byte
}

func (c *Client) dialCDN(ctx context.Context) (net.Conn, error) {
	if c.closed.Load() {
		return nil, net.ErrClosed
	}
	timeout, err := boundedHandshakeTimeout(ctx)
	if err != nil {
		return nil, err
	}
	id, err := cdnNewID()
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	pr, pw := io.Pipe()
	session := &cdnClientConn{parent: c, ctx: lifetime, cancel: cancel, uploadReader: pr, uploadWriter: pw}
	session.tr = &http2.Transport{
		DialTLSContext: func(dialCtx context.Context, _, _ string, _ *tls.STDConfig) (net.Conn, error) {
			raw, err := c.tlsDialer.DialTLSContext(dialCtx, c.serverAddr)
			if err != nil {
				return nil, err
			}
			if raw.ConnectionState().NegotiatedProtocol != http2.NextProtoTLS {
				_ = raw.Close()
				return nil, fmt.Errorf("sbxhttp CDN: standard TLS must negotiate h2, got %q", raw.ConnectionState().NegotiatedProtocol)
			}
			session.mu.Lock()
			if session.closed.Load() {
				session.mu.Unlock()
				_ = raw.Close()
				return nil, net.ErrClosed
			}
			session.sockets = append(session.sockets, raw)
			session.mu.Unlock()
			return raw, nil
		},
		ReadIdleTimeout:  h2ReadIdleTimeout,
		PingTimeout:      h2PingTimeout,
		WriteByteTimeout: h2WriteByteTimeout,
	}
	// Host and SNI must be the Cloudflare-proxied name; never use the origin IP.
	authority := c.config.ServerName()
	if authority == "" {
		session.Close()
		return nil, errors.New("sbxhttp CDN: TLS server_name required")
	}
	downURL := url.URL{Scheme: "https", Host: c.serverAddr.String(), Path: strings.TrimRight(c.target.Path, "/") + "/down/" + id}
	downReq, _ := http.NewRequestWithContext(lifetime, http.MethodGet, downURL.String(), nil)
	downReq.Host = authority
	downReq.Header.Set("X-SBXHTTP-CDN", cdnWireVersion)
	downReq.Header.Set("Cache-Control", "no-store")
	downReq.Header.Set("Accept-Encoding", "identity")
	type result struct {
		body io.ReadCloser
		err  error
	}
	ready := make(chan result)
	deliver := func(result result) {
		select {
		case ready <- result:
		case <-lifetime.Done():
			if result.body != nil {
				_ = result.body.Close()
			}
		}
	}
	go func() {
		resp, err := session.tr.RoundTrip(downReq)
		if err != nil {
			deliver(result{err: err})
			return
		}
		if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 || resp.Header.Get("X-SBXHTTP-CDN") != cdnWireVersion {
			_ = resp.Body.Close()
			deliver(result{err: fmt.Errorf("sbxhttp CDN downlink rejected: %s", resp.Status)})
			return
		}
		b := make([]byte, len(cdnPreamble))
		if _, err = io.ReadFull(resp.Body, b); err != nil {
			_ = resp.Body.Close()
			deliver(result{err: err})
			return
		}
		if string(b) != cdnPreamble {
			_ = resp.Body.Close()
			deliver(result{err: errors.New("sbxhttp CDN: invalid downstream preamble")})
			return
		}
		deliver(result{body: resp.Body})
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ready:
		if r.err != nil {
			_ = session.Close()
			return nil, r.err
		}
		session.download = r.body
	case <-ctx.Done():
		_ = session.Close()
		return nil, ctx.Err()
	case <-timer.C:
		_ = session.Close()
		return nil, context.DeadlineExceeded
	}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		_ = session.Close()
		return nil, net.ErrClosed
	}
	c.cdnSessions[session] = struct{}{}
	c.mu.Unlock()
	upURL := url.URL{Scheme: "https", Host: c.serverAddr.String(), Path: strings.TrimRight(c.target.Path, "/") + "/up/" + id}
	go session.packetUploader(upURL.String(), authority)
	return session, nil
}

// packetUploader batches small early writes with a short bounded delay and
// allows finite POSTs to be in flight concurrently. The sequence number is
// part of the path; the origin reorders them before exposing a byte stream.
func (s *cdnClientConn) packetUploader(baseURL, authority string) {
	chunks := make(chan []byte, 16)
	go func() {
		defer close(chunks)
		b := make([]byte, 64*1024)
		for {
			n, err := s.uploadReader.Read(b)
			if n > 0 {
				data := append([]byte(nil), b[:n]...)
				select {
				case chunks <- data:
				case <-s.ctx.Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	sem := make(chan struct{}, cdnInflight)
	var seq uint64
	var batch []byte
	tick := time.NewTicker(cdnPacketInterval)
	defer tick.Stop()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			return
		}
		payload := batch
		number := seq
		seq++
		batch = nil
		go func() {
			defer func() { <-sem }()
			if err := s.sendPacket(baseURL, authority, number, payload); err != nil {
				_ = s.Close()
			}
		}()
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case data, open := <-chunks:
			if !open {
				flush()
				return
			}
			for len(data) > 0 {
				room := cdnPacketMax - len(batch)
				n := len(data)
				if n > room {
					n = room
				}
				batch = append(batch, data[:n]...)
				data = data[n:]
				if len(batch) == cdnPacketMax {
					flush()
				}
			}
		case <-tick.C:
			flush()
		}
	}
}
func (s *cdnClientConn) sendPacket(baseURL, authority string, seq uint64, data []byte) error {
	packetURL := baseURL + "/" + strconv.FormatUint(seq, 10)
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, packetURL, bytes.NewReader(data))
		if err != nil {
			cancel()
			return err
		}
		req.Host = authority
		req.Header.Set("X-SBXHTTP-CDN", cdnWireVersion)
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Cache-Control", "no-store")
		resp, err := s.tr.RoundTrip(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent && resp.ProtoMajor == 2 {
				cancel()
				return nil
			}
			err = fmt.Errorf("sbxhttp CDN packet %d rejected: %s", seq, resp.Status)
		}
		last = err
		cancel()
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
		}
	}
	return last
}

func (s *cdnClientConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()
	for len(s.pending) == 0 {
		if s.closed.Load() {
			return 0, net.ErrClosed
		}
		frame, err := cdnReadFrame(s.download)
		if err != nil {
			_ = s.Close()
			return 0, err
		}
		s.pending = frame // zero-length heartbeat never enters VLESS/ENC
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}
func (s *cdnClientConn) Write(p []byte) (int, error) {
	if s.closed.Load() {
		return 0, net.ErrClosed
	}
	return s.uploadWriter.Write(p)
}
func (s *cdnClientConn) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		_ = s.uploadWriter.CloseWithError(net.ErrClosed)
		_ = s.uploadReader.CloseWithError(net.ErrClosed)
		if s.download != nil {
			_ = s.download.Close()
		}
		s.mu.Lock()
		sockets := append([]net.Conn(nil), s.sockets...)
		s.mu.Unlock()
		for _, conn := range sockets {
			_ = conn.Close()
		}
		s.tr.CloseIdleConnections()
		s.parent.mu.Lock()
		delete(s.parent.cdnSessions, s)
		s.parent.mu.Unlock()
	})
	return nil
}
func (s *cdnClientConn) LocalAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sockets) > 0 {
		return s.sockets[0].LocalAddr()
	}
	return &net.TCPAddr{}
}
func (s *cdnClientConn) RemoteAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sockets) > 0 {
		return s.sockets[0].RemoteAddr()
	}
	return &net.TCPAddr{}
}
func (*cdnClientConn) SetDeadline(time.Time) error      { return os.ErrInvalid }
func (*cdnClientConn) SetReadDeadline(time.Time) error  { return os.ErrInvalid }
func (*cdnClientConn) SetWriteDeadline(time.Time) error { return os.ErrInvalid }
func (*cdnClientConn) NeedAdditionalReadDeadline() bool { return true }
func (c *Client) cdnReset() {
	c.mu.Lock()
	items := make([]*cdnClientConn, 0, len(c.cdnSessions))
	for x := range c.cdnSessions {
		items = append(items, x)
	}
	c.mu.Unlock()
	for _, x := range items {
		_ = x.Close()
	}
}

// The origin-side HTTP/2 transport is allowed to serve multiple requests and
// multiple CDN edge connections; a session lives in Server, not in a TCP socket.
type cdnServerSession struct {
	ctx          context.Context
	cancel       context.CancelFunc
	conn         *cdnServerConn
	uploadReader *io.PipeReader
	uploadWriter *io.PipeWriter
	upStarted    atomic.Bool
	closeOnce    sync.Once
	packetMu     sync.Mutex
	packetCond   *sync.Cond
	pending      map[uint64][]byte
	next         uint64
}

func (s *cdnServerSession) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.packetMu.Lock()
		if s.packetCond != nil {
			s.packetCond.Broadcast()
		}
		s.packetMu.Unlock()
		_ = s.uploadReader.CloseWithError(net.ErrClosed)
		_ = s.uploadWriter.CloseWithError(net.ErrClosed)
		_ = s.conn.Close()
	})
}

type cdnServerConn struct {
	reader    *io.PipeReader
	writer    http.ResponseWriter
	flush     func() error
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    atomic.Bool
	lastWrite time.Time
}

func (c *cdnServerConn) Read(b []byte) (int, error) { return c.reader.Read(b) }
func (c *cdnServerConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	count := 0
	for len(b) > 0 {
		n := len(b)
		if n > cdnMaxFrame {
			n = cdnMaxFrame
		}
		if err := cdnWriteFrame(c.writer, b[:n]); err != nil {
			return count, err
		}
		if err := c.flush(); err != nil {
			return count, err
		}
		b = b[n:]
		count += n
	}
	c.lastWrite = time.Now()
	return count, nil
}
func (c *cdnServerConn) heartbeat() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return net.ErrClosed
	}
	if time.Since(c.lastWrite) < cdnHeartbeat {
		return nil
	}
	if err := cdnWriteFrame(c.writer, nil); err != nil {
		return err
	}
	if err := c.flush(); err != nil {
		return err
	}
	c.lastWrite = time.Now()
	return nil
}
func (c *cdnServerConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.cancel()
		_ = c.reader.CloseWithError(net.ErrClosed)
	}
	return nil
}
func (*cdnServerConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*cdnServerConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*cdnServerConn) SetDeadline(time.Time) error      { return os.ErrInvalid }
func (*cdnServerConn) SetReadDeadline(time.Time) error  { return os.ErrInvalid }
func (*cdnServerConn) SetWriteDeadline(time.Time) error { return os.ErrInvalid }
func (*cdnServerConn) NeedAdditionalReadDeadline() bool { return true }

func (s *Server) serveCDNHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.ProtoMajor != 2 || r.Header.Get("X-SBXHTTP-CDN") != cdnWireVersion || r.URL.RawQuery != "" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	base := strings.TrimRight(s.path, "/") + "/"
	if !strings.HasPrefix(r.URL.Path, base) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, base), "/")
	if (len(parts) != 2 && len(parts) != 3) || !cdnValidID(parts[1]) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	switch {
	case r.Method == http.MethodGet && parts[0] == "down" && len(parts) == 2:
		s.cdnDown(w, r, parts[1])
	case r.Method == http.MethodPost && parts[0] == "up" && len(parts) == 3:
		seq, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			http.Error(w, "bad sequence", http.StatusBadRequest)
			return
		}
		s.cdnUp(w, r, parts[1], seq)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}
func (s *Server) cdnDown(w http.ResponseWriter, r *http.Request, id string) {
	s.cdnMu.Lock()
	if s.closed.Load() || len(s.cdnSessions) >= cdnSessionLimit || s.cdnSessions[id] != nil {
		s.cdnMu.Unlock()
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	pr, pw := io.Pipe()
	conn := &cdnServerConn{reader: pr, writer: w, flush: func() error { return http.NewResponseController(w).Flush() }, cancel: cancel, lastWrite: time.Now()}
	sess := &cdnServerSession{ctx: ctx, cancel: cancel, conn: conn, uploadReader: pr, uploadWriter: pw, pending: make(map[uint64][]byte)}
	sess.packetCond = sync.NewCond(&sess.packetMu)
	s.cdnSessions[id] = sess
	s.cdnMu.Unlock()
	defer func() { sess.Close(); s.cdnMu.Lock(); delete(s.cdnSessions, id); s.cdnMu.Unlock() }()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-SBXHTTP-CDN", cdnWireVersion)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(cdnPreamble)); err != nil {
		return
	}
	if err := http.NewResponseController(w).Flush(); err != nil {
		return
	}
	ticker := time.NewTicker(cdnHeartbeat)
	defer ticker.Stop()
	timeout := time.NewTimer(cdnUplinkWait)
	defer timeout.Stop()
	done := make(chan struct{})
	source := M.ParseSocksaddr(r.RemoteAddr)
	go sess.assemblePackets()
	go s.handler.NewConnectionEx(ctx, conn, source, M.Socksaddr{}, N.OnceClose(func(error) { close(done) }))
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-timeout.C:
			if !sess.upStarted.Load() {
				return
			}
		case <-ticker.C:
			if err := conn.heartbeat(); err != nil {
				return
			}
		}
	}
}
func (s *cdnServerSession) assemblePackets() {
	for {
		s.packetMu.Lock()
		var data []byte
		for s.ctx.Err() == nil {
			var ok bool
			data, ok = s.pending[s.next]
			if ok {
				delete(s.pending, s.next)
				s.next++
				break
			}
			s.packetCond.Wait()
		}
		s.packetMu.Unlock()
		if s.ctx.Err() != nil {
			return
		}
		if _, err := s.uploadWriter.Write(data); err != nil {
			s.Close()
			return
		}
	}
}
func (s *Server) cdnUp(w http.ResponseWriter, r *http.Request, id string, seq uint64) {
	s.cdnMu.Lock()
	sess := s.cdnSessions[id]
	s.cdnMu.Unlock()
	if sess == nil || sess.ctx.Err() != nil {
		http.Error(w, "unknown session", http.StatusConflict)
		return
	}
	if r.ContentLength > cdnPacketMax {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, cdnPacketMax+1))
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if len(b) == 0 || len(b) > cdnPacketMax {
		http.Error(w, "invalid payload", http.StatusRequestEntityTooLarge)
		return
	}
	sess.packetMu.Lock()
	if seq >= sess.next {
		if seq >= sess.next+cdnPacketWindow {
			sess.packetMu.Unlock()
			http.Error(w, "reorder window exceeded", http.StatusConflict)
			return
		}
		if _, exists := sess.pending[seq]; !exists {
			if len(sess.pending) >= cdnPacketWindow {
				sess.packetMu.Unlock()
				http.Error(w, "reorder buffer full", http.StatusConflict)
				return
			}
			sess.pending[seq] = b
			sess.packetCond.Signal()
		}
	}
	sess.upStarted.Store(true)
	sess.packetMu.Unlock()
	w.Header().Set("X-SBXHTTP-CDN", cdnWireVersion)
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) cdnCloseAll() {
	s.cdnMu.Lock()
	items := make([]*cdnServerSession, 0, len(s.cdnSessions))
	for _, x := range s.cdnSessions {
		items = append(items, x)
	}
	s.cdnMu.Unlock()
	for _, x := range items {
		x.Close()
	}
}

var _ net.Conn = (*cdnClientConn)(nil)
var _ net.Conn = (*cdnServerConn)(nil)
