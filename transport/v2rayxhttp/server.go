//go:build with_xhttp

package v2rayxhttp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/badhttp"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"

	"golang.org/x/net/http2"
)

// Server v5 implements Xray-compatible XHTTP stream-one, stream-up and
// packet-up over HTTP/2. A session ID joins the independent stream-down GET
// with the stream-up request or ordered packet-up requests.
// XMUX is client-side: this server accepts concurrent H2 streams normally.
var _ adapter.V2RayServerTransport = (*Server)(nil)

type Server struct {
	tlsConfig          tls.ServerConfig
	handler            adapter.V2RayServerTransportHandler
	logger             logger.ContextLogger
	path               string
	host               string
	mode               string
	method             string
	noSSE              bool
	padding            intRange
	meta               serverMetaV5
	maxBuffered        int
	keepAlive          intRange
	httpServer         *http.Server
	slowWriteThreshold time.Duration
	h3                 bool
	h3Closer           io.Closer
	activeStreams      atomic.Int64
	idleWarnThreshold  time.Duration

	mu       sync.Mutex
	sessions map[string]*serverSession
	stopping bool
}

func NewServer(ctx context.Context, logger logger.ContextLogger, o option.V2RayXHTTPOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (*Server, error) {
	mode := o.Mode
	if mode == "" {
		mode = modeAuto
	}
	switch mode {
	case modeAuto, modeStreamOne, modeStreamUp, modePacketUp:
	default:
		return nil, E.New("v2ray-xhttp: invalid inbound mode: ", mode)
	}
	if tlsConfig == nil {
		return nil, E.New("v2ray-xhttp inbound: TLS is required")
	}
	meta, err := newServerMetaV5(o)
	if err != nil {
		return nil, err
	}
	padding, err := parseRangeOr(o.XPaddingBytes, "x_padding_bytes", intRange{100, 1000})
	if err != nil {
		return nil, err
	}
	keepAlive, err := parseRangeOr(o.ScStreamUpServerSecs, "sc_stream_up_server_secs", intRange{20, 80})
	if err != nil {
		return nil, err
	}
	method := strings.ToUpper(o.UplinkHTTPMethod)
	if method == "" {
		method = http.MethodPost
	}
	if method == http.MethodGet && mode != modePacketUp && mode != modeAuto {
		return nil, E.New("v2ray-xhttp: GET upload requires packet-up")
	}
	maxBuf := int(o.ScMaxBufferedPosts)
	if maxBuf == 0 {
		maxBuf = 30
	}
	if maxBuf < 0 || maxBuf > 10000 {
		return nil, E.New("v2ray-xhttp: invalid sc_max_buffered_posts")
	}
	headerBytes := normalizeXHTTPHeaderBytes(o.ServerMaxHeaderBytes)
	slowThreshold := time.Duration(0)
	if raw := os.Getenv("SING_BOX_XHTTP_SLOW_MS"); raw != "" {
		if ms, e := strconv.Atoi(raw); e == nil && ms > 0 && ms <= 60000 {
			slowThreshold = time.Duration(ms) * time.Millisecond
		}
	}
	var idleWarnThreshold time.Duration
	if slowThreshold > 0 {
		if raw := os.Getenv("SING_BOX_XHTTP_IDLE_WARN_SEC"); raw != "" {
			if seconds, parseErr := strconv.Atoi(raw); parseErr == nil && seconds >= 10 && seconds <= 3600 {
				idleWarnThreshold = time.Duration(seconds) * time.Second
			}
		}
	}
	s := &Server{
		tlsConfig: tlsConfig, handler: handler, logger: logger,
		path: streamOneServerPath(o), host: o.Host, mode: mode, method: method,
		noSSE: o.NoSSEHeader, padding: padding, meta: meta,
		maxBuffered: maxBuf, keepAlive: keepAlive, slowWriteThreshold: slowThreshold, idleWarnThreshold: idleWarnThreshold,
		sessions: make(map[string]*serverSession),
		h3:       len(tlsConfig.NextProtos()) == 1 && tlsConfig.NextProtos()[0] == "h3",
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	s.httpServer = &http.Server{
		Protocols: protocols,
		Handler:   s, ReadHeaderTimeout: C.TCPTimeout, MaxHeaderBytes: headerBytes,
		BaseContext: func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context { return log.ContextWithNewID(ctx) },
	}
	// As in Xray's hub.go, net/http handles both HTTP/1.1 and the cleartext
	// HTTP/2 preface here. The outer listener still performs REALITY/TLS.
	return s, nil
}

func streamOneServerPath(o option.V2RayXHTTPOptions) string {
	path, _, _ := strings.Cut(o.Path, "?")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	sessionPlace := orDefault(o.SessionPlacement, placementPath)
	seqPlace := orDefault(o.SeqPlacement, placementPath)
	if (sessionPlace == placementPath || seqPlace == placementPath) && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

// getSession never creates a session for an invalid or oversize ID. Before the
// download GET joins a session, the upload buffer expires after 30 seconds,
// like Xray. The session remains alive as long as the GET is active.
func (s *Server) getSession(id string) *serverSession {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	if current := s.sessions[id]; current != nil {
		s.mu.Unlock()
		return current
	}
	if len(s.sessions) >= 2048 {
		s.mu.Unlock()
		return nil
	}
	session := newServerSession(s.maxBuffered)
	s.sessions[id] = session
	s.mu.Unlock()
	time.AfterFunc(30*time.Second, func() {
		if !session.DownloadStarted() {
			s.dropSession(id, session)
		}
	})
	return session
}

func (s *Server) dropSession(id string, current *serverSession) {
	s.mu.Lock()
	if s.sessions[id] == current {
		delete(s.sessions, id)
	}
	s.mu.Unlock()
	current.Close()
}

func (s *Server) writeResponseHeaders(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = "*"
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	if s.meta.sessionPlacement == placementCookie || s.meta.seqPlacement == placementCookie || s.meta.uplinkPlacement == placementCookie || (s.meta.paddingObfs && s.meta.paddingPlacement == placementCookie) {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	if r.Method == http.MethodOptions {
		methods := r.Header.Get("Access-Control-Request-Method")
		if methods == "" {
			methods = "*"
		}
		headers := r.Header.Get("Access-Control-Request-Headers")
		if headers == "" {
			headers = "*"
		}
		w.Header().Set("Access-Control-Allow-Methods", methods)
		w.Header().Set("Access-Control-Allow-Headers", headers)
	}
	s.meta.responsePadding(w, s.padding.rand())
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Stream-one, stream-up, packet-up are HTTP-level modes. For REALITY we
	// typically run H2, but accept HTTP/1.1 for Xray's corresponding mode.
	if r.ProtoMajor != 2 && r.ProtoMajor != 1 && r.ProtoMajor != 3 {
		http.Error(w, "unsupported HTTP version", http.StatusHTTPVersionNotSupported)
		return
	}
	if s.host != "" && !strings.EqualFold(r.Host, s.host) {
		http.NotFound(w, r)
		return
	}
	if !strings.HasPrefix(r.URL.Path, s.path) {
		http.NotFound(w, r)
		return
	}
	sessionID, seqStr, ok := s.meta.extract(r, s.path)
	if !ok {
		http.Error(w, "invalid XHTTP session", http.StatusBadRequest)
		return
	}
	s.writeResponseHeaders(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if !s.meta.paddingValid(r, s.padding) {
		http.Error(w, "invalid XHTTP padding", http.StatusBadRequest)
		return
	}

	mode := s.mode
	if mode == "" {
		mode = modeAuto
	}
	if sessionID == "" {
		if seqStr != "" || r.URL.Path != s.path || (mode != modeAuto && mode != modeStreamOne && mode != modeStreamUp) || r.Method != s.method || r.Method == http.MethodGet {
			http.Error(w, "stream-one not allowed", http.StatusBadRequest)
			return
		}
		s.serveDownload(w, r, r.Body, nil)
		return
	}
	session := s.getSession(sessionID)
	if session == nil {
		http.Error(w, "XHTTP session limit reached", http.StatusServiceUnavailable)
		return
	}
	isUplink := r.Method != http.MethodGet || seqStr != ""
	if isUplink {
		if seqStr == "" { // streamed POST upload
			if mode != modeAuto && mode != modeStreamUp {
				http.Error(w, "stream-up not allowed", http.StatusBadRequest)
				return
			}
			if r.Method != s.method || r.Method == http.MethodGet {
				http.Error(w, "invalid stream-up method", http.StatusMethodNotAllowed)
				return
			}
			s.serveStreamUpload(w, r, session)
			return
		}
		if mode != modeAuto && mode != modePacketUp {
			http.Error(w, "packet-up not allowed", http.StatusBadRequest)
			return
		}
		if r.Method != s.method {
			http.Error(w, "invalid packet-up method", http.StatusMethodNotAllowed)
			return
		}
		seq, e := s.meta.uploadSeq(seqStr)
		if e != nil {
			http.Error(w, "invalid sequence", http.StatusBadRequest)
			return
		}
		payload, e := s.meta.payload(r)
		if e != nil {
			http.Error(w, "invalid upload payload", http.StatusRequestEntityTooLarge)
			return
		}
		if e := session.upload.PushPacketContext(r.Context(), seq, payload); e != nil {
			http.Error(w, "upload queue unavailable: "+e.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet || seqStr != "" {
		http.Error(w, "invalid download request", http.StatusBadRequest)
		return
	}
	if mode == modeStreamOne {
		http.Error(w, "stream-down not allowed", http.StatusBadRequest)
		return
	}
	if !session.BeginDownload() {
		http.Error(w, "duplicate or closed session", http.StatusConflict)
		return
	}
	defer s.dropSession(sessionID, session)
	s.serveDownload(w, r, session.upload, session)
}

// serveDownload only returns after the VLESS handler is done or the HTTP
// request is cancelled. Returning early would invalidate ResponseWriter and
// drop long-running streams. Connection writes retain the established wrapper.
func (s *Server) serveDownload(w http.ResponseWriter, r *http.Request, upload io.Reader, session *serverSession) {
	flush, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "HTTP streaming unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-store")
	if !s.noSSE {
		w.Header().Set("Content-Type", "text/event-stream")
	}
	w.WriteHeader(http.StatusOK)
	s.activeStreams.Add(1)
	defer s.activeStreams.Add(-1)
	// The v4.2 diagnostic writer is retained, not redeclared. The wrapper must
	// be installed before the first Flush so slow header delivery is visible.
	if s.slowWriteThreshold > 0 && s.logger != nil {
		progress := &xhttpStreamProgress{}
		if rc, ok := upload.(io.ReadCloser); ok {
			upload = &xhttpCountingBody{ReadCloser: rc, progress: progress}
		}
		wrapped := &xhttpSlowResponseWriter{
			ResponseWriter: w,
			flusher:        flush,
			threshold:      s.slowWriteThreshold,
			progress:       progress,
			warn: func(op string, took time.Duration) {
				s.logger.WarnContext(r.Context(), "xhttp v5 server ", op,
					" elapsed=", took, " remote=", r.RemoteAddr,
					" active_streams=", s.activeStreams.Load(),
					" bytes_up=", progress.up.Load(), " bytes_down=", progress.down.Load(),
					" context_err=", r.Context().Err())
			},
		}
		finished := make(chan struct{})
		defer close(finished)
		go wrapped.watch(finished, s.idleWarnThreshold)
		w = wrapped
		flush = wrapped
	}
	// Preserve the complete v5 server modes and session lifecycle while
	// propagating HTTP/2 response Flush errors into the VLESS connection.
	// ResponseController is supported by native and diagnostic writers.
	if err := http.NewResponseController(w).Flush(); err != nil {
		if s.logger != nil {
			s.logger.WarnContext(r.Context(), "xhttp initial response flush failed: ", err)
		}
		return
	}
	done := make(chan struct{})
	conn := v2rayhttp.NewHTTP2Wrapper(&v2rayhttp.ServerHTTPConn{
		HTTP2Conn:  v2rayhttp.NewHTTPConn(upload, w),
		Flusher:    flush,
		FlushError: func() error { return http.NewResponseController(w).Flush() },
	})
	s.handler.NewConnectionEx(r.Context(), conn, badhttp.SourceAddress(r), M.Socksaddr{}, N.OnceClose(func(error) { close(done) }))
	select {
	case <-done:
	case <-r.Context().Done():
	}
	// Do not close an active HTTP/2 stream's ResponseWriter before its last
	// serialized Write/Flush has finished. ServerHTTPConn's wrapper lock
	// ensures this call waits for any in-flight Write.
	conn.CloseWrapper()
	_ = session
}

func (s *Server) serveStreamUpload(w http.ResponseWriter, r *http.Request, session *serverSession) {
	if !session.upload.PushStream(r.Body) {
		http.Error(w, "stream-up already started", http.StatusConflict)
		return
	}
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	// Xray writes periodic padding only for legacy Referer clients (or obfs).
	// This response is not the application downlink; the separate GET is.
	keepalive := (r.Header.Get("Referer") != "" || s.meta.paddingObfs) && s.keepAlive.max > 0
	if !keepalive {
		select {
		case <-session.finished:
		case <-r.Context().Done():
		}
		return
	}
	for {
		// Xray emits one padding chunk immediately, then sleeps between
		// subsequent chunks. The upload response is not the download stream.
		if _, err := w.Write([]byte(strings.Repeat("X", s.padding.rand()))); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		wait := time.Duration(s.keepAlive.rand()) * time.Second
		if wait <= 0 {
			wait = 20 * time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-session.finished:
			timer.Stop()
			return
		case <-r.Context().Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Server) Network() []string {
	if s.h3 {
		return []string{N.NetworkUDP}
	}
	return []string{N.NetworkTCP}
}
func (s *Server) Serve(listener net.Listener) error {
	if s.h3 {
		return os.ErrInvalid
	}
	if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
		s.tlsConfig.SetNextProtos(append([]string{http2.NextProtoTLS}, s.tlsConfig.NextProtos()...))
	}
	listener = aTLS.NewListener(listener, s.tlsConfig)
	err := s.httpServer.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func (s *Server) ServePacket(conn net.PacketConn) error {
	if !s.h3 {
		return os.ErrInvalid
	}
	return s.serveHTTP3(conn)
}
func (s *Server) Close() error {
	s.mu.Lock()
	s.stopping = true
	current := s.sessions
	s.sessions = make(map[string]*serverSession)
	h3 := s.h3Closer
	s.mu.Unlock()
	for _, sess := range current {
		sess.Close()
	}
	var h3Err error
	if h3 != nil {
		h3Err = h3.Close()
	}
	h2Err := common.Close(common.PtrOrNil(s.httpServer))
	if h3Err != nil {
		return h3Err
	}
	return h2Err
}

// Match the existing v4.2 server's defensive bound and Xray's 8192-byte default.
func normalizeXHTTPHeaderBytes(configured int) int {
	if configured <= 0 {
		return 8192
	}
	const maxHeaderBytes = 16 << 20
	if configured > maxHeaderBytes {
		return maxHeaderBytes
	}
	return configured
}
