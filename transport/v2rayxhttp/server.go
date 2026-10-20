//go:build with_xhttp

package v2rayxhttp

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
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
	"golang.org/x/net/http2/h2c" //nolint:staticcheck
)

// Server implements the experimental XHTTP stream-one inbound. It deliberately
// does NOT implement stream-up or packet-up; accepting those configurations
// would produce a silently broken service. A bare-path HTTP/2 POST carries one
// bidirectional VLESS connection: request body uplink, response body downlink.
// All authentication remains in the existing VLESS and REALITY handlers.
var _ adapter.V2RayServerTransport = (*Server)(nil)

type Server struct {
	tlsConfig  tls.ServerConfig
	handler    adapter.V2RayServerTransportHandler
	logger     logger.ContextLogger
	path       string
	method     string
	host       string
	noSSE      bool
	padding    intRange
	httpServer *http.Server
	h2Server   *http2.Server
	h2cHandler http.Handler
	// Diagnostics are opt-in. 0 means no per-frame timing overhead.
	slowWriteThreshold time.Duration
}

func NewServer(ctx context.Context, logger logger.ContextLogger, options option.V2RayXHTTPOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (*Server, error) {
	// Make the limited protocol support explicit. In the Xray client,
	// mode=auto over a non-REALITY connection chooses packet-up instead.
	if options.Mode != modeStreamOne {
		return nil, E.New("v2ray-xhttp inbound: only mode=stream-one is implemented; set transport.mode explicitly")
	}
	if options.XPaddingObfsMode {
		return nil, E.New("v2ray-xhttp inbound: x_padding_obfs_mode is not implemented yet")
	}
	if tlsConfig == nil {
		return nil, E.New("v2ray-xhttp inbound: TLS is required for the experimental HTTP/2 stream-one server")
	}
	path := streamOneServerPath(options)
	method := strings.ToUpper(options.UplinkHTTPMethod)
	if method == "" {
		method = http.MethodPost
	}
	if method == http.MethodGet {
		return nil, E.New("v2ray-xhttp inbound: GET cannot carry a stream-one request body")
	}
	padding, err := parseRangeOr(options.XPaddingBytes, "x_padding_bytes", intRange{100, 1000})
	if err != nil {
		return nil, err
	}
	h2 := &http2.Server{}
	// Opt-in diagnostics for high-load stream-one stalls. Never tweak HTTP/2
	// flow-control defaults as a side effect of enabling diagnostics.
	var slowWriteThreshold time.Duration
	if raw := os.Getenv("SING_BOX_XHTTP_SLOW_MS"); raw != "" {
		ms, parseErr := strconv.Atoi(raw)
		if parseErr == nil && ms > 0 && ms <= 60000 {
			slowWriteThreshold = time.Duration(ms) * time.Millisecond
		}
	}
	headerBytes := options.ServerMaxHeaderBytes
	if headerBytes <= 0 {
		headerBytes = http.DefaultMaxHeaderBytes
	}
	s := &Server{tlsConfig: tlsConfig, handler: handler, logger: logger, path: path, method: method, host: options.Host, noSSE: options.NoSSEHeader, padding: padding, h2Server: h2, slowWriteThreshold: slowWriteThreshold}
	s.httpServer = &http.Server{
		Handler:           s,
		ReadHeaderTimeout: C.TCPTimeout,
		MaxHeaderBytes:    headerBytes,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ConnContext:       func(ctx context.Context, _ net.Conn) context.Context { return log.ContextWithNewID(ctx) },
	}
	// The custom REALITY/uTLS listener is not *crypto/tls.Conn, so Go's
	// net/http auto-h2 server detection may not apply. Follow the existing
	// sing-box v2rayhttp server's HTTP/2 preface handling.
	s.h2cHandler = h2c.NewHandler(s, h2)
	return s, nil
}

// streamOneServerPath reproduces the bare-path rule used by our XHTTP client
// and Xray's GetNormalizedPath: path placement (the default) ends in '/'.
func streamOneServerPath(options option.V2RayXHTTPOptions) string {
	path, _, _ := strings.Cut(options.Path, "?")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if (options.SessionPlacement == "" || options.SessionPlacement == placementPath ||
		options.SeqPlacement == "" || options.SeqPlacement == placementPath) && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Go net/http parses the HTTP/2 preface as PRI * HTTP/2.0 on custom
	// REALITY listeners. h2c.NewHandler upgrades it to a real HTTP/2 session.
	if r.Method == "PRI" && len(r.Header) == 0 && r.URL.Path == "*" && r.Proto == "HTTP/2.0" {
		s.h2cHandler.ServeHTTP(w, r)
		return
	}
	if r.ProtoMajor != 2 {
		http.Error(w, "HTTP/2 required", http.StatusHTTPVersionNotSupported)
		return
	}
	if r.URL.Path != s.path || r.Method != s.method || (s.host != "" && !strings.EqualFold(r.Host, s.host)) {
		http.NotFound(w, r)
		return
	}
	// Xray's default XHTTP padding check happens after path/host matching.
	// With obfs mode disabled the value is in Referer?x_padding=..., or
	// in the request URL query when Referer is absent. Length, not content,
	// is validated: accept existing clients which still send '0' padding.
	// Generate the response padding before validation, as Xray does, so
	// a malformed request on the correct path also receives the header.
	if n := s.padding.rand(); n > 0 {
		w.Header().Set("X-Padding", strings.Repeat("X", n))
	}
	if !validLegacyRequestPadding(r, s.padding) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Cache-Control", "no-store")
	if !s.noSSE {
		w.Header().Set("Content-Type", "text/event-stream")
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Only turn on transport timing when explicitly requested. These timings
	// isolate stalls inside ResponseWriter.Write or Flush (HTTP/2 flow control)
	// from waiting on the client request body or the VLESS outbound.
	if s.slowWriteThreshold > 0 && s.logger != nil {
		wrapped := &xhttpSlowResponseWriter{
			ResponseWriter: w,
			flusher:        flusher,
			threshold:      s.slowWriteThreshold,
			warn: func(op string, took time.Duration) {
				s.logger.WarnContext(r.Context(), "xhttp stream-one slow ", op,
					" elapsed=", took, " remote=", r.RemoteAddr)
			},
		}
		w = wrapped
		flusher = wrapped
	}
	done := make(chan struct{})
	conn := v2rayhttp.NewHTTP2Wrapper(&v2rayhttp.ServerHTTPConn{
		HTTP2Conn: v2rayhttp.NewHTTPConn(r.Body, w),
		Flusher:   flusher,
	})
	// The adapter's close callback signals the end of VLESS routing. An
	// aborted HTTP/2 stream must ALSO release this handler immediately.
	s.handler.NewConnectionEx(r.Context(), conn, badhttp.SourceAddress(r), M.Socksaddr{}, N.OnceClose(func(error) { close(done) }))
	select {
	case <-done:
	case <-r.Context().Done():
	}
	conn.CloseWrapper()
}

// validLegacyRequestPadding mirrors Xray's default non-obfuscated XHTTP
// request-padding extraction and range check. Xray validates length only,
// not the filler content. This also keeps old '0'-padding clients compatible.
func validLegacyRequestPadding(r *http.Request, padding intRange) bool {
	if r == nil {
		return false
	}
	var value string
	if referer := r.Header.Get("Referer"); referer != "" {
		u, err := url.Parse(referer)
		if err != nil {
			return false
		}
		value = u.Query().Get("x_padding")
	} else {
		value = r.URL.Query().Get("x_padding")
	}
	return len(value) > 0 && len(value) >= padding.min && len(value) <= padding.max
}

func (s *Server) Network() []string { return []string{N.NetworkTCP} }

func (s *Server) Serve(listener net.Listener) error {
	if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
		s.tlsConfig.SetNextProtos(append([]string{http2.NextProtoTLS}, s.tlsConfig.NextProtos()...))
	}
	listener = aTLS.NewListener(listener, s.tlsConfig)
	return s.httpServer.Serve(listener)
}

func (s *Server) ServePacket(net.PacketConn) error { return os.ErrInvalid }
func (s *Server) Close() error                     { return common.Close(common.PtrOrNil(s.httpServer)) }

// xhttpSlowResponseWriter wraps only the already-accepted response body.
// It deliberately doesn't buffer, batch, or rewrite frames. Writes and Flush
// retain their exact ordering. It is not a throughput optimization.
type xhttpSlowResponseWriter struct {
	http.ResponseWriter
	flusher   http.Flusher
	threshold time.Duration
	warn      func(string, time.Duration)
}

func (w *xhttpSlowResponseWriter) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := w.ResponseWriter.Write(p)
	if elapsed := time.Since(start); elapsed >= w.threshold && w.warn != nil {
		w.warn("Write", elapsed)
	}
	return n, err
}

func (w *xhttpSlowResponseWriter) Flush() {
	start := time.Now()
	w.flusher.Flush()
	if elapsed := time.Since(start); elapsed >= w.threshold && w.warn != nil {
		w.warn("Flush", elapsed)
	}
}
