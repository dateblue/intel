//go:build with_xhttp

package v2rayxhttp

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"

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
	headerBytes := options.ServerMaxHeaderBytes
	if headerBytes <= 0 {
		headerBytes = http.DefaultMaxHeaderBytes
	}
	s := &Server{tlsConfig: tlsConfig, handler: handler, logger: logger, path: path, method: method, host: options.Host, noSSE: options.NoSSEHeader, padding: padding, h2Server: h2}
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
	// Xray uses an X-Padding response header by default; it is header-only
	// noise and must never enter the VLESS data stream.
	if n := s.padding.rand(); n > 0 {
		w.Header().Set("X-Padding", strings.Repeat("0", n))
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

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
