package sbxhttp

import (
	"context"
	stdtls "crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/net/http2"
)

var _ adapter.V2RayServerTransport = (*Server)(nil)

const maxSockets = 512 // explicit bound, not an autoscaling / XMUX algorithm

type Server struct {
	ctx              context.Context
	logger           logger.ContextLogger
	handler          adapter.V2RayServerTransportHandler
	tlsConfig        tls.ServerConfig
	path             string
	listener         net.Listener
	closed           atomic.Bool
	mu               sync.Mutex
	rawSockets       map[net.Conn]struct{}
	reliable         bool
	visionInnerTLS   bool
	privateVisionTLS *stdtls.Config
	reliableSessions map[string]*reliableSession
	reliableStop     chan struct{}
	slots            chan struct{}
}

func NewServer(ctx context.Context, log logger.ContextLogger, options option.V2RaySBXHTTPOptions, config tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (adapter.V2RayServerTransport, error) {
	if config == nil {
		return nil, errors.New("sbxhttp: Reality TLS is required")
	}
	path, err := cleanPath(options.Path)
	if err != nil {
		return nil, err
	}
	config.SetNextProtos([]string{http2.NextProtoTLS})
	if options.Reliable {
		log.Info("sbxhttp: server initialized mode=Replay-v2 path=", path, " required_version=", reliableVersion)
	} else {
		log.Info("sbxhttp: server initialized mode=legacy-v1 path=", path)
	}
	var visionConfig *stdtls.Config
	if options.VisionInnerTLS {
		visionConfig, err = newPrivateVisionServerTLS()
		if err != nil {
			return nil, err
		}
	}
	return &Server{ctx: ctx, logger: log, handler: handler, tlsConfig: config, path: path, rawSockets: make(map[net.Conn]struct{}), slots: make(chan struct{}, maxSockets), reliable: options.Reliable, visionInnerTLS: options.VisionInnerTLS, privateVisionTLS: visionConfig, reliableSessions: make(map[string]*reliableSession), reliableStop: make(chan struct{})}, nil
}
func (s *Server) Network() []string                { return []string{N.NetworkTCP} }
func (s *Server) ServePacket(net.PacketConn) error { return os.ErrInvalid }

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return net.ErrClosed
	}
	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("sbxhttp: server already running")
	}
	s.listener = ln
	s.mu.Unlock()
	if s.reliable {
		go s.sweepReliable()
	}
	for {
		raw, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			return err
		}
		select {
		case s.slots <- struct{}{}:
			s.mu.Lock()
			if s.closed.Load() {
				s.mu.Unlock()
				<-s.slots
				raw.Close()
				return nil
			}
			s.rawSockets[raw] = struct{}{}
			s.mu.Unlock()
			go s.serveOne(raw)
		default:
			_ = raw.Close() // backpressure: bounded concurrent socket count
		}
	}
}
func (s *Server) serveOne(raw net.Conn) {
	defer func() { s.mu.Lock(); delete(s.rawSockets, raw); s.mu.Unlock(); <-s.slots; _ = raw.Close() }()
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	// A stalled/half-open TCP peer must not occupy one of the bounded socket
	// slots indefinitely before completing the Reality/TLS handshake.
	_ = raw.SetDeadline(time.Now().Add(C.TCPTimeout))
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, C.TCPTimeout)
	// REALITY is explicitly authenticated before the HTTP/2 server sees bytes.
	// http.Server.Serve cannot be used here because Reality wraps *tls.Conn:
	// net/http's automatic h2 dispatch relies on the concrete *tls.Conn type.
	conn, err := tls.ServerHandshake(handshakeCtx, raw, s.tlsConfig)
	cancelHandshake()
	if err != nil {
		s.logger.Debug("sbxhttp: Reality handshake: ", err)
		return
	}
	defer conn.Close()
	_ = raw.SetDeadline(time.Time{})
	// A verified private Reality connection may report an empty negotiated
	// ALPN with some uTLS/Reality combinations. In this SB-to-SB transport,
	// both peers explicitly speak HTTP/2, so an absent ALPN is permitted.
	// A positive negotiation of another protocol must still be rejected.
	negotiated := conn.ConnectionState().NegotiatedProtocol
	if !privateH2ALPNAllowed(negotiated) {
		s.logger.Warn("sbxhttp: reject negotiated ALPN ", negotiated, " (expected h2)")
		return
	}
	if negotiated == "" {
		s.logger.Debug("sbxhttp: Reality handshake verified; ALPN absent, using private HTTP/2 framing")
	}
	h2 := newH2Server()
	h2.ServeConn(conn, &http2.ServeConnOpts{
		Context:    ctx,
		Handler:    http.HandlerFunc(s.serveHTTP),
		BaseConfig: &http.Server{ReadHeaderTimeout: C.TCPTimeout, MaxHeaderBytes: http.DefaultMaxHeaderBytes},
	})
}
func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor != 2 || r.Method != http.MethodPost || r.URL.Path != s.path {
		s.logger.Debug("sbxhttp: reject HTTP request remote=", r.RemoteAddr, " proto=", r.Proto, " method=", r.Method, " path=", r.URL.Path)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if s.reliable {
		s.reliableAttach(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := http.NewResponseController(w).Flush(); err != nil {
		s.logger.Debug("sbxhttp: initial Flush: ", err)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	conn := &serverConn{
		reader: r.Body, writer: w,
		flush:   func() error { return http.NewResponseController(w).Flush() },
		closeFn: cancel,
		report: func(stage string, elapsed time.Duration, transferred int) {
			// A Write/Flush was actually pending, unlike a quiet stream.
			// Do not automatically kill it: a slow recipient may be valid.
			s.logger.Debug("sbxhttp: slow-", stage, " remote=", r.RemoteAddr, " duration=", elapsed, " bytes=", transferred)
		},
	}
	done := make(chan struct{})
	source := M.ParseSocksaddr(r.RemoteAddr)
	var app net.Conn = conn
	if s.visionInnerTLS {
		wrapped, wrapErr := privateVisionServerTLS(ctx, conn, s.privateVisionTLS)
		if wrapErr != nil {
			s.logger.Debug("sbxhttp: private Vision inner TLS handshake: ", wrapErr)
			return
		}
		app = wrapped
	}
	s.handler.NewConnectionEx(ctx, app, source, M.Socksaddr{}, N.OnceClose(func(error) { close(done) }))
	select {
	case <-done:
	case <-ctx.Done():
	}
	// The handler may have ended while a Write was blocked. Never wait on
	// its write mutex: the request context termination releases H2 resources.
	_ = conn.Close()
	s.logger.Debug("sbxhttp: session-end remote=", r.RemoteAddr, " upstream_bytes=", conn.bytesIn.Load(), " downstream_bytes=", conn.bytesOut.Load(), " error=", ctx.Err())
}
func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.reliableStop != nil {
		close(s.reliableStop)
	}
	s.mu.Lock()
	ln := s.listener
	sockets := make([]net.Conn, 0, len(s.rawSockets))
	for sock := range s.rawSockets {
		sockets = append(sockets, sock)
	}
	reliable := make([]*reliableSession, 0, len(s.reliableSessions))
	for _, sess := range s.reliableSessions {
		reliable = append(reliable, sess)
	}
	s.mu.Unlock()
	for _, sess := range reliable {
		sess.Close()
	}
	if ln != nil {
		_ = ln.Close()
	}
	for _, sock := range sockets {
		_ = sock.Close()
	}
	return nil
}
