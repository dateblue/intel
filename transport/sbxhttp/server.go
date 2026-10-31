package sbxhttp

import (
	"context"
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
	ctx        context.Context
	logger     logger.ContextLogger
	handler    adapter.V2RayServerTransportHandler
	tlsConfig  tls.ServerConfig
	path       string
	listener   net.Listener
	closed     atomic.Bool
	mu         sync.Mutex
	rawSockets map[net.Conn]struct{}
	slots      chan struct{}
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
	return &Server{ctx: ctx, logger: log, handler: handler, tlsConfig: config, path: path, rawSockets: make(map[net.Conn]struct{}), slots: make(chan struct{}, maxSockets)}, nil
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
	// REALITY is explicitly authenticated before the HTTP/2 server sees bytes.
	// http.Server.Serve cannot be used here because Reality wraps *tls.Conn:
	// net/http's automatic h2 dispatch relies on the concrete *tls.Conn type.
	conn, err := tls.ServerHandshake(ctx, raw, s.tlsConfig)
	if err != nil {
		s.logger.Debug("sbxhttp: Reality handshake: ", err)
		return
	}
	defer conn.Close()
	if conn.ConnectionState().NegotiatedProtocol != http2.NextProtoTLS {
		s.logger.Debug("sbxhttp: reject non-h2 ALPN")
		return
	}
	h2 := &http2.Server{
		MaxConcurrentStreams:         1, // one logical VLESS stream per physical H2 socket
		MaxUploadBufferPerStream:     4 << 20,
		MaxUploadBufferPerConnection: 4 << 20,
	}
	h2.ServeConn(conn, &http2.ServeConnOpts{
		Context:    ctx,
		Handler:    http.HandlerFunc(s.serveHTTP),
		BaseConfig: &http.Server{ReadHeaderTimeout: C.TCPTimeout, MaxHeaderBytes: http.DefaultMaxHeaderBytes},
	})
}
func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor != 2 || r.Method != http.MethodPost || r.URL.Path != s.path {
		http.Error(w, "not found", http.StatusNotFound)
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
	s.handler.NewConnectionEx(ctx, conn, source, M.Socksaddr{}, N.OnceClose(func(error) { close(done) }))
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
	s.mu.Lock()
	ln := s.listener
	sockets := make([]net.Conn, 0, len(s.rawSockets))
	for sock := range s.rawSockets {
		sockets = append(sockets, sock)
	}
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for _, sock := range sockets {
		_ = sock.Close()
	}
	return nil
}
