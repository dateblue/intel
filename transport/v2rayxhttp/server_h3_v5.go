//go:build with_xhttp && with_quic

package v2rayxhttp

import (
	"net"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
	qtls "github.com/sagernet/sing-quic"
)

// HTTP/3 is only negotiated when tls.alpn is exactly ["h3"]. REALITY usually
// uses HTTP/2; h3 requires a compatible QUIC-capable TLS server config.
func (s *Server) serveHTTP3(udp net.PacketConn) error {
	listener, err := qtls.ListenEarly(udp, s.tlsConfig, &quic.Config{
		MaxIdleTimeout:     300 * time.Second,
		MaxIncomingStreams: 1 << 60,
		DisablePathManager: true,
	})
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http3.Server{Handler: s}
	s.mu.Lock()
	s.h3Closer = server
	s.mu.Unlock()
	return server.ServeListener(listener)
}
