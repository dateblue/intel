//go:build with_xhttp && !with_quic

package v2rayxhttp

import (
	"errors"
	"net"
)

func (s *Server) serveHTTP3(net.PacketConn) error {
	return errors.New("v2ray-xhttp inbound: HTTP/3 requires the with_quic build tag")
}
