//go:build with_quic

package include

import (
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/dns/transport/quic"
	"github.com/sagernet/sing-box/protocol/hysteria2"
	_ "github.com/sagernet/sing-box/protocol/naive/quic"
	"github.com/sagernet/sing-box/protocol/tuic"
)

// Only the client outbounds in use. No Hysteria v1, QUIC server inbounds,
// server-side realm, or V2Ray QUIC transport registration.
func registerQUICOutbounds(registry *outbound.Registry) {
	tuic.RegisterOutbound(registry)
	hysteria2.RegisterOutbound(registry)
}

// DoQ and HTTP/3 DNS remain available.
func registerQUICTransports(registry *dns.TransportRegistry) {
	quic.RegisterTransport(registry)
	quic.RegisterHTTP3Transport(registry)
}
