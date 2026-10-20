//go:build with_quic

package include

import (
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/dns/transport/quic"
	"github.com/sagernet/sing-box/protocol/hysteria2"
	_ "github.com/sagernet/sing-box/protocol/naive/quic"
	"github.com/sagernet/sing-box/protocol/tuic"
)

// Keep TUIC v5 server support for the Oracle ARM64 VPS.
func registerQUICInbounds(registry *inbound.Registry) {
	tuic.RegisterInbound(registry)
}

// Only the outbounds in use: TUIC v5 and Hysteria2.
// No Hysteria v1 or V2Ray QUIC transport registration.
func registerQUICOutbounds(registry *outbound.Registry) {
	tuic.RegisterOutbound(registry)
	hysteria2.RegisterOutbound(registry)
}

// DoQ and HTTP/3 DNS remain available.
func registerQUICTransports(registry *dns.TransportRegistry) {
	quic.RegisterTransport(registry)
	quic.RegisterHTTP3Transport(registry)
}
