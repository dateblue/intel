//go:build with_quic

package include

import (
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/dns/transport/quic"
	"github.com/sagernet/sing-box/protocol/hysteria2"
	_ "github.com/sagernet/sing-box/protocol/naive/quic"
	"github.com/sagernet/sing-box/protocol/tuic"
)

// Server inbounds for the user's Oracle VPS nodes.
func registerQUICInbounds(registry *inbound.Registry) {
	hysteria2.RegisterInbound(registry)
	tuic.RegisterInbound(registry)
}

// Client outbounds on all supported platforms.
func registerQUICOutbounds(registry *outbound.Registry) {
	hysteria2.RegisterOutbound(registry)
	tuic.RegisterOutbound(registry)
}

// QUIC and HTTP/3 DNS transports are separate from proxy server inbounds.
func registerQUICTransports(registry *dns.TransportRegistry) {
	quic.RegisterTransport(registry)
	quic.RegisterHTTP3Transport(registry)
}

// Preserve HY2 realm service registration from the original source tree.
func registerQUICServices(registry *service.Registry) {
	hysteria2.RegisterRealmService(registry)
}
