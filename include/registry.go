package include

import (
	"context"

	"github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/dns/transport"
	"github.com/sagernet/sing-box/dns/transport/fakeip"
	"github.com/sagernet/sing-box/dns/transport/hosts"
	"github.com/sagernet/sing-box/dns/transport/local"
	"github.com/sagernet/sing-box/dns/transport/mdns"
	"github.com/sagernet/sing-box/protocol/block"
	"github.com/sagernet/sing-box/protocol/direct"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing-box/protocol/redirect"
	"github.com/sagernet/sing-box/protocol/tun"
	"github.com/sagernet/sing-box/protocol/vless"
	"github.com/sagernet/sing-box/service/api"
	originca "github.com/sagernet/sing-box/service/origin_ca"
	"github.com/sagernet/sing-box/service/resolved"
)

// Context provides the required registries for the user's five transports:
// VLESS with XHTTP/REALITY, Naive, Hysteria2 and TUIC v5.
// The management, DNS and local transparent-proxy building blocks are retained.
func Context(ctx context.Context) context.Context {
	return box.Context(ctx, InboundRegistry(), OutboundRegistry(), EndpointRegistry(), DNSTransportRegistry(), ServiceRegistry(), CertificateProviderRegistry())
}

func InboundRegistry() *inbound.Registry {
	registry := inbound.NewRegistry()

	// Local transparent-proxy entry points, plus TUIC v5 server on Oracle ARM64 VPS.
	tun.RegisterInbound(registry)
	redirect.RegisterRedirect(registry)
	redirect.RegisterTProxy(registry)
	direct.RegisterInbound(registry)
	registerQUICInbounds(registry) // TUIC v5 server

	return registry
}

func OutboundRegistry() *outbound.Registry {
	registry := outbound.NewRegistry()

	// Routing and outbound groups required by existing user configs.
	direct.RegisterOutbound(registry)
	block.RegisterOutbound(registry)
	group.RegisterSelector(registry)
	group.RegisterURLTest(registry)

	// VLESS carries both XHTTP+REALITY and REALITY/Vision.
	vless.RegisterOutbound(registry)
	registerNaiveOutbound(registry)
	registerQUICOutbounds(registry) // Hysteria2 and TUIC v5 only.

	return registry
}

func EndpointRegistry() *endpoint.Registry {
	// Client-only build: WireGuard/OpenVPN/OpenConnect/MASQUE/Tailscale
	// endpoints are intentionally unavailable.
	return endpoint.NewRegistry()
}

func DNSTransportRegistry() *dns.TransportRegistry {
	registry := dns.NewTransportRegistry()

	// Preserve standard DNS transports for Windows/Android/ImmortalWrt.
	transport.RegisterTCP(registry)
	transport.RegisterUDP(registry)
	transport.RegisterTLS(registry)
	transport.RegisterHTTPS(registry)
	hosts.RegisterTransport(registry)
	local.RegisterTransport(registry)
	mdns.RegisterTransport(registry)
	fakeip.RegisterTransport(registry)
	resolved.RegisterTransport(registry)

	// DoQ / HTTP3 DNS remain available for the Wrt configuration.
	registerQUICTransports(registry)
	// No registerDHCPTransport: with_dhcp is deliberately excluded.

	return registry
}

func ServiceRegistry() *service.Registry {
	registry := service.NewRegistry()

	// Keep platform-management services. No Shadowsocks manager or
	// server-side Hysteria realm is registered.
	api.RegisterService(registry)
	resolved.RegisterService(registry)
	registerOOMKillerService(registry)

	return registry
}

func CertificateProviderRegistry() *certificate.Registry {
	registry := certificate.NewRegistry()

	registerACMECertificateProvider(registry)
	originca.RegisterCertificateProvider(registry)

	return registry
}
