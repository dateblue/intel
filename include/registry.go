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
	"github.com/sagernet/sing-box/protocol/naive"
	"github.com/sagernet/sing-box/protocol/redirect"
	"github.com/sagernet/sing-box/protocol/tun"
	"github.com/sagernet/sing-box/protocol/vless"
	"github.com/sagernet/sing-box/service/api"
	originca "github.com/sagernet/sing-box/service/origin_ca"
	"github.com/sagernet/sing-box/service/resolved"
)

// Registries for the requested proxy protocols, client and server where supported.
// XHTTP is currently a client-only VLESS transport; REALITY server TLS is
// available via the VLESS inbound. Keep platform TUN/TProxy, DNS and APIs.
func Context(ctx context.Context) context.Context {
	return box.Context(ctx, InboundRegistry(), OutboundRegistry(), EndpointRegistry(), DNSTransportRegistry(), ServiceRegistry(), CertificateProviderRegistry())
}

func InboundRegistry() *inbound.Registry {
	registry := inbound.NewRegistry()

	// Local transparent proxy (Windows/Android VPN and ImmortalWrt).
	tun.RegisterInbound(registry)
	redirect.RegisterRedirect(registry)
	redirect.RegisterTProxy(registry)
	direct.RegisterInbound(registry)

	// Protocol server inbounds. VLESS is the REALITY server entry point.
	vless.RegisterInbound(registry)
	naive.RegisterInbound(registry)
	registerQUICInbounds(registry) // Hysteria2 and TUIC v5.

	return registry
}

func OutboundRegistry() *outbound.Registry {
	registry := outbound.NewRegistry()

	// Base routes/groups, required by configs and platform controllers.
	direct.RegisterOutbound(registry)
	block.RegisterOutbound(registry)
	group.RegisterSelector(registry)
	group.RegisterURLTest(registry)

	// VLESS carries REALITY/Vision and XHTTP+REALITY.
	vless.RegisterOutbound(registry)
	registerNaiveOutbound(registry)
	registerQUICOutbounds(registry) // Hysteria2 and TUIC v5.

	return registry
}

func EndpointRegistry() *endpoint.Registry {
	// No unused WireGuard/OpenVPN/OpenConnect/MASQUE/Tailscale endpoints.
	return endpoint.NewRegistry()
}

func DNSTransportRegistry() *dns.TransportRegistry {
	registry := dns.NewTransportRegistry()

	transport.RegisterTCP(registry)
	transport.RegisterUDP(registry)
	transport.RegisterTLS(registry)
	transport.RegisterHTTPS(registry)
	hosts.RegisterTransport(registry)
	local.RegisterTransport(registry)
	mdns.RegisterTransport(registry)
	fakeip.RegisterTransport(registry)
	resolved.RegisterTransport(registry)

	registerQUICTransports(registry) // DoQ and HTTP/3 DNS.
	// DHCP DNS discovery deliberately excluded (no with_dhcp tag).

	return registry
}

func ServiceRegistry() *service.Registry {
	registry := service.NewRegistry()

	api.RegisterService(registry)
	resolved.RegisterService(registry)
	registerQUICServices(registry) // Hysteria2 realm (server feature).
	registerOOMKillerService(registry)

	return registry
}

func CertificateProviderRegistry() *certificate.Registry {
	registry := certificate.NewRegistry()

	registerACMECertificateProvider(registry)
	originca.RegisterCertificateProvider(registry)

	return registry
}
