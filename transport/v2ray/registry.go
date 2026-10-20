package v2ray

// lx: extensible client/server transport registries.
//
// Upstream selects the client transport with a hardcoded switch in
// NewClientTransport (transport.go). sing-box-lx replaces that switch with this
// registry so downstream transports (e.g. XHTTP) register themselves via init()
// behind a build tag, keeping the recurring diff to upstream files near zero.
// The built-in transports below reproduce upstream's switch behavior exactly,
// including the QUIC TLS-required check. See SPECS/TASKS/002.

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	"github.com/sagernet/sing-box/transport/v2rayhttpupgrade"
	"github.com/sagernet/sing-box/transport/v2raywebsocket"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// ServerTransportConstructor builds one inbound transport implementation.
// It is deliberately independent of client registration so experimental
// transports can add a server without changing the built-in switch.
type ServerTransportConstructor func(ctx context.Context, logger logger.ContextLogger, options option.V2RayTransportOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (adapter.V2RayServerTransport, error)

var serverTransportRegistry = make(map[string]ServerTransportConstructor)

// RegisterServer registers an inbound transport constructor during init.
func RegisterServer(transportType string, constructor ServerTransportConstructor) {
	serverTransportRegistry[transportType] = constructor
}

func lookupServerTransport(transportType string) (ServerTransportConstructor, bool) {
	constructor, loaded := serverTransportRegistry[transportType]
	return constructor, loaded
}

// ClientTransportConstructor builds a client transport from the full
// V2RayTransportOptions (each transport reads its own sub-options field).
type ClientTransportConstructor func(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayTransportOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error)

var clientTransportRegistry = make(map[string]ClientTransportConstructor)

// RegisterClient registers a client transport constructor for the given type.
// Built-in types are registered below; downstream types (xhttp) register from
// their own package under a build tag.
func RegisterClient(transportType string, constructor ClientTransportConstructor) {
	clientTransportRegistry[transportType] = constructor
}

// lookupClientTransport returns the constructor for a transport type, if any.
func lookupClientTransport(transportType string) (ClientTransportConstructor, bool) {
	constructor, loaded := clientTransportRegistry[transportType]
	return constructor, loaded
}

func init() {
	RegisterClient(C.V2RayTransportTypeHTTP, func(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayTransportOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
		return v2rayhttp.NewClient(ctx, dialer, serverAddr, options.HTTPOptions, tlsConfig)
	})
	RegisterClient(C.V2RayTransportTypeGRPC, func(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayTransportOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
		return NewGRPCClient(ctx, dialer, serverAddr, options.GRPCOptions, tlsConfig)
	})
	RegisterClient(C.V2RayTransportTypeWebsocket, func(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayTransportOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
		return v2raywebsocket.NewClient(ctx, dialer, serverAddr, options.WebsocketOptions, tlsConfig)
	})
	RegisterClient(C.V2RayTransportTypeQUIC, func(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayTransportOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
		if tlsConfig == nil {
			return nil, C.ErrTLSRequired
		}
		return NewQUICClient(ctx, dialer, serverAddr, options.QUICOptions, tlsConfig)
	})
	RegisterClient(C.V2RayTransportTypeHTTPUpgrade, func(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayTransportOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
		return v2rayhttpupgrade.NewClient(ctx, dialer, serverAddr, options.HTTPUpgradeOptions, tlsConfig)
	})
}
