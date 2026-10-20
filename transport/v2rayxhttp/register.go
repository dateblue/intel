//go:build with_xhttp

package v2rayxhttp

import (
	"context"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2ray"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// init registers both XHTTP transport directions. The new server supports
// only HTTP/2 stream-one; client modes are unchanged. Both registrations
// are gated by the existing with_xhttp build tag.
func init() {
	v2ray.RegisterClient(C.V2RayTransportTypeXHTTP, func(ctx context.Context, dialer N.Dialer, serverAddr M.Socksaddr, options option.V2RayTransportOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
		return NewClient(ctx, dialer, serverAddr, options.XHTTPOptions, tlsConfig)
	})
	v2ray.RegisterServer(C.V2RayTransportTypeXHTTP, func(ctx context.Context, logger logger.ContextLogger, options option.V2RayTransportOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (adapter.V2RayServerTransport, error) {
		return NewServer(ctx, logger, options.XHTTPOptions, tlsConfig, handler)
	})
}
