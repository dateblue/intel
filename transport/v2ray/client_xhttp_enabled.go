//go:build with_xhttp

package v2ray

import (
	"context"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayxhttp"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func newXHTTPClient(ctx context.Context, dialer N.Dialer, address M.Socksaddr, options option.V2RayXHTTPOptions, tlsConfig tls.Config) (adapter.V2RayClientTransport, error) {
	return v2rayxhttp.NewClient(ctx, dialer, address, options, tlsConfig)
}
