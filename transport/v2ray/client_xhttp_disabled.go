//go:build !with_xhttp

package v2ray

import (
	"context"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func newXHTTPClient(_ context.Context, _ N.Dialer, _ M.Socksaddr, _ option.V2RayXHTTPOptions, _ tls.Config) (adapter.V2RayClientTransport, error) {
	return nil, E.New("XHTTP client requires build tag with_xhttp")
}
