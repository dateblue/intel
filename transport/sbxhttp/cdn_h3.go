//go:build with_quic

package sbxhttp

import (
	"context"
	stdtls "crypto/tls"
	"fmt"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/quic-go/http3"
	N "github.com/sagernet/sing/common/network"
)

// Cloudflare terminates HTTP/3 at the edge. The origin's HTTP/2 handler and
// SBXHTTP wire framing are unchanged; H3 affects only client-to-edge traffic.
// A private Transport per logical session caps stream use to 1 GET + 8 POSTs.
func newCDNH3Transport(c *Client) (cdnRoundTripper, func(), error) {
	baseTLS, err := c.config.STDConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("sbxhttp CDN H3: standard TLS config required: %w", err)
	}
	clientTLS := baseTLS.Clone()
	clientTLS.NextProtos = []string{http3.NextProtoH3}
	if clientTLS.MinVersion < stdtls.VersionTLS13 {
		clientTLS.MinVersion = stdtls.VersionTLS13
	}
	quicConfig := &quic.Config{
		HandshakeIdleTimeout: 3 * time.Second,
		MaxIdleTimeout:       60 * time.Second,
		KeepAlivePeriod:      20 * time.Second,
	}
	tr := &http3.Transport{
		TLSClientConfig:    clientTLS,
		QUICConfig:         quicConfig,
		DisableCompression: true,
		Dial: func(ctx context.Context, _ string, _ *stdtls.Config, cfg *quic.Config) (*quic.Conn, error) {
			udp, dialErr := c.rawDialer.DialContext(ctx, N.NetworkUDP, c.serverAddr)
			if dialErr != nil {
				return nil, dialErr
			}
			conn, dialErr := quic.DialEarlyConn(ctx, udp, clientTLS, cfg)
			if dialErr != nil {
				_ = udp.Close()
				return nil, dialErr
			}
			go func() {
				<-conn.Context().Done()
				_ = udp.Close()
			}()
			return conn, nil
		},
	}
	// Preserve authority and SNI as the Cloudflare hostname in the caller.
	return tr, func() { _ = tr.Close() }, nil
}
