//go:build !with_quic

package sbxhttp

import "errors"

func newCDNH3Transport(_ *Client) (cdnRoundTripper, func(), error) {
	return nil, nil, errors.New("sbxhttp CDN H3: rebuild sing-box with -tags with_quic")
}
