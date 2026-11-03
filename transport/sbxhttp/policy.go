package sbxhttp

import (
	"context"
	"time"

	C "github.com/sagernet/sing-box/constant"
	"golang.org/x/net/http2"
)

// Health checks are protocol-level PINGs, not application idle limits. They
// keep a quiet connection alive through NAT and detect a blackholed TCP path.
// A healthy, temporarily idle stream is NOT closed by these settings.
const (
	h2ReadIdleTimeout  = 45 * time.Second
	h2PingTimeout      = 20 * time.Second
	h2WriteByteTimeout = 75 * time.Second
)

func boundedHandshakeTimeout(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	timeout := C.TCPTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, context.DeadlineExceeded
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	return timeout, nil
}

func newH2Server() *http2.Server {
	return &http2.Server{
		MaxConcurrentStreams:         1, // one VLESS session per physical HTTP/2 socket
		MaxUploadBufferPerStream:     4 << 20,
		MaxUploadBufferPerConnection: 4 << 20,
		ReadIdleTimeout:              h2ReadIdleTimeout,
		PingTimeout:                  h2PingTimeout,
	}
}
