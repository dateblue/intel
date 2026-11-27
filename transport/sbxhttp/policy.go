package sbxhttp

import (
	"context"
	"sync"
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

// startHandshakeDeadline bounds the time spent waiting for initial HTTP
// response headers. Calling stop after a successful response is essential:
// the established HTTP/2 stream may remain open for hours and must not be
// canceled merely because its connection-establishment timer has elapsed.
func startHandshakeDeadline(cancel context.CancelFunc, timeout time.Duration) func() {
	var access sync.Mutex
	waiting := true
	timer := time.AfterFunc(timeout, func() {
		access.Lock()
		defer access.Unlock()
		if waiting {
			cancel()
		}
	})
	return func() {
		access.Lock()
		waiting = false
		access.Unlock()
		timer.Stop()
	}
}
