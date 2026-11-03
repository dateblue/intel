package sbxhttp

import (
	"context"
	"errors"
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"
)

func TestBoundedHandshakeTimeout(t *testing.T) {
	if timeout, err := boundedHandshakeTimeout(context.Background()); err != nil || timeout != C.TCPTimeout {
		t.Fatalf("no deadline: timeout=%v err=%v", timeout, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	timeout, err := boundedHandshakeTimeout(ctx)
	if err != nil || timeout <= 0 || timeout > 250*time.Millisecond {
		t.Fatalf("short deadline ignored: timeout=%v err=%v", timeout, err)
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err = boundedHandshakeTimeout(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context: %v", err)
	}
	deadline, finish := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer finish()
	if _, err = boundedHandshakeTimeout(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired deadline: %v", err)
	}
}

func TestHTTP2ServerHealthPolicy(t *testing.T) {
	h2 := newH2Server()
	if h2.MaxConcurrentStreams != 1 || h2.ReadIdleTimeout != h2ReadIdleTimeout || h2.PingTimeout != h2PingTimeout {
		t.Fatalf("unexpected server HTTP2 health parameters: %+v", h2)
	}
	if h2.MaxUploadBufferPerStream != 4<<20 || h2.MaxUploadBufferPerConnection != 4<<20 {
		t.Fatal("flow control windows regressed")
	}
}
