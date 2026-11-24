package sbxhttp

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestMultiPoolKeepsIPSocketsSeparate(t *testing.T) {
	c := &Client{multiPools: make(map[string]*multiH2Pool)}
	p1 := c.getMultiPool("172.64.229.97:8443")
	p2 := c.getMultiPool("172.64.229.28:8443")
	if p1 == p2 || p1 != c.getMultiPool("172.64.229.97:8443") {
		t.Fatal("CDN H2 pools must be isolated per physical edge IP")
	}
	c.multiResetPool()
	if !p1.closed || !p2.closed || c.getMultiPool("172.64.229.97:8443") == p1 {
		t.Fatal("network reset must close previous pools and permit new dials")
	}
}

func TestMultiPoolClosedAndCanceledWaiters(t *testing.T) {
	p := newMultiH2Pool()
	p.close()
	_, err := p.acquire(context.Background(), func(context.Context) (*multiClientLane, error) {
		t.Fatal("a closed pool must not dial")
		return nil, nil
	})
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed pool: %v", err)
	}

	p = newMultiH2Pool()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err = p.acquire(ctx, func(ctx context.Context) (*multiClientLane, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dial cancellation: %v", err)
	}
	p.close()
}

func TestMultiUploadClientLifecycle(t *testing.T) {
	c := &Client{multiPools: make(map[string]*multiH2Pool)}
	up := c.getMultiUploadTransport()
	if up != c.getMultiUploadTransport() {
		t.Fatal("packet-up H2 transport must be reusable")
	}
	c.multiResetPool()
	if c.getMultiUploadTransport() == up {
		t.Fatal("network reset failed to reset upload transport")
	}
	c.multiResetPool()
}
