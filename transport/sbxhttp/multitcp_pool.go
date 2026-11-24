package sbxhttp

// CDN-only, deliberately limited H2 socket reuse. This is not Xray XMUX:
// no proxy-level multiplexing, no sharing of logical VLESS streams, and no
// change to the private MultiTCP wire format. Each distinct CF IP has a
// separate pool, preserving independent TCP congestion control across IPs.
// At most two simultaneous downlink GET streams share a physical H2 socket.

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/sagernet/sing-box/common/tls"
	"golang.org/x/net/http2"
)

const (
	multiPoolMaxStreams  = 2
	multiPoolMaxPhysical = 24
	multiPoolKeepIdle    = 2
	multiPoolIdleTimeout = 45 * time.Second
)

type multiH2Entry struct {
	lane      *multiClientLane // physical owner, never attached to a business stream
	active    int
	idleSince time.Time
	retired   bool
}

type multiH2Pool struct {
	mu       sync.Mutex
	entries  []*multiH2Entry
	dialling bool
	closed   bool
	changed  chan struct{}
}

func newMultiH2Pool() *multiH2Pool {
	return &multiH2Pool{changed: make(chan struct{})}
}

func (p *multiH2Pool) wakeLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *multiH2Pool) pruneLocked() {
	now := time.Now()
	retained := p.entries[:0]
	for _, e := range p.entries {
		if e.active == 0 && (e.retired || !e.lane.h2.CanTakeNewRequest() || now.Sub(e.idleSince) > multiPoolIdleTimeout) {
			// No active streams. Safe to tear down a broken/expired socket.
			e.lane.close()
			continue
		}
		retained = append(retained, e)
	}
	p.entries = retained
}

func (p *multiH2Pool) acquire(ctx context.Context, dial func(context.Context) (*multiClientLane, error)) (*multiClientLane, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, net.ErrClosed
		}
		p.pruneLocked()
		for _, e := range p.entries {
			if !e.retired && e.active < multiPoolMaxStreams && e.lane.h2.CanTakeNewRequest() {
				e.active++
				lane := &multiClientLane{h2: e.lane.h2, physical: e.lane.physical, reused: true}
				lane.release = func() { p.release(e) }
				lane.retire = func() { p.retire(e) }
				p.mu.Unlock()
				return lane, nil
			}
		}
		if !p.dialling && len(p.entries) < multiPoolMaxPhysical {
			// Single in-progress TLS handshake per CF IP avoids the 4*N
			// simultaneous dial storm seen in the v0.3 logs. Different IP
			// pools still connect concurrently.
			p.dialling = true
			p.mu.Unlock()
			fresh, err := dial(ctx)
			p.mu.Lock()
			p.dialling = false
			if err != nil || p.closed {
				p.wakeLocked()
				p.mu.Unlock()
				if fresh != nil {
					fresh.close()
				}
				if err != nil {
					return nil, err
				}
				return nil, net.ErrClosed
			}
			e := &multiH2Entry{lane: fresh, active: 1}
			p.entries = append(p.entries, e)
			lane := &multiClientLane{h2: fresh.h2, physical: fresh.physical}
			lane.release = func() { p.release(e) }
			lane.retire = func() { p.retire(e) }
			p.wakeLocked()
			p.mu.Unlock()
			return lane, nil
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// A failed GET stream retires its physical socket *after* other users finish.
// It must not abruptly reset unrelated VLESS sessions that share this H2 TCP.
func (p *multiH2Pool) retire(e *multiH2Entry) {
	p.mu.Lock()
	e.retired = true
	p.wakeLocked()
	p.mu.Unlock()
}

func (p *multiH2Pool) release(e *multiH2Entry) {
	p.mu.Lock()
	if e.active <= 0 { // defensive: lease closure is idempotent
		p.mu.Unlock()
		return
	}
	e.active--
	e.idleSince = time.Now()
	if e.active == 0 {
		idle := 0
		for _, it := range p.entries {
			if it.active == 0 {
				idle++
			}
		}
		if p.closed || e.retired || !e.lane.h2.CanTakeNewRequest() || idle > multiPoolKeepIdle {
			for i, it := range p.entries {
				if it == e {
					p.entries = append(p.entries[:i], p.entries[i+1:]...)
					break
				}
			}
			e.lane.close()
		}
	}
	p.wakeLocked()
	p.mu.Unlock()
}

func (p *multiH2Pool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	for _, e := range p.entries {
		e.lane.close()
	}
	p.entries = nil
	p.wakeLocked()
	p.mu.Unlock()
}

func (c *Client) getMultiPool(target string) *multiH2Pool {
	c.multiPoolMu.Lock()
	defer c.multiPoolMu.Unlock()
	pool := c.multiPools[target]
	if pool == nil {
		pool = newMultiH2Pool()
		c.multiPools[target] = pool
	}
	return pool
}

// Shared *only* for small packet-up POST requests. Dedicated H2 transport
// means the GET downlink TCP flow-control and packet-up are independent.
// HTTP/2's built-in client manages a single connection per origin normally;
// only short upstream POST requests are multiplexed here.
func (c *Client) getMultiUploadTransport() *http2.Transport {
	c.multiUploadMu.Lock()
	defer c.multiUploadMu.Unlock()
	if c.multiUpload == nil {
		c.multiUpload = &http2.Transport{
			DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.STDConfig) (net.Conn, error) {
				// The TLS dialer retains the configured SNI/ALPN independently
				// of which CF IP is chosen for the raw TCP socket.
				index := int(c.multiUploadDials.Add(1) - 1)
				target := c.multiDialTarget(index)
				raw, err := c.tlsDialer.DialTLSContext(ctx, target)
				if err != nil {
					return nil, err
				}
				if raw.ConnectionState().NegotiatedProtocol != http2.NextProtoTLS {
					_ = raw.Close()
					return nil, errors.New("sbxhttp multitcp: packet-up requires h2")
				}
				if c.logger != nil {
					c.logger.Debug("sbxhttp MultiTCP uplink H2 physical local=", raw.LocalAddr(), " remote=", raw.RemoteAddr())
				}
				return raw, nil
			},
			ReadIdleTimeout:  h2ReadIdleTimeout,
			PingTimeout:      h2PingTimeout,
			WriteByteTimeout: h2WriteByteTimeout,
		}
	}
	return c.multiUpload
}

func (c *Client) multiResetPool() {
	c.multiFourBackoff.Store(0)
	c.multiPoolMu.Lock()
	old := c.multiPools
	c.multiPools = make(map[string]*multiH2Pool)
	c.multiPoolMu.Unlock()
	for _, p := range old {
		p.close()
	}
	c.multiUploadMu.Lock()
	oldUpload := c.multiUpload
	c.multiUpload = nil
	c.multiUploadMu.Unlock()
	if oldUpload != nil {
		oldUpload.CloseIdleConnections()
	}
}
