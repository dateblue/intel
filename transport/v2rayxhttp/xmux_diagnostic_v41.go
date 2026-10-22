package v2rayxhttp

// Diagnostic-only tracing for v4.1. No transport settings, timeouts, breaker
// decisions or request contexts are changed. This file can be removed together
// with the instrumentation hooks after collecting field evidence.

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

var (
	xmuxDiagPoolSeq    atomic.Uint64
	xmuxDiagStreamSeq  atomic.Uint64
	xmuxDiagRequestSeq atomic.Uint64
)

func (c *xmuxClient) diagEvent(kind, format string, args ...any) {
	if c == nil || c.manager == nil || c.manager.onEvent == nil {
		return
	}
	c.manager.logf("xmux: diag pool_id=%d event=%s %s", c.diagID, kind, fmt.Sprintf(format, args...))
}

// A 5-second pending event observes a stuck RoundTrip; it does NOT cancel it.
type xmuxRoundTripDiagnostic struct {
	client  *xmuxClient
	id      uint64
	method  string
	start   time.Time
	timer   *time.Timer
	pending atomic.Bool
}

func beginXmuxRoundTrip(client *xmuxClient, request *http.Request) *xmuxRoundTripDiagnostic {
	if client == nil || client.manager == nil || client.manager.onEvent == nil {
		return nil
	}
	d := &xmuxRoundTripDiagnostic{
		client: client,
		id:     xmuxDiagRequestSeq.Add(1),
		method: request.Method,
		start:  time.Now(),
	}
	d.timer = time.AfterFunc(5*time.Second, func() {
		d.pending.Store(true)
		client.diagEvent("roundtrip-pending", "req_id=%d method=%s elapsed=%s", d.id, d.method, time.Since(d.start).Truncate(time.Millisecond))
	})
	return d
}

func (d *xmuxRoundTripDiagnostic) finish(response *http.Response, err error) {
	if d == nil {
		return
	}
	d.timer.Stop()
	elapsed := time.Since(d.start)
	status := 0
	if response != nil {
		status = response.StatusCode
	}
	if err != nil || status != http.StatusOK || elapsed >= time.Second || d.pending.Load() {
		d.client.diagEvent("roundtrip-end", "req_id=%d method=%s elapsed=%s status=%d err=%v failures=%d", d.id, d.method,
			elapsed.Truncate(time.Millisecond), status, err, d.client.consecFails.Load())
	}
}

// One diagnostic record represents one VLESS stream assigned to an XMUX pool
// resource. The watcher reports streams still open after 10s and every 30s.
// It observes progress but never closes or cancels a stream.
type xmuxStreamDiagnostic struct {
	client       *xmuxClient
	id           uint64
	start        time.Time
	firstRead    atomic.Bool
	bytesRead    atomic.Int64
	lastReadNano atomic.Int64
	done         chan struct{}
	timer        *time.Timer
}

func beginXmuxStream(client *xmuxClient) *xmuxStreamDiagnostic {
	if client == nil || client.manager == nil || client.manager.onEvent == nil {
		return nil
	}
	d := &xmuxStreamDiagnostic{
		client: client,
		id:     xmuxDiagStreamSeq.Add(1),
		start:  time.Now(),
		done:   make(chan struct{}),
	}
	client.diagEvent("stream-open", "stream_id=%d active=%d failures=%d", d.id, client.getOpenUsage(), client.consecFails.Load())
	d.timer = time.AfterFunc(10*time.Second, func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-d.done:
				return
			default:
			}
			d.reportPending()
			select {
			case <-d.done:
				return
			case <-ticker.C:
			}
		}
	})
	return d
}

func (d *xmuxStreamDiagnostic) reportPending() {
	if d == nil {
		return
	}
	age := "never"
	if last := d.lastReadNano.Load(); last != 0 {
		age = time.Since(time.Unix(0, last)).Truncate(time.Millisecond).String()
	}
	d.client.diagEvent("stream-pending", "stream_id=%d elapsed=%s first_read=%t bytes_read=%d last_read_ago=%s",
		d.id, time.Since(d.start).Truncate(time.Millisecond), d.firstRead.Load(), d.bytesRead.Load(), age)
}

func (d *xmuxStreamDiagnostic) finish() {
	if d == nil {
		return
	}
	close(d.done) // caller owns exactly-once release via xmuxRelease.once
	d.timer.Stop()
	elapsed := time.Since(d.start)
	if elapsed >= time.Second || !d.firstRead.Load() {
		d.client.diagEvent("stream-close", "stream_id=%d elapsed=%s first_read=%t bytes_read=%d", d.id,
			elapsed.Truncate(time.Millisecond), d.firstRead.Load(), d.bytesRead.Load())
	}
}

func (r *xmuxRelease) diagRead(n int, err error, localClosed bool) {
	if r == nil || r.diag == nil {
		return
	}
	d := r.diag
	if n > 0 {
		d.bytesRead.Add(int64(n))
		d.lastReadNano.Store(time.Now().UnixNano())
		if d.firstRead.CompareAndSwap(false, true) {
			d.client.diagEvent("stream-first-read", "stream_id=%d elapsed=%s bytes=%d", d.id,
				time.Since(d.start).Truncate(time.Millisecond), n)
		}
	}
	if err != nil && err != io.EOF {
		d.client.diagEvent("stream-read-error", "stream_id=%d elapsed=%s local_closed=%t err=%v", d.id,
			time.Since(d.start).Truncate(time.Millisecond), localClosed, err)
	}
}
