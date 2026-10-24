//go:build with_xhttp

package v2rayxhttp

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// The stream-one diagnostic wrapper is opt-in (SING_BOX_XHTTP_SLOW_MS).
// It cannot alter HTTP/2 frame ordering, enforce deadlines, or close a live
// stream. We must not mistake an idle CONNECT tunnel for a dead connection.
// All state used by the observer is either atomic or guarded by mu.
type xhttpStreamProgress struct {
	up, down    atomic.Int64
	lastSuccess atomic.Int64 // UnixNano, only for optional idle diagnostics
}

func (p *xhttpStreamProgress) received(n int) {
	if n > 0 {
		p.up.Add(int64(n))
		p.lastSuccess.Store(time.Now().UnixNano())
	}
}

func (p *xhttpStreamProgress) sent(n int) {
	if n > 0 {
		p.down.Add(int64(n))
		p.lastSuccess.Store(time.Now().UnixNano())
	}
}

type xhttpCountingBody struct {
	io.ReadCloser
	progress *xhttpStreamProgress
}

func (r *xhttpCountingBody) Read(b []byte) (int, error) {
	n, err := r.ReadCloser.Read(b)
	r.progress.received(n)
	return n, err
}

type xhttpSlowResponseWriter struct {
	http.ResponseWriter
	flusher   http.Flusher
	threshold time.Duration
	progress  *xhttpStreamProgress
	warn      func(string, time.Duration)

	mu        sync.Mutex
	operation string
	since     time.Time
	reported  bool
}

func (w *xhttpSlowResponseWriter) begin(op string) {
	w.mu.Lock()
	w.operation = op
	w.since = time.Now()
	w.reported = false
	w.mu.Unlock()
}

func (w *xhttpSlowResponseWriter) end() {
	w.mu.Lock()
	elapsed := time.Since(w.since)
	op := w.operation
	alreadyReported := w.reported
	w.operation = ""
	w.mu.Unlock()
	if w.warn == nil {
		return
	}
	if alreadyReported {
		w.warn(op+" recovered", elapsed)
	} else if elapsed >= w.threshold {
		// Small but slow writes can finish between watchdog ticks.
		w.warn(op+" slow", elapsed)
	}
}

func (w *xhttpSlowResponseWriter) Write(p []byte) (int, error) {
	w.begin("Write")
	n, err := w.ResponseWriter.Write(p)
	if w.progress != nil {
		w.progress.sent(n)
	}
	w.end()
	return n, err
}

// FlushError lets http.ResponseController observe failures without bypassing
// our optional timing diagnostics. Unwrap is used for ordinary capabilities.
func (w *xhttpSlowResponseWriter) FlushError() error {
	w.begin("Flush")
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.end()
	return err
}

func (w *xhttpSlowResponseWriter) Flush() {
	_ = w.FlushError()
}

func (w *xhttpSlowResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// watch reports a blocked HTTP/2 Write/Flush *while it is still blocked*,
// rather than waiting for it to return. It emits at most one warning per
// blocked operation and one recovery notice. It exits with ServeHTTP, not
// merely on context cancellation (cleanup itself can wait for a stuck Write).
// Optional no-progress warnings are informational: legitimate idle tunnels
// also lack traffic, so they MUST NOT automatically trigger reconnection.
func (w *xhttpSlowResponseWriter) watch(finished <-chan struct{}, idleAfter time.Duration) {
	period := w.threshold / 2
	if period < 100*time.Millisecond {
		period = 100 * time.Millisecond
	}
	if period > 5*time.Second {
		period = 5 * time.Second
	}
	if idleAfter > 0 && idleAfter/3 < period {
		period = idleAfter / 3
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	var previouslyIdle bool
	var lastSeenProgress int64
	for {
		select {
		case <-finished:
			return
		case <-ticker.C:
		}
		var op string
		var elapsed time.Duration
		w.mu.Lock()
		if w.operation != "" && !w.reported && time.Since(w.since) >= w.threshold {
			op, elapsed = w.operation, time.Since(w.since)
			w.reported = true
		}
		w.mu.Unlock()
		if op != "" && w.warn != nil {
			w.warn(op+" still blocked", elapsed)
		}
		if idleAfter <= 0 || w.progress == nil || w.warn == nil {
			continue
		}
		// No-progress is only interesting after a real payload previously
		// transferred. No warning on a newly established but idle stream.
		last := w.progress.lastSuccess.Load()
		if last == 0 {
			continue
		}
		if last != lastSeenProgress {
			lastSeenProgress = last
			previouslyIdle = false
		}
		idle := time.Since(time.Unix(0, last))
		if idle >= idleAfter && !previouslyIdle {
			previouslyIdle = true
			w.warn("no progress (may be legitimate idle)", idle)
		}
	}
}
