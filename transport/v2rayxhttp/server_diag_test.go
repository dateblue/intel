//go:build with_xhttp

package v2rayxhttp

import (
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockingXHTTPWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingXHTTPWriter) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.ResponseRecorder.Write(p)
}

// A Write that never returns must generate an early warning, not just a
// misleading slow-write-after-return warning. Release it to verify cleanup.
func TestXHTTPWatchdogWarnsWhileWriteBlocked(t *testing.T) {
	b := &blockingXHTTPWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	warnings := make(chan string, 8)
	w := &xhttpSlowResponseWriter{
		ResponseWriter: b, flusher: b, threshold: 50 * time.Millisecond,
		warn: func(op string, _ time.Duration) { warnings <- op },
	}
	finished := make(chan struct{})
	watchFinished := make(chan struct{})
	go func() { w.watch(finished, 0); close(watchFinished) }()
	writeDone := make(chan struct{})
	go func() { defer close(writeDone); _, _ = w.Write([]byte("ping")) }()
	<-b.entered
	select {
	case op := <-warnings:
		if op != "Write still blocked" {
			t.Fatalf("unexpected first warning %q", op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no watchdog warning while Write was blocked")
	}
	close(b.release)
	select {
	case <-writeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Write not released")
	}
	select {
	case op := <-warnings:
		if op != "Write recovered" {
			t.Fatalf("unexpected recovery %q", op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no recovery warning")
	}
	close(finished)
	select {
	case <-watchFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog leaked after request cleanup")
	}
}

func TestXHTTPCountingBodyPreservesCloseAndBytes(t *testing.T) {
	p := &xhttpStreamProgress{}
	body := &xhttpCountingBody{ReadCloser: io.NopCloser(strings.NewReader("payload")), progress: p}
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "payload" || p.up.Load() != 7 {
		t.Fatalf("unexpected body=%q up=%d err=%v", data, p.up.Load(), err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if p.lastSuccess.Load() == 0 {
		t.Fatal("missing progress timestamp")
	}
}

func TestXHTTPDefaultHeaderBounds(t *testing.T) {
	if got := normalizeXHTTPHeaderBytes(0); got != 8192 {
		t.Fatalf("default header bound=%d", got)
	}
	if got := normalizeXHTTPHeaderBytes(16384); got != 16384 {
		t.Fatalf("explicit header bound=%d", got)
	}
	if got := normalizeXHTTPHeaderBytes(1 << 29); got != 16<<20 {
		t.Fatalf("header bound not capped: %d", got)
	}
}
