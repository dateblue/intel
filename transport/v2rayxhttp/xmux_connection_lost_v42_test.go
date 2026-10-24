package v2rayxhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// The x/net/http2 transport uses an unexported error for a lost ClientConn.
// Classify that condition as fatal for the shared transport, but leave
// ordinary per-stream shutdown and local cancellation alone.
func TestXHTTPV42ClientConnectionLostClassification(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"h2 client connection lost", errors.New("http2: client connection lost"), true},
		{"wrapped h2 connection lost", fmt.Errorf("proxy request: %w", errors.New("http2: client connection lost")), true},
		{"h2 client conn is closed", errors.New("http2: client conn is closed"), true},
		{"wrapped h2 client conn is closed", fmt.Errorf("read failed: %w", errors.New("http2: client conn is closed")), true},
		{"h2 body locally closed", errors.New("http2: response body closed"), false},
		{"h2 stream closed", errors.New("http2: stream closed"), false},
		{"h2 stream reset", errors.New("stream error: stream ID 1; CANCEL"), false},
		{"local context canceled", context.Canceled, false},
		{"normal EOF", io.EOF, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := xhttpTerminalTransportError(tt.err); got != tt.fatal {
				t.Fatalf("xhttpTerminalTransportError(%q)=%t, want %t", tt.err, got, tt.fatal)
			}
		})
	}
}

// A lost shared transport must be retired without killing in-flight streams
// or evicting a healthy sibling. The original XMUX manager intentionally opens
// a replacement while the pool has fewer than max_connections resources;
// expecting the first get() to return the sibling would be a wrong assertion.
func TestXHTTPV42LostConnectionIsolatedRetirement(t *testing.T) {
	var transports []*fixedXmuxConn
	manager := newXmuxManager(xmuxConfig{maxConnections: intRange{2, 2}}, func() xmuxConn {
		idx := len(transports)
		rt := &failingRT{}
		if idx == 0 {
			rt.errs = []error{errors.New("http2: client conn is closed")}
		} else {
			rt.statuses = []int{http.StatusOK}
		}
		conn := &fixedXmuxConn{transport: rt}
		transports = append(transports, conn)
		return conn
	})
	failing, _ := manager.get()
	failing.addOpenUsage(1) // an existing stream must not be force-killed
	healthy, _ := manager.get()
	healthy.addOpenUsage(1)
	request, err := http.NewRequest(http.MethodPost, "https://example.invalid/xhttp-path", strings.NewReader("body"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = failing.roundTrip(request); err == nil {
		t.Fatal("expected lost-connection error")
	}
	if !failing.failing.Load() {
		t.Fatal("HTTP/2 lost connection must immediately mark its own pool failing")
	}
	if healthy.failing.Load() {
		t.Fatal("a healthy sibling pool was marked failing")
	}
	if transports[0].IsClosed() {
		t.Fatal("a live stream was interrupted before release")
	}
	selected, wait := manager.get()
	if selected == nil || selected == failing || wait != 0 {
		t.Fatalf("failed pool must not be reused: selected=%p failing=%p wait=%s", selected, failing, wait)
	}
	// The manager fills the pool to max_connections before reusing it.
	// This is the established XMUX behaviour (TestXmuxMaxConnections), and
	// preserves v4's fast connection ramp-up. After evicting the failed pool,
	// one healthy peer remains, so get() opens a replacement (third ever).
	if len(transports) != 3 || selected.conn != transports[2] {
		t.Fatalf("want a fresh replacement alongside the healthy peer: total_created=%d selected=%p", len(transports), selected)
	}
	if len(manager.clients) != 2 || manager.clients[0] != healthy || manager.clients[1] != selected {
		t.Fatal("replacement did not preserve the healthy pool resource")
	}
	if transports[0].IsClosed() {
		t.Fatal("eviction closed a transport with an active stream")
	}
	if _, err = healthy.roundTrip(request); err != nil {
		t.Fatalf("healthy peer request failed: %v", err)
	}
	failing.addOpenUsage(-1)
	if !transports[0].IsClosed() {
		t.Fatal("failed pool was not closed after last stream")
	}
	if transports[1].IsClosed() {
		t.Fatal("healthy pool was closed")
	}
	healthy.addOpenUsage(-1)
}
