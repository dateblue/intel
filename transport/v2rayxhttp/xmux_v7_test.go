package v2rayxhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type xmuxV7RT func(*http.Request) (*http.Response, error)

func (f xmuxV7RT) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A URL-test's expired context can race a transport's net.ErrClosed/EPIPE.
// Neither outcome is evidence that sibling HTTP/2 streams are unhealthy.
func TestXMUXV7LocalDeadlineDoesNotCondemnPool(t *testing.T) {
	for _, returnedErr := range []error{context.DeadlineExceeded, net.ErrClosed} {
		manager := newXmuxManager(xmuxConfig{maxConnections: intRange{1, 1}}, func() xmuxConn {
			return &fixedXmuxConn{transport: xmuxV7RT(func(r *http.Request) (*http.Response, error) {
				return nil, returnedErr
			})}
		})
		client, _ := manager.getReserved()
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.org/", nil)
		_, err := client.roundTrip(req)
		cancel()
		if !errors.Is(err, returnedErr) {
			t.Fatalf("roundTrip error = %v, want %v", err, returnedErr)
		}
		if client.failing.Load() || client.consecFails.Load() != 0 {
			t.Fatalf("locally expired request condemned healthy pool for %v", returnedErr)
		}
		client.addOpenUsage(-1)
	}
}

func TestXMUXV7UncanceledTransportFailureStillTrips(t *testing.T) {
	manager := newXmuxManager(xmuxConfig{maxConnections: intRange{1, 1}}, func() xmuxConn {
		return &fixedXmuxConn{transport: xmuxV7RT(func(*http.Request) (*http.Response, error) {
			return nil, net.ErrClosed
		})}
	})
	client, _ := manager.getReserved()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.org/", nil)
	_, _ = client.roundTrip(req)
	if !client.failing.Load() {
		t.Fatal("transport loss on a live request was not retired")
	}
	client.addOpenUsage(-1)
}

func TestXMUXV7ZeroByteReadNotSuccess(t *testing.T) {
	client := &xmuxClient{conn: &fixedXmuxConn{}}
	client.addOpenUsage(1)
	client.consecFails.Store(1)
	handle := newXmuxRelease(client)
	breaker := connBreaker{xmux: handle}
	breaker.noteRead(0, nil)
	if n := client.consecFails.Load(); n != 1 {
		t.Fatalf("zero-byte read reset streak to %d", n)
	}
	breaker.noteRead(1, nil)
	if n := client.consecFails.Load(); n != 0 {
		t.Fatalf("nonzero data read failed to reset streak: %d", n)
	}
	handle.release()
}

func TestXMUXV7PayloadWithEOFResetsFailure(t *testing.T) {
	client := &xmuxClient{conn: &fixedXmuxConn{}}
	client.addOpenUsage(1)
	client.consecFails.Store(2)
	handle := newXmuxRelease(client)
	breaker := connBreaker{xmux: handle}
	breaker.noteRead(3, io.EOF)
	if n := client.consecFails.Load(); n != 0 {
		t.Fatalf("valid payload accompanied by EOF failed to reset streak: %d", n)
	}
	handle.release()
}

func TestXMUXV7RetiredPoolDoesNotCutActiveStream(t *testing.T) {
	conn := &fixedXmuxConn{}
	c := &xmuxClient{conn: conn}
	c.addOpenUsage(1)
	c.close()
	if conn.IsClosed() {
		t.Fatal("retirement closed a live stream")
	}
	r := newXmuxRelease(c)
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() { defer group.Done(); r.releaseWithReason("local-close") }()
	}
	group.Wait()
	if c.getOpenUsage() != 0 || !conn.IsClosed() {
		t.Fatal("retired pool not closed once after final release")
	}
}

func TestXMUXV7RetiredIdleDiagnostic(t *testing.T) {
	var messages []string
	manager := &xmuxManager{onEvent: func(format string, args ...any) {
		messages = append(messages, fmt.Sprintf(format, args...))
	}}
	client := &xmuxClient{manager: manager, diagID: 27}
	client.addOpenUsage(1)
	client.closed = true
	stream := &xmuxStreamDiagnostic{client: client, id: 28, start: time.Now().Add(-6 * time.Minute)}
	stream.reportPending()
	stream.reportPending()
	if len(messages) != 1 {
		t.Fatalf("six-minute idle stream reported %d times, want once", len(messages))
	}
	if !strings.Contains(messages[0], "retired=true") || !strings.Contains(messages[0], "pool_active=1") {
		t.Fatalf("old stream diagnostic lacks retirement/occupancy: %s", messages[0])
	}
}
