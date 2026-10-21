//go:build with_xhttp

package v2rayxhttp

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// This exercises the actual stream-one ServeHTTP handler and its explicit
// http2.Server limits, unlike httptest.ResponseRecorder (which has no h2
// frame/window/cancellation behavior). TLS/REALITY are deliberately outside
// the test: h2c feeds exactly the same post-handshake HTTP/2 wire protocol.
func newXHTTP2TestServer(t *testing.T) (*Server, *http2.Transport, string) {
	t.Helper()
	s := &Server{path: "/xhttp/", method: http.MethodPost, padding: intRange{100, 100}, handler: &loadEchoHandler{}}
	h2 := &http2.Server{MaxHeaderListSize: uint32(normalizeXHTTPHeaderBytes(0))}
	ts := httptest.NewServer(h2c.NewHandler(s, h2))
	t.Cleanup(ts.Close)
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	t.Cleanup(transport.CloseIdleConnections)
	return s, transport, ts.URL
}

func TestXHTTP2RealStreamAndOversizedHeaders(t *testing.T) {
	_, transport, base := newXHTTP2TestServer(t)
	request := func(extra string) *http.Request {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/xhttp/", strings.NewReader("ping"))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Referer", base+"/xhttp/?x_padding="+strings.Repeat("X", 100))
		if extra != "" {
			r.Header.Set("X-Oversized-Header", extra)
		}
		return r
	}
	resp, err := transport.RoundTrip(request(""))
	if err != nil {
		t.Fatalf("normal HTTP/2 stream-one request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != "ping" {
		t.Fatalf("status=%d body=%q err=%v", resp.StatusCode, body, err)
	}
	// HTTP/2 limits are on the decoded header list, not the Huffman size.
	resp, err = transport.RoundTrip(request(strings.Repeat("A", 16*1024)))
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("oversized HTTP/2 headers accepted")
		}
	}
}

func TestXHTTP2CancellationReleasesStream(t *testing.T) {
	s, transport, base := newXHTTP2TestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pr, pw := io.Pipe()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/xhttp/", pr)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Referer", base+"/xhttp/?x_padding="+strings.Repeat("X", 100))
	response, err := transport.RoundTrip(request)
	if err != nil {
		_ = pw.CloseWithError(err)
		t.Fatalf("roundtrip: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		_ = pw.Close()
		t.Fatalf("status=%d", response.StatusCode)
	}
	// Ensure the handler has started before cancelling; otherwise this
	// test could incorrectly pass while ServeHTTP had not yet incremented.
	startedBy := time.Now().Add(2 * time.Second)
	for s.activeStreams.Load() == 0 && time.Now().Before(startedBy) {
		time.Sleep(10 * time.Millisecond)
	}
	if s.activeStreams.Load() == 0 {
		t.Fatal("stream handler was not observed before cancellation")
	}
	// Abort a half-finished upload/response without an EOF. The request
	// context must stop ServeHTTP and reclaim its active-stream counter.
	cancel()
	_ = pw.Close()
	_ = response.Body.Close()
	deadline := time.Now().Add(3 * time.Second)
	for s.activeStreams.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := s.activeStreams.Load(); got != 0 {
		t.Fatalf("stream remains active after cancel: %d", got)
	}
}
