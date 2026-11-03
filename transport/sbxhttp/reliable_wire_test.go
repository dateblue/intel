package sbxhttp

import (
	"bytes"
	"context"
	stdtls "crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/net/http2"
)

// This tests the exact constructor used by reliableAttempt. A malformed
// header here causes server-side `reject reliable version ... version=`.
func TestReplayV2RequiredHeaders(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	req := newReliableRequest(context.Background(), "example.com:2083", "/sbxhttp", "www.intel.com", id, io.NopCloser(bytes.NewReader(nil)))
	if got := req.Header.Get("X-SBXHTTP-Version"); got != reliableVersion {
		t.Fatalf("Replay version = %q, want %q", got, reliableVersion)
	}
	if got := req.Header.Get("X-SBXHTTP-Session"); got != id {
		t.Fatalf("Replay session = %q, want %q", got, id)
	}
	if req.Host != "www.intel.com" || req.Method != http.MethodPost || req.URL.Path != "/sbxhttp" {
		t.Fatalf("unexpected Replay request: host=%q method=%q path=%q", req.Host, req.Method, req.URL.Path)
	}
	if req.ContentLength != -1 {
		t.Fatalf("full-duplex streaming requires unknown length, got %d", req.ContentLength)
	}
}

// Verify that Go's HTTP/2 encoder and decoder preserve the custom version and
// session headers on the wire, independently of VLESS encryption or Vision.
func TestReplayV2HeadersOverHTTP2(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef"
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 || r.Method != http.MethodPost {
			t.Errorf("request is not HTTP/2 POST: %s %s", r.Proto, r.Method)
		}
		if got := r.Header.Get("X-SBXHTTP-Version"); got != reliableVersion {
			t.Errorf("wire version = %q, want %q", got, reliableVersion)
		}
		if got := r.Header.Get("X-SBXHTTP-Session"); got != id {
			t.Errorf("wire session = %q, want %q", got, id)
		}
		w.Header().Set("X-SBXHTTP-Version", reliableVersion)
		w.WriteHeader(http.StatusOK)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	tr := &http2.Transport{TLSClientConfig: &stdtls.Config{InsecureSkipVerify: true}} // isolated httptest cert
	defer tr.CloseIdleConnections()
	req := newReliableRequest(context.Background(), u.Host, "/sbxhttp", "", id, io.NopCloser(strings.NewReader("payload")))
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("bad response: %s %s", resp.Proto, resp.Status)
	}
	if got := resp.Header.Get("X-SBXHTTP-Version"); got != reliableVersion {
		t.Errorf("response version = %q, want %q", got, reliableVersion)
	}
}
