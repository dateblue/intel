package sbxhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReliableCancellationPreservesHalfClose(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		eofAllowed bool
		wantCancel bool
	}{
		{"upload-complete", nil, false, false},
		{"request-body-eof", io.EOF, true, false},
		{"upload-error", io.ErrUnexpectedEOF, false, true},
		{"request-body-error", io.ErrUnexpectedEOF, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cancelReliableOnFailure(tc.err, cancel, tc.eofAllowed)
			gotCanceled := ctx.Err() != nil
			if gotCanceled != tc.wantCancel {
				t.Fatalf("canceled=%v, want=%v", gotCanceled, tc.wantCancel)
			}
		})
	}
}

// An HTTP/2 request can finish uploading while the response remains in flight.
// Cancellation after upload completes would send RST_STREAM CANCEL and lose the
// response. Exercise a real HTTP/2 connection rather than a mock writer.
func TestReliableHTTP2UploadHalfCloseKeepsDownload(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		_, _ = w.Write([]byte("downlink-response"))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()

	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, // local httptest server only
	}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/sbxhttp", reader)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("expected HTTP/2, got %s", resp.Proto)
	}

	// The uploader has ended cleanly; the request-body close is intentional.
	// The transport context must remain live to receive the response.
	cancelReliableOnFailure(nil, cancel, false)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "downlink-response" {
		t.Fatalf("downlink=%q", got)
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("request context canceled before downlink completed")
	}
}
