//go:build with_xhttp

package v2rayxhttp

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/net/http2"
)

// The transport handler reads the upload and copies it into the download.
// The callback must be sent after its forwarding goroutine terminates.
type loadEchoHandler struct{}

func (*loadEchoHandler) NewConnectionEx(_ context.Context, conn net.Conn, _ M.Socksaddr, _ M.Socksaddr, onClose N.CloseHandlerFunc) {
	go func() {
		defer onClose(nil)
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()
}

// Run with XHTTP_STRESS=1. Exercises the real HTTP/2 transport rather than
// httptest.ResponseRecorder; reproduces simultaneous request-body upload and
// response-body download over multiple long-lived streams.
func TestXHTTPStreamOneConcurrentLargeTransfer(t *testing.T) {
	if os.Getenv("XHTTP_STRESS") != "1" {
		t.Skip("set XHTTP_STRESS=1 to run the bulk HTTP/2 loopback load test")
	}
	const concurrent = 4
	const size = 64 << 20 // 4 streams x 64 MiB = 256 MiB both directions
	s := &Server{path: "/xhttp/", method: http.MethodPost, padding: intRange{100, 100}, handler: &loadEchoHandler{}}
	hts := httptest.NewUnstartedServer(s)
	hts.EnableHTTP2 = true
	hts.StartTLS()
	defer hts.Close()
	h2 := &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // only local httptest certificate
	defer h2.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	chunk := bytes.Repeat([]byte("stream-one-xhttp-integrity-"), 1024)
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pr, pw := io.Pipe()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, hts.URL+"/xhttp/", pr)
			if err != nil {
				t.Errorf("stream %d request: %v", i, err)
				return
			}
			request.Header.Set("Referer", hts.URL+"/xhttp/?x_padding="+strings.Repeat("X", 100))
			request.Header.Set("Content-Type", "application/grpc")
			go func() {
				defer pw.Close()
				var left int64 = size
				for left > 0 {
					n := min(int64(len(chunk)), left)
					if _, err := pw.Write(chunk[:int(n)]); err != nil {
						return
					}
					left -= n
				}
			}()
			resp, err := h2.RoundTrip(request)
			if err != nil {
				t.Errorf("stream %d roundtrip: %v", i, err)
				_ = pw.CloseWithError(err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("stream %d status %d", i, resp.StatusCode)
				return
			}
			n, err := io.Copy(io.Discard, resp.Body)
			if err != nil || n != size {
				t.Errorf("stream %d read=%d want=%d err=%v", i, n, size, err)
			}
		}(i)
	}
	wg.Wait()
}

// A small isolated check that opt-in timing wrappers preserve Write and Flush
// semantics and do not alter bytes.
func TestXHTTPSlowWriterDoesNotChangeBytes(t *testing.T) {
	rec := httptest.NewRecorder()
	var warnings atomic.Int32
	w := &xhttpSlowResponseWriter{
		ResponseWriter: rec, flusher: rec, threshold: time.Nanosecond,
		warn: func(string, time.Duration) { warnings.Add(1) },
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	if got := rec.Body.String(); got != "hello" {
		t.Fatal(strconv.Quote(got))
	}
	if !rec.Flushed {
		t.Fatal("no flush")
	}
}
