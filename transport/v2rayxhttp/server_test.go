//go:build with_xhttp

package v2rayxhttp

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// echoTransportHandler stands in for VLESS during transport-layer tests.
// The real inbound additionally parses and authenticates the VLESS handshake.
type echoTransportHandler struct{}

func (*echoTransportHandler) NewConnectionEx(_ context.Context, conn net.Conn, _ M.Socksaddr, _ M.Socksaddr, onClose N.CloseHandlerFunc) {
	defer onClose(nil)
	defer conn.Close()
	var p [4]byte
	if _, err := io.ReadFull(conn, p[:]); err != nil {
		return
	}
	_, _ = conn.Write(p[:])
}

func TestStreamOneServerPath(t *testing.T) {
	cases := []struct {
		opts option.V2RayXHTTPOptions
		want string
	}{
		{option.V2RayXHTTPOptions{Path: "xhttp"}, "/xhttp/"},
		{option.V2RayXHTTPOptions{Path: "/xhttp/"}, "/xhttp/"},
		{option.V2RayXHTTPOptions{Path: "/xhttp?token=1"}, "/xhttp/"},
		{option.V2RayXHTTPOptions{Path: "/xhttp", SessionPlacement: "query", SeqPlacement: "header"}, "/xhttp"},
		{option.V2RayXHTTPOptions{}, "/"},
	}
	for _, tc := range cases {
		if got := streamOneServerPath(tc.opts); got != tc.want {
			t.Errorf("path for %+v = %q; want %q", tc.opts, got, tc.want)
		}
	}
}

func TestXHTTPServerRequiresExplicitStreamOne(t *testing.T) {
	for _, mode := range []string{"", "auto", "stream-up", "packet-up"} {
		if _, err := NewServer(context.Background(), nil, option.V2RayXHTTPOptions{Mode: mode}, nil, &echoTransportHandler{}); err == nil {
			t.Fatalf("mode %q unexpectedly accepted", mode)
		}
	}
}

func TestXHTTPServerOnlyHTTP2BarePath(t *testing.T) {
	s := &Server{path: "/xhttp/", method: http.MethodPost, handler: &echoTransportHandler{}}
	for _, tc := range []struct {
		name, method, path string
		protoMajor         int
		wantStatus         int
	}{
		{"http1", http.MethodPost, "/xhttp/", 1, http.StatusHTTPVersionNotSupported},
		{"bad path", http.MethodPost, "/xhttp/session", 2, http.StatusNotFound},
		{"bad method", http.MethodGet, "/xhttp/", 2, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "https://test.invalid"+tc.path, nil)
			req.ProtoMajor = tc.protoMajor
			req.ProtoMinor = 0
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if got := rec.Code; got != tc.wantStatus {
				t.Fatalf("status = %d, want %d", got, tc.wantStatus)
			}
		})
	}
}

func TestXHTTPServerStreamOneEcho(t *testing.T) {
	s := &Server{path: "/xhttp/", method: http.MethodPost, handler: &echoTransportHandler{}}
	req := httptest.NewRequest(http.MethodPost, "https://test.invalid/xhttp/?x_padding=123", io.NopCloser(io.LimitReader(&oneShotReader{data: []byte("ping")}, 4)))
	req.ProtoMajor = 2
	req.ProtoMinor = 0
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "ping" {
		t.Fatalf("body = %q, want ping", got)
	}
}

type oneShotReader struct{ data []byte }

func (r *oneShotReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}
