//go:build with_xhttp

package v2rayxhttp

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/option"
)

func TestServerV5ExtractPlacements(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options option.V2RayXHTTPOptions
		path    string
		header  string
	}{
		{"path", option.V2RayXHTTPOptions{Path: "/xhttp"}, "/xhttp/sid/42", ""},
		{"query", option.V2RayXHTTPOptions{Path: "/xhttp", SessionPlacement: "query", SeqPlacement: "query"}, "/xhttp?x_session=sid&x_seq=42", ""},
		{"header", option.V2RayXHTTPOptions{Path: "/xhttp", SessionPlacement: "header", SeqPlacement: "header"}, "/xhttp", "X-Session"},
		{"cookie", option.V2RayXHTTPOptions{Path: "/xhttp", SessionPlacement: "cookie", SeqPlacement: "cookie"}, "/xhttp", "cookie"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, e := newServerMetaV5(tc.options)
			if e != nil {
				t.Fatal(e)
			}
			u, e := url.Parse(tc.path)
			if e != nil {
				t.Fatal(e)
			}
			r := &http.Request{URL: u, Header: make(http.Header)}
			if tc.header == "X-Session" {
				r.Header.Set("X-Session", "sid")
				r.Header.Set("X-Seq", "42")
			}
			if tc.header == "cookie" {
				r.AddCookie(&http.Cookie{Name: "x_session", Value: "sid"})
				r.AddCookie(&http.Cookie{Name: "x_seq", Value: "42"})
			}
			session, seq, ok := m.extract(r, streamOneServerPath(tc.options))
			if !ok || session != "sid" || seq != "42" {
				t.Fatalf("got %q %q ok=%v", session, seq, ok)
			}
		})
	}
}
func TestServerV5LegacyPadding(t *testing.T) {
	m, e := newServerMetaV5(option.V2RayXHTTPOptions{})
	if e != nil {
		t.Fatal(e)
	}
	r := &http.Request{URL: &url.URL{Path: "/xhttp/"}, Header: make(http.Header)}
	r.Header.Set("Referer", "https://example.com/xhttp/?x_padding="+strings.Repeat("0", 200))
	if !m.paddingValid(r, intRange{100, 1000}) {
		t.Fatal("should accept legacy referer padding")
	}
	if m.paddingValid(r, intRange{250, 1000}) {
		t.Fatal("should reject too-short padding")
	}
}
func TestServerV5Payloads(t *testing.T) {
	m, e := newServerMetaV5(option.V2RayXHTTPOptions{})
	if e != nil {
		t.Fatal(e)
	}
	r := &http.Request{Body: ioNopCloser{strings.NewReader("packet")}, ContentLength: 6, Header: make(http.Header)}
	payload, e := m.payload(r)
	if e != nil || string(payload) != "packet" {
		t.Fatalf("payload %q err %v", payload, e)
	}
}

type ioNopCloser struct{ *strings.Reader }

func (ioNopCloser) Close() error { return nil }

func TestServerV5DefaultUplinkPlacementMatchesXray(t *testing.T) {
	m, err := newServerMetaV5(option.V2RayXHTTPOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if m.uplinkPlacement != placementBody {
		t.Fatalf("default uplink placement %q; want body", m.uplinkPlacement)
	}
}
