package sbxhttp

import "testing"

func TestPrivateH2ALPNAllowed(t *testing.T) {
	cases := []struct {
		protocol string
		allow    bool
	}{
		{"h2", true},
		{"", true},
		{"http/1.1", false},
		{"h3", false},
	}
	for _, tc := range cases {
		if got := privateH2ALPNAllowed(tc.protocol); got != tc.allow {
			t.Fatalf("ALPN %q: allowed=%t; want %t", tc.protocol, got, tc.allow)
		}
	}
}
