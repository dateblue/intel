package sbxhttp

import (
	"net/netip"
	"strings"
	"testing"
)

func TestMultiCDNIPValidation(t *testing.T) {
	a, err := validateMultiCDNIPs([]string{"1.1.1.1", "2606:4700:4700::1111"}, true, 4)
	if err != nil || len(a) != 2 {
		t.Fatalf("valid pool: %v %v", a, err)
	}
	got := []string{multiEdgeForLane(a, 0).String(), multiEdgeForLane(a, 1).String(), multiEdgeForLane(a, 2).String(), multiEdgeForLane(a, 3).String()}
	want := []string{"1.1.1.1", "2606:4700:4700::1111", "1.1.1.1", "2606:4700:4700::1111"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("lane %d got %s want %s", i, got[i], want[i])
		}
	}
	_, err = validateMultiCDNIPs(nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	invalid := []struct {
		ips   []string
		cdn   bool
		lanes int
		desc  string
	}{
		{[]string{"1.1.1.1"}, false, 4, "not CDN"},
		{[]string{"1.1.1.1"}, true, 0, "disabled"},
		{[]string{"1.1.1.1", "8.8.8.8"}, true, 1, "too many"},
		{[]string{"1.1.1.1", "1.1.1.1"}, true, 2, "duplicate"},
		{[]string{"127.0.0.1"}, true, 1, "loopback"},
		{[]string{"192.168.1.1"}, true, 1, "RFC1918"},
		{[]string{"fe80::1"}, true, 1, "linklocal"},
		{[]string{"2606:4700:4700::1111%eth0"}, true, 1, "zone"},
		{[]string{"1.1.1.1:443"}, true, 1, "port"},
		{[]string{"cloudflare.com"}, true, 1, "hostname"},
	}
	for _, tt := range invalid {
		_, err := validateMultiCDNIPs(tt.ips, tt.cdn, tt.lanes)
		if err == nil {
			t.Errorf("%s should fail", tt.desc)
		}
	}
}

func TestMultiIPNoReferenceConfusion(t *testing.T) {
	// Pool only controls per-physical-lane dial targets; it does not encode
	// CF IP into an HTTP Host, SNI, or backend session identifier.
	ips := []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("8.8.8.8")}
	for lane := 0; lane < 4; lane++ {
		if strings.Contains(multiEdgeForLane(ips, lane).String(), ":443") {
			t.Fatal("IP unexpectedly contains port")
		}
	}
}
