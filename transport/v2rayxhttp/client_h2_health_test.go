package v2rayxhttp

import (
	"testing"
	"time"
)

func TestXHTTPH2HealthTimers(t *testing.T) {
	tests := []struct {
		name                      string
		input, wantIdle, wantPing time.Duration
	}{
		{"defaults", 0, 15 * time.Second, 7 * time.Second},
		{"explicit", 25 * time.Second, 25 * time.Second, 7 * time.Second},
		{"disabled", -time.Second, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idle, ping := xhttpH2HealthTimers(tc.input)
			if idle != tc.wantIdle || ping != tc.wantPing {
				t.Fatalf("got (%s, %s), want (%s, %s)", idle, ping, tc.wantIdle, tc.wantPing)
			}
		})
	}
}

func TestXHTTPTCPUserTimeoutConfig(t *testing.T) {
	cases := []struct {
		raw  string
		want int
	}{
		{"0", 0},
		{"5000", 5000},
		{"120000", 120000},
		{"18000", 18000},
		{"garbage", defaultXHTTPTCPUserTimeoutMS},
		{"100", defaultXHTTPTCPUserTimeoutMS},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv("SING_BOX_XHTTP_TCP_USER_TIMEOUT_MS", tc.raw)
			if got := xhttpTCPUserTimeoutMS(nil); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// The production constructor must arm the PING check even when the config has
// no xmux section; previously this left ReadIdleTimeout at zero (disabled).
func TestXHTTPDefaultClientEnablesH2PING(t *testing.T) {
	// Keep this test limited to helper defaults. NewClient transport integration
	// is covered by TestNewClientPoolFollowsVersion in http_version_test.go.
	idle, ping := xhttpH2HealthTimers(0)
	if idle <= 0 || ping <= 0 || ping >= idle {
		t.Fatalf("unsafe default HTTP/2 timers: idle=%s ping=%s", idle, ping)
	}
}
