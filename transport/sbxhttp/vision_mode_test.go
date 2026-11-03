package sbxhttp

import "testing"

func TestPrivateVisionModeNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		header  string
		ok      bool
	}{
		{"legacy client and server", false, "", true},
		{"matched private Vision", true, privateVisionModeTLS13, true},
		{"Vision client to legacy server", false, privateVisionModeTLS13, false},
		{"legacy client to Vision server", true, "", false},
		{"unknown mode", true, "inner-tls13-v2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := privateVisionModeHeaderMatches(tc.enabled, tc.header); got != tc.ok {
				t.Fatalf("header accepted=%v, want %v", got, tc.ok)
			}
		})
	}
}

func TestPrivateVisionModeNeverImplicitlyEnablesReplay(t *testing.T) {
	for _, tc := range []struct{ vision, reliable, rejects bool }{
		{false, false, false}, {false, true, false},
		{true, false, false}, {true, true, true},
	} {
		err := validatePrivateVisionMode(tc.vision, tc.reliable)
		if (err != nil) != tc.rejects {
			t.Fatalf("vision=%v replay=%v: got %v", tc.vision, tc.reliable, err)
		}
	}
}
