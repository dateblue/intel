package sbxhttp

import (
	"strings"
	"testing"
)

func TestSingleModeRejectsRemovedMultiTCPOptions(t *testing.T) {
	cases := []struct {
		name      string
		lanes     int
		ips       []string
		wantError bool
	}{
		{"default single", 0, nil, false},
		{"explicit zero", 0, []string{}, false},
		{"removed down_connections", 2, nil, true},
		{"removed cdn_ips", 0, []string{"172.64.229.97"}, true},
		{"both removed settings", 6, []string{"172.64.229.97"}, true},
		{"negative removed setting", -1, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := rejectMultiTCPOptions(tc.lanes, tc.ips)
			if (err != nil) != tc.wantError {
				t.Fatalf("down_connections=%d cdn_ips=%v: error=%v", tc.lanes, tc.ips, err)
			}
			if tc.wantError && !strings.Contains(err.Error(), "down_connections") {
				t.Fatalf("unhelpful error: %v", err)
			}
		})
	}
}
