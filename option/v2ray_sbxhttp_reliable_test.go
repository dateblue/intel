package option

import (
	"encoding/json"
	"testing"
)

func TestSBXHTTPReliableJSONOption(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  bool
	}{
		{`{"type":"sbxhttp","path":"/sbxhttp","reliable":true}`, true},
		{`{"type":"sbxhttp","path":"/sbxhttp","reliable":false}`, false},
		{`{"type":"sbxhttp","path":"/sbxhttp"}`, false},
	} {
		var o V2RayTransportOptions
		if err := json.Unmarshal([]byte(tc.input), &o); err != nil {
			t.Fatalf("decode %s: %v", tc.input, err)
		}
		if o.Type != "sbxhttp" || o.SBXHTTPOptions.Path != "/sbxhttp" || o.SBXHTTPOptions.Reliable != tc.want {
			t.Fatalf("unexpected option from %s: %#v", tc.input, o.SBXHTTPOptions)
		}
	}
}
