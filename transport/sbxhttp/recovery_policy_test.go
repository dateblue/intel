package sbxhttp

import (
	"encoding/json"
	"github.com/sagernet/sing-box/option"
	"testing"
	"time"
)

func TestRecoveryPolicyBackwardsCompatible(t *testing.T) {
	for _, profile := range []string{"", "balanced"} {
		p, err := selectRecoveryPolicy(profile, "", false)
		if err != nil || p.readIdle != h2ReadIdleTimeout || p.ping != h2PingTimeout || p.writeByte != h2WriteByteTimeout {
			t.Fatalf("balanced policy changed for %q: %+v %v", profile, p, err)
		}
	}
}

func TestRecoveryPolicyFastIsStrictlyOptIn(t *testing.T) {
	p, err := selectRecoveryPolicy("fast", "", false)
	if err != nil || p.readIdle != 30*time.Second || p.ping != 15*time.Second || p.writeByte != 60*time.Second {
		t.Fatalf("fast policy unexpectedly changed: %+v %v", p, err)
	}
	if p.readIdle+p.ping >= h2ReadIdleTimeout+h2PingTimeout {
		t.Fatalf("fast policy does not shorten blackhole detection window: %+v", p)
	}
	for _, args := range []struct {
		mode     string
		reliable bool
	}{{"cdn", false}, {"", true}, {"cdn", true}} {
		if _, err := selectRecoveryPolicy("fast", args.mode, args.reliable); err == nil {
			t.Fatalf("fast must be rejected for mode=%q reliable=%v", args.mode, args.reliable)
		}
	}
	if _, err := selectRecoveryPolicy("unknown", "", false); err == nil {
		t.Fatal("unrecognized recovery profile accepted")
	}
}

// Ensure the optional field survives the real V2Ray transport JSON union.
// This does not change any on-wire SB-XHTTP messages or the default config.
func TestRecoveryProfileConfigJSON(t *testing.T) {
	var transport option.V2RayTransportOptions
	if err := json.Unmarshal([]byte(`{"type":"sbxhttp","path":"/sbxhttp","reliable":false,"vision_inner_tls":true,"recovery_profile":"fast"}`), &transport); err != nil {
		t.Fatal(err)
	}
	if transport.SBXHTTPOptions.RecoveryProfile != "fast" || !transport.SBXHTTPOptions.VisionInnerTLS {
		t.Fatalf("recovery profile or Vision mode lost during JSON decoding: %+v", transport.SBXHTTPOptions)
	}
	encoded, err := json.Marshal(transport)
	if err != nil {
		t.Fatal(err)
	}
	var decoded option.V2RayTransportOptions
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SBXHTTPOptions.RecoveryProfile != "fast" {
		t.Fatalf("profile missing after JSON round trip: %s", encoded)
	}
}
