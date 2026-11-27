package sbxhttp

import (
	"errors"
	"time"
)

// h2HealthPolicy controls HTTP/2 liveness checks, not application stream idle
// deadlines. A quiet, healthy session remains open indefinitely. These are
// protocol-level PINGs and write-stall safeguards, not replay/reconnect logic.
type h2HealthPolicy struct {
	readIdle  time.Duration
	ping      time.Duration
	writeByte time.Duration
}

const (
	recoveryProfileBalanced = "balanced"
	recoveryProfileFast     = "fast"
)

func selectRecoveryPolicy(profile, mode string, reliable bool) (h2HealthPolicy, error) {
	balanced := h2HealthPolicy{h2ReadIdleTimeout, h2PingTimeout, h2WriteByteTimeout}
	switch profile {
	case "", recoveryProfileBalanced:
		return balanced, nil
	case recoveryProfileFast:
		// Fast detection is explicitly opt-in and only valid for the independent
		// direct H2 transport. CDN and Replay-v2 already have their own recovery
		// semantics; changing them here would silently alter known-good behavior.
		if mode != "" || reliable {
			return h2HealthPolicy{}, errors.New("sbxhttp: recovery_profile=fast is only supported for direct reliable=false mode")
		}
		// On a completely silent, blackholed H2 connection, a missed PING is
		// recognized earlier than the balanced 45s + 20s policy. The 15s PING
		// grace avoids killing a live-but-congested connection after one slow RTT.
		return h2HealthPolicy{30 * time.Second, 15 * time.Second, 60 * time.Second}, nil
	default:
		return h2HealthPolicy{}, errors.New("sbxhttp: recovery_profile must be balanced or fast")
	}
}
