package sbxhttp

import "errors"

// The inner TLS is a private, authenticated-REALITY-only Vision adapter, NOT
// VLESS Encryption. Both endpoints must opt in to the same HTTP/2 byte stream.
const (
	privateVisionModeHeader = "X-SBXHTTP-Vision"
	privateVisionModeTLS13  = "inner-tls13-v1"
)

func privateVisionModeHeaderMatches(enabled bool, received string) bool {
	if enabled {
		return received == privateVisionModeTLS13
	}
	return received == ""
}

// Keep Replay-v2 compiled, but do not blend its still-unverified half-close
// with this independent Vision experiment. Re-enable after full duplex tests.
func validatePrivateVisionMode(visionInnerTLS, reliable bool) error {
	if visionInnerTLS && reliable {
		return errors.New("sbxhttp: vision_inner_tls requires reliable=false until full-duplex Replay/Vision validation")
	}
	return nil
}
