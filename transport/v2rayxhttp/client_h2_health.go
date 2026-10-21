package v2rayxhttp

import "time"

// A dead XHTTP/TCP connection can otherwise remain ESTABLISHED through many
// rounds of exponential RTO. http2.Transport's read-idle PING is a protocol-
// level liveness check; it does not inject probe bytes into VLESS payloads.
// In particular, keepAlivePeriod=0 is the *normal* default from normalizeXmux,
// not an explicit request to disable the HTTP/2 health check.
const (
	defaultXHTTPH2ReadIdle    = 15 * time.Second
	defaultXHTTPH2PingTimeout = 7 * time.Second
)

func xhttpH2HealthTimers(keepAlivePeriod time.Duration) (readIdle, pingTimeout time.Duration) {
	if keepAlivePeriod < 0 {
		// Existing explicit XMUX negative keep-alive value means disabled.
		return 0, 0
	}
	readIdle = keepAlivePeriod
	if readIdle == 0 {
		readIdle = defaultXHTTPH2ReadIdle
	}
	return readIdle, defaultXHTTPH2PingTimeout
}
