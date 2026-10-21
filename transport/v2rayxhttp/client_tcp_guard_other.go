//go:build !linux

package v2rayxhttp

import "net"

// Windows and other platforms keep their native TCP timeout policy; the
// HTTP/2 PING timeout still detects silent peers on those platforms.
func setXHTTPTCPUserTimeout(_ net.Conn, _ int) error { return nil }
