//go:build linux

package v2rayxhttp

import (
	"net"
	"syscall"

	"github.com/sagernet/sing/common"
	"golang.org/x/sys/unix"
)

func setXHTTPTCPUserTimeout(conn net.Conn, timeoutMS int) error {
	// DefaultDialer may wrap a TCP socket in sing-box's tracking/counter
	// layers. Use the project's existing unwrapping helper, rather than a
	// direct *net.TCPConn assertion that would silently skip those wrappers.
	syscallConn, ok := common.Cast[syscall.Conn](conn)
	if !ok {
		return nil // detour or virtual conn without a kernel TCP socket
	}
	raw, err := syscallConn.SyscallConn()
	if err != nil {
		return err
	}
	var socketError error
	if err := raw.Control(func(fd uintptr) {
		socketError = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT, timeoutMS)
	}); err != nil {
		return err
	}
	return socketError
}
