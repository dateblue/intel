//go:build linux

package v2rayxhttp

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

func TestXHTTPClientTCPUserTimeoutSetOnSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := setXHTTPTCPUserTimeout(conn, 15000); err != nil {
		t.Fatal(err)
	}
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var value int
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		value, sockErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	}); err != nil {
		t.Fatal(err)
	}
	if sockErr != nil {
		t.Fatal(sockErr)
	}
	if value != 15000 {
		t.Fatalf("TCP_USER_TIMEOUT=%d, want 15000", value)
	}
}
