package v2rayxhttp

import (
	"context"
	"net"
	"os"
	"strconv"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

// The kernel's default TCP retransmission window may last minutes.  XHTTP
// stream-one multiplexes many application streams over a single long-lived TCP
// socket. An aggressive TCP_USER_TIMEOUT can turn a transient high-load stall
// into ETIMEDOUT, killing all streams sharing that socket. Leave the kernel
// default unchanged unless the operator explicitly opts in via the env var.
// The setting is deliberately scoped to XHTTP HTTP/2 client TCP dials only.
const defaultXHTTPTCPUserTimeoutMS = 0

func xhttpTCPUserTimeoutMS(warn func(string)) int {
	raw, present := os.LookupEnv("SING_BOX_XHTTP_TCP_USER_TIMEOUT_MS")
	if !present || raw == "" {
		return defaultXHTTPTCPUserTimeoutMS
	}
	value, err := strconv.Atoi(raw)
	if err == nil && (value == 0 || (value >= 5000 && value <= 120000)) {
		return value
	}
	if warn != nil {
		warn("invalid SING_BOX_XHTTP_TCP_USER_TIMEOUT_MS; using kernel default (0)")
	}
	return defaultXHTTPTCPUserTimeoutMS
}

type xhttpGuardDialer struct {
	N.Dialer
	timeoutMS int
	warn      func(string)
}

func (d *xhttpGuardDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	if d.timeoutMS > 0 {
		if err := setXHTTPTCPUserTimeout(conn, d.timeoutMS); err != nil && d.warn != nil {
			d.warn("unable to configure XHTTP TCP_USER_TIMEOUT: " + err.Error())
		}
	}
	return conn, nil
}
