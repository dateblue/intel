package sbxhttp

import "context"

// The HTTP/2 request uses a detached context because VLESS's early-write
// connection must outlive DialContext. However, while the response headers
// have NOT arrived, cancellation of the original dial must still win.
func cancelPendingDial(ctx context.Context, conn *clientConn) {
	if ctx.Done() == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			if conn.publish(nil, ctx.Err()) {
				_ = conn.Close()
			}
		case <-conn.ready:
		}
	}()
}
