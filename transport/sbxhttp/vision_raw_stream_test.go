package sbxhttp

import (
	"context"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// Vision switches to the underlying byte stream after inspecting the inner TLS
// records. Verify both sides can exchange raw bidirectional bytes at that point.
// This checks the carrier contract, not sing-vmess's Vision state machine.
func TestPrivateVisionTLSToRawStreamCarrier(t *testing.T) {
	cfg, err := newPrivateVisionServerTLS()
	if err != nil {
		t.Fatal(err)
	}
	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		inner, err := privateVisionServerTLS(ctx, serverRaw, cfg)
		if err != nil {
			result <- err
			return
		}
		// After the raw phase, close the carrier first: close_notify is
		// not part of the Vision-bypassed logical payload.
		defer func() { _ = serverRaw.Close(); _ = inner.Close() }()
		buf := make([]byte, 4)
		if _, err = io.ReadFull(inner, buf); err != nil {
			result <- err
			return
		}
		if string(buf) != "ping" {
			result <- errors.New("unexpected TLS-phase input")
			return
		}
		if _, err = inner.Write([]byte("pong")); err != nil {
			result <- err
			return
		}
		raw := inner.(*stdtls.Conn).NetConn()
		if _, err = io.ReadFull(raw, buf); err != nil {
			result <- err
			return
		}
		if string(buf) != "data" {
			result <- errors.New("unexpected raw Vision-phase input")
			return
		}
		_, err = raw.Write([]byte("done"))
		result <- err
	}()
	inner, err := privateVisionClientTLS(ctx, clientRaw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inner.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4)
	if _, err := io.ReadFull(inner, b); err != nil {
		t.Fatal(err)
	}
	if string(b) != "pong" {
		t.Fatalf("TLS-phase response=%q", b)
	}
	raw := inner.(*stdtls.Conn).NetConn()
	if _, err := raw.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(raw, b); err != nil {
		t.Fatal(err)
	}
	if string(b) != "done" {
		t.Fatalf("raw Vision-phase response=%q", b)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestPrivateVisionTLSRejectsStalledHandshake(t *testing.T) {
	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := privateVisionClientTLS(ctx, clientRaw); err == nil {
		t.Fatal("handshake unexpectedly completed with a silent peer")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("stalled handshake ignored context cancellation")
	}
}
