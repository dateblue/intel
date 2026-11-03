package sbxhttp

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestPrivateVisionTLSOverLogicalStream(t *testing.T) {
	cfg, err := newPrivateVisionServerTLS()
	if err != nil {
		t.Fatal(err)
	}
	clientRaw, serverRaw := net.Pipe()
	defer clientRaw.Close()
	defer serverRaw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make(chan error, 1)
	go func() {
		s, err := privateVisionServerTLS(ctx, serverRaw, cfg)
		if err != nil {
			errs <- err
			return
		}
		defer func() { _ = serverRaw.Close(); _ = s.Close() }()
		buf := make([]byte, 5)
		if _, err = io.ReadFull(s, buf); err == nil && string(buf) != "hello" {
			t.Errorf("received %q", buf)
		}
		if err == nil {
			_, err = s.Write([]byte("world"))
		}
		errs <- err
	}()
	c, err := privateVisionClientTLS(ctx, clientRaw)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = clientRaw.Close(); _ = c.Close() }()
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 5)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatal(err)
	}
	if string(b) != "world" {
		t.Fatalf("wrong reply: %q", b)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}
