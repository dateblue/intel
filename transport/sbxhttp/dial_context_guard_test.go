package sbxhttp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestPendingDialCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	defer pr.Close()
	c := newClientConn(pw, func() {}, nil)
	cancelPendingDial(ctx, c)
	cancel()
	select {
	case <-c.ready:
	case <-time.After(time.Second):
		t.Fatal("pending dial was not cancelled")
	}
	var b [1]byte
	if _, err := c.Read(b[:]); !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err=%v", err)
	}
}
func TestSuccessfulDialSurvivesDialContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	defer pr.Close()
	c := newClientConn(pw, func() {}, nil)
	cancelPendingDial(ctx, c)
	if !c.publish(io.NopCloser(&finiteReader{data: []byte("ok")}), nil) {
		t.Fatal("response not accepted")
	}
	cancel()
	var b [2]byte
	n, err := c.Read(b[:])
	if err != nil || n != 2 || string(b[:]) != "ok" {
		t.Fatalf("n=%d err=%v", n, err)
	}
	c.Close()
}

type finiteReader struct{ data []byte }

func (r *finiteReader) Read(b []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(b, r.data)
	r.data = r.data[n:]
	return n, nil
}
