package sbxhttp

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPathValidation(t *testing.T) {
	for _, tc := range []struct {
		path string
		ok   bool
	}{
		{"", true}, {"/sbxhttp", true}, {"/foo/bar", true},
		{"abc", false}, {"/../etc", false}, {"/foo?x=1", false}, {"/x#y", false}, {"/x%2f", false},
	} {
		_, err := cleanPath(tc.path)
		if (err == nil) != tc.ok {
			t.Fatalf("path=%q err=%v", tc.path, err)
		}
	}
}

func TestEarlyCloseUnblocksRead(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := newClientConn(pw, cancel, nil)
	done := make(chan error, 1)
	go func() { b := make([]byte, 1); _, err := c.Read(b); done <- err }()
	c.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("unexpected read error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read remained blocked after Close")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("request context not cancelled")
	}
}

func TestCloseDoesNotWaitForBlockedServerWrite(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	writer := writeFunc(func(b []byte) (int, error) { close(started); <-release; return len(b), nil })
	ctx, cancel := context.WithCancel(context.Background())
	c := &serverConn{reader: io.NopCloser(strings.NewReader("")), writer: writer, flush: func() error { return nil }, closeFn: cancel}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = c.Write([]byte("test")) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("writer not entered")
	}
	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked behind Write")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("Close did not cancel server context")
	}
	close(release)
	wg.Wait()
}

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(b []byte) (int, error) { return f(b) }

func TestClientPublishAndRead(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	_, cancel := context.WithCancel(context.Background())
	c := newClientConn(pw, cancel, nil)
	c.publish(io.NopCloser(strings.NewReader("hello")), nil)
	b := make([]byte, 5)
	n, err := c.Read(b)
	if err != nil || n != 5 || string(b) != "hello" {
		t.Fatalf("n=%d err=%v data=%q", n, err, b)
	}
	c.Close()
}
