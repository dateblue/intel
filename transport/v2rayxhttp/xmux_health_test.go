package v2rayxhttp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
)

func TestXHTTPTransportDeathClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"cancel", context.Canceled, false},
		{"eof", io.EOF, false},
		{"deadline", context.DeadlineExceeded, true},
		{"unexpectedEOF", io.ErrUnexpectedEOF, true},
		{"closed", net.ErrClosed, true},
		{"fileClosed", os.ErrClosed, true},
		{"generic", errors.New("remote application error"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := xhttpTerminalTransportError(tc.err); got != tc.want {
				t.Fatalf("classification=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestXHTTPImmediateRetirementWithoutClosingActiveStream(t *testing.T) {
	manager, fakes := poolOf(t, xmuxConfig{maxConnections: intRange{1, 1}})
	old, _ := manager.get()
	old.addOpenUsage(1)
	old.noteTerminalFailure()
	if cause := old.evictCause(); cause != "failing" {
		t.Fatalf("cause=%q", cause)
	}
	manager.resetBackoff()
	newer, _ := manager.get()
	if newer == old {
		t.Fatal("reused a terminal connection")
	}
	if fakes()[0].closes() != 0 {
		t.Fatal("retirement killed an active stream")
	}
	old.addOpenUsage(-1)
	if fakes()[0].closes() != 1 {
		t.Fatal("did not release retired connection after last stream")
	}
}
