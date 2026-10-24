package v2rayhttp

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestV6ServerHTTPConnPropagatesFlushFailure(t *testing.T) {
	expected := errors.New("HTTP/2 remote stream closed")
	var out bytes.Buffer
	conn := &ServerHTTPConn{
		HTTP2Conn:  NewHTTPConn(strings.NewReader(""), &out),
		FlushError: func() error { return expected },
	}
	n, err := conn.Write([]byte("payload"))
	if n != 7 || err == nil {
		t.Fatalf("Write = (%d,%v), expected payload bytes and flush failure", n, err)
	}
}
