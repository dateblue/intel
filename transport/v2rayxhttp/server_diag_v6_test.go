//go:build with_xhttp

package v2rayxhttp

import (
	"errors"
	"net/http/httptest"
	"testing"
)

type v6FaultFlushWriter struct {
	*httptest.ResponseRecorder
	problem error
}

func (w *v6FaultFlushWriter) FlushError() error { return w.problem }

func TestV6DiagnosticWrapperPreservesFlushError(t *testing.T) {
	expected := errors.New("flow-control stream reset")
	inner := &v6FaultFlushWriter{ResponseRecorder: httptest.NewRecorder(), problem: expected}
	w := &xhttpSlowResponseWriter{ResponseWriter: inner, flusher: inner}
	if err := w.FlushError(); !errors.Is(err, expected) {
		t.Fatalf("FlushError=%v; expected remote stream failure", err)
	}
}
