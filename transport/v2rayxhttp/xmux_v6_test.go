package v2rayxhttp

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// The production acquisition must account for a stream BEFORE another caller
// can check max_concurrency, even if 100 DialContext calls begin together.
func TestV6AtomicXMUXAdmission(t *testing.T) {
	m, _ := poolOf(t, xmuxConfig{maxConcurrency: intRange{2, 2}})
	const users = 96
	ready := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, users)
	var wg sync.WaitGroup
	for i := 0; i < users; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			c, err := m.getContext(context.Background())
			if err != nil {
				errCh <- err
				return
			}
			// Hold our reservation across the concurrent admission burst.
			<-release
			newXmuxRelease(c).release()
		}()
	}
	close(ready)
	// Wait for every reservation by polling manager bookkeeping. This loop
	// terminates quickly even on busy CI, and fails cleanly if scheduling hangs.
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.access.Lock()
		count := 0
		over := false
		for _, c := range m.clients {
			n := c.getOpenUsage()
			count += n
			if n > 2 {
				over = true
			}
		}
		m.access.Unlock()
		if over {
			close(release)
			wg.Wait()
			t.Fatal("max_concurrency was exceeded")
		}
		if count == users {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			wg.Wait()
			t.Fatalf("only %d/%d reservations completed", count, users)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	m.access.Lock()
	defer m.access.Unlock()
	for _, c := range m.clients {
		if n := c.getOpenUsage(); n != 0 {
			t.Fatalf("leaked %d stream reservations", n)
		}
	}
}

func TestV6PoolPrefersLessLoadedHealthyConnection(t *testing.T) {
	m, _ := poolOf(t, xmuxConfig{maxConnections: intRange{2, 2}})
	busy, _ := m.get()
	idle, _ := m.get()
	busy.addOpenUsage(25)
	idle.addOpenUsage(1)
	got, _ := m.getReserved()
	if got != idle {
		t.Fatal("new stream assigned to the busy HTTP/2 pool")
	}
	newXmuxRelease(got).release()
	busy.addOpenUsage(-25)
	idle.addOpenUsage(-1)
}

func TestV6PoolAbortOnlyOnDefinitiveTransportLoss(t *testing.T) {
	transport := &http2XmuxConn{transport: &http2.Transport{}}
	a, b := net.Pipe()
	defer b.Close()
	tracked, err := transport.trackSocket(a)
	if err != nil {
		t.Fatal(err)
	}
	if xhttpDefinitiveTransportLoss(context.DeadlineExceeded) {
		t.Fatal("per-request deadline must not kill a physical socket")
	}
	if !xhttpDefinitiveTransportLoss(&net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}) {
		t.Fatal("TCP read timeout must be definitive")
	}
	if !xhttpDefinitiveTransportLoss(errors.New("http2: client conn is closed")) {
		t.Fatal("H2 dead-client signal not classified")
	}
	transport.abortDead()
	if !transport.IsClosed() {
		t.Fatal("dead transport remains reusable")
	}
	if _, err := tracked.Write([]byte("x")); err == nil {
		t.Fatal("known-dead socket survived abort")
	}
	transport.socketMu.Lock()
	outstanding := len(transport.sockets)
	transport.socketMu.Unlock()
	if outstanding != 0 {
		t.Fatalf("%d sockets leaked", outstanding)
	}
	transport.abortDead() // idempotent
}

func TestV6DeadPoolSuccessCannotDisarmRecovery(t *testing.T) {
	m, _ := poolOf(t, xmuxConfig{})
	c, _ := m.get()
	c.noteTerminalFailure()
	if !m.backoffArmed.Load() {
		t.Fatal("breaker not armed")
	}
	c.noteSuccess() // a racing read from a different stream on same pool
	if !m.backoffArmed.Load() {
		t.Fatal("a condemned pool disarmed the global backoff")
	}
}

func TestV6BackoffResetRequiresCurrentGeneration(t *testing.T) {
	m, _ := poolOf(t, xmuxConfig{maxConnections: intRange{2, 2}})
	old, _ := m.get()
	m.noteBreakerTrip()
	old.noteSuccess()
	if !m.backoffArmed.Load() {
		t.Fatal("old connection wrongly canceled recovery backoff")
	}
	replacement, _ := m.get()
	if replacement == old {
		t.Fatal("expected second resource for recovery")
	}
	replacement.noteSuccess()
	if m.backoffArmed.Load() {
		t.Fatal("healthy replacement did not reset backoff")
	}
}
