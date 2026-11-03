package sbxhttp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestReplayFraming(t *testing.T) {
	var b bytes.Buffer
	if err := writeReplayFrame(&b, replayFrame{kind: frameData, offset: 42, data: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	f, err := readReplayFrame(&b)
	if err != nil || f.offset != 42 || string(f.data) != "hello" {
		t.Fatalf("%+v %v", f, err)
	}
	bad := make([]byte, 13)
	bad[0] = frameData
	binary.BigEndian.PutUint32(bad[9:], frameLimit+1)
	if _, err = readReplayFrame(bytes.NewReader(bad)); err == nil {
		t.Fatal("oversized frame accepted")
	}
}
func TestReplayBufferACK(t *testing.T) {
	b := newReplayBuffer()
	if err := b.put(context.Background(), []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	pos, data := b.sliceAt(0)
	if pos != 0 || string(data) != "abcdef" {
		t.Fatal(pos, string(data))
	}
	if err := b.ack(3); err != nil {
		t.Fatal(err)
	}
	pos, data = b.sliceAt(0)
	if pos != 3 || string(data) != "def" {
		t.Fatal(pos, string(data))
	}
	if err := b.ack(7); err == nil {
		t.Fatal("invalid ACK accepted")
	}
}
func TestReplaySessionAcrossTwoPhysicalConnections(t *testing.T) {
	c := newReliableSession(context.Background())
	defer c.Close()
	s := newReliableSession(context.Background())
	defer s.Close()
	appC := c.conn()
	appS := s.conn()
	x, y := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	go c.connectReplayFrames(ctx, x)
	go s.connectReplayFrames(ctx, y)
	payload := bytes.Repeat([]byte("abcdefgh"), 4096)
	done := make(chan error, 1)
	go func() { _, err := appC.Write(payload); done <- err }()
	out := make([]byte, len(payload))
	if _, err := io.ReadFull(appS, out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, out) {
		t.Fatal("payload corrupted")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer blocked")
	}
	cancel()
	x.Close()
	y.Close()
	// A fresh socket must not duplicate bytes already consumed by appS.
	time.Sleep(10 * time.Millisecond)
	x2, y2 := net.Pipe()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go c.connectReplayFrames(ctx2, x2)
	go s.connectReplayFrames(ctx2, y2)
	data2 := []byte("resume")
	go appC.Write(data2)
	got := make([]byte, len(data2))
	if _, err := io.ReadFull(appS, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data2, got) {
		t.Fatalf("got %q", got)
	}
}

func TestReplayDuplicateAndPartialOverlap(t *testing.T) {
	s := newReliableSession(context.Background())
	defer s.Close()
	app := s.conn()
	first := make(chan error, 1)
	go func() { first <- s.acceptData(0, []byte("abcd")) }()
	data := make([]byte, 4)
	if _, err := io.ReadFull(app, data); err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcd" {
		t.Fatal(string(data))
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := s.acceptData(0, []byte("abcd")); err != nil {
		t.Fatal(err)
	}
	next := make(chan error, 1)
	go func() { next <- s.acceptData(2, []byte("cdef")) }()
	data2 := make([]byte, 2)
	if _, err := io.ReadFull(app, data2); err != nil {
		t.Fatal(err)
	}
	if string(data2) != "ef" {
		t.Fatal(string(data2))
	}
	if err := <-next; err != nil {
		t.Fatal(err)
	}
	if s.ackOffset() != 6 {
		t.Fatalf("ack=%d", s.ackOffset())
	}
	if err := s.acceptData(8, []byte("gap")); err == nil {
		t.Fatal("sequence gap accepted")
	}
}

func TestReplayBufferCapacityAndBackpressure(t *testing.T) {
	q := newReplayBuffer()
	if err := q.put(context.Background(), make([]byte, replayCapacity)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- q.put(ctx, []byte("rest")) }()
	select {
	case <-done:
		t.Fatal("write should block on full replay buffer")
	case <-time.After(20 * time.Millisecond):
	}
	if err := q.ack(4); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ACK did not release backpressure")
	}
}

func TestReplayFinalBytesBeforeFIN(t *testing.T) {
	c := newReliableSession(context.Background())
	defer c.Close()
	s := newReliableSession(context.Background())
	defer s.Close()
	client := c.conn()
	server := s.conn()
	a, b := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.connectReplayFrames(ctx, a)
	go s.connectReplayFrames(ctx, b)
	data := []byte("last bytes must be delivered before FIN")
	writeDone := make(chan struct{})
	go func() { _, _ = client.Write(data); client.Close(); close(writeDone) }()
	got := make([]byte, len(data))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, got) {
		t.Fatal("last bytes corrupted")
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("logical close hung")
	}
	// A FIN from the client ends the server's receive side but must not
	// destroy the entire logical session, which may still send a reply.
	eof := make(chan error, 1)
	go func() { var one [1]byte; _, err := server.Read(one[:]); eof <- err }()
	select {
	case err := <-eof:
		if err != io.EOF {
			t.Fatalf("expected EOF after FIN, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("application read did not see FIN as EOF")
	}
	if s.ctx.Err() != nil {
		t.Fatal("FIN closed whole logical session")
	}
}

func TestPhysicalResetPreservesLogicalSession(t *testing.T) {
	s := newReliableSession(context.Background())
	defer s.Close()
	physicalCtx, cancel := context.WithCancel(context.Background())
	s.setPhysicalCancel(cancel)
	s.resetPhysical()
	select {
	case <-physicalCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("socket not reset")
	}
	if s.ctx.Err() != nil {
		t.Fatal("logical session was destroyed by interface reset")
	}
}

// Under receiver backpressure, the send direction must still be able to
// observe the last committed ACK. Previously acceptData held rxMu across
// net.Pipe.Write, creating a potential bidirectional deadlock.
func TestReplayACKReadNotBlockedBySlowConsumer(t *testing.T) {
	s := newReliableSession(context.Background())
	defer s.Close()
	writeDone := make(chan error, 1)
	go func() { writeDone <- s.acceptData(0, []byte("x")) }()
	// Ensure the write has reached net.Pipe before checking the short lock.
	time.Sleep(15 * time.Millisecond)
	ackDone := make(chan uint64, 1)
	go func() { ackDone <- s.ackOffset() }()
	select {
	case got := <-ackDone:
		if got != 0 {
			t.Fatalf("ACK before delivery: %d", got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ackOffset blocked on stalled logical reader")
	}
	// Release blocked write by consuming one byte.
	got := make([]byte, 1)
	if _, err := io.ReadFull(s.conn(), got); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not finish")
	}
	if gotAck := s.ackOffset(); gotAck != 1 {
		t.Fatalf("ACK after delivery: %d", gotAck)
	}
}

// After VLESS has completed, the peer may have queued more request DATA
// concurrently with the response. Closing the logical app must not make
// those late bytes destroy an HTTP/2 downlink which still has response data.
func TestReplayLateUplinkAfterAppClose(t *testing.T) {
	s := newReliableSession(context.Background())
	defer s.Close()
	s.finishApp()
	if err := s.acceptData(0, []byte("late-data")); err != nil {
		t.Fatalf("late uplink caused physical attempt failure: %v", err)
	}
	if got := s.ackOffset(); got != uint64(len("late-data")) {
		t.Fatalf("unexpected ACK for retired uplink: %d", got)
	}
	// Duplicate frames are still safely ignored and a genuine future gap
	// remains invalid; do not disable replay sequence validation.
	if err := s.acceptData(0, []byte("late-data")); err != nil {
		t.Fatalf("duplicate late data: %v", err)
	}
	if err := s.acceptData(11, []byte("bad")); err == nil {
		t.Fatal("sequence gap accepted after app close")
	}
}

func TestReplayCloseDuringBlockedUplink(t *testing.T) {
	s := newReliableSession(context.Background())
	defer s.Close()
	errCh := make(chan error, 1)
	go func() { errCh <- s.acceptData(0, []byte("pending")) }()
	// The logical app intentionally does not consume pending bytes.
	time.Sleep(15 * time.Millisecond)
	s.finishApp()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("concurrent app close leaked to physical attempt: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending uplink remained blocked after app close")
	}
	if got := s.ackOffset(); got != uint64(len("pending")) {
		t.Fatalf("partial/incorrect retired ACK: %d", got)
	}
}

// A resumed physical connection begins with sent=0, but the session may
// already have cumulatively ACKed its entire final payload on a previous
// connection. FIN must carry that cumulative offset, not the local per-attempt
// sent cursor (zero), or the receiver rejects it as a sequence gap.
func TestReplayFINAfterFullyAckedResume(t *testing.T) {
	s := newReliableSession(context.Background())
	defer s.Close()
	const response = "prior-response"
	if err := s.tx.put(context.Background(), []byte(response)); err != nil {
		t.Fatal(err)
	}
	if err := s.tx.ack(uint64(len(response))); err != nil {
		t.Fatal(err)
	}
	s.tx.done()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	var wire bytes.Buffer
	_ = s.sendFrames(ctx, &wire, func() error { return nil })
	sawFIN := false
	for wire.Len() > 0 {
		frame, err := readReplayFrame(&wire)
		if err != nil {
			t.Fatal(err)
		}
		if frame.kind == frameFin {
			sawFIN = true
			if frame.offset != uint64(len(response)) {
				t.Fatalf("resumed FIN offset=%d, want %d", frame.offset, len(response))
			}
		}
	}
	if !sawFIN {
		t.Fatal("FIN was not sent")
	}
}

// A peer FIN terminates only its upload; its response may still be queued
// in the opposite direction. The logical session must stay alive.
func TestReplayPeerFINDoesNotAbortResponse(t *testing.T) {
	s := newReliableSession(context.Background())
	defer s.Close()
	if err := s.tx.put(context.Background(), []byte("pending-response")); err != nil {
		t.Fatal(err)
	}
	var fin bytes.Buffer
	if err := writeReplayFrame(&fin, replayFrame{kind: frameFin, offset: 0}); err != nil {
		t.Fatal(err)
	}
	_ = s.recvFrames(&fin)
	if s.ctx.Err() != nil {
		t.Fatal("peer FIN canceled pending response")
	}
}

// A complete two-direction exchange must deliver the response after upload EOF.
// The physical transport is an in-memory full-duplex pipe, so this tests the
// Replay sequence and logical EOF behavior without TCP/Reality variables.
func TestReplayHalfCloseDeliversResponse(t *testing.T) {
	c := newReliableSession(context.Background())
	defer c.Close()
	s := newReliableSession(context.Background())
	defer s.Close()
	x, y := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go c.connectReplayFrames(ctx, x)
	go s.connectReplayFrames(ctx, y)
	client, server := c.conn(), s.conn()
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := c.tx.put(ctx, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	c.tx.done()
	var req [4]byte
	if _, err := io.ReadFull(server, req[:]); err != nil {
		t.Fatalf("server upload: %v", err)
	}
	if string(req[:]) != "ping" {
		t.Fatalf("request=%q", req)
	}
	var one [1]byte
	if _, err := server.Read(one[:]); err != io.EOF {
		t.Fatalf("expected upload EOF, got %v", err)
	}
	sendDone := make(chan error, 1)
	go func() { _, err := server.Write([]byte("pong")); _ = server.Close(); sendDone <- err }()
	var reply [4]byte
	if _, err := io.ReadFull(client, reply[:]); err != nil {
		t.Fatalf("client download: %v", err)
	}
	if string(reply[:]) != "pong" {
		t.Fatalf("response=%q", reply)
	}
	if err := <-sendDone; err != nil {
		t.Fatalf("server reply write: %v", err)
	}
}
