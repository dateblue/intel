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
	select {
	case <-s.ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("FIN not received after final ACK")
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
