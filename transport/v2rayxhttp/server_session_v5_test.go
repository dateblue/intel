//go:build with_xhttp

package v2rayxhttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestServerV5PacketOutOfOrder(t *testing.T) {
	q := newServerUploadQueue(4)
	defer q.Close()
	if !q.PushPacket(2, []byte("CCC")) || !q.PushPacket(1, []byte("BBB")) || !q.PushPacket(0, []byte("AAAA")) {
		t.Fatal("push failed")
	}
	buf := make([]byte, 3)
	var got []byte
	for len(got) < 10 {
		n, e := q.Read(buf)
		if e != nil {
			t.Fatal(e)
		}
		got = append(got, buf[:n]...)
	}
	if !bytes.Equal(got, []byte("AAAABBBCCC")) {
		t.Fatalf("read %q", got)
	}
}
func TestServerV5PacketBackpressureAndMissingSequence(t *testing.T) {
	q := newServerUploadQueue(3)
	defer q.Close()
	if !q.PushPacket(3, []byte("three")) || !q.PushPacket(4, []byte("four")) {
		t.Fatal("initial pushes failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := q.PushPacketContext(ctx, 5, []byte("five")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queue should wait for capacity, got %v", err)
	}
	if !q.PushPacket(3, []byte("duplicate")) {
		t.Fatal("duplicate must be idempotent")
	}
	if !q.PushPacket(0, []byte("first")) {
		t.Fatal("next sequence was starved by out-of-order packets")
	}
	buf := make([]byte, 10)
	n, err := q.Read(buf)
	if err != nil || string(buf[:n]) != "first" {
		t.Fatalf("read n=%d err=%v body=%q", n, err, buf[:n])
	}
}
func TestServerV5CloseUnblocksRead(t *testing.T) {
	q := newServerUploadQueue(3)
	result := make(chan error, 1)
	go func() { var data [16]byte; _, e := q.Read(data[:]); result <- e }()
	q.Close()
	select {
	case e := <-result:
		if e != io.EOF {
			t.Fatalf("got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("reader not unblocked")
	}
}
func TestServerV5StreamUpload(t *testing.T) {
	q := newServerUploadQueue(3)
	defer q.Close()
	if !q.PushStream(bytes.NewBufferString("upload")) {
		t.Fatal("stream push")
	}
	if q.PushStream(bytes.NewBuffer(nil)) {
		t.Fatal("double stream push")
	}
	if q.PushPacket(0, []byte("no")) {
		t.Fatal("packet accepted after stream")
	}
	data := make([]byte, 6)
	n, e := q.Read(data)
	if e != nil || string(data[:n]) != "upload" {
		t.Fatalf("read n=%d err=%v %q", n, e, data[:n])
	}
}
func TestServerV5SessionSingleDownload(t *testing.T) {
	s := newServerSession(3)
	if !s.BeginDownload() {
		t.Fatal("initial GET rejected")
	}
	if s.BeginDownload() {
		t.Fatal("duplicate GET accepted")
	}
	s.Close()
	select {
	case <-s.finished:
	default:
		t.Fatal("not closed")
	}
	if s.BeginDownload() {
		t.Fatal("closed session restarted")
	}
}

func TestServerV5PacketModeCannotBecomeStream(t *testing.T) {
	q := newServerUploadQueue(3)
	defer q.Close()
	if !q.PushPacket(0, []byte("one")) {
		t.Fatal("push packet")
	}
	b := make([]byte, 3)
	if n, e := q.Read(b); e != nil || n != 3 {
		t.Fatalf("read n=%d err=%v", n, e)
	}
	if q.PushStream(bytes.NewBufferString("stream")) {
		t.Fatal("must reject a stream after packet-up started")
	}
}
func TestServerV5CloseCancelsPendingPacket(t *testing.T) {
	q := newServerUploadQueue(2)
	if !q.PushPacket(2, []byte("two")) {
		t.Fatal("first packet")
	}
	result := make(chan error, 1)
	go func() { result <- q.PushPacketContext(context.Background(), 3, []byte("three")) }()
	q.Close()
	select {
	case e := <-result:
		if !errors.Is(e, io.ErrClosedPipe) {
			t.Fatalf("waiter error=%v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("pending packet did not stop")
	}
}
