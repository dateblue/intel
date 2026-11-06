package sbxhttp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCDNFrameAndHeartbeat(t *testing.T) {
	rec := httptest.NewRecorder()
	c := &cdnServerConn{writer: rec, flush: func() error { return nil }, lastWrite: time.Now().Add(-time.Minute)}
	input := bytes.Repeat([]byte("p"), cdnMaxFrame*2+17)
	n, err := c.Write(input)
	if err != nil || n != len(input) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	c.lastWrite = time.Now().Add(-time.Minute)
	if err := c.heartbeat(); err != nil {
		t.Fatal(err)
	}
	r := bytes.NewReader(rec.Body.Bytes())
	var got []byte
	heartbeats := 0
	for r.Len() > 0 {
		block, err := cdnReadFrame(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(block) == 0 {
			heartbeats++
			continue
		}
		got = append(got, block...)
	}
	if !bytes.Equal(got, input) || heartbeats != 1 {
		t.Fatalf("corrupt frames: bytes=%d heartbeats=%d", len(got), heartbeats)
	}
}
func TestCDNRejectOversizedFrame(t *testing.T) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, cdnMaxFrame+1)
	if _, err := cdnReadFrame(bytes.NewReader(b)); err == nil {
		t.Fatal("accepted oversized frame")
	}
	if err := cdnWriteFrame(io.Discard, make([]byte, cdnMaxFrame+1)); err == nil {
		t.Fatal("accepted oversized write")
	}
}
func TestCDNSessionID(t *testing.T) {
	id, err := cdnNewID()
	if err != nil || !cdnValidID(id) {
		t.Fatalf("id=%q err=%v", id, err)
	}
	if cdnValidID(strings.Repeat("x", 32)) {
		t.Fatal("accepted non-hex id")
	}
	if cdnValidID(id[:30]) {
		t.Fatal("accepted truncated id")
	}
}

func TestCDNPacketReorder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	s := &cdnServerSession{ctx: ctx, cancel: cancel, uploadReader: reader, uploadWriter: writer, pending: map[uint64][]byte{}, conn: &cdnServerConn{reader: reader, cancel: cancel}}
	s.packetCond = sync.NewCond(&s.packetMu)
	defer s.Close()
	go s.assemblePackets()
	s.packetMu.Lock()
	s.pending[1] = []byte("world")
	s.packetCond.Signal()
	s.packetMu.Unlock()
	s.packetMu.Lock()
	s.pending[0] = []byte("hello")
	s.packetCond.Signal()
	s.packetMu.Unlock()
	result := make(chan string, 1)
	go func() {
		b := make([]byte, 10)
		_, err := io.ReadFull(reader, b)
		if err != nil {
			result <- err.Error()
			return
		}
		result <- string(b)
	}()
	select {
	case got := <-result:
		if got != "helloworld" {
			t.Fatalf("out of order: %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("reassembler stalled")
	}
}
