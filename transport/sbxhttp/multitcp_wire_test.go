package sbxhttp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

type shortWriter struct{ out bytes.Buffer }

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > 7 {
		p = p[:7]
	}
	return w.out.Write(p)
}
func TestMultiFramingShortWriter(t *testing.T) {
	w := new(shortWriter)
	in := bytes.Repeat([]byte("y"), multiChunk)
	if err := multiWriteFrame(w, multiFrame{offset: 123456789, data: in}); err != nil {
		t.Fatal(err)
	}
	read, err := multiReadFrame(bytes.NewReader(w.out.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if read.offset != 123456789 || !bytes.Equal(in, read.data) {
		t.Fatalf("frame mismatch %d", read.offset)
	}
}
func TestMultiFramingHeartbeatAndBounds(t *testing.T) {
	w := new(bytes.Buffer)
	if err := multiWriteFrame(w, multiFrame{}); err != nil {
		t.Fatal(err)
	}
	read, err := multiReadFrame(w)
	if err != nil || len(read.data) != 0 {
		t.Fatalf("bad heartbeat: %v", err)
	}
	if err := multiWriteFrame(io.Discard, multiFrame{data: make([]byte, multiChunk+1)}); err == nil {
		t.Fatal("accepted oversized frame")
	}
	h := make([]byte, 12)
	binary.BigEndian.PutUint32(h[8:], multiChunk+1)
	if _, err := multiReadFrame(bytes.NewReader(h)); err == nil {
		t.Fatal("accepted oversized inbound frame")
	}
}
func TestMultiFramingTruncation(t *testing.T) {
	w := new(bytes.Buffer)
	if err := multiWriteFrame(w, multiFrame{offset: 0, data: []byte("abcdef")}); err != nil {
		t.Fatal(err)
	}
	b := w.Bytes()
	_, err := multiReadFrame(bytes.NewReader(b[:len(b)-2]))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want unexpected EOF: %v", err)
	}
}
func TestMultiOutOfOrderAssembly(t *testing.T) {
	a := newMultiReassembler()
	for _, f := range []multiFrame{{offset: 5, data: []byte("World")}, {offset: 0, data: []byte("Hello")}, {offset: 10, data: []byte("!")}} {
		if err := a.insert(f); err != nil {
			t.Fatal(err)
		}
	}
	var out []byte
	for {
		data := a.pop()
		if data == nil {
			break
		}
		out = append(out, data...)
	}
	if string(out) != "HelloWorld!" || a.buffered != 0 || a.next != 11 {
		t.Fatalf("order failed: %s, buffered=%d, next=%d", out, a.buffered, a.next)
	}
}
func TestMultiDuplicateAndBounds(t *testing.T) {
	a := newMultiReassembler()
	if err := a.insert(multiFrame{offset: 0, data: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := a.insert(multiFrame{offset: 0, data: []byte("a")}); err == nil {
		t.Fatal("accepted duplicate")
	}
	if err := a.insert(multiFrame{offset: multiReorderLimit + 1, data: []byte("x")}); err == nil {
		t.Fatal("accepted too-distant data")
	}
	if s := a.pop(); string(s) != "a" {
		t.Fatal("pop failed")
	}
	if err := a.insert(multiFrame{offset: 0, data: []byte("a")}); err == nil {
		t.Fatal("accepted already delivered data")
	}
}
