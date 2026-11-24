package sbxhttp

import (
	"bytes"
	"fmt"
	"testing"
)

// Simulate six optional paths. Paths 1..5 fail with their assigned frames
// missing. The single surviving path can supply all gaps using the original
// absolute offsets. Reassembler must emit exact-once ordered bytes.
func TestMultiResilientFiveFailedOneSurvives(t *testing.T) {
	original := make([]byte, 6*13*multiChunk)
	for i := range original {
		original[i] = byte(i*37 + 17)
	}
	a := newMultiReassembler()
	var delivered bytes.Buffer
	missing := make([]multiFrame, 0)
	for offset := 0; offset < len(original); offset += multiChunk {
		f := multiFrame{offset: uint64(offset), data: append([]byte(nil), original[offset:offset+multiChunk]...)}
		if offset/multiChunk%6 == 0 { // sole surviving lane
			if err := a.insertResilient(f); err != nil {
				t.Fatal(err)
			}
		} else {
			missing = append(missing, f)
		}
		for {
			p := a.pop()
			if p == nil {
				break
			}
			delivered.Write(p)
		}
	}
	for _, f := range missing {
		if err := a.insertResilient(f); err != nil {
			t.Fatal(err)
		}
		if err := a.insertResilient(f); err != nil {
			t.Fatalf("duplicate before delivery: %v", err)
		}
		for {
			p := a.pop()
			if p == nil {
				break
			}
			delivered.Write(p)
		}
	}
	if !bytes.Equal(delivered.Bytes(), original) {
		t.Fatalf("only one healthy lane did not recover %d bytes", len(original))
	}
	if err := a.insertResilient(multiFrame{offset: 0, data: original[:multiChunk]}); err != nil {
		t.Fatal("delivered replay should be ignored:", err)
	}
}

func TestMultiResilientCorruptDuplicateRejected(t *testing.T) {
	a := newMultiReassembler()
	f := multiFrame{offset: multiChunk, data: bytes.Repeat([]byte{0x31}, multiChunk)}
	if err := a.insertResilient(f); err != nil {
		t.Fatal(err)
	}
	duplicate := multiFrame{offset: f.offset, data: bytes.Repeat([]byte{0x32}, multiChunk)}
	if err := a.insertResilient(duplicate); err == nil {
		t.Fatal("accepted corrupt duplicate")
	}
}

func TestMultiResilientBoundedReorder(t *testing.T) {
	a := newMultiReassembler()
	if err := a.insertResilient(multiFrame{offset: uint64(multiReorderLimit) + 1, data: []byte("x")}); err == nil {
		t.Fatal("accepted beyond the reorder distance bound")
	}
	for i := 1; i <= multiReorderLimit/multiChunk; i++ {
		f := multiFrame{offset: uint64(i * multiChunk), data: make([]byte, multiChunk)}
		if err := a.insertResilient(f); err != nil {
			t.Fatal(fmt.Errorf("i=%d: %w", i, err))
		}
	}
	if err := a.insertResilient(multiFrame{offset: 0, data: []byte("x")}); err == nil {
		t.Fatal("accepted reorder buffer overflow")
	}
}
