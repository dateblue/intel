package sbxhttp

// Standalone, standard-library-only MultiTCP frame and reassembly machinery.
// Kept separate so it can be regression-tested with stock Go without the
// sing-box dependency graph.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	multiChunk        = 32 * 1024
	multiReorderLimit = 4 << 20
)

type multiFrame struct {
	offset uint64
	data   []byte
}

func multiWriteFrame(w io.Writer, f multiFrame) error {
	if len(f.data) > multiChunk {
		return fmt.Errorf("sbxhttp multitcp: frame too big %d", len(f.data))
	}
	var h [12]byte
	binary.BigEndian.PutUint64(h[:8], f.offset)
	binary.BigEndian.PutUint32(h[8:12], uint32(len(f.data)))
	if err := multiWriteAll(w, h[:]); err != nil {
		return err
	}
	return multiWriteAll(w, f.data)
}
func multiWriteAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func multiReadFrame(r io.Reader) (multiFrame, error) {
	var h [12]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return multiFrame{}, err
	}
	size := binary.BigEndian.Uint32(h[8:12])
	if size > multiChunk {
		return multiFrame{}, fmt.Errorf("sbxhttp multitcp: oversized frame %d", size)
	}
	f := multiFrame{offset: binary.BigEndian.Uint64(h[:8])}
	if size == 0 {
		return f, nil
	} // heartbeat
	f.data = make([]byte, size)
	_, err := io.ReadFull(r, f.data)
	return f, err
}

// Ordered assembly is deliberately bounded. It never emits out-of-order data
// into VLESS/ENC/Vision. Blocking on the application reader naturally creates
// per-lane H2 flow-control backpressure through the bounded channel.
type multiReassembler struct {
	next     uint64
	buffered int
	pending  map[uint64][]byte
}

func newMultiReassembler() *multiReassembler {
	return &multiReassembler{pending: make(map[uint64][]byte)}
}
func (a *multiReassembler) insert(f multiFrame) error {
	if len(f.data) == 0 {
		return nil
	}
	if f.offset < a.next {
		return fmt.Errorf("sbxhttp multitcp: stale/overlapping offset %d < %d", f.offset, a.next)
	}
	if f.offset-a.next > multiReorderLimit {
		return fmt.Errorf("sbxhttp multitcp: reorder distance exceeds %d", multiReorderLimit)
	}
	if a.buffered+len(f.data) > multiReorderLimit {
		return errors.New("sbxhttp multitcp: reorder buffer full")
	}
	if _, exists := a.pending[f.offset]; exists {
		return errors.New("sbxhttp multitcp: duplicate frame offset")
	}
	a.pending[f.offset] = f.data
	a.buffered += len(f.data)
	return nil
}

// insertResilient accepts an exact retransmission, but still rejects corrupt
// partial overlaps and inconsistent duplicate payloads. Used only by CDN v2.
func (a *multiReassembler) insertResilient(f multiFrame) error {
	if len(f.data) == 0 {
		return nil
	}
	end := f.offset + uint64(len(f.data))
	if end < f.offset {
		return errors.New("sbxhttp multitcp: offset overflow")
	}
	if f.offset < a.next {
		if end <= a.next {
			return nil
		} // complete duplicate already delivered
		return fmt.Errorf("sbxhttp multitcp: partial retransmit overlap %d < %d", f.offset, a.next)
	}
	if existing, ok := a.pending[f.offset]; ok {
		if len(existing) != len(f.data) {
			return errors.New("sbxhttp multitcp: duplicate length mismatch")
		}
		for i := range existing {
			if existing[i] != f.data[i] {
				return errors.New("sbxhttp multitcp: duplicate content mismatch")
			}
		}
		return nil
	}
	return a.insert(f)
}

func (a *multiReassembler) pop() []byte {
	data, ok := a.pending[a.next]
	if !ok {
		return nil
	}
	delete(a.pending, a.next)
	a.next += uint64(len(data))
	a.buffered -= len(data)
	return data
}
