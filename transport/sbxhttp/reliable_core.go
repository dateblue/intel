package sbxhttp

// Experimental SB-XHTTP v2: bounded, bidirectional byte-offset replay across
// new H2+REALITY sockets. VLESS itself still sees exactly one net.Conn.
// Wire format is private and NOT Xray XHTTP / VLESS Encryption.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	reliableVersion      = "2"
	replayCapacity       = 4 << 20
	frameLimit           = 16 << 10
	frameData       byte = 1
	frameAck        byte = 2
	frameFin        byte = 3
)

var errSequenceGap = errors.New("sbxhttp: out-of-order data outside replay window")

type replayBuffer struct {
	mu      sync.Mutex
	data    []byte
	start   uint64
	eof     bool
	changed chan struct{}
	space   chan struct{}
}

func newReplayBuffer() *replayBuffer {
	return &replayBuffer{changed: make(chan struct{}, 1), space: make(chan struct{}, 1)}
}
func (q *replayBuffer) signal() {
	select {
	case q.changed <- struct{}{}:
	default:
	}
}
func (q *replayBuffer) put(ctx context.Context, b []byte) error {
	for len(b) > 0 {
		q.mu.Lock()
		capacity := replayCapacity - len(q.data)
		if capacity > len(b) {
			capacity = len(b)
		}
		if capacity > 0 {
			q.data = append(q.data, b[:capacity]...)
			b = b[capacity:]
			q.signal()
		}
		q.mu.Unlock()
		if capacity == 0 {
			select {
			case <-q.space:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}
func (q *replayBuffer) done() { q.mu.Lock(); q.eof = true; q.signal(); q.mu.Unlock() }
func (q *replayBuffer) ack(n uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if n < q.start {
		return nil
	}
	if n > q.start+uint64(len(q.data)) {
		return fmt.Errorf("sbxhttp: invalid ACK %d (buffer [%d,%d])", n, q.start, q.start+uint64(len(q.data)))
	}
	size := int(n - q.start)
	if size > 0 {
		copy(q.data, q.data[size:])
		q.data = q.data[:len(q.data)-size]
		q.start = n
		q.signal()
		select {
		case q.space <- struct{}{}:
		default:
		}
	}
	return nil
}
func (q *replayBuffer) sliceAt(n uint64) (uint64, []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if n < q.start {
		n = q.start
	}
	end := q.start + uint64(len(q.data))
	if n >= end {
		return n, nil
	}
	count := int(end - n)
	if count > frameLimit {
		count = frameLimit
	}
	data := append([]byte(nil), q.data[int(n-q.start):int(n-q.start)+count]...)
	return n, data
}
func (q *replayBuffer) eofAndDrained() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.eof && len(q.data) == 0
}
func (q *replayBuffer) pending() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.data) }

// v2 frames are (kind:1, offset:8, length:4, payload:length).
// One byte stream maps to one continuous offset space per direction.
type replayFrame struct {
	kind   byte
	offset uint64
	data   []byte
}

func writeReplayFrame(w io.Writer, f replayFrame) error {
	if len(f.data) > frameLimit {
		return errors.New("sbxhttp: frame too large")
	}
	if f.kind != frameAck && f.kind != frameData && f.kind != frameFin {
		return errors.New("sbxhttp: invalid frame type")
	}
	if (f.kind == frameAck || f.kind == frameFin) && len(f.data) != 0 {
		return errors.New("sbxhttp: ACK must have no payload")
	}
	var h [13]byte
	h[0] = f.kind
	binary.BigEndian.PutUint64(h[1:9], f.offset)
	binary.BigEndian.PutUint32(h[9:13], uint32(len(f.data)))
	if n, err := w.Write(h[:]); err != nil {
		return err
	} else if n != len(h) {
		return io.ErrShortWrite
	}
	if len(f.data) > 0 {
		n, err := w.Write(f.data)
		if err == nil && n != len(f.data) {
			return io.ErrShortWrite
		}
		return err
	}
	return nil
}
func readReplayFrame(r io.Reader) (replayFrame, error) {
	var h [13]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return replayFrame{}, err
	}
	n := binary.BigEndian.Uint32(h[9:13])
	if n > frameLimit {
		return replayFrame{}, errors.New("sbxhttp: oversized frame")
	}
	f := replayFrame{kind: h[0], offset: binary.BigEndian.Uint64(h[1:9])}
	if f.kind != frameData && f.kind != frameAck && f.kind != frameFin {
		return replayFrame{}, errors.New("sbxhttp: invalid frame kind")
	}
	if (f.kind == frameAck || f.kind == frameFin) && n != 0 {
		return replayFrame{}, errors.New("sbxhttp: invalid ACK length")
	}
	if n > 0 {
		f.data = make([]byte, n)
		if _, err := io.ReadFull(r, f.data); err != nil {
			return replayFrame{}, err
		}
	}
	return f, nil
}

type reliableSession struct {
	ctx            context.Context
	cancel         context.CancelFunc
	app            net.Conn
	peer           net.Conn
	tx             *replayBuffer
	rxMu           sync.Mutex
	rxWriteMu      sync.Mutex // serialize peer writes without blocking ACK reads
	rx             uint64
	ackChanged     chan struct{}
	closed         atomic.Bool
	closeOnce      sync.Once
	finishOnce     sync.Once
	lastDetached   atomic.Int64
	transitionMu   sync.Mutex
	attachMu       sync.Mutex
	physicalMu     sync.Mutex
	physicalCancel context.CancelFunc
	activeCancel   context.CancelFunc
	activeDone     chan struct{}
}

func newReliableSession(parent context.Context) *reliableSession {
	ctx, cancel := context.WithCancel(parent)
	app, peer := net.Pipe()
	s := &reliableSession{ctx: ctx, cancel: cancel, app: app, peer: peer, tx: newReplayBuffer(), ackChanged: make(chan struct{}, 1)}
	go s.pump()
	return s
}
func (s *reliableSession) conn() net.Conn { return &reliableAppConn{Conn: s.app, s: s} }

type reliableAppConn struct {
	net.Conn
	s *reliableSession
}

func (c *reliableAppConn) Close() error                   { c.s.finishApp(); return nil }
func (*reliableAppConn) NeedAdditionalReadDeadline() bool { return true }

// Complete the VLESS application side without discarding bytes already
// queued for HTTP/2 retransmission. An upper bound prevents stale sessions.
func (s *reliableSession) finishApp() {
	s.finishOnce.Do(func() {
		_ = s.app.Close()
		time.AfterFunc(8*time.Second, s.Close)
	})
}

// Change the physical connection without discarding this logical VLESS
// session. Called on interface updates; only the H2 attempt is cancelled.
func (s *reliableSession) setPhysicalCancel(cancel context.CancelFunc) {
	s.physicalMu.Lock()
	s.physicalCancel = cancel
	s.physicalMu.Unlock()
}
func (s *reliableSession) clearPhysicalCancel() {
	s.physicalMu.Lock()
	s.physicalCancel = nil
	s.physicalMu.Unlock()
}
func (s *reliableSession) resetPhysical() {
	s.physicalMu.Lock()
	cancel := s.physicalCancel
	s.physicalMu.Unlock()
	if cancel != nil {
		cancel()
	}
}
func (s *reliableSession) Close() {
	s.closeOnce.Do(func() { s.closed.Store(true); s.cancel(); _ = s.peer.Close(); _ = s.app.Close(); s.tx.signal() })
}
func (s *reliableSession) pump() {
	b := make([]byte, frameLimit)
	for {
		n, err := s.peer.Read(b)
		if n > 0 {
			if e := s.tx.put(s.ctx, b[:n]); e != nil {
				return
			}
		}
		if err != nil {
			s.tx.done()
			return
		}
	}
}
func (s *reliableSession) ackOffset() uint64 { s.rxMu.Lock(); defer s.rxMu.Unlock(); return s.rx }
func (s *reliableSession) acceptData(seq uint64, b []byte) error {
	// A logical reader may stall while the remote application is busy. Never
	// hold rxMu across net.Pipe.Write: the outbound HTTP/2 writer needs
	// ackOffset() to make progress concurrently, otherwise both directions
	// can deadlock under backpressure.
	// Serialize deliveries (including any overlapping physical attempts)
	// separately from the short lock protecting the acknowledged offset.
	s.rxWriteMu.Lock()
	defer s.rxWriteMu.Unlock()
	s.rxMu.Lock()
	current := s.rx
	s.rxMu.Unlock()
	if seq > current {
		return errSequenceGap
	}
	if seq+uint64(len(b)) <= current {
		return nil
	}
	b = b[int(current-seq):]
	for len(b) > 0 {
		_ = s.peer.SetWriteDeadline(time.Now().Add(logicalWriteStall))
		n, err := s.peer.Write(b)
		_ = s.peer.SetWriteDeadline(time.Time{})
		if n > 0 {
			s.rxMu.Lock()
			s.rx += uint64(n)
			s.rxMu.Unlock()
			select {
			case s.ackChanged <- struct{}{}:
			default:
			}
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
func (s *reliableSession) recvFrames(r io.Reader) error {
	for {
		f, err := readReplayFrame(r)
		if err != nil {
			return err
		}
		switch f.kind {
		case frameAck:
			if err := s.tx.ack(f.offset); err != nil {
				return err
			}
		case frameData:
			if err := s.acceptData(f.offset, f.data); err != nil {
				return err
			}
		case frameFin:
			if f.offset != s.ackOffset() {
				return errSequenceGap
			}
			s.Close()
			return io.EOF
		}
	}
}

// sendFrames is scoped to one H2 socket; each new socket begins replay at
// the last cumulatively acknowledged byte. TCP/HTTP2 already handles packet loss.
func (s *reliableSession) sendFrames(ctx context.Context, w io.Writer, flush func() error) error {
	sent := uint64(0)
	lastAck := uint64(^uint64(0))
	finSent := false
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.ctx.Err(); err != nil {
			return err
		}
		ack := s.ackOffset()
		if ack != lastAck {
			if err := writeReplayFrame(w, replayFrame{kind: frameAck, offset: ack}); err != nil {
				return err
			}
			if err := flush(); err != nil {
				return err
			}
			lastAck = ack
		}
		pos, chunk := s.tx.sliceAt(sent)
		if len(chunk) > 0 {
			if err := writeReplayFrame(w, replayFrame{kind: frameData, offset: pos, data: chunk}); err != nil {
				return err
			}
			if err := flush(); err != nil {
				return err
			}
			sent = pos + uint64(len(chunk))
			continue
		}
		if !finSent && s.tx.eofAndDrained() {
			if err := writeReplayFrame(w, replayFrame{kind: frameFin, offset: sent}); err != nil {
				return err
			}
			if err := flush(); err != nil {
				return err
			}
			finSent = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-s.tx.changed:
		case <-s.ackChanged:
		case <-ticker.C:
		}
	}
}

// connectReplayFrames can be used to test the framing independently of H2.
func (s *reliableSession) connectReplayFrames(ctx context.Context, rw io.ReadWriteCloser) error {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.recvFrames(rw); cancel() }()
	err := s.sendFrames(ctx, rw, func() error { return nil })
	cancel()
	_ = rw.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	return err
}

// Stay bounded even if the local VLESS handler stops consuming a pending DATA
// frame. Idle connections with no outstanding writes are unaffected.
const logicalWriteStall = 75 * time.Second
