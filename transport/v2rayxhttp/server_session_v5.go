//go:build with_xhttp

package v2rayxhttp

import (
	"context"
	"io"
	"sync"
	"time"
)

// serverUploadQueue is an ordered upload reader for XHTTP packet-up and
// stream-up. A bounded out-of-order map avoids unbounded memory when a peer
// posts a large number of packets ahead of the download request.
// Read has a single consumer (the VLESS connection); Push may be concurrent.
type serverUploadQueue struct {
	mu          sync.Mutex
	signal      chan struct{}
	stopped     chan struct{}
	space       chan struct{}
	closed      bool
	stream      io.Reader
	streamSet   bool
	packetSet   bool
	packets     map[uint64][]byte
	current     []byte
	next        uint64
	maxBuffered int
}

func newServerUploadQueue(maxBuffered int) *serverUploadQueue {
	if maxBuffered < 1 {
		maxBuffered = 30
	}
	return &serverUploadQueue{
		signal: make(chan struct{}, 1), stopped: make(chan struct{}), space: make(chan struct{}, 1),
		packets: make(map[uint64][]byte), maxBuffered: maxBuffered,
	}
}

func (q *serverUploadQueue) wake() {
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *serverUploadQueue) PushStream(body io.Reader) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.streamSet || q.packetSet {
		return false
	}
	q.streamSet = true
	q.stream = body
	q.wake()
	return true
}

// PushPacketContext provides bounded backpressure instead of rejecting a
// temporary burst. This matches Xray's blocking channel Push behavior while
// reserving a slot for the missing next packet to avoid reordering deadlock.
// The request context cancels the wait when the client disconnects.
func (q *serverUploadQueue) PushPacketContext(ctx context.Context, seq uint64, payload []byte) error {
	for {
		q.mu.Lock()
		if q.closed || q.streamSet {
			q.mu.Unlock()
			return io.ErrClosedPipe
		}
		if seq < q.next {
			q.mu.Unlock()
			return nil
		}
		if _, present := q.packets[seq]; present {
			q.mu.Unlock()
			return nil
		}
		// Out-of-order packets may occupy all but one slot; the missing
		// next sequence can always be accepted to unblock the reader.
		limit := q.maxBuffered
		if seq > q.next {
			limit--
		}
		if len(q.packets) < limit {
			q.packetSet = true
			q.packets[seq] = append([]byte(nil), payload...)
			q.wake()
			q.mu.Unlock()
			return nil
		}
		q.mu.Unlock()
		select {
		case <-q.space:
		case <-q.stopped:
			return io.ErrClosedPipe
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// PushPacket is retained as a convenient API for unit tests. Production HTTP
// handlers use PushPacketContext with the request cancellation context.
func (q *serverUploadQueue) PushPacket(seq uint64, payload []byte) bool {
	return q.PushPacketContext(context.Background(), seq, payload) == nil
}

func (q *serverUploadQueue) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return 0, io.EOF
		}
		if q.streamSet {
			reader := q.stream
			q.mu.Unlock()
			return reader.Read(p)
		}
		if len(q.current) == 0 {
			if data, ok := q.packets[q.next]; ok {
				delete(q.packets, q.next)
				q.next++
				select {
				case q.space <- struct{}{}:
				default:
				}
				q.current = data
				// Empty packet is legitimate: skip it rather than stalling.
				if len(q.current) == 0 {
					q.mu.Unlock()
					continue
				}
			}
		}
		if len(q.current) > 0 {
			n := copy(p, q.current)
			q.current = q.current[n:]
			q.mu.Unlock()
			return n, nil
		}
		q.mu.Unlock()
		select {
		case <-q.signal:
		case <-q.stopped:
			return 0, io.EOF
		}
	}
}

func (q *serverUploadQueue) Close() {
	var closer io.Closer
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		close(q.stopped)
		q.packets = nil
		q.current = nil
		closer, _ = q.stream.(io.Closer)
	}
	q.mu.Unlock()
	if closer != nil {
		_ = closer.Close()
	}
}

type serverSession struct {
	upload          *serverUploadQueue
	mu              sync.Mutex
	downloadStarted bool
	ended           bool
	// Notify stream-up handlers and the expiration goroutine.
	finished chan struct{}
	created  time.Time
}

func newServerSession(maxBuffered int) *serverSession {
	return &serverSession{upload: newServerUploadQueue(maxBuffered), finished: make(chan struct{}), created: time.Now()}
}

func (s *serverSession) BeginDownload() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || s.downloadStarted {
		return false
	}
	s.downloadStarted = true
	return true
}

func (s *serverSession) DownloadStarted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.downloadStarted
}

func (s *serverSession) Close() {
	s.mu.Lock()
	if !s.ended {
		s.ended = true
		close(s.finished)
	}
	s.mu.Unlock()
	s.upload.Close()
}
