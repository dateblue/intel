package sbxhttp

import (
	"bytes"
	"context"
	M "github.com/sagernet/sing/common/metadata"
	"net/netip"
	"sync"
	"testing"
)

// This tests that one application stream is distributed across independent
// lane queues and that no frame offset is duplicated or skipped.
func TestMultiServerDownStriping(t *testing.T) {
	const lanes = 4
	const chunks = 48
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &multiServerSession{ctx: ctx, laneCount: lanes, lanes: make([]*multiServerLane, lanes), laneSpace: make(chan struct{}, 1)}
	var mu sync.Mutex
	var frames []multiFrame
	var used [lanes]int
	var wg sync.WaitGroup
	processed := make(chan struct{}, chunks)
	for i := 0; i < lanes; i++ {
		l := &multiServerLane{queue: make(chan multiFrame, multiLaneQueue)}
		s.lanes[i] = l
		wg.Add(1)
		go func(i int, q <-chan multiFrame) {
			defer wg.Done()
			for {
				select {
				case f := <-q:
					mu.Lock()
					frames = append(frames, f)
					used[i]++
					mu.Unlock()
					processed <- struct{}{}
					select {
					case s.laneSpace <- struct{}{}:
					default:
					}
				case <-ctx.Done():
					return
				}
			}
		}(i, l.queue)
	}
	original := bytes.Repeat([]byte("m"), chunks*multiChunk)
	n, err := s.writeDown(original)
	if err != nil || n != len(original) {
		t.Fatalf("write n=%d error=%v", n, err)
	}
	for i := 0; i < chunks; i++ {
		<-processed
	}
	cancel()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(frames) != chunks {
		t.Fatalf("expected %d chunks, got %d", chunks, len(frames))
	}
	for _, u := range used {
		if u == 0 {
			t.Fatalf("lane starved: %v", used)
		}
	}
	a := newMultiReassembler()
	for i := len(frames) - 1; i >= 0; i-- {
		if err := a.insert(frames[i]); err != nil {
			t.Fatal(err)
		}
	}
	var out []byte
	for {
		b := a.pop()
		if b == nil {
			break
		}
		out = append(out, b...)
	}
	if !bytes.Equal(out, original) {
		t.Fatal("striped frames did not reconstruct original stream")
	}
}
func TestMultiClientStreamStriping(t *testing.T) {
	ctx := context.Background()
	m := &multiClientConn{ctx: ctx, streamSpace: make(chan struct{}, 1), lanes: make([]*multiClientLane, 4)}
	for i := range m.lanes {
		m.lanes[i] = &multiClientLane{streamQueue: make(chan multiFrame, multiLaneQueue)}
	}
	data := bytes.Repeat([]byte("x"), 16*multiChunk)
	n, err := m.streamWrite(data)
	if err != nil || n != len(data) {
		t.Fatalf("stream write n=%d error=%v", n, err)
	}
	a := newMultiReassembler()
	for _, l := range m.lanes {
		if len(l.streamQueue) == 0 {
			t.Fatal("missing upload lane")
		}
		for len(l.streamQueue) > 0 {
			if err := a.insert(<-l.streamQueue); err != nil {
				t.Fatal(err)
			}
		}
	}
	var out []byte
	for {
		b := a.pop()
		if b == nil {
			break
		}
		out = append(out, b...)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("stream-up striping lost bytes")
	}
}

// Dial targets differ physically for CDN IP pool, but stay identical for
// Reality direct. Both up/down H2 requests run on these exact sockets.
func TestMultiCDNLanePhysicalDialTargets(t *testing.T) {
	orig := M.ParseSocksaddrHostPort("proxy.example.test", 443)
	pool := []netip.Addr{netip.MustParseAddr("1.1.1.1"), netip.MustParseAddr("2606:4700:4700::1111")}
	c := &Client{cdnMode: true, serverAddr: orig, multiCDNIPs: pool}
	got := []string{c.multiDialTarget(0).String(), c.multiDialTarget(1).String(), c.multiDialTarget(2).String()}
	expected := []string{"1.1.1.1:443", "[2606:4700:4700::1111]:443", "1.1.1.1:443"}
	for i := range got {
		if got[i] != expected[i] {
			t.Fatalf("CDN lane %d target %q != %q", i, got[i], expected[i])
		}
	}
	c.cdnMode = false
	if c.multiDialTarget(1).String() != orig.String() {
		t.Fatal("direct path must remain untouched")
	}
}
