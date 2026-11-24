package sbxhttp

// Experimental MultiTCP transport for private sing-box <-> sing-box deployments.
// Unlike XMUX, it stripes ONE logical VLESS byte stream across 1/2/4 independently
// dialled TCP+TLS+HTTP/2 connections. Each ClientConn is constructed over an
// explicitly dialled socket: an HTTP/2 Stream is NOT counted as another lane.
//
// Direct: independent REALITY + H2 sockets. CDN: independent normal TLS + H2
// sockets to the edge, while the origin still receives regular H2 requests.
// The v1 experiment fails closed if any downlink lane breaks: it does NOT
// promise zero-loss migration or hide upstream retransmission problems.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"golang.org/x/net/http2"
)

const (
	multiHeader        = "X-SBXHTTP-Multi"
	multiWireVersion   = "1"
	multiPreamble      = "SBXM1"
	multiLaneQueue     = 8
	multiSessionLimit  = 96 // includes incomplete unauthenticated sessions
	multiAttachTimeout = 30 * time.Second
)

type multiClientLane struct {
	physical     net.Conn
	h2           *http2.ClientConn
	body         io.ReadCloser
	streamReader *io.PipeReader
	streamWriter *io.PipeWriter
	streamQueue  chan multiFrame
	rxBytes      atomic.Uint64 // actual downlink payload bytes received from this CF IP
}

func (l *multiClientLane) close() {
	if l.body != nil {
		_ = l.body.Close()
	}
	if l.streamReader != nil {
		_ = l.streamReader.CloseWithError(net.ErrClosed)
	}
	if l.streamWriter != nil {
		_ = l.streamWriter.CloseWithError(net.ErrClosed)
	}
	if l.h2 != nil {
		_ = l.h2.Close()
	}
	if l.physical != nil {
		_ = l.physical.Close()
	}
}

type multiClientConn struct {
	parent       *Client
	ctx          context.Context
	cancel       context.CancelFunc
	lanes        []*multiClientLane
	upReader     *io.PipeReader
	upWriter     *io.PipeWriter
	downReader   *io.PipeReader
	downWriter   *io.PipeWriter
	frames       chan multiFrame
	streamMu     sync.Mutex
	streamOffset uint64
	streamNext   int
	streamSpace  chan struct{}
	closed       atomic.Bool
	closeOnce    sync.Once
	delivered    atomic.Uint64 // ordered bytes delivered to the application
}

func (c *Client) dialMultiTCP(ctx context.Context) (net.Conn, error) {
	if c.closed.Load() {
		return nil, net.ErrClosed
	}
	timeout, err := boundedHandshakeTimeout(ctx)
	if err != nil {
		return nil, err
	}
	id, err := cdnNewID()
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	ur, uw := io.Pipe()
	dr, dw := io.Pipe()
	m := &multiClientConn{parent: c, ctx: lifetime, cancel: cancel, upReader: ur, upWriter: uw, downReader: dr, downWriter: dw, frames: make(chan multiFrame, 4*c.multiDown), streamSpace: make(chan struct{}, 1)}
	if !c.cdnMode {
		// In contrast to http2.Transport.RoundTrip, NewClientConn cannot silently
		// reuse another lane's TCP connection or silently create additional dials.
		m.lanes = make([]*multiClientLane, 0, c.multiDown)
		dialCtx, stopDial := context.WithTimeout(ctx, timeout)
		defer stopDial()
		for i := 0; i < c.multiDown; i++ {
			l, err := c.newMultiLane(dialCtx, i)
			if err != nil {
				_ = m.Close()
				return nil, fmt.Errorf("sbxhttp multitcp: lane %d dial: %w", i, err)
			}
			m.lanes = append(m.lanes, l)
			if c.logger != nil {
				c.logger.Debug("sbxhttp MultiTCP lane=", i+1, "/", c.multiDown,
					" local=", l.physical.LocalAddr(), " remote=", l.physical.RemoteAddr(),
					" independently dialled H2/TCP")
			}
			urlValue := url.URL{Scheme: "https", Host: c.serverAddr.String(), Path: strings.TrimRight(c.target.Path, "/") + "/multi/down/" + id + "/" + strconv.Itoa(i)}
			req, _ := http.NewRequestWithContext(lifetime, http.MethodGet, urlValue.String(), nil)
			m.setHeaders(req)
			type result struct {
				body io.ReadCloser
				err  error
			}
			resultCh := make(chan result, 1)
			go func(lane *multiClientLane) {
				resp, err := lane.h2.RoundTrip(req)
				if err != nil {
					resultCh <- result{err: err}
					return
				}
				if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK || resp.Header.Get(multiHeader) != multiWireVersion {
					_ = resp.Body.Close()
					resultCh <- result{err: fmt.Errorf("multi down GET rejected: %s", resp.Status)}
					return
				}
				pre := make([]byte, len(multiPreamble))
				if _, err := io.ReadFull(resp.Body, pre); err != nil {
					_ = resp.Body.Close()
					resultCh <- result{err: err}
					return
				}
				if string(pre) != multiPreamble {
					_ = resp.Body.Close()
					resultCh <- result{err: errors.New("multi down bad preamble")}
					return
				}
				resultCh <- result{body: resp.Body}
			}(l)
			select {
			case res := <-resultCh:
				if res.err != nil {
					_ = m.Close()
					return nil, res.err
				}
				l.body = res.body
			case <-dialCtx.Done():
				_ = m.Close()
				return nil, dialCtx.Err()
			}
		}
	} else {
		// Establish all physical TLS/TCP/H2 lanes concurrently. In v0.2 these
		// were dialled AND their GET preambles awaited serially, amplifying
		// latency tests by N handshakes. Each result owns its socket until the
		// coordinator has collected ALL results (including on errors).
		m.lanes = make([]*multiClientLane, c.multiDown)
		dialCtx, stopDial := context.WithTimeout(ctx, timeout)
		defer stopDial()
		type laneResult struct {
			index int
			lane  *multiClientLane
			err   error
		}
		results := make(chan laneResult, c.multiDown)
		for i := 0; i < c.multiDown; i++ {
			go func(index int) {
				l, err := c.newMultiLane(dialCtx, index)
				if err != nil {
					results <- laneResult{index: index, err: err}
					return
				}
				urlValue := url.URL{Scheme: "https", Host: c.serverAddr.String(), Path: strings.TrimRight(c.target.Path, "/") + "/multi/down/" + id + "/" + strconv.Itoa(index)}
				req, err := http.NewRequestWithContext(lifetime, http.MethodGet, urlValue.String(), nil)
				if err == nil {
					m.setHeaders(req)
					var resp *http.Response
					resp, err = l.h2.RoundTrip(req)
					if err == nil {
						if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK || resp.Header.Get(multiHeader) != multiWireVersion {
							err = fmt.Errorf("multi down GET rejected: %s", resp.Status)
						} else {
							pre := make([]byte, len(multiPreamble))
							_, err = io.ReadFull(resp.Body, pre)
							if err == nil && string(pre) != multiPreamble {
								err = errors.New("multi down bad preamble")
							}
						}
						if err != nil {
							_ = resp.Body.Close()
						} else {
							l.body = resp.Body
						}
					}
				}
				if err != nil {
					l.close()
					results <- laneResult{index: index, err: err}
					return
				}
				results <- laneResult{index: index, lane: l}
			}(i)
		}
		var firstErr error
		for remaining := c.multiDown; remaining > 0; remaining-- {
			// Timeout cancels the GET context and closes any partially opened H2
			// connection; we still collect every result so no socket is orphaned.
			select {
			case result := <-results:
				if result.err != nil && firstErr == nil {
					firstErr = fmt.Errorf("sbxhttp multitcp: lane %d dial: %w", result.index, result.err)
					m.cancel()
					stopDial()
				}
				m.lanes[result.index] = result.lane
			case <-dialCtx.Done():
				if firstErr == nil {
					firstErr = dialCtx.Err()
				}
				m.cancel()
				stopDial()
				// Don't count this event as a completed worker. Once contexts are
				// cancelled, collect their socket-owning results below.
				result := <-results
				m.lanes[result.index] = result.lane
			}
		}
		if firstErr != nil {
			_ = m.Close()
			return nil, firstErr
		}
		if c.logger != nil {
			for i, l := range m.lanes {
				c.logger.Debug("sbxhttp MultiTCP lane=", i+1, "/", c.multiDown,
					" local=", l.physical.LocalAddr(), " remote=", l.physical.RemoteAddr(),
					" independently dialled H2/TCP")
			}
		}
	}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		_ = m.Close()
		return nil, net.ErrClosed
	}
	c.multiSessions[m] = struct{}{}
	c.mu.Unlock()
	go m.assemble()
	if c.cdnMode && c.logger != nil {
		go m.logMultiStats()
	}
	for _, l := range m.lanes {
		go m.downReaderLoop(l)
	}
	if c.cdnMode {
		go m.packetUploader(id)
	} else {
		for i, l := range m.lanes {
			l.streamReader, l.streamWriter = io.Pipe()
			l.streamQueue = make(chan multiFrame, multiLaneQueue)
			go m.streamFrameWriter(l)
			go m.streamUploader(id, i, l)
		}
	}
	if c.visionInnerTLS {
		wrapped, err := privateVisionClientTLS(ctx, m)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		return wrapped, nil
	}
	return m, nil
}

// multiDialTarget must be used for each independent physical H2/TCP lane.
// SNI and HTTP :authority remain the original CDN hostname; no IP leaks into
// application headers or the TLS server_name just because a TCP dial targets it.
func (c *Client) multiDialTarget(laneIndex int) M.Socksaddr {
	if c.cdnMode && len(c.multiCDNIPs) > 0 {
		edge := multiEdgeForLane(c.multiCDNIPs, laneIndex)
		return M.ParseSocksaddrHostPort(edge.String(), c.serverAddr.Port)
	}
	return c.serverAddr
}

func (c *Client) newMultiLane(ctx context.Context, laneIndex int) (*multiClientLane, error) {
	// This only controls the client -> CF edge hop. The CF -> VPS connection
	// and anycast PoP selection are entirely controlled by Cloudflare.
	dialTarget := c.multiDialTarget(laneIndex)
	raw, err := c.tlsDialer.DialTLSContext(ctx, dialTarget)
	if err != nil {
		return nil, err
	}
	proto := raw.ConnectionState().NegotiatedProtocol
	if c.cdnMode && proto != http2.NextProtoTLS {
		_ = raw.Close()
		return nil, fmt.Errorf("multi CDN requires h2, got %q", proto)
	}
	if !c.cdnMode && !privateH2ALPNAllowed(proto) {
		_ = raw.Close()
		return nil, fmt.Errorf("multi Reality wrong ALPN %q", proto)
	}
	tr := &http2.Transport{ReadIdleTimeout: h2ReadIdleTimeout, PingTimeout: h2PingTimeout, WriteByteTimeout: h2WriteByteTimeout}
	clientConn, err := tr.NewClientConn(raw)
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return &multiClientLane{physical: raw, h2: clientConn}, nil
}
func (m *multiClientConn) setHeaders(req *http.Request) {
	req.Host = m.parent.config.ServerName()
	req.Header.Set(multiHeader, multiWireVersion)
	req.Header.Set("X-SBXHTTP-Lanes", strconv.Itoa(m.parent.multiDown))
	req.Header.Set("Cache-Control", "no-store, no-transform")
	req.Header.Set("Accept-Encoding", "identity")
	if m.parent.visionInnerTLS {
		req.Header.Set(privateVisionModeHeader, privateVisionModeTLS13)
	}
}
func (m *multiClientConn) downReaderLoop(l *multiClientLane) {
	for {
		f, err := multiReadFrame(l.body)
		if err != nil {
			_ = m.Close()
			return
		}
		if len(f.data) == 0 {
			continue
		}
		if m.parent.cdnMode {
			l.rxBytes.Add(uint64(len(f.data)))
		}
		select {
		case m.frames <- f:
		case <-m.ctx.Done():
			return
		}
	}
}
func (m *multiClientConn) assemble() {
	a := newMultiReassembler()
	for {
		select {
		case <-m.ctx.Done():
			return
		case f := <-m.frames:
			if err := a.insert(f); err != nil {
				_ = m.Close()
				return
			}
			for {
				data := a.pop()
				if len(data) == 0 {
					break
				}
				if _, err := m.downWriter.Write(data); err != nil {
					_ = m.Close()
					return
				}
				m.delivered.Add(uint64(len(data)))
			}
		}
	}
}

// Payload-only telemetry; it distinguishes a connected lane from a lane
// actually carrying useful download bytes. Reports 10-second rates only for
// active sessions at debug level; no timer/goroutine survives Close.
func (m *multiClientConn) logMultiStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	previous := make([]uint64, len(m.lanes))
	var prevDelivered uint64
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			values := make([]string, len(m.lanes))
			var interval uint64
			for i, l := range m.lanes {
				current := l.rxBytes.Load()
				delta := current - previous[i]
				previous[i] = current
				interval += delta
				values[i] = fmt.Sprintf("lane%d=%s:%.2fMbps", i+1, l.physical.RemoteAddr(), float64(delta)*8/10e6)
			}
			delivered := m.delivered.Load()
			deltaDelivered := delivered - prevDelivered
			prevDelivered = delivered
			if interval < 128*1024 && deltaDelivered < 128*1024 {
				continue
			}
			m.parent.logger.Debug("sbxhttp MultiTCP down 10s ", strings.Join(values, " "),
				" delivered=", fmt.Sprintf("%.2fMbps", float64(deltaDelivered)*8/10e6))
		}
	}
}

// Bounded upload batching. Requests are distributed over the same explicit
// physical H2 lanes, so BOTH directions have multiple independent TCP cwnds.
func (m *multiClientConn) packetUploader(id string) {
	chunks := make(chan []byte, 16)
	go func() {
		defer close(chunks)
		buf := make([]byte, 64*1024)
		for {
			n, err := m.upReader.Read(buf)
			if n > 0 {
				b := append([]byte(nil), buf[:n]...)
				select {
				case chunks <- b:
				case <-m.ctx.Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	ticker := time.NewTicker(cdnPacketInterval)
	defer ticker.Stop()
	limiter := make(chan struct{}, cdnInflight)
	var seq uint64
	var batch []byte
	flush := func() {
		if len(batch) == 0 {
			return
		}
		select {
		case limiter <- struct{}{}:
		case <-m.ctx.Done():
			return
		}
		p := batch
		no := seq
		seq++
		batch = nil
		go func() {
			defer func() { <-limiter }()
			if err := m.sendPacket(id, no, p); err != nil {
				_ = m.Close()
			}
		}()
	}
	for {
		select {
		case <-m.ctx.Done():
			return
		case b, ok := <-chunks:
			if !ok {
				flush()
				return
			}
			for len(b) > 0 {
				n := cdnPacketMax - len(batch)
				if n > len(b) {
					n = len(b)
				}
				batch = append(batch, b[:n]...)
				b = b[n:]
				if len(batch) == cdnPacketMax {
					flush()
				}
			}
		case <-ticker.C:
			flush()
		}
	}
}
func (m *multiClientConn) sendPacket(id string, seq uint64, p []byte) error {
	path := strings.TrimRight(m.parent.target.Path, "/") + "/multi/up/" + id + "/" + strconv.FormatUint(seq, 10)
	target := url.URL{Scheme: "https", Host: m.parent.serverAddr.String(), Path: path}
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		if err := m.ctx.Err(); err != nil {
			return err
		}
		deadline, cancel := context.WithTimeout(m.ctx, 20*time.Second)
		req, err := http.NewRequestWithContext(deadline, http.MethodPost, target.String(), bytes.NewReader(p))
		if err != nil {
			cancel()
			return err
		}
		m.setHeaders(req)
		req.Header.Set("Content-Type", "application/octet-stream")
		// Prefer a different physical connection for retries.
		l := m.lanes[(int(seq%uint64(len(m.lanes)))+attempt)%len(m.lanes)]
		resp, err := l.h2.RoundTrip(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent && resp.ProtoMajor == 2 && resp.Header.Get(multiHeader) == multiWireVersion {
				cancel()
				return nil
			}
			if !cdnRetryableStatus(resp.StatusCode) {
				cancel()
				return fmt.Errorf("multitcp packet %d rejected: %s", seq, resp.Status)
			}
			err = fmt.Errorf("multitcp packet %d transient: %s", seq, resp.Status)
		}
		last = err
		cancel()
		if attempt == 3 {
			break
		}
		timer := time.NewTimer(time.Duration(200*(1<<attempt)+int(seq%7)*13) * time.Millisecond)
		select {
		case <-m.ctx.Done():
			timer.Stop()
			return m.ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

// REALITY/direct uplink uses one long-lived streaming POST per physical TCP
// lane, avoiding CDN-style POST/response RTT amplification. A bounded queue
// per lane provides flow-control and lets the writer bypass a slow lane.
func (m *multiClientConn) streamWrite(p []byte) (int, error) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()
	count := 0
	for len(p) > 0 {
		if m.ctx.Err() != nil {
			return count, net.ErrClosed
		}
		n := len(p)
		if n > multiChunk {
			n = multiChunk
		}
		f := multiFrame{offset: m.streamOffset, data: append([]byte(nil), p[:n]...)}
		sent := false
		for !sent {
			for i := 0; i < len(m.lanes); i++ {
				j := (m.streamNext + i) % len(m.lanes)
				select {
				case m.lanes[j].streamQueue <- f:
					m.streamNext = (j + 1) % len(m.lanes)
					sent = true
				default:
				}
				if sent {
					break
				}
			}
			if !sent {
				select {
				case <-m.streamSpace:
				case <-m.ctx.Done():
					return count, net.ErrClosed
				}
			}
		}
		m.streamOffset += uint64(n)
		count += n
		p = p[n:]
	}
	return count, nil
}
func (m *multiClientConn) streamFrameWriter(l *multiClientLane) {
	for {
		select {
		case <-m.ctx.Done():
			return
		case f := <-l.streamQueue:
			select {
			case m.streamSpace <- struct{}{}:
			default:
			}
			if err := multiWriteFrame(l.streamWriter, f); err != nil {
				_ = m.Close()
				return
			}
		}
	}
}
func (m *multiClientConn) streamUploader(id string, index int, l *multiClientLane) {
	path := strings.TrimRight(m.parent.target.Path, "/") + "/multi/streamup/" + id + "/" + strconv.Itoa(index)
	target := url.URL{Scheme: "https", Host: m.parent.serverAddr.String(), Path: path}
	req, err := http.NewRequestWithContext(m.ctx, http.MethodPost, target.String(), l.streamReader)
	if err != nil {
		_ = m.Close()
		return
	}
	req.ContentLength = -1 // streaming upload: never mark the pipe as an empty request body
	m.setHeaders(req)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := l.h2.RoundTrip(req)
	if err != nil {
		_ = m.Close()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 2 || resp.Header.Get(multiHeader) != multiWireVersion {
		_ = m.Close()
		return
	}
	// The server keeps the HTTP response open while it receives the body.
	// A broken uplink is detected by the writer or the request context.
	<-m.ctx.Done()
}

func (m *multiClientConn) Read(p []byte) (int, error) {
	return m.downReader.Read(p)
}
func (m *multiClientConn) Write(p []byte) (int, error) {
	if m.closed.Load() {
		return 0, net.ErrClosed
	}
	if !m.parent.cdnMode {
		return m.streamWrite(p)
	}
	return m.upWriter.Write(p)
}
func (m *multiClientConn) Close() error {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		m.cancel()
		_ = m.upWriter.CloseWithError(net.ErrClosed)
		_ = m.upReader.CloseWithError(net.ErrClosed)
		_ = m.downWriter.CloseWithError(net.ErrClosed)
		_ = m.downReader.CloseWithError(net.ErrClosed)
		for _, lane := range m.lanes {
			if lane != nil {
				lane.close()
			}
		}
		m.parent.mu.Lock()
		delete(m.parent.multiSessions, m)
		m.parent.mu.Unlock()
	})
	return nil
}
func (m *multiClientConn) LocalAddr() net.Addr {
	if len(m.lanes) > 0 && m.lanes[0].physical != nil {
		return m.lanes[0].physical.LocalAddr()
	}
	return &net.TCPAddr{}
}
func (m *multiClientConn) RemoteAddr() net.Addr {
	if len(m.lanes) > 0 && m.lanes[0].physical != nil {
		return m.lanes[0].physical.RemoteAddr()
	}
	return &net.TCPAddr{}
}
func (*multiClientConn) SetDeadline(time.Time) error      { return os.ErrInvalid }
func (*multiClientConn) SetReadDeadline(time.Time) error  { return os.ErrInvalid }
func (*multiClientConn) SetWriteDeadline(time.Time) error { return os.ErrInvalid }
func (*multiClientConn) NeedAdditionalReadDeadline() bool { return true }

// Server-side session is independent of the individual GET request contexts.
// The first GET creates it; all lanes must join before the VLESS handler starts.
type multiServerLane struct {
	queue   chan multiFrame
	pending atomic.Int64 // enqueued + in-flight downlink frames
	flushNS atomic.Int64 // origin-side H2 Flush EWMA
}
type multiServerSession struct {
	parent         *Server
	id             string
	ctx            context.Context
	cancel         context.CancelFunc
	source         M.Socksaddr
	laneCount      int
	lanes          []*multiServerLane
	ready          int
	mutex          sync.Mutex
	startOnce      sync.Once
	closeOnce      sync.Once
	downWriteMu    sync.Mutex
	nextOffset     uint64
	nextLane       int
	laneSpace      chan struct{}
	upReader       *io.PipeReader
	upWriter       *io.PipeWriter
	upStarted      atomic.Bool
	packetMu       sync.Mutex
	packetCond     *sync.Cond
	nextPacket     uint64
	packets        map[uint64][]byte
	upFrames       chan multiFrame
	streamAttached []bool
}

func (s *multiServerSession) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		_ = s.upReader.CloseWithError(net.ErrClosed)
		_ = s.upWriter.CloseWithError(net.ErrClosed)
		s.packetMu.Lock()
		s.packetCond.Broadcast()
		s.packetMu.Unlock()
		s.parent.multiMu.Lock()
		if s.parent.multiSessions[s.id] == s {
			delete(s.parent.multiSessions, s.id)
		}
		s.parent.multiMu.Unlock()
	})
}
func (s *multiServerSession) start() {
	s.startOnce.Do(func() {
		if s.parent.cdnMode {
			go s.assemblePackets()
		} else {
			go s.assembleStream()
		}
		go func() {
			var app net.Conn = &multiServerConn{session: s}
			if s.parent.visionInnerTLS {
				wrapped, err := privateVisionServerTLS(s.ctx, app, s.parent.privateVisionTLS)
				if err != nil {
					s.Close()
					return
				}
				app = wrapped
			}
			s.parent.handler.NewConnectionEx(s.ctx, app, s.source, M.Socksaddr{}, N.OnceClose(func(error) { s.Close() }))
		}()
	})
}
func (s *multiServerSession) assemblePackets() {
	for {
		s.packetMu.Lock()
		var data []byte
		for s.ctx.Err() == nil {
			if x, ok := s.packets[s.nextPacket]; ok {
				data = x
				delete(s.packets, s.nextPacket)
				s.nextPacket++
				break
			}
			s.packetCond.Wait()
		}
		s.packetMu.Unlock()
		if s.ctx.Err() != nil {
			return
		}
		if _, err := s.upWriter.Write(data); err != nil {
			s.Close()
			return
		}
	}
}
func (s *multiServerSession) assembleStream() {
	a := newMultiReassembler()
	for {
		select {
		case <-s.ctx.Done():
			return
		case f := <-s.upFrames:
			if err := a.insert(f); err != nil {
				s.Close()
				return
			}
			for {
				data := a.pop()
				if len(data) == 0 {
					break
				}
				if _, err := s.upWriter.Write(data); err != nil {
					s.Close()
					return
				}
			}
		}
	}
}

// Keep the established REALITY/direct MultiTCP scheduler bit-for-bit
// equivalent to v0.2. CDN alone uses the new congestion-sensitive selector.
func (s *multiServerSession) writeDownRoundRobin(b []byte) (int, error) {
	s.downWriteMu.Lock()
	defer s.downWriteMu.Unlock()
	written := 0
	for len(b) > 0 {
		if s.ctx.Err() != nil {
			return written, net.ErrClosed
		}
		n := len(b)
		if n > multiChunk {
			n = multiChunk
		}
		f := multiFrame{offset: s.nextOffset, data: append([]byte(nil), b[:n]...)}
		sent := false
		for !sent {
			for i := 0; i < s.laneCount; i++ {
				index := (s.nextLane + i) % s.laneCount
				select {
				case s.lanes[index].queue <- f:
					s.nextLane = (index + 1) % s.laneCount
					sent = true
				default:
				}
				if sent {
					break
				}
			}
			if !sent {
				select {
				case <-s.laneSpace:
				case <-s.ctx.Done():
					return written, net.ErrClosed
				}
			}
		}
		s.nextOffset += uint64(n)
		written += n
		b = b[n:]
	}
	return written, nil
}

func (s *multiServerSession) writeDown(b []byte) (int, error) {
	if s.parent == nil || !s.parent.cdnMode {
		return s.writeDownRoundRobin(b)
	}
	s.downWriteMu.Lock()
	defer s.downWriteMu.Unlock()
	written := 0
	for len(b) > 0 {
		if s.ctx.Err() != nil {
			return written, net.ErrClosed
		}
		n := len(b)
		if n > multiChunk {
			n = multiChunk
		}
		f := multiFrame{offset: s.nextOffset, data: append([]byte(nil), b[:n]...)}
		for {
			var load [4]multiLaneLoad // lane count is validated to 1/2/4
			for i, lane := range s.lanes {
				load[i] = multiLaneLoad{
					pending:   lane.pending.Load(),
					flushNS:   lane.flushNS.Load(),
					available: len(lane.queue) < cap(lane.queue),
				}
			}
			index := selectMultiLane(load[:s.laneCount], s.nextLane)
			if index < 0 {
				select {
				case <-s.laneSpace:
				case <-s.ctx.Done():
					return written, net.ErrClosed
				}
				continue
			}
			lane := s.lanes[index]
			lane.pending.Add(1)
			select {
			case lane.queue <- f:
				s.nextLane = (index + 1) % s.laneCount
			case <-s.ctx.Done():
				lane.pending.Add(-1)
				return written, net.ErrClosed
			default:
				lane.pending.Add(-1)
				continue
			}
			break
		}
		s.nextOffset += uint64(n)
		written += n
		b = b[n:]
	}
	return written, nil
}
func (s *Server) serveMultiHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.ProtoMajor != 2 || r.URL.RawQuery != "" || r.Header.Get(multiHeader) != multiWireVersion || !privateVisionModeHeaderMatches(s.visionInnerTLS, r.Header.Get(privateVisionModeHeader)) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	base := strings.TrimRight(s.path, "/") + "/multi/"
	if !strings.HasPrefix(r.URL.Path, base) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, base), "/")
	if len(parts) != 3 || !cdnValidID(parts[1]) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	count, err := strconv.Atoi(r.Header.Get("X-SBXHTTP-Lanes"))
	if err != nil || count < 1 || count > s.multiDown || (count != 1 && count != 2 && count != 4) {
		http.Error(w, "unsupported lane count", http.StatusBadRequest)
		return
	}
	switch {
	case parts[0] == "down" && r.Method == http.MethodGet:
		lane, err := strconv.Atoi(parts[2])
		if err != nil || lane < 0 || lane >= count {
			http.Error(w, "invalid lane", http.StatusBadRequest)
			return
		}
		s.multiDownGET(w, r, parts[1], lane, count)
	case parts[0] == "streamup" && r.Method == http.MethodPost && !s.cdnMode:
		index, err := strconv.Atoi(parts[2])
		if err != nil || index < 0 || index >= count {
			http.Error(w, "invalid stream lane", http.StatusBadRequest)
			return
		}
		s.multiStreamPOST(w, r, parts[1], index, count)
	case parts[0] == "up" && r.Method == http.MethodPost && s.cdnMode:
		seq, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			http.Error(w, "invalid sequence", http.StatusBadRequest)
			return
		}
		s.multiUpPOST(w, r, parts[1], seq, count)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}
func (s *Server) multiDownGET(w http.ResponseWriter, r *http.Request, id string, idx, count int) {
	s.multiMu.Lock()
	sess := s.multiSessions[id]
	if sess == nil {
		if s.closed.Load() || len(s.multiSessions) >= multiSessionLimit {
			s.multiMu.Unlock()
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
		ur, uw := io.Pipe()
		sess = &multiServerSession{parent: s, id: id, ctx: ctx, cancel: cancel, source: M.ParseSocksaddr(r.RemoteAddr), laneCount: count, lanes: make([]*multiServerLane, count), laneSpace: make(chan struct{}, 1), upReader: ur, upWriter: uw, packets: make(map[uint64][]byte), upFrames: make(chan multiFrame, 4*count), streamAttached: make([]bool, count)}
		sess.packetCond = sync.NewCond(&sess.packetMu)
		s.multiSessions[id] = sess
		go func() {
			t := time.NewTimer(multiAttachTimeout)
			defer t.Stop()
			select {
			case <-sess.ctx.Done():
				return
			case <-t.C:
			}
			sess.mutex.Lock()
			ready := sess.ready
			sess.mutex.Unlock()
			if ready < count {
				sess.Close()
			}
		}()
	}
	if sess.laneCount != count || sess.ctx.Err() != nil || sess.lanes[idx] != nil {
		s.multiMu.Unlock()
		http.Error(w, "duplicate or invalid lane", http.StatusConflict)
		return
	}
	lane := &multiServerLane{queue: make(chan multiFrame, multiLaneQueue)}
	sess.lanes[idx] = lane
	s.multiMu.Unlock()

	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set(multiHeader, multiWireVersion)
	w.WriteHeader(http.StatusOK)
	if err := multiWriteAll(w, []byte(multiPreamble)); err != nil {
		sess.Close()
		return
	}
	if err := controller.Flush(); err != nil {
		sess.Close()
		return
	}
	sess.mutex.Lock()
	sess.ready++
	isReady := sess.ready == sess.laneCount
	sess.mutex.Unlock()
	if isReady {
		sess.start()
		// A peer that opens all lanes but never sends VLESS handshakes must
		// not retain an unauthenticated server session indefinitely.
		go func() {
			t := time.NewTimer(cdnUplinkWait)
			defer t.Stop()
			select {
			case <-sess.ctx.Done():
				return
			case <-t.C:
			}
			if !sess.upStarted.Load() {
				sess.Close()
			}
		}()
	}
	// Any lane failing invalidates this v1 session. The explicit close removes
	// it from the map; other lanes terminate via the session's context.
	defer sess.Close()
	ticker := time.NewTicker(cdnHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-r.Context().Done():
			return
		case f := <-lane.queue:
			maxBatch := 1
			if s.cdnMode {
				maxBatch = 4
			}
			// One Flush for up to four already queued frames lowers H2
			// scheduling overhead without delaying a lone small frame.
			var start time.Time
			if s.cdnMode {
				start = time.Now()
			}
			frames := 0
			for {
				if err := multiWriteFrame(w, f); err != nil {
					return
				}
				frames++
				if frames >= maxBatch {
					break
				}
				select {
				case f = <-lane.queue:
				default:
					goto flushed
				}
			}
		flushed:
			if err := controller.Flush(); err != nil {
				return
			}
			if s.cdnMode {
				perFrameNS := time.Since(start).Nanoseconds() / int64(frames)
				for {
					old := lane.flushNS.Load()
					if lane.flushNS.CompareAndSwap(old, updateMultiFlushCost(old, perFrameNS)) {
						break
					}
				}
				lane.pending.Add(-int64(frames))
			}
			select {
			case sess.laneSpace <- struct{}{}:
			default:
			}
		case <-ticker.C:
			if err := multiWriteFrame(w, multiFrame{}); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		}
	}
}

// One HTTP/2 request body per lane. These POST requests stay open while the
// logical VLESS session lives. No per-128KiB HTTP request cost on direct links.
func (s *Server) multiStreamPOST(w http.ResponseWriter, r *http.Request, id string, index, count int) {
	s.multiMu.Lock()
	sess := s.multiSessions[id]
	if sess == nil || sess.ctx.Err() != nil || sess.laneCount != count || sess.streamAttached[index] {
		s.multiMu.Unlock()
		http.Error(w, "unknown or duplicate stream lane", http.StatusConflict)
		return
	}
	sess.streamAttached[index] = true
	s.multiMu.Unlock()
	defer sess.Close()
	w.Header().Set(multiHeader, multiWireVersion)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if err := http.NewResponseController(w).Flush(); err != nil {
		return
	}
	for {
		f, err := multiReadFrame(r.Body)
		if err != nil {
			return
		}
		if len(f.data) == 0 {
			continue
		}
		sess.upStarted.Store(true)
		select {
		case sess.upFrames <- f:
		case <-sess.ctx.Done():
			return
		}
	}
}

func (s *Server) multiUpPOST(w http.ResponseWriter, r *http.Request, id string, seq uint64, count int) {
	s.multiMu.Lock()
	sess := s.multiSessions[id]
	s.multiMu.Unlock()
	if sess == nil || sess.ctx.Err() != nil || sess.laneCount != count {
		http.Error(w, "unknown session", http.StatusConflict)
		return
	}
	if r.ContentLength > cdnPacketMax {
		http.Error(w, "packet too large", http.StatusRequestEntityTooLarge)
		return
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, cdnPacketMax+1))
	if err != nil || len(payload) == 0 || len(payload) > cdnPacketMax {
		http.Error(w, "bad packet", http.StatusRequestEntityTooLarge)
		return
	}
	sess.packetMu.Lock()
	if seq >= sess.nextPacket {
		if seq-sess.nextPacket >= cdnPacketWindow {
			sess.packetMu.Unlock()
			http.Error(w, "reorder window", http.StatusConflict)
			return
		}
		if _, already := sess.packets[seq]; !already && len(sess.packets) >= cdnPacketWindow {
			sess.packetMu.Unlock()
			http.Error(w, "reorder window", http.StatusConflict)
			return
		}
		if _, ok := sess.packets[seq]; !ok {
			sess.packets[seq] = payload
			sess.packetCond.Signal()
		}
	}
	sess.upStarted.Store(true)
	sess.packetMu.Unlock()
	w.Header().Set(multiHeader, multiWireVersion)
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) multiCloseAll() {
	s.multiMu.Lock()
	items := make([]*multiServerSession, 0, len(s.multiSessions))
	for _, sess := range s.multiSessions {
		items = append(items, sess)
	}
	s.multiMu.Unlock()
	for _, sess := range items {
		sess.Close()
	}
}

type multiServerConn struct{ session *multiServerSession }

func (c *multiServerConn) Read(b []byte) (int, error)     { return c.session.upReader.Read(b) }
func (c *multiServerConn) Write(b []byte) (int, error)    { return c.session.writeDown(b) }
func (c *multiServerConn) Close() error                   { c.session.Close(); return nil }
func (*multiServerConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*multiServerConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*multiServerConn) SetDeadline(time.Time) error      { return os.ErrInvalid }
func (*multiServerConn) SetReadDeadline(time.Time) error  { return os.ErrInvalid }
func (*multiServerConn) SetWriteDeadline(time.Time) error { return os.ErrInvalid }
func (*multiServerConn) NeedAdditionalReadDeadline() bool { return true }

var _ net.Conn = (*multiClientConn)(nil)
var _ net.Conn = (*multiServerConn)(nil)
