package sbxhttp

// CDN MultiTCP v2 (SB-XHTTP v0.5). A candidate address is an optional path,
// never a mandatory participant in a logical VLESS session. The first working
// GET establishes the session; other GETs can join, leave and rejoin freely.
// Downlink data remains available at the origin until cumulatively ACKed.
// This wire protocol is private to sing-box and requires v0.5 at BOTH ends.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/common/tls"
	M "github.com/sagernet/sing/common/metadata"
	"golang.org/x/net/http2"
)

const (
	multiResWindow       = 3 << 20 // stay below 4-MiB client reorder window
	multiResRetry        = 900 * time.Millisecond
	multiResSessionGrace = 45 * time.Second // no live GET => grace for reattach
)

type multiResFlight struct {
	frame    multiFrame
	owner    *multiServerLane
	lastSend time.Time
}

// Addresses come from cdn_ips, with server as an optional last-resort entry.
// Different candidate IPs can route to the same CF PoP; that is irrelevant to
// correctness. Keep the original SNI and HTTP :authority on every request.
func (c *Client) multiResCandidates() []M.Socksaddr {
	out := make([]M.Socksaddr, 0, len(c.multiCDNIPs)+1)
	seen := make(map[string]bool)
	for _, ip := range c.multiCDNIPs {
		target := M.ParseSocksaddrHostPort(ip.String(), c.serverAddr.Port)
		if !seen[target.String()] {
			out = append(out, target)
			seen[target.String()] = true
		}
	}
	if !seen[c.serverAddr.String()] {
		out = append(out, c.serverAddr)
	}
	return out
}

// A failed edge is cooled down exponentially, shared across sessions. Success
// clears that particular edge. Broken candidates are retried in the background
// but never block a connected healthy GET lane.
func (c *Client) multiResBad(target M.Socksaddr) {
	c.multiEdgeMu.Lock()
	key := target.String()
	failures := c.multiEdgeFailures[key] + 1
	if failures > 4 {
		failures = 4
	}
	c.multiEdgeFailures[key] = failures
	delay := time.Second * time.Duration(1<<failures)
	c.multiBadUntil[key] = time.Now().Add(delay)
	c.multiEdgeMu.Unlock()
}
func (c *Client) multiResGood(target M.Socksaddr) {
	c.multiEdgeMu.Lock()
	delete(c.multiEdgeFailures, target.String())
	delete(c.multiBadUntil, target.String())
	c.multiEdgeMu.Unlock()
}

func (m *multiClientConn) chooseResEdge(slot, attempt int) (M.Socksaddr, time.Duration, bool) {
	candidates := m.parent.multiResCandidates()
	used := make(map[string]bool)
	m.lanesMu.RLock()
	for _, lane := range m.lanes {
		if lane != nil && lane.physical != nil {
			used[lane.physical.RemoteAddr().String()] = true
		}
	}
	m.lanesMu.RUnlock()
	m.parent.multiEdgeMu.Lock()
	defer m.parent.multiEdgeMu.Unlock()
	shortest := 2 * time.Second
	for i := range candidates {
		target := candidates[(slot+attempt+i)%len(candidates)]
		if used[target.String()] {
			continue
		}
		if until := m.parent.multiBadUntil[target.String()]; time.Now().Before(until) {
			if d := time.Until(until); d < shortest {
				shortest = d
			}
			continue
		}
		return target, 0, true
	}
	// Other lanes already occupy all working IPs; stay passive for now.
	// No extra physical TCP is required for the surviving lane to work.
	if shortest < 150*time.Millisecond {
		shortest = 150 * time.Millisecond
	}
	return M.Socksaddr{}, shortest, false
}

func (c *Client) dialMultiResilient(ctx context.Context) (net.Conn, error) {
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
	count := c.multiDown
	m := &multiClientConn{
		parent: c, ctx: lifetime, cancel: cancel, resilient: true,
		sessionID: id, laneCount: count, lanes: make([]*multiClientLane, count),
		upReader: ur, upWriter: uw, downReader: dr, downWriter: dw,
		frames: make(chan multiFrame, 4*count), streamSpace: make(chan struct{}, 1),
		readyCh: make(chan struct{}),
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
	go m.packetUploader(id)
	go m.resAckUploader()
	if c.logger != nil {
		go m.logMultiStats()
	}
	// Start only two candidate workers. Optional lanes are admitted after
	// the session has delivered enough data to justify additional sockets.
	// If neither initial worker can attach, resExpandWorkers probes the
	// remaining candidates after a short grace period for availability.
	go m.resLaneWorker(0)
	if count > 1 {
		go m.resLaneWorker(1)
	}
	if count > 2 {
		go m.resExpandWorkers(2, count)
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-m.readyCh:
	case <-ctx.Done():
		_ = m.Close()
		return nil, ctx.Err()
	case <-t.C:
		_ = m.Close()
		return nil, fmt.Errorf("sbxhttp MultiTCP v0.5: no reachable CDN IP within %s", timeout)
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

// Spare IPs do not get connected until a session proves it needs throughput.
// If initial candidates fail, probe the others after 800 ms rather than
// requiring one predetermined primary IP to be alive.
func (m *multiClientConn) resExpandWorkers(start, count int) {
	ticker := time.NewTicker(120 * time.Millisecond)
	defer ticker.Stop()
	launch := start
	started := time.Now()
	lastExpansion := time.Time{}
	for launch < count {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}
		ready := false
		select {
		case <-m.readyCh:
			ready = true
		default:
		}
		m.lanesMu.RLock()
		active := 0
		for _, l := range m.lanes {
			if l != nil {
				active++
			}
		}
		m.lanesMu.RUnlock()
		if !ready && time.Since(started) < 800*time.Millisecond {
			continue
		}
		// 3rd at 256 KiB, 4th 512 KiB, 5th 1 MiB, 6th 2 MiB.
		threshold := uint64(256*1024) << uint(launch-2)
		// Do not create optional lanes merely because a short request has
		// remained open for a few seconds. Only a sustained transfer earns
		// more CDN sockets; the two primary workers already search the full
		// candidate list when their edges fail.
		if ready && active > 0 {
			if m.delivered.Load() < threshold {
				continue
			}
			// Give the last admitted auxiliary time to prove useful before
			// adding another TCP. Long downloads still grow toward six paths.
			if !lastExpansion.IsZero() && time.Since(lastExpansion) < 2*time.Second {
				continue
			}
		}

		go m.resLaneWorker(launch)
		lastExpansion = time.Now()
		launch++
		// If there is no reachable primary, probe all remaining candidates.
	}
}

func (m *multiClientConn) resLaneWorker(slot int) {
	// Stagger optional lanes to avoid 6-way dial bursts, but start alternative
	// candidates quickly so one dead IP cannot hold the initial connection.
	if slot > 0 {
		t := time.NewTimer(time.Duration(slot) * 100 * time.Millisecond)
		select {
		case <-t.C:
		case <-m.ctx.Done():
			t.Stop()
			return
		}
	}
	attempt := 0
	for m.ctx.Err() == nil {
		target, delay, ok := m.chooseResEdge(slot, attempt)
		if !ok {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-m.ctx.Done():
				timer.Stop()
				return
			}
			continue
		}
		attempt++
		if err := m.resAttach(slot, target); err != nil {
			if m.ctx.Err() != nil {
				return
			}
			m.parent.multiResBad(target)
			if m.parent.logger != nil {
				m.parent.logger.Debug("sbxhttp MultiTCP v0.5 optional lane=", slot+1,
					" edge=", target, " unavailable: ", err)
			}
			continue
		}
		// resAttach returns when this GET closes. Other lanes and the VLESS
		// application remain alive. Pending data are recovered by origin ACK.
		if m.ctx.Err() == nil {
			m.parent.multiResBad(target)
		}
	}
}

func (m *multiClientConn) resAttach(slot int, target M.Socksaddr) error {
	dialCtx, cancelDial := context.WithTimeout(m.ctx, 5*time.Second)
	lane, err := m.parent.newMultiResLane(dialCtx, target)
	cancelDial()
	if err != nil {
		return err
	}
	defer lane.close()
	reqCtx, cancelReq := context.WithCancel(m.ctx)
	defer cancelReq()
	path := strings.TrimRight(m.parent.target.Path, "/") + "/multi/down/" + m.sessionID + "/" + strconv.Itoa(slot)
	urlValue := m.parent.target
	urlValue.Path = path
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, urlValue.String(), nil)
	if err != nil {
		return err
	}
	m.setHeaders(req)
	response := make(chan struct {
		r *http.Response
		e error
	})
	go func() {
		resp, err := lane.h2.RoundTrip(req)
		select {
		case response <- struct {
			r *http.Response
			e error
		}{resp, err}:
		case <-reqCtx.Done():
			if resp != nil {
				_ = resp.Body.Close()
			}
		}
	}()
	var resp *http.Response
	timer := time.NewTimer(5 * time.Second)
	select {
	case result := <-response:
		resp, err = result.r, result.e
	case <-timer.C:
		cancelReq()
		lane.close()
		return context.DeadlineExceeded
	case <-m.ctx.Done():
		timer.Stop()
		return m.ctx.Err()
	}
	timer.Stop()
	if err != nil {
		if lane.retire != nil {
			lane.retire()
		}
		return err
	}
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK || resp.Header.Get(multiHeader) != multiResVersion {
		_ = resp.Body.Close()
		return fmt.Errorf("v0.5 GET rejected: %s (server requires v0.5)", resp.Status)
	}
	pre := make([]byte, len(multiPreamble))
	if _, err = io.ReadFull(resp.Body, pre); err != nil {
		_ = resp.Body.Close()
		return err
	}
	if string(pre) != multiPreamble {
		_ = resp.Body.Close()
		return errors.New("v0.5 bad downlink preamble")
	}
	lane.body = resp.Body
	m.lanesMu.Lock()
	m.lanes[slot] = lane
	m.lanesMu.Unlock()
	m.parent.multiResGood(target)
	m.readyOnce.Do(func() { close(m.readyCh) })
	if m.parent.logger != nil {
		m.parent.logger.Debug("sbxhttp MultiTCP v0.5 active lane=", slot+1,
			" edge=", target, " H2_reused=", lane.reused)
	}
	m.downReaderLoop(lane)
	if lane.retire != nil && m.ctx.Err() == nil {
		lane.retire()
	}
	m.lanesMu.Lock()
	if m.lanes[slot] == lane {
		m.lanes[slot] = nil
	}
	m.lanesMu.Unlock()
	return io.EOF
}

// POST uplink and cumulative ACK each use independent H2 transport(s) keyed
// by the edge. The dead downlink pool is never a prerequisite for the uplink.
func (c *Client) multiResUploadTransport(target M.Socksaddr) *http2.Transport {
	c.multiUploadMu.Lock()
	defer c.multiUploadMu.Unlock()
	key := target.String()
	if tr := c.multiUploadByIP[key]; tr != nil {
		return tr
	}
	tr := &http2.Transport{
		DialTLSContext: func(ctx context.Context, _, _ string, _ *tls.STDConfig) (net.Conn, error) {
			raw, err := c.tlsDialer.DialTLSContext(ctx, target)
			if err != nil {
				return nil, err
			}
			if raw.ConnectionState().NegotiatedProtocol != http2.NextProtoTLS {
				_ = raw.Close()
				return nil, errors.New("multi CDN POST requires h2")
			}
			return raw, nil
		},
		ReadIdleTimeout: h2ReadIdleTimeout, PingTimeout: h2PingTimeout,
		WriteByteTimeout: h2WriteByteTimeout,
	}
	c.multiUploadByIP[key] = tr
	return tr
}

func (m *multiClientConn) sendResilientPOST(path string, body []byte) error {
	// ACKs must not wait behind all ordinary packet-up POST slots: the
	// server's send window depends on ACK progress to keep streaming.
	isAck := strings.Contains(path, "/multi/ack/")
	if !isAck && m.parent.multiUpSlots != nil {
		select {
		case m.parent.multiUpSlots <- struct{}{}:
			defer func() { <-m.parent.multiUpSlots }()
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}
	// All candidates are optional. A proven working IP is preferred; an
	// unresponsive IP is cooled down, so a healthy remaining IP is enough.
	for tries := 0; ; tries++ {
		if err := m.ctx.Err(); err != nil {
			return err
		}
		target, delay, ok := m.chooseResUpEdge(tries)
		if !ok {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-m.ctx.Done():
				timer.Stop()
				return m.ctx.Err()
			}
			continue
		}
		reqCtx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
		urlValue := m.parent.target
		urlValue.Path = path
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, urlValue.String(), bytes.NewReader(body))
		if err != nil {
			cancel()
			return err
		}
		m.setHeaders(req)
		if isAck {
			req.Header.Set("X-SBXHTTP-Lane-RX", m.resRXReport())
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		tr := m.parent.multiResUploadTransport(target)
		resp, err := tr.RoundTrip(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
			_ = resp.Body.Close()
			if resp.ProtoMajor == 2 && resp.StatusCode == http.StatusNoContent && resp.Header.Get(multiHeader) == multiResVersion {
				cancel()
				m.parent.multiResGood(target)
				return nil
			}
			err = fmt.Errorf("v0.5 POST rejected: %s", resp.Status)
		}
		cancel()
		if m.parent.logger != nil && tries%12 == 0 {
			m.parent.logger.Debug("sbxhttp v0.5 uplink retry: ", err)
		}
		m.parent.multiResBad(target)
		tr.CloseIdleConnections()
	}
}
func (m *multiClientConn) chooseResUpEdge(attempt int) (M.Socksaddr, time.Duration, bool) {
	candidates := m.parent.multiResCandidates()
	live := make(map[string]bool)
	m.lanesMu.RLock()
	for _, lane := range m.lanes {
		if lane != nil && lane.physical != nil {
			live[lane.physical.RemoteAddr().String()] = true
		}
	}
	m.lanesMu.RUnlock()
	m.parent.multiEdgeMu.Lock()
	defer m.parent.multiEdgeMu.Unlock()
	// Every other attempt is allowed to reuse a live GET edge even if a
	// previous POST temporarily penalized it. A known working downlink must
	// not be ignored for 16 s when it is the ONLY surviving candidate.
	if attempt%2 == 0 {
		for _, target := range candidates {
			if live[target.String()] {
				return target, 0, true
			}
		}
	}
	shortest := 2 * time.Second
	for i := range candidates {
		target := candidates[(attempt+i)%len(candidates)]
		if until := m.parent.multiBadUntil[target.String()]; time.Now().Before(until) {
			if d := time.Until(until); d < shortest {
				shortest = d
			}
			continue
		}
		return target, 0, true
	}
	// If there are no fresh edges, a physically active downlink is still
	// worth probing immediately on the POST plane.
	for _, target := range candidates {
		if live[target.String()] {
			return target, 0, true
		}
	}
	if shortest < 150*time.Millisecond {
		shortest = 150 * time.Millisecond
	}
	return M.Socksaddr{}, shortest, false
}

// Physical per-lane bytes observed at the Windows client. This rides in the
// existing ACK POST, so it does not create a new polling connection or require
// a separate feedback request. Report format: "0:12345,1:67890" (max 6).
func (m *multiClientConn) resRXReport() string {
	m.lanesMu.RLock()
	defer m.lanesMu.RUnlock()
	var parts []string
	for slot, lane := range m.lanes {
		if lane != nil {
			parts = append(parts, strconv.Itoa(slot)+":"+strconv.FormatUint(lane.rxBytes.Load(), 10))
		}
	}
	return strings.Join(parts, ",")
}

func (m *multiClientConn) resAckUploader() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var last uint64
	var lastReport string
	var lastSent time.Time
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
		}
		value := m.ackOffset.Load()
		report := m.resRXReport()
		if value <= last && (report == lastReport || time.Since(lastSent) < 500*time.Millisecond) {
			continue
		}
		path := strings.TrimRight(m.parent.target.Path, "/") + "/multi/ack/" + m.sessionID + "/" + strconv.FormatUint(value, 10)
		if err := m.sendResilientPOST(path, nil); err != nil {
			if m.parent.logger != nil {
				m.parent.logger.Debug("sbxhttp v0.5 ACK retry: ", err)
			}
			continue // next tick retries latest cumulative offset
		}
		last = value
		lastReport = report
		lastSent = time.Now()
	}
}

// SERVER: a GET is an optional attach, not the owner of its VLESS session.
func (s *Server) multiResGET(w http.ResponseWriter, r *http.Request, id string, slot, count int) {
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
		sess = &multiServerSession{
			parent: s, id: id, ctx: ctx, cancel: cancel, source: M.ParseSocksaddr(r.RemoteAddr),
			laneCount: count, lanes: make([]*multiServerLane, count),
			laneSpace: make(chan struct{}, 1), upReader: ur, upWriter: uw,
			packets: make(map[uint64][]byte), upFrames: make(chan multiFrame, 4*count),
			streamAttached: make([]bool, count), resilient: true, resWake: make(chan struct{}, 1), resRepairWake: make(chan struct{}, 1),
			resUnacked: make(map[uint64]*multiResFlight), resLastAlive: time.Now(), resLastAckAt: time.Now(),
		}
		sess.packetCond = sync.NewCond(&sess.packetMu)
		s.multiSessions[id] = sess
		go sess.resMaintenance()
	}
	if sess.ctx.Err() != nil || !sess.resilient || sess.laneCount != count {
		s.multiMu.Unlock()
		http.Error(w, "invalid session", http.StatusConflict)
		return
	}
	sess.resMu.Lock()
	if sess.lanes[slot] != nil {
		sess.resMu.Unlock()
		s.multiMu.Unlock()
		http.Error(w, "duplicate lane", http.StatusConflict)
		return
	}
	lane := &multiServerLane{queue: make(chan multiFrame, multiCDNLaneQueue)}
	sess.lanes[slot] = lane
	if sess.resPrimary == nil {
		sess.resPrimary = lane
		if s.logger != nil {
			s.logger.Debug("sbxhttp CDN v0.6 primary lane=", slot+1)
		}
	}
	sess.resAlive++
	sess.resLastAlive = time.Now()
	sess.resSignal()
	sess.resMu.Unlock()
	s.multiMu.Unlock()

	defer func() {
		sess.resMu.Lock()
		if sess.lanes[slot] == lane {
			sess.lanes[slot] = nil
			if sess.resPrimary == lane {
				sess.resPrimary = nil
				for newSlot, candidate := range sess.lanes {
					if candidate != nil && candidate != lane {
						sess.resPrimary = candidate
						if s.logger != nil {
							s.logger.Debug("sbxhttp CDN v0.6 primary failover to lane=", newSlot+1)
						}
						break
					}
				}
			}
			sess.resAlive--
			sess.resLastAlive = time.Now()
			for _, f := range sess.resUnacked {
				if f.owner == lane {
					f.owner = nil
					f.lastSend = time.Time{}
				}
			}
			sess.resSignal()
		}
		sess.resMu.Unlock()
	}()
	ctl := http.NewResponseController(w)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set(multiHeader, multiResVersion)
	w.WriteHeader(http.StatusOK)
	if err := multiWriteAll(w, []byte(multiPreamble)); err != nil {
		return
	}
	if err := ctl.Flush(); err != nil {
		return
	}
	sess.start() // one healthy lane is sufficient
	go sess.resUplinkTimeout()
	ticker := time.NewTicker(cdnHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-r.Context().Done():
			return
		case f := <-lane.queue:
			start := time.Now()
			if err := multiWriteFrame(w, f); err != nil {
				return
			}
			if err := ctl.Flush(); err != nil {
				return
			}
			cost := time.Since(start).Nanoseconds()
			for {
				old := lane.flushNS.Load()
				if lane.flushNS.CompareAndSwap(old, updateMultiFlushCost(old, cost)) {
					break
				}
			}
			lane.pending.Add(-1)
			sess.resSignal()
		case <-ticker.C:
			if err := multiWriteFrame(w, multiFrame{}); err != nil {
				return
			}
			if err := ctl.Flush(); err != nil {
				return
			}
		}
	}
}

// Created only once per session via startOnce; redundant timer goroutines are
// harmless but avoid by atomically claiming the first timer initialization.
func (s *multiServerSession) resUplinkTimeout() {
	if !s.resUpTimer.CompareAndSwap(false, true) {
		return
	}
	timer := time.NewTimer(cdnUplinkWait)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return
	case <-timer.C:
		if !s.upStarted.Load() {
			s.Close()
		}
	}
}

func (s *multiServerSession) resSignal() {
	select {
	case s.resWake <- struct{}{}:
	default:
	}
	select {
	case s.resRepairWake <- struct{}{}:
	default:
	}
}

// Caller holds resMu. The primary role is stable while that GET is active.
// All lanes compete by actual client receive rate, not H2 Flush duration.
// Fast secondary paths may win MOST frames while a slower healthy primary
// remains the session's continuity anchor.
func (s *multiServerSession) resChooseLane(avoid *multiServerLane) *multiServerLane {
	now := time.Now()
	loads := make([]multiAdaptiveLane, len(s.lanes))
	outstanding := make(map[*multiServerLane]int, len(s.lanes))
	for _, flight := range s.resUnacked {
		if flight.owner != nil {
			outstanding[flight.owner] += len(flight.frame.data)
		}
	}
	for i, lane := range s.lanes {
		if lane == nil || lane == avoid {
			continue
		}
		loads[i] = multiAdaptiveLane{
			live:          len(lane.queue) < cap(lane.queue),
			primary:       lane == s.resPrimary,
			receivedBPS:   lane.resMeasuredBPS,
			receivedAt:    lane.resReceivedAt,
			pending:       lane.pending.Load(),
			unacked:       outstanding[lane],
			virtualFinish: lane.resVirtualFinish,
			stalls:        lane.stallCount.Load(),
		}
	}
	idx, finish := multiAdaptivePick(loads, now, multiChunk)
	if idx < 0 {
		return nil
	}
	s.lanes[idx].resVirtualFinish = finish
	s.nextLane = (idx + 1) % len(s.lanes)
	return s.lanes[idx]
}

// The client reports monotone received-byte counters (not speed guesses).
// We calculate a smoothed receive rate on the server. A source that becomes
// silent loses its large auxiliary budget after 3s; it can still be probed.
// Invalid/oversized headers are ignored: feedback is optional, never fatal.
func (s *multiServerSession) resIngestRX(header string, now time.Time) {
	if len(header) > 240 {
		return
	}
	for _, entry := range strings.Split(header, ",") {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			continue
		}
		slot, err1 := strconv.Atoi(parts[0])
		total, err2 := strconv.ParseUint(parts[1], 10, 64)
		if err1 != nil || err2 != nil || slot < 0 || slot >= len(s.lanes) {
			continue
		}
		lane := s.lanes[slot]
		if lane == nil {
			continue
		}
		if lane.resReceivedAt.IsZero() || total < lane.resReceivedBytes {
			lane.resReceivedBytes = total
			lane.resReceivedAt = now
			lane.resMeasuredBPS = 0
			continue
		}
		delta := total - lane.resReceivedBytes
		elapsed := now.Sub(lane.resReceivedAt)
		if elapsed < 250*time.Millisecond {
			continue
		}
		if delta > 0 {
			measured := float64(delta) / elapsed.Seconds()
			if measured > multiAdaptiveMaximumBPS {
				measured = multiAdaptiveMaximumBPS
			}
			if lane.resMeasuredBPS == 0 || elapsed >= 2*time.Second {
				lane.resMeasuredBPS = measured
			} else {
				lane.resMeasuredBPS = 0.65*lane.resMeasuredBPS + 0.35*measured
			}
			lane.resReceivedAt = now
			lane.resReceivedBytes = total
			// A previously penalized lane may recover; positive client RX
			// feedback gradually restores its eligibility.
			if old := lane.stallCount.Load(); old > 0 {
				lane.stallCount.CompareAndSwap(old, old-1)
			}
		} else if elapsed > 2*time.Second {
			lane.resMeasuredBPS = 0
			// keep last successful sample time to indicate stale auxiliary
		}
	}
}

func (s *multiServerSession) writeDownResilient(b []byte) (int, error) {
	s.downWriteMu.Lock()
	defer s.downWriteMu.Unlock()
	written := 0
	for len(b) > 0 {
		if err := s.ctx.Err(); err != nil {
			return written, net.ErrClosed
		}
		n := len(b)
		if n > multiChunk {
			n = multiChunk
		}
		var wake <-chan struct{}
		s.resMu.Lock()
		// Do not advance a lane's virtual schedule when the global ACK
		// window is full; no payload has actually been assigned yet.
		if s.resInFlight+n > multiResWindow {
			wake = s.resWake
			s.resMu.Unlock()
			select {
			case <-wake:
			case <-s.ctx.Done():
				return written, net.ErrClosed
			}
			continue
		}
		lane := s.resChooseLane(nil)
		if lane == nil {
			wake = s.resWake
			s.resMu.Unlock()
			select {
			case <-wake:
			case <-s.ctx.Done():
				return written, net.ErrClosed
			}
			continue
		}
		f := multiFrame{offset: s.nextOffset, data: append([]byte(nil), b[:n]...)}
		s.resUnacked[f.offset] = &multiResFlight{frame: f, owner: lane, lastSend: time.Now()}
		s.resInFlight += n
		s.nextOffset += uint64(n)
		lane.pending.Add(1)
		lane.queue <- f // capacity was checked with resMu held
		s.resMu.Unlock()
		written += n
		b = b[n:]
	}
	return written, nil
}

func (s *Server) multiResACK(w http.ResponseWriter, r *http.Request, id string, ack uint64, count int) {
	s.multiMu.Lock()
	sess := s.multiSessions[id]
	s.multiMu.Unlock()
	if sess == nil || !sess.resilient || sess.ctx.Err() != nil || sess.laneCount != count || r.ContentLength > 0 {
		http.Error(w, "invalid session", http.StatusConflict)
		return
	}
	sess.resMu.Lock()
	sess.resIngestRX(r.Header.Get("X-SBXHTTP-Lane-RX"), time.Now())
	if ack >= sess.resAcked && ack <= sess.nextOffset {
		if ack > sess.resAcked {
			sess.resLastAckAt = time.Now()
		}
		sess.resAcked = ack
		for offset, f := range sess.resUnacked {
			if offset+uint64(len(f.frame.data)) <= ack {
				sess.resInFlight -= len(f.frame.data)
				delete(sess.resUnacked, offset)
			}
		}
		sess.resSignal()
	}
	sess.resMu.Unlock()
	w.Header().Set(multiHeader, multiResVersion)
	w.WriteHeader(http.StatusNoContent)
}

func (s *multiServerSession) resMaintenance() {
	ticker := time.NewTicker(120 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		case <-s.resRepairWake:
		}
		s.resMu.Lock()
		if s.resAlive == 0 && time.Since(s.resLastAlive) > multiResSessionGrace {
			s.resMu.Unlock()
			s.Close()
			return
		}
		now := time.Now()
		// Give priority to *all* frames orphaned by a failed lane, in offset
		// order. Relying on one retransmission per 900 ms would throttle the
		// surviving link to a few hundred kbps. A capped memory window keeps
		// recovery bounded even if ACK POSTs temporarily fail.
		for step := 0; step < 16; step++ {
			var orphan *multiResFlight
			for _, f := range s.resUnacked {
				if f.owner != nil {
					continue
				}
				if orphan == nil || f.frame.offset < orphan.frame.offset {
					orphan = f
				}
			}
			if orphan == nil {
				break
			}
			lane := s.resChooseLane(nil)
			if lane == nil {
				break
			}
			orphan.owner = lane
			orphan.lastSend = now
			lane.pending.Add(1)
			lane.queue <- orphan.frame
		}
		// If an apparently live lane has stalled, re-send the oldest
		// unacknowledged frame onto a different live lane. The receiving
		// reassembler deduplicates complete frames by absolute offset.
		var earliest *multiResFlight
		for _, f := range s.resUnacked {
			if earliest == nil || f.frame.offset < earliest.frame.offset {
				earliest = f
			}
		}
		// TCP already retransmits on a single surviving GET. Replaying the
		// same payload on that *same* live TCP only increases congestion.
		// Cross-lane replay is useful only when cumulative ACK has actually
		// stalled, not merely because a frame is 450ms old while ACKs flow.
		if earliest != nil && earliest.owner != nil && s.resAlive > 1 &&
			now.Sub(s.resLastAckAt) >= multiResRetry &&
			now.Sub(earliest.lastSend) >= multiResRetry {
			previous := earliest.owner
			alternate := s.resChooseLane(previous)
			if alternate != nil {
				previous.stallCount.Add(1)
				earliest.owner = alternate
				earliest.lastSend = now
				alternate.pending.Add(1)
				alternate.queue <- earliest.frame
			}
		}
		s.resMu.Unlock()
	}
}
