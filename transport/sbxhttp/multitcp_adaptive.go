package sbxhttp

// CDN MultiTCP v0.6 sender-side additive scheduler. A primary is stable while
// healthy, but *not* guaranteed the largest share: faster auxiliary lanes may
// carry most bytes. Only client-reported receive progress can earn a large
// auxiliary share. All inputs are session-local and guarded by resMu.

import "time"

const (
	multiAdaptiveDefaultPrimaryBPS = float64(4 << 20)
	multiAdaptiveDefaultAuxBPS     = float64(1 << 20)
	multiAdaptiveMinimumBPS        = float64(64 << 10)
	multiAdaptiveMaximumBPS        = float64(64 << 20)
	multiAdaptiveFeedbackStale     = 3 * time.Second
	multiAdaptiveAuxProbeLimit     = 4 * multiChunk
	multiAdaptivePrimaryBootLimit  = 16 * multiChunk
)

type multiAdaptiveLane struct {
	live          bool
	primary       bool
	receivedBPS   float64
	receivedAt    time.Time
	pending       int64
	unacked       int
	virtualFinish time.Time
	stalls        int64
}

// multiAdaptivePick returns the lane and the new virtual completion time.
// Unknown auxiliary paths receive only a bounded probe window. Once they
// demonstrate good receive rate they may become the largest data contributor,
// but they NEVER cause primary role reassignment by speed alone.
func multiAdaptivePick(lanes []multiAdaptiveLane, now time.Time, frameBytes int) (int, time.Time) {
	chosen := -1
	var soonest time.Time
	var updated time.Time
	healthy := 0
	nonStalled := 0
	for _, lane := range lanes {
		if lane.live {
			healthy++
			if lane.stalls < 3 {
				nonStalled++
			}
		}
	}
	for i, lane := range lanes {
		if !lane.live || lane.pending >= int64(multiCDNLaneQueue) {
			continue
		}
		if nonStalled > 0 && lane.stalls >= 3 {
			continue
		}
		rate := multiAdaptiveDefaultAuxBPS
		capBytes := multiAdaptiveAuxProbeLimit
		if lane.primary {
			rate = multiAdaptiveDefaultPrimaryBPS
			capBytes = multiAdaptivePrimaryBootLimit
		}
		fresh := !lane.receivedAt.IsZero() && now.Sub(lane.receivedAt) <= multiAdaptiveFeedbackStale
		if fresh && lane.receivedBPS > 0 {
			// Observed rate is demand-limited, not necessarily path capacity.
			// A small optimistic factor lets healthy auxiliaries grow their
			// share instead of being permanently starved by the first probe.
			rate = 1.4 * lane.receivedBPS
			if rate < multiAdaptiveMinimumBPS {
				rate = multiAdaptiveMinimumBPS
			}
			if rate > multiAdaptiveMaximumBPS {
				rate = multiAdaptiveMaximumBPS
			}
			// Approx. 150ms of successful receive bandwidth, not unlimited
			// per-lane backlog. A slow path therefore cannot hog the shared
			// 3-MiB cumulative-ACK window.
			capBytes = int(rate * 0.15)
			if capBytes < 2*multiChunk {
				capBytes = 2 * multiChunk
			}
			if capBytes > 48*multiChunk {
				capBytes = 48 * multiChunk
			}
		} else if !lane.receivedAt.IsZero() && !lane.primary {
			// The auxiliary was probed before, but client reception stopped.
			// Keep a tiny probe opportunity without forcing it into the main
			// data path; recovery is possible after new receive feedback.
			rate = multiAdaptiveMinimumBPS
			capBytes = 2 * multiChunk
		}
		if healthy == 1 && capBytes < multiAdaptivePrimaryBootLimit {
			// Always preserve the only survivor's full useful send window.
			capBytes = multiResWindow
		}
		if lane.unacked+frameBytes > capBytes {
			continue
		}
		finish := lane.virtualFinish
		if finish.Before(now) {
			finish = now
		}
		step := time.Duration(float64(frameBytes) / rate * float64(time.Second))
		if step < time.Microsecond {
			step = time.Microsecond
		}
		finish = finish.Add(step)
		// Healthy primary wins an *exact* tie; never beats an auxiliary with
		// a genuinely earlier estimated delivery completion.
		if chosen < 0 || finish.Before(soonest) || (finish.Equal(soonest) && lane.primary && !lanes[chosen].primary) {
			chosen, soonest, updated = i, finish, finish
		}
	}
	return chosen, updated
}
