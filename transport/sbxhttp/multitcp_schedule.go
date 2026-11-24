package sbxhttp

// Multi-TCP v0.3 sender-side lane selection. It uses bounded per-lane pending
// frames and measured origin-side H2 Flush time. This is a heuristic, NOT an
// end-to-end Cloudflare throughput measurement: it avoids building up frames
// behind a currently blocked origin->CF lane without claiming global fairness.

const (
	multiDefaultFlushNS int64 = 1_000_000   // 1 ms baseline before an observation
	multiMinFlushNS     int64 = 250_000     // don't over-react to sub-ms writes
	multiMaxFlushNS     int64 = 100_000_000 // cap cost at 100 ms
)

type multiLaneLoad struct {
	pending   int64
	flushNS   int64
	available bool
}

// selectMultiLane returns an available lane whose estimated finish time is
// minimal. Equal-cost choices rotate to avoid starving secondary CDN IPs.
// -1 means all queues full, at which point caller applies backpressure.
func selectMultiLane(load []multiLaneLoad, next int) int {
	if len(load) == 0 {
		return -1
	}
	chosen := -1
	var cheapest int64
	for step := 0; step < len(load); step++ {
		i := (next + step) % len(load)
		item := load[i]
		if !item.available {
			continue
		}
		cost := item.flushNS
		if cost <= 0 {
			cost = multiDefaultFlushNS
		}
		if cost < multiMinFlushNS {
			cost = multiMinFlushNS
		}
		if cost > multiMaxFlushNS {
			cost = multiMaxFlushNS
		}
		pending := item.pending
		if pending < 0 {
			pending = 0
		}
		if pending > 4096 {
			pending = 4096
		}
		score := (pending + 1) * cost
		if chosen == -1 || score < cheapest {
			chosen, cheapest = i, score
		}
	}
	return chosen
}

// updateMultiFlushCost uses EWMA (7/8 previous, 1/8 observed) to avoid
// aggressively moving data in response to one sporadic latency spike.
func updateMultiFlushCost(previous, observed int64) int64 {
	if observed < multiMinFlushNS {
		observed = multiMinFlushNS
	}
	if observed > multiMaxFlushNS {
		observed = multiMaxFlushNS
	}
	if previous <= 0 {
		return observed
	}
	return previous + (observed-previous)/8
}
