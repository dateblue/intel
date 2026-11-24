package sbxhttp

import (
	"testing"
	"time"
)

func TestMultiAdaptiveStablePrimaryAuxCanBeFaster(t *testing.T) {
	now := time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC)
	lanes := []multiAdaptiveLane{
		{live: true, primary: true, receivedBPS: 2 << 20, receivedAt: now},
		{live: true, primary: false, receivedBPS: 12 << 20, receivedAt: now},
	}
	count := []int{0, 0}
	for i := 0; i < 250; i++ {
		idx, finish := multiAdaptivePick(lanes, now, multiChunk)
		if idx < 0 {
			t.Fatalf("no available lane at %d", i)
		}
		count[idx]++
		lanes[idx].virtualFinish = finish
	}
	if !lanes[0].primary || lanes[1].primary {
		t.Fatal("scheduler must not promote faster auxiliary")
	}
	if count[1] <= count[0]*3 {
		t.Fatalf("fast auxiliary should get majority, got %+v", count)
	}
	if count[0] == 0 {
		t.Fatalf("main path starved: %+v", count)
	}
}

func TestMultiAdaptiveUnknownAuxSmallProbe(t *testing.T) {
	now := time.Now()
	lanes := []multiAdaptiveLane{{live: true, primary: true}, {live: true}}
	lanes[1].unacked = multiAdaptiveAuxProbeLimit
	idx, _ := multiAdaptivePick(lanes, now, multiChunk)
	if idx != 0 {
		t.Fatalf("unproven auxiliary should not hold unlimited data, selected %d", idx)
	}
}

func TestMultiAdaptiveSingleSurvivorAlwaysEligible(t *testing.T) {
	now := time.Now()
	lanes := []multiAdaptiveLane{{live: false, primary: true}, {live: true, stalls: 10}}
	idx, _ := multiAdaptivePick(lanes, now, multiChunk)
	if idx != 1 {
		t.Fatalf("last working path must survive: %d", idx)
	}
}

func TestMultiAdaptiveAllStalledStillProgress(t *testing.T) {
	now := time.Now()
	lanes := []multiAdaptiveLane{{live: true, stalls: 4, primary: true}, {live: true, stalls: 6}}
	idx, _ := multiAdaptivePick(lanes, now, multiChunk)
	if idx < 0 {
		t.Fatal("all penalized lanes must not deadlock")
	}
}

func TestMultiAdaptiveStaleSlowAuxBackoff(t *testing.T) {
	now := time.Now()
	lanes := []multiAdaptiveLane{{live: true, primary: true, receivedBPS: 4 << 20, receivedAt: now},
		{live: true, receivedBPS: 20 << 20, receivedAt: now.Add(-5 * time.Second), unacked: 2 * multiChunk}}
	idx, _ := multiAdaptivePick(lanes, now, multiChunk)
	if idx != 0 {
		t.Fatalf("stale auxiliary must receive tiny probe only, selected %d", idx)
	}
}
