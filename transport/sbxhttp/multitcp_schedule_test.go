package sbxhttp

import "testing"

func TestMultiLaneSelectorFairAtEqualCost(t *testing.T) {
	loads := []multiLaneLoad{{available: true}, {available: true}}
	if selectMultiLane(loads, 0) != 0 || selectMultiLane(loads, 1) != 1 {
		t.Fatal("equal-cost lane selection should rotate")
	}
}
func TestMultiLaneSelectorBypassesBlockedLane(t *testing.T) {
	loads := []multiLaneLoad{{available: true, pending: 5, flushNS: 40_000_000}, {available: true, pending: 1, flushNS: 1_000_000}}
	if got := selectMultiLane(loads, 0); got != 1 {
		t.Fatalf("should use faster less-congested lane, got %d", got)
	}
	loads[1].available = false
	if got := selectMultiLane(loads, 1); got != 0 {
		t.Fatalf("should fall back to available lane, got %d", got)
	}
	loads[0].available = false
	if selectMultiLane(loads, 0) != -1 {
		t.Fatal("all lanes unavailable")
	}
}
func TestMultiFlushEWMA(t *testing.T) {
	a := updateMultiFlushCost(0, 8_000_000)
	if a != 8_000_000 {
		t.Fatalf("initial=%d", a)
	}
	b := updateMultiFlushCost(a, 16_000_000)
	if b != 9_000_000 {
		t.Fatalf("filtered=%d", b)
	}
	if updateMultiFlushCost(0, 100) != multiMinFlushNS {
		t.Fatal("low bound")
	}
	if updateMultiFlushCost(0, 900_000_000) != multiMaxFlushNS {
		t.Fatal("high bound")
	}
}
