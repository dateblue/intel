package sbxhttp

import (
	"testing"
	"time"
)

func TestMultiAdaptiveReportFeedback(t *testing.T) {
	now := time.Now()
	lane := &multiServerLane{}
	sess := &multiServerSession{lanes: []*multiServerLane{lane}}
	sess.resIngestRX("0:1000", now)
	sess.resIngestRX("0:1059576", now.Add(time.Second))
	if lane.resMeasuredBPS < 1<<20 || lane.resMeasuredBPS > 1.1*(1<<20) {
		t.Fatalf("1 MiB/s receive report not reflected: %f", lane.resMeasuredBPS)
	}
	before := lane.resMeasuredBPS
	sess.resIngestRX("junk,7:99999999", now.Add(1500*time.Millisecond))
	if lane.resMeasuredBPS != before {
		t.Fatal("invalid report must not change throughput")
	}
	// A client may reattach in the same slot with a fresh byte counter.
	sess.resIngestRX("0:1", now.Add(2*time.Second))
	if lane.resMeasuredBPS != 0 {
		t.Fatal("reconnected lane must restart sampling")
	}
}
