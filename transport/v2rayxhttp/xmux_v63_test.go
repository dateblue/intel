package v2rayxhttp

import (
	"errors"
	"testing"
	"time"
)

// A resource may be rotated out while old application streams remain alive.
// Its later failure is local, not grounds to back off a healthy replacement.
func TestV63RetiredPoolTerminalErrorDoesNotTripGlobalBreaker(t *testing.T) {
	m, _ := poolOf(t, xmuxConfig{})
	old, _ := m.getReserved()
	old.unreusableAt = timeNow().Add(-time.Second)
	replacement, _ := m.getReserved()
	if replacement == old {
		t.Fatal("expired pool was reused")
	}
	if old.getOpenUsage() != 1 {
		t.Fatal("retired pool did not retain its active stream")
	}
	before := m.breakerEpoch
	old.noteTerminalError(errors.New("http2: client conn is closed"))
	if m.backoffArmed.Load() || m.breakerEpoch != before {
		t.Fatal("retired pool incorrectly tripped global recovery breaker")
	}
	if !old.failing.Load() {
		t.Fatal("old pool must still be marked unusable")
	}
	if replacement.failing.Load() {
		t.Fatal("unrelated replacement was marked failing")
	}
	newXmuxRelease(old).release()
	newXmuxRelease(replacement).release()
}

func TestV63RetiredPoolStreamErrorsDoNotTripGlobalBreaker(t *testing.T) {
	m, _ := poolOf(t, xmuxConfig{})
	old, _ := m.getReserved()
	old.unreusableAt = timeNow().Add(-time.Second)
	replacement, _ := m.getReserved()
	for i := int32(0); i < xmuxBreakerThreshold; i++ {
		old.noteFailure()
	}
	if !old.failing.Load() {
		t.Fatal("retired resource was not marked failing")
	}
	if m.backoffArmed.Load() || m.breakerEpoch != 0 {
		t.Fatal("retired stream failures armed global breaker")
	}
	replacement.noteTerminalFailure()
	if !m.backoffArmed.Load() || m.breakerEpoch != 1 {
		t.Fatal("active pool terminal failure must still trip breaker")
	}
	old.noteSuccess()
	if !m.backoffArmed.Load() {
		t.Fatal("late old success disarmed active recovery")
	}
	newXmuxRelease(old).release()
	newXmuxRelease(replacement).release()
}

func TestV63ActiveBreakerSurvivesConcurrentRetirement(t *testing.T) {
	m, _ := poolOf(t, xmuxConfig{})
	current, _ := m.getReserved()
	current.noteTerminalFailure()
	if !m.backoffArmed.Load() {
		t.Fatal("active failure never armed breaker")
	}
	// Evicting an already condemned connection must not clear the backoff.
	_, _ = m.get()
	if !m.backoffArmed.Load() {
		t.Fatal("eviction cleared active failure backoff")
	}
	newXmuxRelease(current).release()
}
