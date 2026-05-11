package circuitbreaker_test

import (
	"testing"
	"time"

	"github.com/keyork/rc_keyork/internal/circuitbreaker"
)

func newMgr() *circuitbreaker.Manager {
	return circuitbreaker.NewManager(circuitbreaker.Config{
		WindowDur:    5 * time.Minute,
		MinRequests:  5,
		FailureRatio: 0.8,
		OpenDur:      60 * time.Second,
	})
}

func TestClosedByDefault(t *testing.T) {
	m := newMgr()
	if !m.Allow("example.com") {
		t.Fatal("new breaker should allow requests")
	}
}

func TestTripsAfterThreshold(t *testing.T) {
	m := newMgr()
	for i := 0; i < 5; i++ {
		m.Allow("bad.com")
		m.Record("bad.com", false)
	}
	if m.Allow("bad.com") {
		t.Fatal("breaker should be open after 5 failures")
	}
	if !m.IsOpen("bad.com") {
		t.Fatal("IsOpen should be true")
	}
}

func TestDoesNotTripBelowMinRequests(t *testing.T) {
	m := newMgr()
	for i := 0; i < 4; i++ {
		m.Allow("few.com")
		m.Record("few.com", false)
	}
	if !m.Allow("few.com") {
		t.Fatal("should not trip with fewer than minRequests calls")
	}
}

func TestSuccessKeepsClose(t *testing.T) {
	m := newMgr()
	for i := 0; i < 10; i++ {
		m.Allow("ok.com")
		m.Record("ok.com", true)
	}
	if !m.Allow("ok.com") {
		t.Fatal("all-success domain should stay closed")
	}
}
