// Package circuitbreaker implements a per-domain circuit breaker to protect
// the worker pool from cascading failures when a vendor API is unavailable.
//
// Each domain gets its own breaker with an independent state machine:
//
//	Closed ──[failure rate ≥ threshold]──► Open
//	  ▲                                       │
//	  │                              openDur cooldown
//	  │                                       │
//	  └──[probe succeeds]◄── Half-Open ◄──────┘
//
// State is stored in process memory (sync.Map). Multiple instances do NOT share
// state — each judges independently. This means instances may briefly disagree
// about a breaker state, but avoids the latency of a shared store on the
// hot delivery path.
package circuitbreaker

import (
	"log/slog"
	"sync"
	"time"
)

type state int

const (
	stateClosed   state = iota // requests flow normally
	stateOpen                  // requests are rejected until openUntil
	stateHalfOpen              // one probe request allowed per openDur interval
)

// call records a single HTTP call outcome within the sliding window.
type call struct {
	at      time.Time
	success bool
}

// breaker is the per-domain state machine. All fields are protected by mu.
type breaker struct {
	mu          sync.Mutex
	state       state
	calls       []call    // sliding window of recent call outcomes
	openUntil   time.Time // when the breaker should transition to half-open
	lastProbeAt time.Time // last time a probe was allowed in half-open state

	// Configuration (copied from Config at creation time).
	windowDur    time.Duration
	minRequests  int
	failureRatio float64
	openDur      time.Duration
}

// Manager holds one breaker per domain and creates new ones lazily.
type Manager struct {
	mu       sync.Mutex
	breakers map[string]*breaker
	cfg      Config
}

// Config controls the tripping thresholds shared by all breakers in a Manager.
type Config struct {
	// WindowDur is the width of the sliding window over which the failure rate
	// is measured.
	WindowDur time.Duration
	// MinRequests is the minimum number of calls within the window before the
	// breaker can trip. Prevents tripping on very low traffic.
	MinRequests int
	// FailureRatio is the fraction of calls that must fail before the breaker
	// trips (e.g. 0.8 = 80%).
	FailureRatio float64
	// OpenDur is how long the breaker stays open, and also the interval between
	// probe attempts in half-open state.
	OpenDur time.Duration
}

// NewManager creates a Manager with the given configuration.
func NewManager(cfg Config) *Manager {
	return &Manager{
		breakers: make(map[string]*breaker),
		cfg:      cfg,
	}
}

// get returns the breaker for domain, creating one if it does not exist.
func (m *Manager) get(domain string) *breaker {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.breakers[domain]
	if !ok {
		b = &breaker{
			windowDur:    m.cfg.WindowDur,
			minRequests:  m.cfg.MinRequests,
			failureRatio: m.cfg.FailureRatio,
			openDur:      m.cfg.OpenDur,
		}
		m.breakers[domain] = b
	}
	return b
}

// Allow returns true if the domain's breaker permits a request.
// In half-open state only one probe per openDur interval is allowed through.
func (m *Manager) Allow(domain string) bool {
	return m.get(domain).allow()
}

// Record records the outcome of a completed call to domain.
// It must be called after every call for which Allow returned true.
func (m *Manager) Record(domain string, success bool) {
	m.get(domain).record(success)
}

// IsOpen returns true if the breaker for domain is currently open (tripped).
func (m *Manager) IsOpen(domain string) bool {
	b := m.get(domain)
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == stateOpen
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	switch b.state {
	case stateClosed:
		return true
	case stateOpen:
		if now.After(b.openUntil) {
			b.state = stateHalfOpen
			b.lastProbeAt = now
			slog.Debug("circuit half-open: probe allowed", "component", "circuitbreaker")
			return true
		}
		return false
	case stateHalfOpen:
		if now.Sub(b.lastProbeAt) >= b.openDur {
			b.lastProbeAt = now
			slog.Debug("circuit half-open: another probe allowed", "component", "circuitbreaker")
			return true
		}
		return false
	}
	return true
}

func (b *breaker) record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.calls = append(b.calls, call{at: now, success: success})

	// Prune calls outside the sliding window. Copy the surviving slice to a
	// fresh backing array to release memory from the pruned prefix.
	cutoff := now.Add(-b.windowDur)
	i := 0
	for i < len(b.calls) && b.calls[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		pruned := make([]call, len(b.calls)-i)
		copy(pruned, b.calls[i:])
		b.calls = pruned
	}

	switch b.state {
	case stateHalfOpen:
		if success {
			b.state = stateClosed
			b.calls = nil
			slog.Info("circuit closed: probe succeeded", "component", "circuitbreaker")
		} else {
			b.state = stateOpen
			b.openUntil = now.Add(b.openDur)
			slog.Warn("circuit re-opened: probe failed", "component", "circuitbreaker")
		}
	case stateClosed:
		b.maybeTrip(now)
	}
}

// maybeTrip trips the breaker if the failure rate in the current window exceeds
// the configured threshold. Called only when the breaker is already closed.
func (b *breaker) maybeTrip(now time.Time) {
	if len(b.calls) < b.minRequests {
		return
	}
	failures := 0
	for _, c := range b.calls {
		if !c.success {
			failures++
		}
	}
	ratio := float64(failures) / float64(len(b.calls))
	if ratio >= b.failureRatio {
		b.state = stateOpen
		b.openUntil = now.Add(b.openDur)
		slog.Warn("circuit opened",
			"component", "circuitbreaker",
			"failure_ratio", ratio,
			"failures", failures,
			"total", len(b.calls),
			"open_until", b.openUntil,
		)
	}
}
