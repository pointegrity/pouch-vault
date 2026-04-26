package main

import (
	"sync/atomic"
	"time"
)

// Status is the shared in-memory observability surface for the
// daemon. Each component (stream, heartbeat, receiver) updates
// fields it owns; the local UI reads them.
//
// All time fields are unix-nanos via atomic.Int64. Zero = never.
// Strings that change after start are protected by atomic.Pointer
// so the UI handler reads them without locking.
type Status struct {
	// Set once at startup, read forever.
	AnchorName string
	AnchorID   string
	Version    string
	Mode       string // "pull" or "push"
	Hostname   string
	PouchURL   string
	DBPath     string
	StartedAt  time.Time

	// Updated by stream.go on connect/disconnect.
	streamConnected atomic.Bool
	lastConnectedAt atomic.Int64
	lastDisconnect  atomic.Pointer[string] // last disconnect reason, if any

	// Updated by heartbeat.go on each successful HB.
	lastHeartbeatAt    atomic.Int64
	lastHeartbeatError atomic.Pointer[string]

	// Updated by recv.go (push mode) and stream.go (pull mode) when
	// a drop is persisted.
	lastDropReceivedAt atomic.Int64
	lastDropID         atomic.Pointer[string]
}

// status is the singleton, accessed by every package. (We're in a
// single main package so this is fine; no import cycle.)
var status = &Status{}

// MarkConnected is called when the stream connection comes up.
func (s *Status) MarkConnected() {
	s.streamConnected.Store(true)
	s.lastConnectedAt.Store(time.Now().UnixNano())
	empty := ""
	s.lastDisconnect.Store(&empty)
}

// MarkDisconnected is called when the stream loop ends with a reason.
func (s *Status) MarkDisconnected(reason string) {
	s.streamConnected.Store(false)
	s.lastDisconnect.Store(&reason)
}

// MarkHeartbeat records a successful heartbeat send.
func (s *Status) MarkHeartbeat() {
	s.lastHeartbeatAt.Store(time.Now().UnixNano())
	empty := ""
	s.lastHeartbeatError.Store(&empty)
}

// MarkHeartbeatError records a failed heartbeat attempt.
func (s *Status) MarkHeartbeatError(err string) {
	s.lastHeartbeatError.Store(&err)
}

// MarkDropReceived records that a drop was just persisted.
func (s *Status) MarkDropReceived(dropID string) {
	s.lastDropReceivedAt.Store(time.Now().UnixNano())
	s.lastDropID.Store(&dropID)
}
