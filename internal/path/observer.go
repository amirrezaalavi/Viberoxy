package path

import (
	"sync"
	"time"
)

// EventKind discriminates the observability events a path reports to its
// observer (SPEC-M / F-16). The host package (package main) maps them onto
// the Prometheus metrics; internal/path never imports the registry itself,
// so there is no import cycle and no metric knowledge below this layer.
type EventKind uint8

const (
	// EventEjection: health just ejected this path (SPEC-H.3). Reason is
	// the rule that fired ("consecutive_distinct" or "fail_ratio").
	EventEjection EventKind = iota
	// EventSample: one performance sample was folded into the EWMAs
	// (TTFB / goodput). TTFB and GoodputBps carry the sample values.
	EventSample
)

// Event is one observability event for one path generation.
type Event struct {
	Kind EventKind
	// Path is the generation the event belongs to (never nil when
	// emitted); its ID/Slot are the metric label values.
	Path *Path
	// Reason is the stable reason string for EventEjection.
	Reason string
	// TTFB / GoodputBps are the raw sample for EventSample (0 = unknown).
	TTFB       time.Duration
	GoodputBps float64
}

var (
	observerMu sync.RWMutex
	observer   func(Event)
)

// SetObserver installs the host's observability hook (nil uninstalls it).
// It is wired once at process start (metrics.go's init); the hook must be
// safe for concurrent use and must not block.
func SetObserver(fn func(Event)) {
	observerMu.Lock()
	observer = fn
	observerMu.Unlock()
}

// notify delivers ev to the installed observer, if any. Called from
// RecordOutcome (ejection) and RecordHealth (sample), with no path lock
// held, so the observer may take its own locks freely.
func notify(ev Event) {
	observerMu.RLock()
	fn := observer
	observerMu.RUnlock()
	if fn != nil {
		fn(ev)
	}
}
