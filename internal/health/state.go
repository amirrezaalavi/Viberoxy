package health

import (
	"net"
	"sync"
	"time"
)

// entry is one classified outcome in a path's sliding window.
type entry struct {
	at   time.Time
	key  string // destination HOST (port stripped), for the distinctness guard
	fail bool
}

// State is ONE path's health state (SPEC-H.2/H.3/H.4/H.5): the sliding
// outcome window, ejection with exponential backoff, and the canary streak
// that drives half-open re-admission. The zero value is ready to use.
//
// The state is owned by the path generation it belongs to (internal/path
// embeds one), so a replacement path starts with a clean window. All
// methods take an explicit `now` — the injected clock — and are safe for
// concurrent use.
type State struct {
	mu sync.Mutex

	// entries is the sliding window, oldest first; only OK and HardFail
	// outcomes are recorded (Neutral is ignored, SPEC-H.1).
	entries []entry

	// suspect is the ejection flag: true from ejection until half-open
	// re-admission. While suspect the path is not admitted for selection.
	suspect bool
	// ejectedUntil is the backoff deadline (SPEC-H.4); canary successes
	// only count toward re-admission once now >= ejectedUntil.
	ejectedUntil time.Time
	// ejectCount drives the backoff (30s * 2^n, cap 10min) and decays one
	// step after HealthDecayAfter of continuous health.
	ejectCount int
	// healthySince marks the start of the current healthy streak; zero
	// while suspect or after a HARD_FAIL.
	healthySince time.Time
	// canaryOK counts consecutive canary successes: the half-open streak
	// while suspect, and — since it also accumulates on a healthy path —
	// the recovery streak a health-drained path uses to come back (F-02).
	// Any failed canary or ejection resets it to zero.
	canaryOK int
}

// Snapshot is a consistent view of a State for tests and diagnostics.
type Snapshot struct {
	Total       int // outcomes currently in the window
	Fails       int // HARD_FAILs in the window
	FailRatio   float64
	Consecutive int // length of the trailing HARD_FAIL run
	DistinctRun int // distinct destination hosts in that run
	Ejected     bool
	EjectCount  int
	BackoffLeft time.Duration // time until half-open probing starts
}

// destKey reduces a dial target ("host:port") to the destination HOST the
// distinctness guard counts. Anything that is not a host:port pair is used
// verbatim.
func destKey(dest string) string {
	if host, _, err := net.SplitHostPort(dest); err == nil && host != "" {
		return host
	}
	return dest
}

// Record folds one classified relay/dial outcome into the window and
// returns true exactly when THIS outcome newly ejects the path (the caller
// mirrors that onto the path lifecycle state, e.g. Active -> Suspect).
// Neutral outcomes are ignored entirely.
func (s *State) Record(now time.Time, dest string, o Outcome) bool {
	ejected, _ := s.RecordWithReason(now, dest, o)
	return ejected
}

// RecordWithReason is Record plus the SPEC-H.3 rule that fired, as a short
// stable reason string for viberoxy_path_ejections_total (F-16, SPEC-M):
// "consecutive_distinct" (rule A) or "fail_ratio" (rule B). The reason is
// meaningful only when ejected is true.
func (s *State) RecordWithReason(now time.Time, dest string, o Outcome) (ejected bool, reason string) {
	if o == Neutral {
		return false, "" // client abort: not evidence about the path
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.prune(now)
	s.entries = append(s.entries, entry{at: now, key: destKey(dest), fail: o == HardFail})
	if len(s.entries) > WindowSize {
		s.entries = append(s.entries[:0], s.entries[len(s.entries)-WindowSize:]...)
	}

	if o == HardFail {
		s.healthySince = time.Time{} // any hard failure breaks the healthy streak
	} else if !s.suspect && s.healthySince.IsZero() {
		s.healthySince = now
	}
	s.decay(now)

	if s.suspect {
		return false, "" // already ejected; the window keeps recording for diagnostics
	}
	if hit, why := s.ejectRuleHit(); hit {
		s.eject(now)
		return true, why
	}
	return false, ""
}

// NoteCanary folds one canary interval result for this path (SPEC-H.5)
// and returns true exactly when it re-admits an ejected path (the caller
// mirrors that onto the path lifecycle state, e.g. Suspect -> Active).
//
// Re-admission is half-open: while the ejection backoff has not elapsed,
// successes do not count; once it has, HalfOpenSuccesses consecutive
// canary successes are required, and any failed canary resets the streak.
func (s *State) NoteCanary(now time.Time, ok bool) (recovered bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !ok {
		s.canaryOK = 0
		s.healthySince = time.Time{}
		return false
	}
	if s.suspect {
		if now.Before(s.ejectedUntil) {
			s.canaryOK = 0 // still in backoff: no half-open probing yet
			return false
		}
		s.canaryOK++
		if s.canaryOK < HalfOpenSuccesses {
			return false
		}
		s.suspect = false
		s.ejectedUntil = time.Time{}
		s.canaryOK = 0
		s.healthySince = now
		return true
	}
	if s.healthySince.IsZero() {
		s.healthySince = now
	}
	// Consecutive successes accumulate on a healthy path too: that streak
	// is the recovery signal for a drained path (F-02), exposed through
	// CanaryStreak. Half-open re-admission above still owns its own count.
	s.canaryOK++
	s.decay(now)
	return false
}

// CanaryStreak reports the number of consecutive canary successes since the
// last failed canary (0 right after a failure or an ejection). It is the
// streak behind half-open re-admission while suspect and, on a healthy
// path, the recovery signal a health-drained path returns to Active on
// (F-02 — "canary successes").
func (s *State) CanaryStreak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canaryOK
}

// Ejected reports whether the path is currently ejected (not admitted for
// new traffic until half-open re-admission).
func (s *State) Ejected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.suspect
}

// EjectCount reports how many times the path has been ejected without an
// intervening HealthDecayAfter of health (the backoff exponent).
func (s *State) EjectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ejectCount
}

// BackoffRemaining reports how much of the current ejection backoff is
// left at now (0 once half-open probing may start).
func (s *State) BackoffRemaining(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.suspect {
		return 0
	}
	if d := s.ejectedUntil.Sub(now); d > 0 {
		return d
	}
	return 0
}

// Snapshot prunes the window as of now and returns a consistent view.
func (s *State) Snapshot(now time.Time) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(now)
	snap := Snapshot{
		Total:      len(s.entries),
		Ejected:    s.suspect,
		EjectCount: s.ejectCount,
	}
	for _, e := range s.entries {
		if e.fail {
			snap.Fails++
		}
	}
	if snap.Total > 0 {
		snap.FailRatio = float64(snap.Fails) / float64(snap.Total)
	}
	run, distinct := s.tailRun()
	snap.Consecutive = run
	snap.DistinctRun = distinct
	if s.suspect {
		if d := s.ejectedUntil.Sub(now); d > 0 {
			snap.BackoffLeft = d
		}
	}
	return snap
}

// prune drops outcomes older than WindowAge (SPEC-H.2).
func (s *State) prune(now time.Time) {
	cutoff := now.Add(-WindowAge)
	i := 0
	for i < len(s.entries) && s.entries[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		s.entries = append(s.entries[:0], s.entries[i:]...)
	}
}

// tailRun returns the length of the trailing HARD_FAIL run and the number
// of distinct destination hosts inside it.
func (s *State) tailRun() (run, distinct int) {
	seen := make(map[string]struct{})
	for i := len(s.entries) - 1; i >= 0; i-- {
		if !s.entries[i].fail {
			break
		}
		run++
		seen[s.entries[i].key] = struct{}{}
	}
	return run, len(seen)
}

// ejectRuleHit evaluates SPEC-H.3 and reports which rule fired: rule A
// (">= EjectConsecutive consecutive HARD_FAILs across >= EjectDistinctDest
// distinct destination HOSTS") returns "consecutive_distinct"; rule B
// (">= EjectMinOutcomes window outcomes with a fail ratio >= EjectFailRatio")
// returns "fail_ratio"; no hit returns ("", false). Caller holds s.mu.
func (s *State) ejectRuleHit() (bool, string) {
	// Rule A: consecutive failures spanning distinct destinations.
	if run, distinct := s.tailRun(); run >= EjectConsecutive && distinct >= EjectDistinctDest {
		return true, "consecutive_distinct"
	}
	// Rule B: enough outcomes, high enough failure ratio.
	if len(s.entries) >= EjectMinOutcomes {
		fails := 0
		for _, e := range s.entries {
			if e.fail {
				fails++
			}
		}
		if float64(fails)/float64(len(s.entries)) >= EjectFailRatio {
			return true, "fail_ratio"
		}
	}
	return false, ""
}

// eject flips the path out of selection and arms its backoff (SPEC-H.4).
// Caller holds s.mu and knows the path is not already suspect.
func (s *State) eject(now time.Time) {
	s.suspect = true
	s.ejectedUntil = now.Add(Backoff(s.ejectCount))
	s.ejectCount++
	s.canaryOK = 0
	s.healthySince = time.Time{}
}

// Backoff returns the ejection duration for the given eject count:
// BaseBackoff * 2^count, capped at MaxBackoff (SPEC-H.4).
func Backoff(count int) time.Duration {
	if count < 0 {
		count = 0
	}
	d := BaseBackoff
	for i := 0; i < count; i++ {
		d *= 2
		if d >= MaxBackoff {
			return MaxBackoff
		}
	}
	return d
}

// decay drops ejectCount one step once the path has been healthy for
// HealthDecayAfter (SPEC-H.4). Caller holds s.mu.
func (s *State) decay(now time.Time) {
	if s.suspect || s.ejectCount == 0 || s.healthySince.IsZero() {
		return
	}
	if now.Sub(s.healthySince) >= HealthDecayAfter {
		s.ejectCount--
		s.healthySince = now // next step needs another full HealthDecayAfter
	}
}
