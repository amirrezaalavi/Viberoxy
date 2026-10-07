// Package health owns viberoxy's WAN health subsystem (SPEC-H of the
// review report): passive outcome classification, a per-path sliding
// outcome window with ejection and exponential backoff, and the active
// canary policy (endpoints, jittered interval, the all-endpoints-fail rule
// and the global environmental breaker).
//
// Clock injection: every time-dependent function takes an explicit
// `now time.Time` (or a *rand.Rand for jitter). Production callers pass
// time.Now()/a seeded source; tests pass a fake clock and advance it, so
// window math, backoff doubling/decay and half-open re-admission are fully
// deterministic (T-HLT-01..06).
//
// The package deliberately knows nothing about WAN slots or *path.Path:
// it is pure policy. Callers (relay.go, path.go, main.go) mirror the
// returned decisions onto path/slot lifecycle state.
package health

import "time"

// Outcome is SPEC-H.1's classification of one proxied connection, computed
// at relay end from (up, down, error, duration) — or at dial end for a
// dial-stage failure.
type Outcome uint8

const (
	// OK — bytes reached the client (down > 0).
	OK Outcome = iota
	// HardFail — the path failed: dial error, or the client sent data and
	// nothing ever came back.
	HardFail
	// Neutral — the client aborted before sending anything (up == 0 &&
	// down == 0). Ignored: it says nothing about the path.
	Neutral
)

func (o Outcome) String() string {
	switch o {
	case OK:
		return "ok"
	case HardFail:
		return "hard_fail"
	case Neutral:
		return "neutral"
	default:
		return "unknown"
	}
}

const (
	// TFirst is SPEC-H.1's first-byte bound: when the client has sent
	// data, no upstream byte arriving within TFirst of the relay start is
	// a HARD_FAIL.
	TFirst = 8 * time.Second

	// WindowAge / WindowSize are SPEC-H.2's sliding window: the last 60s
	// of outcomes, at most 30 of them, whichever bound is smaller.
	WindowAge  = 60 * time.Second
	WindowSize = 30

	// EjectConsecutive + EjectDistinctDest is SPEC-H.3 rule A: eject on
	// >= 3 consecutive HARD_FAILs spanning >= 3 distinct destination
	// HOSTS. The distinctness guard keeps one dead target site from
	// ejecting a healthy path (T-HLT-06).
	EjectConsecutive  = 3
	EjectDistinctDest = 3

	// EjectMinOutcomes + EjectFailRatio is SPEC-H.3 rule B: eject when
	// the window holds >= 8 outcomes with a HARD_FAIL ratio >= 50%.
	// Deliberately un-guarded by destination distinctness: a path that
	// black-holes half of everything it carries must go even when all
	// those connections share one destination (RT-02).
	EjectMinOutcomes = 8
	EjectFailRatio   = 0.5

	// BaseBackoff / MaxBackoff are SPEC-H.4: ejectFor = 30s * 2^ejectCount,
	// capped at 10 minutes; HealthDecayAfter is how long a path must stay
	// healthy before ejectCount decays one step.
	BaseBackoff      = 30 * time.Second
	MaxBackoff       = 10 * time.Minute
	HealthDecayAfter = 10 * time.Minute

	// CanaryIntervalMin/Max + CanaryJitter are SPEC-H.5: canaries run
	// every 15–30s with ±20% jitter.
	CanaryIntervalMin = 15 * time.Second
	CanaryIntervalMax = 30 * time.Second
	CanaryJitter      = 0.2

	// HalfOpenSuccesses is SPEC-H.5: an ejected path is re-admitted after
	// this many consecutive canary successes (half-open probing starts
	// once the ejection backoff has elapsed).
	HalfOpenSuccesses = 2

	// EnvFailRatio is SPEC-H.6: when this fraction of paths fails its
	// canaries within the SAME interval, the failure is environmental —
	// nothing is ejected or drained, only the env_degraded gauge moves.
	EnvFailRatio = 0.75
)

// Result is the raw evidence Classify turns into an Outcome (SPEC-H.1).
// Err is the splice error at close (nil for a clean close); DialErr is a
// dial-stage failure, which never reaches the splice; Duration is the relay
// duration; TFirst overrides the default first-byte bound (0 => TFirst).
type Result struct {
	Up       int64
	Down     int64
	Err      error
	DialErr  error
	Duration time.Duration
	TFirst   time.Duration
}

func (r Result) tFirst() time.Duration {
	if r.TFirst > 0 {
		return r.TFirst
	}
	return TFirst
}

// Classify implements SPEC-H.1:
//
//	OK        — down > 0.
//	HARD_FAIL — dial error; OR up > 0 && down == 0 at close; OR no first
//	            byte within T_FIRST while the client sent data.
//	NEUTRAL   — up == 0 && down == 0 (client abort) — ignore.
//
// The two HARD_FAIL close clauses are listed separately because the spec
// is: a relay that outlived T_FIRST with no byte back, and one that ended
// earlier with no byte back, are both "the client sent data and nothing
// ever came back" — no upstream byte ever arrived either way, so both are
// hard failures; the T_FIRST branch is the timed flavor of that rule.
func Classify(r Result) Outcome {
	if r.DialErr != nil {
		return HardFail // dial error
	}
	if r.Down > 0 {
		return OK // success rule: bytes reached the client
	}
	if r.Up > 0 && r.Duration >= r.tFirst() {
		return HardFail // no first byte within T_FIRST while the client sent data
	}
	if r.Up > 0 {
		return HardFail // up > 0 && down == 0 at close
	}
	return Neutral // up == 0 && down == 0: client abort
}
