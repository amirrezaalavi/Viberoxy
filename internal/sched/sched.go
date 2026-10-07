// Package sched ranks the WAN paths a connection may be handed and
// reserves the chosen path atomically (review finding F-15: selection
// quality).
//
// The package never decides ELIGIBILITY — that belongs to the WAN pool
// (slot state, fail threshold, health ejection, tried-set exclusion,
// draining last resort). It receives already-partitioned candidate tiers
// and only answers "which of these", plus the atomic reservation that
// makes the answer and the load accounting one indivisible step.
package sched

import (
	"log/slog"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
	"viberoxy/internal/affinity"
	"viberoxy/internal/path"
)

// Request is one selection: the eligible candidates partitioned into the
// pool's three fallback tiers (each in slot order), evaluated strictly in
// order — Routable first; Degraded only when Routable is empty (the
// documented "no routable WAN" degradation); LastResort only when both are
// empty (the documented draining fallback). A nil/empty everything yields
// (nil, nil): the caller keeps its no-candidate semantics.
type Request struct {
	// Routable holds the healthy Active candidates (pool-computed: Active
	// slot, under threshold, live process, not ejected, not tried).
	Routable []*path.Path
	// Degraded holds every untried Active candidate, used only when
	// Routable is empty — over-threshold and health-ejected paths may
	// appear here (availability over preference; production never hands
	// out a retired generation through this tier).
	Degraded []*path.Path
	// LastResort holds untried Draining candidates: only when both other
	// tiers are empty, so a drain never attracts a new connection while
	// any Active path exists (F-02).
	LastResort []*path.Path

	// ClientID and SiteKey are the F-05 affinity key: when both are
	// non-empty the HRW (rendezvous) pick over the chosen tier is tried
	// first and only spilled to P2C when that path carries more than
	// AffinitySpillFactor x the tier's median inflight. Either empty
	// selects with pure P2C.
	ClientID string
	SiteKey  string
}

// Select picks one candidate from req's tiers and reserves it BEFORE
// returning: the rank decision and Inflight.Add(1) happen inside one
// critical section, so a concurrent selection can never act on the cost
// this one just used (no select/reserve TOCTOU — T-SEL-04). The returned
// release undoes exactly that reservation and must be called exactly once
// by the winner's owner (a nil path returns a nil release).
//
// Ranking among the tier's paths is power-of-two-choices (T-SEL-02/03):
// two distinct candidates are sampled, the cheaper one wins, and a tie is
// broken by a seeded coin — never by slot index (T-SEL-06). With an
// affinity key (F-05) the HRW pick is tried first and only spilled to
// P2C when it carries more than AffinitySpillFactor x the tier's median
// inflight (availability over stickiness).
func Select(req Request) (*path.Path, func()) {
	selMu.Lock()
	defer selMu.Unlock()

	tier := req.Routable
	if len(tier) == 0 {
		slog.Warn("no routable WAN: falling back to degraded slots")
		tier = req.Degraded
	}
	if len(tier) == 0 {
		slog.Warn("no active WAN: draining slot selected as last resort")
		tier = req.LastResort
	}
	if len(tier) == 0 {
		return nil, nil
	}

	chosen := pickLocked(tier, req)
	// Atomic with the ranking above: same critical section, so no other
	// Select can observe or act on the pre-reservation costs.
	chosen.Reserve()
	return chosen, func() { chosen.Release() }
}

// selMu serializes every Select (rank + reserve are one step) and guards
// rng. It is deliberately package-wide: selections are nanoseconds long,
// and one lock keeps the atomicity claim trivial to audit.
var (
	selMu sync.Mutex
	rng   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// SeedRand replaces the selection RNG source. Tests call it for
// deterministic picks; production never does. Safe to call between
// selections (it takes selMu), never concurrently with itself.
func SeedRand(seed int64) {
	selMu.Lock()
	rng = rand.New(rand.NewSource(seed))
	selMu.Unlock()
}

const (
	// TTFBFloor is the latency term's minimum: a path with no TTFB
	// sample yet costs as if it answered in 50ms, so silence never makes
	// a path look infinitely fast.
	TTFBFloor = 50 * time.Millisecond

	// RampWindow / RampStart shape the slow-start ramp: a freshly
	// activated path starts at RampStart of its full weight and reaches
	// 1.0 after RampWindow — young paths are ramped in instead of
	// being fed at full rate with no history.
	RampWindow = 60 * time.Second
	RampStart  = 0.1

	// MinWeight / MaxWeight clamp the goodput/median ratio so one
	// path's outlier goodput cannot dominate the cost of the tier.
	MinWeight = 0.25
	MaxWeight = 4.0

	// AffinitySpillFactor is AFFINITY_BULK_SPILL (F-05): a sticky HRW
	// pick carrying more than this multiple of the eligible tier's
	// median inflight spills to P2C — availability over stickiness.
	AffinitySpillFactor = 3.0
)

// cost is (Inflight+1) x max(ewmaTTFB, 50ms floor) / weight: expected
// time-to-serve under the current load, discounted by how much the path
// has proven it can carry.
func cost(p *path.Path, medianGoodput float64, now time.Time) float64 {
	ttfb := p.TTFB()
	if ttfb < TTFBFloor {
		ttfb = TTFBFloor
	}
	return float64(p.Inflight.Load()+1) * float64(ttfb) / weight(p, medianGoodput, now)
}

// weight = clamp(goodputEWMA/medianGoodput, 0.25, 4) x ramp(age) x
// stateFactor. A tier with no goodput signal at all (median 0) uses a
// neutral ratio of 1 instead of the clamp, so a cold pool balances on
// load alone.
func weight(p *path.Path, medianGoodput float64, now time.Time) float64 {
	var goodput float64
	switch {
	case medianGoodput > 0 && !math.IsNaN(medianGoodput) && !math.IsInf(medianGoodput, 0):
		goodput = clamp(p.GoodputBps()/medianGoodput, MinWeight, MaxWeight)
	default:
		goodput = 1
	}
	return goodput * ramp(now.Sub(p.CreatedAt)) * stateFactor(p.GetState())
}

// ramp is the slow-start factor: min(1, RampStart + (1-RampStart) x
// age/RampWindow).
func ramp(age time.Duration) float64 {
	if age <= 0 {
		return RampStart
	}
	f := RampStart + (1-RampStart)*float64(age)/float64(RampWindow)
	if f > 1 {
		return 1
	}
	return f
}

// stateFactor disfavors (never excludes) non-Active generations inside a
// tier: within the Degraded tier a healthy-but-over-threshold path should
// beat an ejected one, and the Draining LastResort tier is uniformly
// penalized without becoming unincludable (availability is preserved).
func stateFactor(s path.State) float64 {
	switch s {
	case path.Active:
		return 1.0
	case path.Suspect:
		return 1.5
	case path.Draining:
		return 2.0
	case path.Probation, path.Dead:
		return 4.0
	default:
		return 1.0
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// medianGoodput is the tier's median goodput EWMA (zeros included; the
// even-length case averages the two middle values). Zero when the tier
// carries no signal, which weight() reads as "neutral".
func medianGoodput(cands []*path.Path) float64 {
	if len(cands) == 0 {
		return 0
	}
	vals := make([]float64, 0, len(cands))
	for _, p := range cands {
		vals = append(vals, p.GoodputBps())
	}
	sort.Float64s(vals)
	return medianOf(vals)
}

func medianOf(vals []float64) float64 {
	n := len(vals)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return vals[n/2]
	}
	return (vals[n/2-1] + vals[n/2]) / 2
}

// pickLocked ranks the tier: with an affinity key (F-05) the HRW pick is
// tried first and accepted while it is within AffinitySpillFactor x the
// tier's median inflight; otherwise — and always without a key — the tier
// goes through P2C. Caller holds selMu.
func pickLocked(tier []*path.Path, req Request) *path.Path {
	if req.ClientID != "" && req.SiteKey != "" {
		if stick := affinity.Pick(req.ClientID, req.SiteKey, tier); stick != nil {
			if float64(stick.Conns()) <= AffinitySpillFactor*medianConns(tier) {
				return stick
			}
			// AFFINITY_BULK_SPILL: the sticky path carries more than
			// AffinitySpillFactor x the eligible tier's median —
			// availability over stickiness; fall through to P2C.
		}
	}
	return p2c(tier)
}

// medianConns is the tier's median clamped connection count (even-length
// averages the two middle values) — the reference load the sticky path is
// compared against in the bulk-spill rule.
func medianConns(cands []*path.Path) float64 {
	if len(cands) == 0 {
		return 0
	}
	vals := make([]float64, 0, len(cands))
	for _, p := range cands {
		vals = append(vals, float64(p.Conns()))
	}
	sort.Float64s(vals)
	return medianOf(vals)
}

// p2c samples two distinct candidates and returns the cheaper one; exactly
// tied costs go to a coin flip over the two samples — never to slot
// order.
func p2c(tier []*path.Path) *path.Path {
	if len(tier) == 1 {
		return tier[0]
	}
	now := time.Now()
	med := medianGoodput(tier)

	i := rng.Intn(len(tier))
	j := (i + 1 + rng.Intn(len(tier)-1)) % len(tier)
	a, b := tier[i], tier[j]
	ca, cb := cost(a, med, now), cost(b, med, now)
	if costsTie(ca, cb) {
		if rng.Intn(2) == 0 {
			return a
		}
		return b
	}
	if ca < cb {
		return a
	}
	return b
}

// costsTie reports whether two costs are EXACTLY equal — the case the
// random tie-break exists for (T-SEL-06). Equal inputs compute
// bitwise-equal costs (same formula, same IEEE-754 arithmetic), and the
// ramp's clamp to 1.0 past RampWindow makes genuinely-equal generations
// (same load, same latency floor, same weight) land on exactly equal
// costs. A near-equal-but-not-equal cost is a real (if small)
// preference — typically the slow-start ramp's intended gradient between
// generations of different age — and is decided by value, never by slot
// order.
func costsTie(a, b float64) bool { return a == b }
