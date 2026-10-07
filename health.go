package main

import (
	"io"
	"net/http"
	"strconv"
	"sync"
	"viberoxy/internal/health"
	"viberoxy/internal/path"
)

// NewObservabilityHandler returns an http.Handler exposing:
//   - /metrics  — Prometheus text-format exposition
//   - /healthz  — liveness probe (always 200 when the process is up)
//   - /readyz   — readiness probe (200 iff at least one WAN is active)
func NewObservabilityHandler(pool *WANPool) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshPoolMetrics(pool)
		metricsRegistry.ServeHTTP(w, r)
	}))

	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok\n")
	}))

	mux.Handle("/readyz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if pool != nil && pool.RoutableCount(DefaultFailThreshold) >= 1 {
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "ready\n")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "not ready: no routable WANs\n")
	}))

	return mux
}

// bool01 maps a Go bool onto a Prometheus 0/1 gauge value.
func bool01(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// refreshPoolMetrics syncs pool-derived gauges before each scrape so /metrics
// always reflects the current state: the environment breaker gauge (SPEC-H.6,
// read straight from internal/health — the single source of truth), the WAN
// gauges and the per-path series.
func refreshPoolMetrics(pool *WANPool) {
	metricEnvDegraded.Set(bool01(health.EnvDegraded()))
	if pool == nil {
		return
	}
	metricWansActive.Set(float64(pool.ActiveCount()))
	for i := range pool.Slots {
		if speed := pool.SlotSpeedMbps(i); speed > 0 {
			metricWanSpeedMbps.Set(speed, strconv.Itoa(i))
		}
	}
	refreshPathSeries(pool)
}

// trackedPath is a path generation whose series are published: the current
// occupant of a slot, or a retired generation that still carries in-flight
// connections (its drain must stay visible until the last flow finishes).
type trackedPath struct {
	p    *path.Path
	id   string
	slot string
}

var (
	pathSeriesMu sync.Mutex
	trackedPaths = map[uint64]*trackedPath{}
)

// refreshPathSeries publishes viberoxy_path_state / _inflight / _goodput_bps
// for every path generation that must be visible — each slot's current
// occupant plus retired generations still holding connections — and deletes
// the series of generations that are gone (not current, nothing in flight),
// so cardinality follows live path generations instead of growing forever.
func refreshPathSeries(pool *WANPool) {
	pathSeriesMu.Lock()
	defer pathSeriesMu.Unlock()

	current := make(map[uint64]bool, len(pool.Slots))
	for i, slot := range pool.Slots {
		slot.mu.Lock()
		cur := slot.Current.Load()
		slot.mu.Unlock()
		if cur == nil {
			continue
		}
		t, ok := trackedPaths[cur.ID]
		if !ok {
			t = &trackedPath{p: cur, id: strconv.FormatUint(cur.ID, 10), slot: strconv.Itoa(i)}
			trackedPaths[cur.ID] = t
		}
		current[cur.ID] = true
		publishPathSeries(t)
	}

	for id, t := range trackedPaths {
		if current[id] {
			continue
		}
		if t.p == nil || t.p.Conns() == 0 {
			// Retired generation with nothing in flight: forget it and
			// drop its series (counters keep their totals; only the
			// state/inflight/goodput gauges follow the generation).
			metricPathState.Delete(t.id, t.slot)
			metricPathInflight.Delete(t.id, t.slot)
			metricPathGoodput.Delete(t.id, t.slot)
			delete(trackedPaths, id)
			continue
		}
		publishPathSeries(t) // retired but still draining: keep reporting
	}
}

// publishPathSeries writes one generation's gauges (state as the raw
// internal/path.State value: 0=probation, 1=active, 2=suspect,
// 3=draining, 4=dead).
func publishPathSeries(t *trackedPath) {
	metricPathState.Set(float64(t.p.GetState()), t.id, t.slot)
	metricPathInflight.Set(float64(t.p.Conns()), t.id, t.slot)
	metricPathGoodput.Set(t.p.GoodputBps(), t.id, t.slot)
}
