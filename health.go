package main

import (
	"io"
	"net/http"
	"strconv"
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
		if pool != nil && pool.ActiveCount() >= 1 {
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, "ready\n")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "not ready: no active WANs\n")
	}))

	return mux
}

// refreshPoolMetrics syncs pool-derived gauges before each scrape so /metrics
// always reflects the current state.
func refreshPoolMetrics(pool *WANPool) {
	if pool == nil {
		return
	}
	metricWansActive.Set(float64(pool.ActiveCount()))
	for i := range pool.Slots {
		if speed := pool.SlotSpeedMbps(i); speed > 0 {
			metricWanSpeedMbps.Set(speed, strconv.Itoa(i))
		}
	}
}
