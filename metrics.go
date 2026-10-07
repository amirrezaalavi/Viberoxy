package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"viberoxy/internal/health"
	"viberoxy/internal/path"
	"viberoxy/internal/retry"
)

// histogramBuckets are the fixed bucket edges (in seconds) shared by all
// histograms. Buckets are cumulative, matching the Prometheus convention.
var histogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16}

// version is the build version. Override at link time with
// -ldflags "-X main.version=v1.2.3".
var version = "dev"

type metricKind int

const (
	kindGauge metricKind = iota
	kindCounter
	kindHistogram
)

func (k metricKind) String() string {
	switch k {
	case kindGauge:
		return "gauge"
	case kindCounter:
		return "counter"
	case kindHistogram:
		return "histogram"
	default:
		return "unknown"
	}
}

// Metric is a single named metric with optional labels, implemented as a
// hand-rolled Prometheus text-format collector using only the stdlib.
type Metric struct {
	Name   string
	Help   string
	Kind   metricKind
	Labels []string

	mu     sync.Mutex
	series map[string]*seriesState
}

type seriesState struct {
	labels  []string
	value   float64 // gauge/counter value
	count   uint64  // histogram observation count
	sum     float64 // histogram sum of observations
	buckets []uint64
}

func newMetric(kind metricKind, name, help string, labels []string) *Metric {
	if name == "" {
		panic("metrics: metric name must not be empty")
	}
	return &Metric{
		Name:   name,
		Help:   help,
		Kind:   kind,
		Labels: labels,
		series: make(map[string]*seriesState),
	}
}

// NewGauge creates a gauge metric (last-set value wins).
func NewGauge(name, help string, labels ...string) *Metric {
	return newMetric(kindGauge, name, help, labels)
}

// NewCounter creates a counter metric (monotonically increasing).
func NewCounter(name, help string, labels ...string) *Metric {
	return newMetric(kindCounter, name, help, labels)
}

// NewHistogram creates a histogram metric over the fixed bucket set.
func NewHistogram(name, help string, labels ...string) *Metric {
	return newMetric(kindHistogram, name, help, labels)
}

func (m *Metric) stateFor(labelValues []string) *seriesState {
	if len(labelValues) != len(m.Labels) {
		panic(fmt.Sprintf("metrics: %s: got %d label values, want %d", m.Name, len(labelValues), len(m.Labels)))
	}
	key := strings.Join(labelValues, "\x00")
	s, ok := m.series[key]
	if !ok {
		s = &seriesState{
			labels:  append([]string(nil), labelValues...),
			buckets: make([]uint64, len(histogramBuckets)),
		}
		m.series[key] = s
	}
	return s
}

// Set stores a gauge value.
func (m *Metric) Set(v float64, labelValues ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateFor(labelValues).value = v
}

// Add increments a counter (or gauge) by delta.
func (m *Metric) Add(delta float64, labelValues ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateFor(labelValues).value += delta
}

// Inc increments a counter (or gauge) by one.
func (m *Metric) Inc(labelValues ...string) {
	m.Add(1, labelValues...)
}

// Observe records one histogram observation.
func (m *Metric) Observe(v float64, labelValues ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.stateFor(labelValues)
	s.count++
	s.sum += v
	for i, b := range histogramBuckets {
		if v <= b {
			s.buckets[i]++
		}
	}
}

// Value returns the current gauge/counter value, or the observation count for
// histograms. Intended for tests and debugging.
func (m *Metric) Value(labelValues ...string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.series[strings.Join(labelValues, "\x00")]
	if !ok {
		return 0
	}
	if m.Kind == kindHistogram {
		return float64(s.count)
	}
	return s.value
}

// Delete removes one series (identified by its label values) from the
// metric. Used to retire per-path series once their generation is gone,
// so cardinality stays bounded by the live path generations. No-op when
// the series was never recorded.
func (m *Metric) Delete(labelValues ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.series, strings.Join(labelValues, "\x00"))
}

// Registry holds a set of metrics and renders them in Prometheus text format.
type Registry struct {
	mu      sync.Mutex
	metrics []*Metric
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Register(m *Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, m)
}

// Write renders all metrics in Prometheus exposition format.
func (r *Registry) Write(w io.Writer) error {
	r.mu.Lock()
	metrics := append([]*Metric(nil), r.metrics...)
	r.mu.Unlock()
	for _, m := range metrics {
		m.writeTo(w)
	}
	return nil
}

// ServeHTTP implements http.Handler for the /metrics endpoint.
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := r.Write(w); err != nil {
		slog.Error("metrics write failed", "error", err)
	}
}

func (m *Metric) writeTo(w io.Writer) {
	m.mu.Lock()
	states := make([]*seriesState, 0, len(m.series))
	for _, s := range m.series {
		states = append(states, s)
	}
	m.mu.Unlock()

	sort.Slice(states, func(i, j int) bool {
		return strings.Join(states[i].labels, "\x00") < strings.Join(states[j].labels, "\x00")
	})

	fmt.Fprintf(w, "# HELP %s %s\n", m.Name, m.Help)
	fmt.Fprintf(w, "# TYPE %s %s\n", m.Name, m.Kind)

	for _, s := range states {
		switch m.Kind {
		case kindHistogram:
			for i, b := range histogramBuckets {
				le := strconv.FormatFloat(b, 'g', -1, 64)
				fmt.Fprintf(w, "%s_bucket%s %d\n", m.Name, m.labelString(s.labels, "le", le), s.buckets[i])
			}
			fmt.Fprintf(w, "%s_bucket%s %d\n", m.Name, m.labelString(s.labels, "le", "+Inf"), s.count)
			fmt.Fprintf(w, "%s_sum%s %s\n", m.Name, m.labelString(s.labels, "", ""), strconv.FormatFloat(s.sum, 'g', -1, 64))
			fmt.Fprintf(w, "%s_count%s %d\n", m.Name, m.labelString(s.labels, "", ""), s.count)
		default:
			fmt.Fprintf(w, "%s%s %s\n", m.Name, m.labelString(s.labels, "", ""), strconv.FormatFloat(s.value, 'g', -1, 64))
		}
	}
}

// labelString renders "{name=\"value\",...}" for a series, optionally appending
// an extra label (used for histogram "le"). Returns "" when unlabeled.
func (m *Metric) labelString(labels []string, extraName, extraValue string) string {
	if len(labels) == 0 && extraName == "" {
		return ""
	}
	var sb strings.Builder
	sb.WriteByte('{')
	for i, name := range m.Labels {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(name)
		sb.WriteString("=\"")
		sb.WriteString(escapeLabelValue(labels[i]))
		sb.WriteByte('"')
	}
	if extraName != "" {
		if len(labels) > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(extraName)
		sb.WriteString("=\"")
		sb.WriteString(extraValue)
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return sb.String()
}

func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

// Global observability metrics shared across the process.
//
// SPEC-M (F-16) metrics live next to the historical ones. Their labels:
// "path" is the path generation ID (internal/path Path.ID — monotonic,
// never reused), "slot" the stable WAN slot index; "path" and "slot" are
// therefore always distinct values.
var (
	metricsRegistry        = NewRegistry()
	metricWansActive       = NewGauge("viberoxy_wans_active", "Number of active WAN slots.")
	metricWanSpeedMbps     = NewGauge("viberoxy_wan_speed_mbps", "Last measured speed in Mbps per WAN slot.", "index")
	metricWanStability     = NewGauge("viberoxy_wan_stability", "Stability score per WAN slot (distinct exit IPs minus 1; 0 = stable or unprobed).", "index")
	metricProxyConnections = NewCounter("viberoxy_proxy_connections_total", "Total CONNECT attempts handled by the proxy.", "wan", "proto")
	metricProxyBytes       = NewCounter("viberoxy_proxy_bytes_total", "Bytes relayed through the proxy.", "wan", "direction")
	metricProxyLatency     = NewHistogram("viberoxy_proxy_latency_seconds", "Tunnel latency from CONNECT to close, in seconds.")
	metricTestDuration     = NewHistogram("viberoxy_test_duration_seconds", "Duration of one speed test, in seconds.")
	metricBuildInfo        = NewGauge("viberoxy_build_info", "Build information.", "version")

	// Per-path state and performance (refreshed from the pool on each
	// /metrics scrape; the TTFB histogram is fed per sample by the path
	// observer, never re-observed at scrape time).
	metricPathState    = NewGauge("viberoxy_path_state", "Path lifecycle state: 0=probation, 1=active, 2=suspect, 3=draining, 4=dead.", "path", "slot")
	metricPathInflight = NewGauge("viberoxy_path_inflight", "Connections in flight on this path generation.", "path", "slot")
	metricPathTTFB     = NewHistogram("viberoxy_path_ttfb_seconds", "Time-to-first-byte samples observed on paths, in seconds.")
	metricPathGoodput  = NewGauge("viberoxy_path_goodput_bps", "Goodput EWMA of this path generation in bits per second.", "path", "slot")

	// Classified connection outcomes and health ejections (fed at relay
	// end from health.ClassifyReason, and from the path observer on
	// ejection).
	metricConnOutcome   = NewCounter("viberoxy_conn_outcome_total", "Classified relay outcomes per path generation (SPEC-H.1).", "path", "outcome", "reason")
	metricPathEjections = NewCounter("viberoxy_path_ejections_total", "Health ejections per path generation, by the SPEC-H.3 rule that fired.", "path", "reason")

	// Retry, rotation, environment and selection behavior.
	metricRetryTotal          = NewCounter("viberoxy_retry_total", "Dial-stage retry events by stage and outcome.", "stage", "outcome")
	metricSwapTotal           = NewCounter("viberoxy_swap_total", "Rotation decisions by result (swapped|cooldown|dwell|hysteresis|no_candidate).", "result")
	metricEnvDegraded         = NewGauge("viberoxy_env_degraded", "1 while the environmental canary breaker is tripped (SPEC-H.6), else 0.")
	metricSelectionLastResort = NewCounter("viberoxy_selection_last_resort_total", "Selections that had to fall back to a degraded (or, last resort, draining) path.")

	// Drains and the must-stay-zero session-reset counter.
	metricDrainSeconds        = NewHistogram("viberoxy_drain_seconds", "Time a slot spent draining before it was reaped or un-drained, in seconds.")
	metricSessionResetOnDrain = NewCounter("viberoxy_session_reset_on_drain_total", "Sessions reset because a path drained. MUST stay 0: a drain never resets a live session.")
)

// recordConnOutcome counts one classified relay outcome against the path
// generation that carried it (F-16 / SPEC-M). Called at relay end with the
// result of health.ClassifyReason — the very classification the health
// window is fed — so the metric and the window can never disagree.
func recordConnOutcome(p *path.Path, o health.Outcome, reason string) {
	if p == nil {
		return
	}
	metricConnOutcome.Inc(strconv.FormatUint(p.ID, 10), o.String(), reason)
}

// init registers every metric and wires the hooks the internal packages
// expose (retry events, path ejections/samples). Those packages cannot
// import this registry — package main sits above them — so the hook is
// installed here, once, instead of an import cycle existing.
func init() {
	metricsRegistry.Register(metricWansActive)
	metricsRegistry.Register(metricWanSpeedMbps)
	metricsRegistry.Register(metricWanStability)
	metricsRegistry.Register(metricProxyConnections)
	metricsRegistry.Register(metricProxyBytes)
	metricsRegistry.Register(metricProxyLatency)
	metricsRegistry.Register(metricTestDuration)
	metricsRegistry.Register(metricBuildInfo)
	metricsRegistry.Register(metricPathState)
	metricsRegistry.Register(metricPathInflight)
	metricsRegistry.Register(metricPathTTFB)
	metricsRegistry.Register(metricPathGoodput)
	metricsRegistry.Register(metricConnOutcome)
	metricsRegistry.Register(metricPathEjections)
	metricsRegistry.Register(metricRetryTotal)
	metricsRegistry.Register(metricSwapTotal)
	metricsRegistry.Register(metricEnvDegraded)
	metricsRegistry.Register(metricSelectionLastResort)
	metricsRegistry.Register(metricDrainSeconds)
	metricsRegistry.Register(metricSessionResetOnDrain)
	metricBuildInfo.Set(1, version)

	// Zero-valued series that must be visible from the first scrape:
	// env_degraded starts healthy, no selection has fallen back yet, and
	// session_reset_on_drain_total must read 0 forever (SPEC-M).
	metricEnvDegraded.Set(0)
	metricSelectionLastResort.Set(0)
	metricSessionResetOnDrain.Set(0)

	// internal/retry -> viberoxy_retry_total{stage,outcome}.
	retry.SetEventHook(func(stage, outcome string) {
		metricRetryTotal.Inc(stage, outcome)
	})

	// internal/path events -> ejection counter + TTFB histogram.
	path.SetObserver(func(ev path.Event) {
		if ev.Path == nil {
			return
		}
		switch ev.Kind {
		case path.EventEjection:
			metricPathEjections.Inc(strconv.FormatUint(ev.Path.ID, 10), ev.Reason)
		case path.EventSample:
			if ev.TTFB > 0 {
				metricPathTTFB.Observe(ev.TTFB.Seconds())
			}
		}
	})
}
