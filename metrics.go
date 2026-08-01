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
var (
	metricsRegistry        = NewRegistry()
	metricWansActive       = NewGauge("viberoxy_wans_active", "Number of active WAN slots.")
	metricWanSpeedMbps     = NewGauge("viberoxy_wan_speed_mbps", "Last measured speed in Mbps per WAN slot.", "index")
	metricProxyConnections = NewCounter("viberoxy_proxy_connections_total", "Total CONNECT attempts handled by the proxy.", "wan", "proto")
	metricProxyBytes       = NewCounter("viberoxy_proxy_bytes_total", "Bytes relayed through the proxy.", "wan", "direction")
	metricProxyLatency     = NewHistogram("viberoxy_proxy_latency_seconds", "Tunnel latency from CONNECT to close, in seconds.")
	metricTestDuration     = NewHistogram("viberoxy_test_duration_seconds", "Duration of one speed test, in seconds.")
	metricBuildInfo        = NewGauge("viberoxy_build_info", "Build information.", "version")
)

func init() {
	metricsRegistry.Register(metricWansActive)
	metricsRegistry.Register(metricWanSpeedMbps)
	metricsRegistry.Register(metricProxyConnections)
	metricsRegistry.Register(metricProxyBytes)
	metricsRegistry.Register(metricProxyLatency)
	metricsRegistry.Register(metricTestDuration)
	metricsRegistry.Register(metricBuildInfo)
	metricBuildInfo.Set(1, version)
}
