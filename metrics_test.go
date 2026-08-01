package main

import (
	"math"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestRegistry_ExpositionFormat(t *testing.T) {
	reg := NewRegistry()
	g := NewGauge("test_gauge", "A test gauge.", "wan")
	reg.Register(g)
	g.Set(42, "0")
	g.Set(7, "1")

	var sb strings.Builder
	if err := reg.Write(&sb); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	out := sb.String()

	for _, want := range []string{
		"# HELP test_gauge A test gauge.",
		"# TYPE test_gauge gauge",
		`test_gauge{wan="0"} 42`,
		`test_gauge{wan="1"} 7`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q in:\n%s", want, out)
		}
	}
}

func TestCounter_Increments(t *testing.T) {
	reg := NewRegistry()
	c := NewCounter("test_counter_total", "A test counter.", "proto")
	reg.Register(c)
	c.Inc("connect")
	c.Add(4, "connect")

	var sb strings.Builder
	if err := reg.Write(&sb); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	out := sb.String()

	for _, want := range []string{
		"# HELP test_counter_total A test counter.",
		"# TYPE test_counter_total counter",
		`test_counter_total{proto="connect"} 5`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q in:\n%s", want, out)
		}
	}

	if v := c.Value("connect"); v != 5 {
		t.Errorf("Value = %v, want 5", v)
	}
}

func TestHistogram_Observe(t *testing.T) {
	reg := NewRegistry()
	h := NewHistogram("test_histogram_seconds", "A test histogram.")
	reg.Register(h)
	h.Observe(0.03)
	h.Observe(0.25)
	h.Observe(1.0)

	var sb strings.Builder
	if err := reg.Write(&sb); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	out := sb.String()

	for _, want := range []string{
		"# HELP test_histogram_seconds A test histogram.",
		"# TYPE test_histogram_seconds histogram",
		`test_histogram_seconds_bucket{le="0.05"} 1`,
		`test_histogram_seconds_bucket{le="0.1"} 1`,
		`test_histogram_seconds_bucket{le="0.25"} 2`,
		`test_histogram_seconds_bucket{le="0.5"} 2`,
		`test_histogram_seconds_bucket{le="1"} 3`,
		`test_histogram_seconds_bucket{le="2"} 3`,
		`test_histogram_seconds_bucket{le="4"} 3`,
		`test_histogram_seconds_bucket{le="8"} 3`,
		`test_histogram_seconds_bucket{le="16"} 3`,
		`test_histogram_seconds_bucket{le="+Inf"} 3`,
		`test_histogram_seconds_count 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q in:\n%s", want, out)
		}
	}

	found := false
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "test_histogram_seconds_sum ") {
			v, err := strconv.ParseFloat(strings.TrimPrefix(line, "test_histogram_seconds_sum "), 64)
			if err != nil {
				t.Fatalf("parse sum line %q: %v", line, err)
			}
			if math.Abs(v-1.28) > 1e-9 {
				t.Errorf("sum = %v, want ~1.28", v)
			}
			found = true
		}
	}
	if !found {
		t.Error("exposition missing test_histogram_seconds_sum line")
	}
}

func TestHistogram_WithLabels(t *testing.T) {
	reg := NewRegistry()
	h := NewHistogram("test_labelled_seconds", "help", "wan")
	reg.Register(h)
	h.Observe(0.5, "0")

	var sb strings.Builder
	if err := reg.Write(&sb); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	out := sb.String()

	for _, want := range []string{
		`test_labelled_seconds_bucket{wan="0",le="0.5"} 1`,
		`test_labelled_seconds_bucket{wan="0",le="+Inf"} 1`,
		`test_labelled_seconds_sum{wan="0"} 0.5`,
		`test_labelled_seconds_count{wan="0"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q in:\n%s", want, out)
		}
	}
}

func TestLabelEscaping(t *testing.T) {
	reg := NewRegistry()
	g := NewGauge("test_escape", "help", "lbl")
	reg.Register(g)
	g.Set(1, "a\"b\\c\nd")

	var sb strings.Builder
	if err := reg.Write(&sb); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if !strings.Contains(sb.String(), `test_escape{lbl="a\"b\\c\nd"} 1`) {
		t.Errorf("label escaping failed, got:\n%s", sb.String())
	}
}

func TestObservabilityHandler(t *testing.T) {
	pool := NewWANPool(1, 10700)
	h := NewObservabilityHandler(pool)

	// /healthz is always 200.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 {
		t.Errorf("healthz = %d, want 200", rec.Code)
	}

	// /readyz is 503 with no active WANs.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 503 {
		t.Errorf("readyz (no wans) = %d, want 503", rec.Code)
	}

	// /readyz is 200 with an active WAN.
	pool.Slots[0].State = StateActive
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 200 {
		t.Errorf("readyz (active) = %d, want 200", rec.Code)
	}

	// /metrics serves the global registry in Prometheus text format.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Errorf("metrics = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("metrics Content-Type = %q, want text/plain", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"# HELP viberoxy_wans_active",
		"# TYPE viberoxy_wans_active gauge",
		"# HELP viberoxy_proxy_connections_total",
		"# TYPE viberoxy_proxy_connections_total counter",
		"# TYPE viberoxy_proxy_latency_seconds histogram",
		`viberoxy_build_info{version="dev"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q in:\n%s", want, body)
		}
	}
}

func TestRefreshPoolMetrics(t *testing.T) {
	pool := NewWANPool(2, 10700)
	pool.Slots[0].State = StateActive
	pool.SetSlotSpeedMbps(0, 12.5)

	refreshPoolMetrics(pool)

	if v := metricWansActive.Value(); v != 1 {
		t.Errorf("wans_active = %v, want 1", v)
	}
	if v := metricWanSpeedMbps.Value("0"); v != 12.5 {
		t.Errorf("wan_speed_mbps[0] = %v, want 12.5", v)
	}
	if v := metricWanSpeedMbps.Value("1"); v != 0 {
		t.Errorf("wan_speed_mbps[1] = %v, want 0 (no speed recorded)", v)
	}
}

func TestParseConfig_MetricsEnv(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "METRICS_PORT", "9090")
	setenv(t, "ACCESS_LOG", "false")

	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig() error: %v", err)
	}
	if cfg.MetricsPort != 9090 {
		t.Errorf("MetricsPort = %d, want 9090", cfg.MetricsPort)
	}
	if cfg.AccessLog != false {
		t.Errorf("AccessLog = %v, want false", cfg.AccessLog)
	}
}

func TestParseConfig_InvalidMetricsPort(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "METRICS_PORT", "70000")

	if _, err := parseConfig(); err == nil {
		t.Fatal("expected error for METRICS_PORT=70000, got nil")
	}
}

func TestParseConfig_InvalidAccessLog(t *testing.T) {
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
	setenv(t, "ACCESS_LOG", "maybe")

	if _, err := parseConfig(); err == nil {
		t.Fatal("expected error for ACCESS_LOG=maybe, got nil")
	}
}
