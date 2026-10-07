package health

import (
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultCanaryEndpoints is the built-in HEALTH_ENDPOINTS value: two
// independent endpoints operated by different providers, so one outage (or
// one SPOF like api.ipify.org) cannot fail a path's canary on its own
// (F-07). ipify is deliberately absent — it is for exit-IP reporting only,
// never for health.
const DefaultCanaryEndpoints = "https://www.gstatic.com/generate_204,https://www.cloudflare.com/cdn-cgi/trace"

// EndpointsFromEnv parses HEALTH_ENDPOINTS (comma- and/or
// whitespace-separated URLs). Unset, empty or all-blank falls back to
// DefaultCanaryEndpoints; entries that are blank after trimming are
// dropped. It never returns an empty slice — a canary with zero endpoints
// could not fail, which would silently disable health.
func EndpointsFromEnv() []string {
	return ParseEndpoints(os.Getenv("HEALTH_ENDPOINTS"))
}

// ParseEndpoints is EndpointsFromEnv's pure core (env reading kept out so
// tests can drive it directly).
func ParseEndpoints(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	if len(out) == 0 {
		return strings.Split(DefaultCanaryEndpoints, ",")
	}
	return out
}

// PathCanary is one path's canary result for one interval. Index is the
// caller's slot identity — health knows nothing about paths or slots.
type PathCanary struct {
	Index int
	OK    bool
	RTT   time.Duration
}

// PathOK folds one path's per-endpoint canary results into the path-level
// verdict: a path's canary fails only if ALL endpoints failed (SPEC-H.5).
func PathOK(endpointOK ...bool) bool {
	for _, ok := range endpointOK {
		if ok {
			return true
		}
	}
	return false
}

// ApplyCanary evaluates one canary interval across every path (SPEC-H.6):
// it reports whether the global environmental breaker tripped — >=75% of
// paths failed their canaries within the SAME interval — and which paths
// should have their canary failure recorded.
//
// When degraded it returns a EMPTY failing list: an environmental failure
// must not eject, drain or kill anything (T-HLT-03 / T-CHAOS-05) — the
// caller only raises the env_degraded gauge and keeps serving.
func ApplyCanary(results []PathCanary) (degraded bool, failing []int) {
	fails := 0
	for _, r := range results {
		if !r.OK {
			fails++
		}
	}
	degraded = EnvFailure(fails, len(results))
	if degraded {
		return true, nil
	}
	for _, r := range results {
		if !r.OK {
			failing = append(failing, r.Index)
		}
	}
	return false, failing
}

// EnvFailure is the SPEC-H.6 breaker predicate: at least EnvFailRatio of
// the total failed. Zero paths never degrade.
func EnvFailure(fails, total int) bool {
	if total <= 0 {
		return false
	}
	return float64(fails)/float64(total) >= EnvFailRatio
}

// envDegraded is the process-wide viberoxy_env_degraded gauge (SPEC-H.6).
// It lives here rather than in metrics.go because that file is owned by a
// parallel task; main.go sets it from ApplyCanary's verdict and logs it.
var envDegraded atomic.Bool

// SetEnvDegraded moves the env_degraded gauge.
func SetEnvDegraded(v bool) { envDegraded.Store(v) }

// EnvDegraded reads the env_degraded gauge.
func EnvDegraded() bool { return envDegraded.Load() }

// JitterInterval returns the next canary interval: base ± CanaryJitter,
// clamped back into the 15–30s band (SPEC-H.5). r is the injected random
// source (nil uses the global source — tests pass a seeded *rand.Rand).
func JitterInterval(base time.Duration, r *rand.Rand) time.Duration {
	if base < CanaryIntervalMin {
		base = CanaryIntervalMin
	}
	if base > CanaryIntervalMax {
		base = CanaryIntervalMax
	}
	f := 1 + (r.Float64()*2-1)*CanaryJitter // [0.8, 1.2]
	d := time.Duration(float64(base) * f)
	if d < CanaryIntervalMin {
		d = CanaryIntervalMin
	}
	if d > CanaryIntervalMax {
		d = CanaryIntervalMax
	}
	return d
}
