// Package affinity decides which eligible WAN path a (client, site)
// connection should stick to (review finding F-05: no session affinity).
//
// The mechanism is highest-random-weight ("rendezvous") hashing: for a
// key of (clientID, siteKey), every eligible path scores
// hash64(clientID || siteKey || path.ID) and the maximum wins. HRW gives
// the two properties the stickiness needs:
//
//   - determinism: the same key always maps to the same path while that
//     path stays eligible (no shared state, no coordination), and
//   - minimal movement: removing one path only remaps the keys that were
//     on it — everyone else keeps their path (T-AFF-02).
//
// Stickiness is a preference, not a promise: the scheduler may spill a
// sticky pick that is carrying bulk load (AFFINITY_BULK_SPILL), and a
// host may opt out entirely via NO_AFFINITY_DOMAINS.
package affinity

import (
	"encoding/binary"
	"hash/fnv"
	"net"
	"os"
	"strings"
	"viberoxy/internal/path"
)

// Pick returns the HRW winner for (clientID, siteKey) among cands — the
// path with the highest hash64(clientID || siteKey || path.ID) — or nil
// for an empty candidate set. The hash is FNV-1a 64 over clientID, a NUL
// separator, siteKey, another NUL and the little-endian path ID: the
// separators keep ("ab", "c") from colliding with ("a", "bc"). Identical
// inputs always produce the same winner (a score tie resolves to the
// first candidate, i.e. slot order — deterministic, and astronomically
// unlikely on 64 bits anyway).
func Pick(clientID, siteKey string, cands []*path.Path) *path.Path {
	if len(cands) == 0 {
		return nil
	}
	base := make([]byte, 0, len(clientID)+len(siteKey)+2)
	base = append(base, clientID...)
	base = append(base, 0)
	base = append(base, siteKey...)
	base = append(base, 0)

	h := fnv.New64a()
	idBuf := make([]byte, 8)
	var best *path.Path
	var bestScore uint64
	for _, p := range cands {
		binary.LittleEndian.PutUint64(idBuf, p.ID)
		h.Reset()
		_, _ = h.Write(base)
		_, _ = h.Write(idBuf)
		score := h.Sum64()
		if best == nil || score > bestScore {
			best, bestScore = p, score
		}
	}
	return best
}

// SiteKey reduces a target authority ("host", "host:port", "[v6]:port")
// to the affinity site key:
//
//   - IP literals key on the canonical IP itself (net.IP.String), so
//     1.2.3.4:443 and 1.2.3.4:80 share a key;
//   - hostnames key on their registrable domain (eTLD+1) heuristic:
//     the last two labels, unless those two form a known second-level
//     exception (co.uk, com.au, co.jp, ... — see secondLevelExceptions),
//     in which case the last three labels are used. www.example.com and
//     api.example.com therefore share one key; a.b.example.co.uk and
//     example.co.uk do NOT collapse onto the same key.
//
// SIMPLIFICATION (documented): this is an embedded heuristic, not the
// full Public Suffix List — a suffix absent from the exception list
// reduces one label too early (e.g. "x.github.io" would reduce to
// "github.io"). Extending secondLevelExceptions is the maintenance knob.
// inet_aton-style IPv4 shorthands ("127.1", "0x7f000001") are not
// recognised as IP literals here and key on their textual form; the
// front-ends receive canonical addresses from the protocol parsers.
func SiteKey(hostport string) string {
	host := strings.ToLower(hostname(hostport))
	if host == "" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	labels := strings.Split(host, ".")
	if len(labels) <= 2 {
		return host
	}
	suffix := labels[len(labels)-2] + "." + labels[len(labels)-1]
	if secondLevelExceptions[suffix] {
		return strings.Join(labels[len(labels)-3:], ".")
	}
	return strings.Join(labels[len(labels)-2:], ".")
}

// hostname strips the port (and IPv6 brackets / trailing root dot) from a
// target authority and lower-cases what is left.
func hostname(hostport string) string {
	h := strings.TrimSpace(hostport)
	if host, _, err := net.SplitHostPort(h); err == nil {
		return strings.TrimSuffix(host, ".")
	}
	return strings.TrimSuffix(strings.Trim(h, "[]"), ".")
}

// secondLevelExceptions lists the second-level registry suffixes the
// eTLD+1 heuristic treats as two-label boundaries (the common country
// namespaces where the registrable domain is <name>.<second>.<cc>).
// Entries are lower-case without a leading dot. This is the embedded,
// deliberately modest heuristic set — see SiteKey's simplification note.
var secondLevelExceptions = map[string]bool{
	// United Kingdom
	"co.uk": true, "org.uk": true, "ac.uk": true, "gov.uk": true,
	"net.uk": true, "ltd.uk": true, "plc.uk": true,
	// Australia
	"com.au": true, "net.au": true, "org.au": true, "edu.au": true,
	"gov.au": true, "id.au": true,
	// Japan
	"co.jp": true, "or.jp": true, "ne.jp": true, "ac.jp": true,
	"go.jp": true, "ad.jp": true,
	// Brazil
	"com.br": true, "net.br": true, "org.br": true, "gov.br": true,
	// South Korea
	"co.kr": true, "ne.kr": true, "or.kr": true, "re.kr": true,
	"go.kr": true, "pe.kr": true,
	// Turkey
	"com.tr": true, "net.tr": true, "org.tr": true, "gov.tr": true,
	"edu.tr": true,
	// China
	"com.cn": true, "net.cn": true, "org.cn": true, "gov.cn": true,
	"edu.cn": true, "ac.cn": true,
	// South Africa
	"co.za": true, "org.za": true, "net.za": true, "gov.za": true,
	"ac.za": true,
	// New Zealand
	"co.nz": true, "net.nz": true, "org.nz": true, "govt.nz": true,
	"ac.nz": true,
	// India
	"co.in": true, "net.in": true, "org.in": true, "gov.in": true,
	"ac.in": true, "firm.in": true, "gen.in": true, "ind.in": true,
	// Israel
	"co.il": true, "org.il": true, "net.il": true, "ac.il": true,
	"gov.il": true, "k12.il": true,
	// Argentina / Mexico
	"com.ar": true, "net.ar": true, "org.ar": true, "gob.ar": true,
	"edu.ar": true, "com.mx": true, "net.mx": true, "org.mx": true,
	"gob.mx": true, "edu.mx": true,
	// Singapore / Hong Kong / Taiwan
	"com.sg": true, "net.sg": true, "org.sg": true, "gov.sg": true,
	"edu.sg": true, "com.hk": true, "net.hk": true, "org.hk": true,
	"edu.hk": true, "gov.hk": true, "com.tw": true, "net.tw": true,
	"org.tw": true, "edu.tw": true, "gov.tw": true,
	// Malaysia / Thailand / UAE
	"com.my": true, "net.my": true, "org.my": true, "gov.my": true,
	"edu.my": true, "co.th": true, "ac.th": true, "go.th": true,
	"in.th": true, "or.th": true, "co.ae": true, "net.ae": true,
	"org.ae": true, "ac.ae": true, "gov.ae": true,
}

// NoAffinityFromEnv parses NO_AFFINITY_DOMAINS: a comma- and/or
// whitespace-separated list of hosts (or parent domains) that opt out of
// stickiness. It is read at request time — the same dual-read pattern
// subs.AllowHTTPFromEnv uses for ALLOW_HTTP_SUBSCRIPTION (environment
// values cannot change under a running process). Unset or empty yields no
// exclusions (everything sticks).
func NoAffinityFromEnv() []string {
	return ParseNoAffinity(os.Getenv("NO_AFFINITY_DOMAINS"))
}

// ParseNoAffinity is NoAffinityFromEnv's pure core (env kept out so tests
// can drive it directly). Entries are trimmed, lower-cased and stripped
// of a leading dot; blank entries are dropped.
func ParseNoAffinity(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		e := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(f)), ".")
		if e != "" {
			out = append(out, e)
		}
	}
	return out
}

// Sticky reports whether stickiness applies to hostport: false when the
// host is opted out through NO_AFFINITY_DOMAINS (exact match or as a
// parent-domain suffix — "example.com" covers "www.example.com"),
// otherwise true. An empty/blank host reports false (there is nothing to
// key on; the scheduler falls back to P2C anyway).
func Sticky(hostport string) bool {
	host := strings.ToLower(hostname(hostport))
	if host == "" {
		return false
	}
	for _, entry := range NoAffinityFromEnv() {
		if host == entry || strings.HasSuffix(host, "."+entry) {
			return false
		}
	}
	return true
}
