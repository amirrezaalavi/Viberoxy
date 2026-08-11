package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Route is the egress decision for one connection target.
type Route int

const (
	// RouteWAN sends the connection through the xray WAN pool (VPN).
	RouteWAN Route = iota
	// RouteDirect sends the connection straight to the target from the
	// local machine, bypassing the WAN pool.
	RouteDirect
)

func (r Route) String() string {
	switch r {
	case RouteWAN:
		return "wan"
	case RouteDirect:
		return "direct"
	default:
		return "unknown"
	}
}

// RouteMode selects the routing policy.
type RouteMode string

const (
	// RouteAllProxy: every connection goes through the WAN pool. This is
	// the historical viberoxy behavior and the default.
	RouteAllProxy RouteMode = "all-proxy"
	// RouteProxyDefault: everything goes through the WAN pool except hosts
	// in the direct list (e.g. country-local domains) which go straight.
	RouteProxyDefault RouteMode = "proxy-default"
	// RouteDirectDefault: everything goes straight except hosts in the
	// proxy list (e.g. geo-blocked domains) which go through the WAN pool.
	RouteDirectDefault RouteMode = "direct-default"
)

func ParseRouteMode(s string) (RouteMode, error) {
	switch RouteMode(s) {
	case RouteAllProxy, RouteProxyDefault, RouteDirectDefault:
		return RouteMode(s), nil
	default:
		return "", fmt.Errorf("invalid route mode %q (want all-proxy | proxy-default | direct-default)", s)
	}
}

// Router decides, per target host, whether a connection should use the WAN
// pool or go direct. Rules are plain suffix lists; "direct" and "proxy" are
// mutually exclusive decisions resolved by the mode:
//
//	all-proxy:      always RouteWAN
//	proxy-default:  RouteDirect only if host matches a direct suffix
//	direct-default: RouteWAN only if host matches a proxy suffix
//
// On a conflict (host in both lists) the explicit list wins: in
// proxy-default the direct list wins; in direct-default the proxy list wins.
type Router struct {
	Mode           RouteMode
	directSuffixes []string
	proxySuffixes  []string
}

// NewRouter builds a Router from parsed mode + suffix lists.
func NewRouter(mode RouteMode, directSuffixes, proxySuffixes []string) *Router {
	return &Router{
		Mode:           mode,
		directSuffixes: directSuffixes,
		proxySuffixes:  proxySuffixes,
	}
}

// Decide returns the egress route for a target host (hostname, IP literal,
// or host:port — the port is stripped). Matching is case-insensitive and a
// trailing dot is normalized.
func (r *Router) Decide(target string) Route {
	if r == nil {
		return RouteWAN
	}

	switch r.Mode {
	case RouteProxyDefault:
		if matchesAny(target, r.directSuffixes) {
			return RouteDirect
		}
		return RouteWAN
	case RouteDirectDefault:
		if matchesAny(target, r.proxySuffixes) {
			return RouteWAN
		}
		return RouteDirect
	default: // RouteAllProxy and anything unknown: proxy everything
		return RouteWAN
	}
}

// normalizeHost strips a :port suffix and a trailing dot, and lowercases.
func normalizeHost(target string) string {
	host := target
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		// Only strip when the suffix after ':' is numeric (a port), so
		// bare IPv6 literals like [::1] survive without mangling.
		if j := i + 1; j < len(host) && host[j] >= '0' && host[j] <= '9' {
			host = host[:i]
		}
	}
	host = strings.TrimSuffix(host, ".")
	return strings.ToLower(host)
}

// matchesAny reports whether host matches any suffix. A suffix that starts
// with a dot (".example.com") matches the bare domain and all subdomains. A
// suffix without a dot ("plain.local") matches only that exact host.
func matchesAny(target string, suffixes []string) bool {
	host := normalizeHost(target)
	if host == "" {
		return false
	}
	for _, s := range suffixes {
		s = strings.ToLower(s)
		if strings.HasPrefix(s, ".") {
			// ".example.com" matches "example.com" and "a.example.com"
			if host == s[1:] || strings.HasSuffix(host, s) {
				return true
			}
		} else {
			// no dot: exact host match only
			if host == s {
				return true
			}
		}
	}
	return false
}

// parseSuffixList splits a comma-separated env list into normalized
// suffix entries. Entries without a leading dot get one (".example.com")
// except bare single-label names which stay exact ("localhost"). Blank
// entries are dropped.
func parseSuffixList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		part = strings.ToLower(part)
		if !strings.HasPrefix(part, ".") && strings.Contains(part, ".") {
			part = "." + part
		}
		out = append(out, part)
	}
	return out
}

// loadSuffixFile reads a newline-separated domain list file. Lines starting
// with '#' and blank lines are ignored. Entries get the same normalization
// as parseSuffixList.
func loadSuffixFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return parseSuffixList(strings.Join(lines, ",")), nil
}
