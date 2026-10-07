// Package auth implements the F-14 security primitives: proxy/API credential
// checking (constant-time), the bind-address exposure policy, and the
// private-target (SSRF) blocklist.
//
// Design: pure functions plus thin env loaders. internal/proxycfg validates
// the same env vars at startup (hard-exit on bad values); the enforcement
// points re-read the env at request time — the same dual-read pattern
// subs.AllowHTTPFromEnv uses for ALLOW_HTTP_SUBSCRIPTION. Environment values
// cannot change under a running process, so re-reading is safe and keeps the
// front-ends free of config plumbing.
package auth

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// LoopbackHost is the default bind address: proxy, SOCKS5, API and metrics
// listeners bind loopback only unless LISTEN_ADDR says otherwise.
const LoopbackHost = "127.0.0.1"

// UserCred is one proxy user:password pair from PROXY_USERS.
type UserCred struct {
	User string
	Pass string
}

// ParseUsers parses a comma-separated list of user:password entries
// (PROXY_USERS). The password may itself contain colons: only the first
// colon of an entry separates user from password. Empty input yields no
// users. Error messages never echo the secret material back: they are
// logged at startup.
func ParseUsers(s string) ([]UserCred, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	entries := strings.Split(s, ",")
	users := make([]UserCred, 0, len(entries))
	for i, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, fmt.Errorf("entry %d is empty", i+1)
		}
		sep := strings.IndexByte(entry, ':')
		if sep < 0 {
			// Do not echo the entry: it may be a password pasted
			// without its username.
			return nil, fmt.Errorf("entry %d is not a user:password pair", i+1)
		}
		user, pass := entry[:sep], entry[sep+1:]
		if user == "" {
			return nil, fmt.Errorf("entry %d has an empty username", i+1)
		}
		if pass == "" {
			return nil, fmt.Errorf("entry %d has an empty password", i+1)
		}
		users = append(users, UserCred{User: user, Pass: pass})
	}
	return users, nil
}

// CheckUsers reports whether user/pass matches any configured credential.
// Every entry is compared with crypto/subtle.ConstantTimeCompare and the
// result is accumulated without early exit, so neither the position of the
// matching entry nor a partial prefix leaks through timing.
func CheckUsers(users []UserCred, user, pass string) bool {
	match := 0
	for _, u := range users {
		userOK := subtle.ConstantTimeCompare([]byte(u.User), []byte(user))
		passOK := subtle.ConstantTimeCompare([]byte(u.Pass), []byte(pass))
		match |= userOK & passOK
	}
	return match == 1
}

// ParseBasic extracts user and password from a "Basic ..." header value
// (Proxy-Authorization). The scheme is matched case-insensitively; the
// credential is base64(userid ":" password) split at the first colon.
func ParseBasic(header string) (user, pass string, ok bool) {
	const prefix = "basic "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(header[len(prefix):]))
	if err != nil {
		return "", "", false
	}
	decoded := string(raw)
	sep := strings.IndexByte(decoded, ':')
	if sep < 0 {
		return "", "", false
	}
	return decoded[:sep], decoded[sep+1:], true
}

// CheckBearer reports whether an "Authorization" header value carries the
// expected bearer token. Comparison is constant-time; an unconfigured
// (empty) token never validates.
func CheckBearer(header, token string) bool {
	if token == "" {
		return false
	}
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	got := strings.TrimSpace(header[len(prefix):])
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

// UsersFromEnv loads PROXY_USERS. required reports whether the variable is
// set at all — a malformed value is still required (fail closed: callers get
// an error, zero usable users, and must reject everything) so a typo cannot
// silently disable authentication.
func UsersFromEnv() (users []UserCred, required bool, err error) {
	v := os.Getenv("PROXY_USERS")
	if v == "" {
		return nil, false, nil
	}
	users, perr := ParseUsers(v)
	return users, true, perr
}

// TokenFromEnv loads API_TOKEN (validated at startup by proxycfg.ParseConfig).
func TokenFromEnv() string {
	return os.Getenv("API_TOKEN")
}

// AllowPrivateTargetsFromEnv reports whether ALLOW_PRIVATE_TARGETS opts in
// to proxying connections to loopback/link-local/private destinations.
// Unset or unparsable means blocked (fail closed; ParseConfig hard-exits on
// unparsable values at startup).
func AllowPrivateTargetsFromEnv() bool {
	v := os.Getenv("ALLOW_PRIVATE_TARGETS")
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	return err == nil && b
}

// BindAllowed reports whether a non-loopback bind address is permitted:
// authentication is configured (PROXY_USERS or API_TOKEN) or the operator
// explicitly set ALLOW_PUBLIC=true.
func BindAllowed(usersConfigured, tokenConfigured, allowPublic bool) bool {
	return usersConfigured || tokenConfigured || allowPublic
}

// BindAllowedFromEnv is BindAllowed over the live environment.
func BindAllowedFromEnv() bool {
	_, usersConfigured, _ := UsersFromEnv()
	tokenConfigured := os.Getenv("API_TOKEN") != ""
	allowPublic, _ := strconv.ParseBool(os.Getenv("ALLOW_PUBLIC"))
	return BindAllowed(usersConfigured, tokenConfigured, allowPublic)
}

// IsLoopbackHost reports whether host (an IP literal or a localhost name)
// is a loopback bind address. Empty or non-loopback values — including the
// empty host that means "all interfaces" — are not loopback.
func IsLoopbackHost(host string) bool {
	if ip := parseIPLiteral(host); ip != nil {
		return ip.IsLoopback()
	}
	return isLocalHostName(host)
}

// EnsureBindAllowed is the defense-in-depth gate used by the listeners:
// loopback binds always; anything else only with auth configured or
// ALLOW_PUBLIC=true. proxycfg.ParseConfig enforces the same policy at
// startup with a hard exit; this second check protects direct callers that
// bypass ParseConfig.
func EnsureBindAllowed(host string) error {
	if IsLoopbackHost(host) {
		return nil
	}
	if BindAllowedFromEnv() {
		return nil
	}
	return fmt.Errorf("refusing to bind non-loopback address %q: set PROXY_USERS or API_TOKEN for authentication, or ALLOW_PUBLIC=true to accept public exposure", host)
}

// TargetAllowed reports whether the front-ends may open a connection to
// hostport. Private destinations (loopback, link-local incl. cloud
// metadata, RFC1918/ULA, CGNAT, unspecified) are blocked unless
// allowPrivate is set. Hostnames are checked for well-known local names and
// otherwise passed through without DNS resolution: targets are dialed
// remotely through the WAN pool, so resolving locally would add latency and
// a fresh failure mode without stopping a determined DNS rebind. IPv4
// literals are normalised through the inet_aton-style alternate forms
// (127.1, 2130706433, 0177.0.0.1, 0x7f000001) so shorthand cannot bypass
// the blocklist.
func TargetAllowed(hostport string, allowPrivate bool) bool {
	if allowPrivate {
		return true
	}
	host := targetHostOf(hostport)
	if host == "" {
		return false
	}
	if isLocalHostName(host) {
		return false
	}
	if ip := parseIPLiteral(host); ip != nil {
		return !isBlockedIP(ip)
	}
	return true
}

// targetHostOf extracts the host from an authority ("host:port",
// "[v6]:port") or returns the input when it carries no port.
func targetHostOf(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return strings.Trim(strings.TrimSpace(hostport), "[]")
}

// isLocalHostName matches the well-known names that resolve to loopback.
func isLocalHostName(host string) bool {
	h := strings.ToLower(host)
	switch {
	case h == "localhost", h == "localhost.localdomain":
		return true
	case h == "ip6-localhost", h == "ip6-loopback":
		return true
	case strings.HasSuffix(h, ".localhost"):
		return true
	}
	return false
}

// isBlockedIP reports whether ip sits in a range a proxy must never dial at
// the operator's request: loopback, link-local (cloud metadata lives at
// 169.254.169.254), RFC1918/ULA private, CGNAT shared space, unspecified.
func isBlockedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil {
		// 100.64.0.0/10 (RFC 6598 shared address space).
		if ip4[0] == 100 && ip4[1]&0xc0 == 64 {
			return true
		}
	}
	return false
}

// parseIPLiteral normalises a host to an IP when it is one, covering zone
// ids (fe80::1%lo0), the FQDN root dot (127.0.0.1.) and the inet_aton
// alternate IPv4 forms resolvers still accept at dial time.
func parseIPLiteral(host string) net.IP {
	h := host
	if i := strings.IndexByte(h, '%'); i >= 0 {
		h = h[:i] // IPv6 zone id
	}
	h = strings.TrimSuffix(h, ".")
	if h == "" {
		return nil
	}
	if ip := net.ParseIP(h); ip != nil {
		return ip
	}
	if ip, ok := parseIPv4Shorthand(h); ok {
		return ip
	}
	return nil
}

// parseIPv4Shorthand parses inet_aton-style IPv4 literals: 1-4 dot parts,
// each plain decimal except a 0x hex prefix or a leading-zero octal prefix,
// with the last part filling the remaining bytes ("127.1" = 127.0.0.1,
// "2130706433" = 127.0.0.1, "0177.0.0.1" = 127.0.0.1). Returns ok=false for
// anything else — the caller then treats the host as a hostname.
func parseIPv4Shorthand(s string) (net.IP, bool) {
	if s == "" {
		return nil, false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && r != '.' && r != 'x' && r != 'X' &&
			(r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return nil, false
		}
	}
	parts := strings.Split(s, ".")
	if len(parts) > 4 {
		return nil, false
	}
	vals := make([]uint64, len(parts))
	for i, p := range parts {
		if p == "" {
			return nil, false
		}
		if strings.HasPrefix(p, "0x") || strings.HasPrefix(p, "0X") {
			if len(p) == 2 {
				return nil, false
			}
			v, err := strconv.ParseUint(p[2:], 16, 64)
			if err != nil {
				return nil, false
			}
			vals[i] = v
			continue
		}
		base := 10
		if len(p) > 1 && p[0] == '0' {
			base = 8
		}
		v, err := strconv.ParseUint(p, base, 64)
		if err != nil {
			return nil, false
		}
		vals[i] = v
	}
	// All parts except the last must fit in one byte.
	for _, v := range vals[:len(vals)-1] {
		if v > 255 {
			return nil, false
		}
	}
	// The last part fills the remaining (5-len(parts)) bytes.
	lastBits := uint((5 - len(parts)) * 8)
	if vals[len(vals)-1] >= uint64(1)<<lastBits {
		return nil, false
	}
	b := make([]byte, 4)
	for i := 0; i < len(vals)-1; i++ {
		b[i] = byte(vals[i])
	}
	last := vals[len(vals)-1]
	for j := len(b) - 1; j >= len(vals)-1; j-- {
		b[j] = byte(last & 0xff)
		last >>= 8
	}
	return net.IP(b), true
}

// RequireBearer gates next with an Authorization: Bearer header when token
// is configured (a pass-through otherwise), answering 403 on a missing or
// incorrect token. Used for /api/* (api.go) and the metrics endpoint
// (main.go).
func RequireBearer(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !CheckBearer(r.Header.Get("Authorization"), token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="viberoxy"`)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
