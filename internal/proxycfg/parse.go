package proxycfg

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
)

// ParseConfigs parses a subscription body (base64-encoded or plain text).
// Links that would build a broken xray outbound are rejected at parse time
// and dropped; each rejection is logged exactly once with its per-config
// reason. Only valid configs are returned.
func ParseConfigs(body string) []*ProxyConfig {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}

	lines := decodeLines(body)
	var configs []*ProxyConfig
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cfg, err := ParseSingleErr(line)
		if err != nil {
			slog.Warn("skipping invalid config", "reason", err.Error(), "link", truncateForLog(line))
			continue
		}
		configs = append(configs, cfg)
	}
	return configs
}

// truncateForLog bounds the link echoed in a rejection log line.
func truncateForLog(s string) string {
	const maxRunes = 120
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "..."
}

func decodeLines(body string) []string {
	if decoded, err := Base64Decode(body); err == nil {
		decoded = strings.ReplaceAll(decoded, "\r\n", "\n")
		return strings.Split(decoded, "\n")
	}
	body = strings.ReplaceAll(body, "\r\n", "\n")
	return strings.Split(body, "\n")
}

// IsXraySupported reports whether a parsed config can be rendered as a real
// xray outbound. hysteria2, tuic and wireguard parse fine, but buildOutbound
// maps them to a "freedom" outbound — direct egress, i.e. a traffic leak — so
// they must never be promoted to a WAN slot. Configs are not removed from the
// subscription; they are simply skipped during promotion.
func IsXraySupported(cfg *ProxyConfig) bool {
	if cfg == nil {
		return false
	}
	switch cfg.Protocol {
	case "hysteria2", "tuic", "wireguard":
		return false
	}
	return true
}

// ParseSingle parses one sharelink URI and returns nil when the link is
// invalid (including links that parse but would build a broken xray
// outbound). Use ParseSingleErr to get the rejection reason.
func ParseSingle(raw string) *ProxyConfig {
	cfg, _ := ParseSingleErr(raw)
	return cfg
}

// ParseSingleErr parses one sharelink URI. On rejection it returns a nil
// config and an error whose message is the specific reason ("vmess: missing
// uuid", "vless: xhttp missing path", `unsupported scheme "foo"`, ...), so
// callers can log exactly why a config never entered the pool. Valid links
// return (cfg, nil).
func ParseSingleErr(raw string) (*ProxyConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, errors.New("empty link")
	}

	before, fragment := extractFragment(raw)

	proto := extractProtocol(before)
	if proto == "" {
		return nil, errors.New("missing scheme")
	}

	var cfg *ProxyConfig
	var err error
	switch proto {
	case "ss":
		cfg, err = parseShadowsocks(before)
	case "vmess":
		cfg, err = parseVMess(before)
	case "vless":
		cfg, err = parseVLess(before)
	case "trojan":
		cfg, err = parseTrojan(before)
	case "hysteria2", "hy2":
		cfg, err = parseHysteria2(before)
	case "tuic":
		cfg, err = parseTUIC(before)
	case "wireguard":
		cfg, err = parseWireGuard(before)
	case "socks5", "socks4", "socks":
		cfg, err = parseSocks(before)
	default:
		return nil, fmt.Errorf("unsupported scheme %q", proto)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", proto, err)
	}
	if cfg == nil {
		return nil, fmt.Errorf("%s: invalid link", proto)
	}

	cfg.Raw = raw
	if cfg.Name == "" {
		cfg.Name, _ = url.QueryUnescape(fragment)
	}

	switch proto {
	case "hy2":
		cfg.Protocol = "hysteria2"
	case "socks", "socks4":
		cfg.Protocol = "socks5"
	default:
		cfg.Protocol = proto
	}

	return cfg, nil
}

func extractFragment(raw string) (string, string) {
	before, after, _ := strings.Cut(raw, "#")
	return before, after
}

func extractProtocol(raw string) string {
	proto, _, found := strings.Cut(raw, "://")
	if !found {
		return ""
	}
	return strings.ToLower(proto)
}

func Base64Decode(s string) (string, error) {
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding} {
		if decoded, err := enc.DecodeString(s); err == nil {
			return string(decoded), nil
		}
	}
	s = addBase64Padding(s)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding} {
		if decoded, err := enc.DecodeString(s); err == nil {
			return string(decoded), nil
		}
	}
	return "", errors.New("base64 decode failed")
}

func addBase64Padding(s string) string {
	switch len(s) % 4 {
	case 2:
		return s + "=="
	case 3:
		return s + "="
	}
	return s
}

func splitHostPort(s string) (string, string) {
	if s == "" {
		return "", ""
	}
	if qIdx := strings.IndexByte(s, '?'); qIdx != -1 {
		s = s[:qIdx]
	}
	if s[0] == '[' {
		end := strings.IndexByte(s, ']')
		if end == -1 {
			return "", ""
		}
		host := s[1:end]
		if end+1 < len(s) && s[end+1] == ':' {
			return host, s[end+2:]
		}
		return host, ""
	}
	host, port, _ := strings.Cut(s, ":")
	return host, port
}

func defaultPort(portStr string, def int) int {
	if portStr == "" {
		return def
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p < 1 || p > 65535 {
		return def
	}
	return p
}

// requireMethodPassword rejects ss userinfo that does not carry a usable
// method:password pair — without both, the shadowsocks outbound cannot work.
func requireMethodPassword(userinfo string) error {
	method, password, ok := strings.Cut(userinfo, ":")
	if !ok || method == "" || password == "" {
		return errors.New("missing method:password")
	}
	return nil
}

// requirePort parses a strict 1..65535 port (no default: protocols without a
// default port must carry one).
func requirePort(portStr string) (int, error) {
	if portStr == "" {
		return 0, errors.New("missing port")
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p < 1 || p > 65535 {
		return 0, errors.New("invalid port")
	}
	return p, nil
}

func parseShadowsocks(raw string) (*ProxyConfig, error) {
	const prefix = "ss://"
	rest := raw[len(prefix):]

	userinfoB64, hostPart, found := strings.Cut(rest, "@")
	if found {
		userinfo, err := Base64Decode(userinfoB64)
		if err != nil {
			return nil, errors.New("invalid base64 userinfo")
		}
		if err := requireMethodPassword(userinfo); err != nil {
			return nil, err
		}
		host, portStr := splitHostPort(hostPart)
		if host == "" {
			return nil, errors.New("missing server")
		}
		port, err := requirePort(portStr)
		if err != nil {
			return nil, err
		}
		return &ProxyConfig{Server: host, Port: port}, nil
	}

	decoded, err := Base64Decode(rest)
	if err != nil {
		return nil, errors.New("invalid base64 payload")
	}
	userinfo, hostPart, found := strings.Cut(decoded, "@")
	if !found {
		return nil, errors.New("missing server")
	}
	if err := requireMethodPassword(userinfo); err != nil {
		return nil, err
	}
	host, portStr := splitHostPort(hostPart)
	if host == "" {
		return nil, errors.New("missing server")
	}
	port, err := requirePort(portStr)
	if err != nil {
		return nil, err
	}
	return &ProxyConfig{Server: host, Port: port}, nil
}

func parseVMess(raw string) (*ProxyConfig, error) {
	const prefix = "vmess://"
	b64 := raw[len(prefix):]

	data, err := Base64Decode(b64)
	if err != nil {
		return nil, errors.New("invalid base64")
	}

	var v map[string]interface{}
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		return nil, errors.New("invalid json")
	}

	// id (UUID) is mandatory: buildOutbound would otherwise fall back to a
	// zero UUID and the outbound can never authenticate.
	if id, _ := v["id"].(string); id == "" {
		return nil, errors.New("missing uuid")
	}

	server, _ := v["add"].(string)
	if server == "" {
		server, _ = v["address"].(string)
	}
	if server == "" {
		return nil, errors.New("missing server")
	}

	rawPort, exists := v["port"]
	if !exists {
		return nil, errors.New("missing port")
	}
	var port int
	switch p := rawPort.(type) {
	case float64:
		port = int(p)
	case string:
		if p == "" {
			return nil, errors.New("missing port")
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, errors.New("invalid port")
		}
		port = n
	default:
		return nil, errors.New("invalid port")
	}
	if port < 1 || port > 65535 {
		return nil, errors.New("invalid port")
	}

	name, _ := v["ps"].(string)

	return &ProxyConfig{Server: server, Port: port, Name: name}, nil
}

// checkXHTTP validates the xhttp transport parameters. An xhttp outbound
// without path, host or mode cannot be rendered into a working xray config,
// so the link is rejected at parse time.
func checkXHTTP(q url.Values) error {
	if q.Get("type") != "xhttp" {
		return nil
	}
	if q.Get("path") == "" {
		return errors.New("xhttp missing path")
	}
	if q.Get("host") == "" {
		return errors.New("xhttp missing host")
	}
	if q.Get("mode") == "" {
		return errors.New("xhttp missing mode")
	}
	return nil
}

func parseVLess(raw string) (*ProxyConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid url")
	}
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("missing server")
	}
	port, err := requirePort(u.Port())
	if err != nil {
		return nil, err
	}
	if err := checkXHTTP(u.Query()); err != nil {
		return nil, err
	}
	return &ProxyConfig{Server: host, Port: port}, nil
}

func parseTrojan(raw string) (*ProxyConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid url")
	}
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("missing server")
	}
	port, err := requirePort(u.Port())
	if err != nil {
		return nil, err
	}
	if err := checkXHTTP(u.Query()); err != nil {
		return nil, err
	}
	return &ProxyConfig{Server: host, Port: port}, nil
}

func parseHysteria2(raw string) (*ProxyConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid url")
	}
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("missing server")
	}
	port := defaultPort(u.Port(), 443)
	if port < 1 || port > 65535 {
		return nil, errors.New("invalid port")
	}
	return &ProxyConfig{Server: host, Port: port}, nil
}

func parseTUIC(raw string) (*ProxyConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid url")
	}
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("missing server")
	}
	port := defaultPort(u.Port(), 443)
	if port < 1 || port > 65535 {
		return nil, errors.New("invalid port")
	}
	return &ProxyConfig{Server: host, Port: port}, nil
}

func parseWireGuard(raw string) (*ProxyConfig, error) {
	const prefix = "wireguard://"
	rest := raw[len(prefix):]

	if strings.Contains(rest, "@") {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, errors.New("invalid url")
		}
		host := u.Hostname()
		if host == "" {
			return nil, errors.New("missing server")
		}
		port := defaultPort(u.Port(), 51820)
		if port < 1 || port > 65535 {
			return nil, errors.New("invalid port")
		}
		return &ProxyConfig{Server: host, Port: port}, nil
	}

	data, err := Base64Decode(rest)
	if err != nil {
		return nil, errors.New("invalid base64")
	}

	var wg struct {
		Endpoint string `json:"endpoint"`
		Server   string `json:"server"`
	}
	if err := json.Unmarshal([]byte(data), &wg); err != nil {
		return nil, errors.New("invalid json")
	}

	endpoint := wg.Endpoint
	if endpoint == "" {
		endpoint = wg.Server
	}
	if endpoint == "" {
		return nil, errors.New("missing server")
	}

	host, portStr := splitHostPort(endpoint)
	if host == "" {
		return nil, errors.New("missing server")
	}
	port := defaultPort(portStr, 51820)

	return &ProxyConfig{Server: host, Port: port}, nil
}

func parseSocks(raw string) (*ProxyConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid url")
	}
	host := u.Hostname()
	if host == "" {
		return nil, errors.New("missing server")
	}
	port := defaultPort(u.Port(), 1080)
	if port < 1 || port > 65535 {
		return nil, errors.New("invalid port")
	}
	return &ProxyConfig{Server: host, Port: port}, nil
}
