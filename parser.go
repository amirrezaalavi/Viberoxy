package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
)

type ProxyConfig struct {
	Protocol string
	Server   string
	Port     int
	Name     string
	Raw      string
}

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
		if c := ParseSingle(line); c != nil {
			configs = append(configs, c)
		}
	}
	return configs
}

func decodeLines(body string) []string {
	if decoded, err := base64Decode(body); err == nil {
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

func ParseSingle(raw string) *ProxyConfig {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	before, fragment := extractFragment(raw)

	proto := extractProtocol(before)
	if proto == "" {
		return nil
	}

	var cfg *ProxyConfig
	switch proto {
	case "ss":
		cfg = parseShadowsocks(before)
	case "vmess":
		cfg = parseVMess(before)
	case "vless":
		cfg = parseVLess(before)
	case "trojan":
		cfg = parseTrojan(before)
	case "hysteria2", "hy2":
		cfg = parseHysteria2(before)
	case "tuic":
		cfg = parseTUIC(before)
	case "wireguard":
		cfg = parseWireGuard(before)
	case "socks5", "socks4", "socks":
		cfg = parseSocks(before)
	default:
		return nil
	}
	if cfg == nil {
		return nil
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

	return cfg
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

func base64Decode(s string) (string, error) {
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

func parseShadowsocks(raw string) *ProxyConfig {
	const prefix = "ss://"
	rest := raw[len(prefix):]

	userinfoB64, hostPart, found := strings.Cut(rest, "@")
	if found {
		userinfo, err := base64Decode(userinfoB64)
		if err != nil {
			return nil
		}
		_, _, _ = strings.Cut(userinfo, ":")
		host, portStr := splitHostPort(hostPart)
		if host == "" || portStr == "" {
			return nil
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port < 1 || port > 65535 {
			return nil
		}
		return &ProxyConfig{Server: host, Port: port}
	}

	decoded, err := base64Decode(rest)
	if err != nil {
		return nil
	}
	_, hostPart, found = strings.Cut(decoded, "@")
	if !found {
		return nil
	}
	host, portStr := splitHostPort(hostPart)
	if host == "" || portStr == "" {
		return nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil
	}
	return &ProxyConfig{Server: host, Port: port}
}

func parseVMess(raw string) *ProxyConfig {
	const prefix = "vmess://"
	b64 := raw[len(prefix):]

	data, err := base64Decode(b64)
	if err != nil {
		return nil
	}

	var v map[string]interface{}
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		return nil
	}

	server, _ := v["add"].(string)
	if server == "" {
		server, _ = v["address"].(string)
	}
	if server == "" {
		return nil
	}

	port := 0
	if p, ok := v["port"].(float64); ok {
		port = int(p)
	} else if p, ok := v["port"].(string); ok {
		port, _ = strconv.Atoi(p)
	}
	if port < 1 || port > 65535 {
		return nil
	}

	name, _ := v["ps"].(string)

	return &ProxyConfig{Server: server, Port: port, Name: name}
}

func parseVLess(raw string) *ProxyConfig {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	host := u.Hostname()
	if host == "" {
		return nil
	}
	portStr := u.Port()
	if portStr == "" {
		return nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil
	}
	return &ProxyConfig{Server: host, Port: port}
}

func parseTrojan(raw string) *ProxyConfig {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	host := u.Hostname()
	if host == "" {
		return nil
	}
	portStr := u.Port()
	if portStr == "" {
		return nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil
	}
	return &ProxyConfig{Server: host, Port: port}
}

func parseHysteria2(raw string) *ProxyConfig {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	host := u.Hostname()
	if host == "" {
		return nil
	}
	port := defaultPort(u.Port(), 443)
	if port < 1 || port > 65535 {
		return nil
	}
	return &ProxyConfig{Server: host, Port: port}
}

func parseTUIC(raw string) *ProxyConfig {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	host := u.Hostname()
	if host == "" {
		return nil
	}
	port := defaultPort(u.Port(), 443)
	if port < 1 || port > 65535 {
		return nil
	}
	return &ProxyConfig{Server: host, Port: port}
}

func parseWireGuard(raw string) *ProxyConfig {
	const prefix = "wireguard://"
	rest := raw[len(prefix):]

	if strings.Contains(rest, "@") {
		u, err := url.Parse(raw)
		if err != nil {
			return nil
		}
		host := u.Hostname()
		if host == "" {
			return nil
		}
		port := defaultPort(u.Port(), 51820)
		if port < 1 || port > 65535 {
			return nil
		}
		return &ProxyConfig{Server: host, Port: port}
	}

	data, err := base64Decode(rest)
	if err != nil {
		return nil
	}

	var wg struct {
		Endpoint string `json:"endpoint"`
		Server   string `json:"server"`
	}
	if err := json.Unmarshal([]byte(data), &wg); err != nil {
		return nil
	}

	endpoint := wg.Endpoint
	if endpoint == "" {
		endpoint = wg.Server
	}
	if endpoint == "" {
		return nil
	}

	host, portStr := splitHostPort(endpoint)
	if host == "" {
		return nil
	}
	port := defaultPort(portStr, 51820)

	return &ProxyConfig{Server: host, Port: port}
}

func parseSocks(raw string) *ProxyConfig {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	host := u.Hostname()
	if host == "" {
		return nil
	}
	port := defaultPort(u.Port(), 1080)
	if port < 1 || port > 65535 {
		return nil
	}
	return &ProxyConfig{Server: host, Port: port}
}
