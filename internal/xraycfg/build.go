package xraycfg

import (
	"encoding/json"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"viberoxy/internal/proxycfg"
)

// xray JSON config generation: BuildXrayConfig renders one proxy config into
// an xray configuration document. Extracted verbatim from package main's
// xray.go (task-1-3); behavior is unchanged.

// BuildXrayConfig renders the xray JSON for one proxy config. When
// muxEnabled is omitted it defaults to true: client connections to this
// xray instance then share a single multiplexed upstream connection,
// amortizing the TLS/protocol handshake per connection. Mux is only emitted
// on proxied outbounds (never on the freedom fallback, and never on socks5
// outbounds whose plain-proxy remote cannot demultiplex smux).
func BuildXrayConfig(cfg *proxycfg.ProxyConfig, inboundPort int, muxEnabled ...bool) ([]byte, error) {
	mux := true
	if len(muxEnabled) > 0 {
		mux = muxEnabled[0]
	}

	inboundSettings, err := json.Marshal(map[string]interface{}{
		"udp": false,
	})
	if err != nil {
		return nil, err
	}

	conf := XrayConfig{
		Log: LogConfig{LogLevel: "none"},
		Inbounds: []InboundConfig{
			{
				Port:     inboundPort,
				Listen:   "127.0.0.1",
				Protocol: "socks",
				Settings: inboundSettings,
			},
		},
		Outbounds: buildOutbound(cfg, mux),
	}

	return json.MarshalIndent(conf, "", "  ")
}

func buildOutbound(cfg *proxycfg.ProxyConfig, muxEnabled bool) []OutboundConfig {
	mux := (*MuxConfig)(nil)
	if muxEnabled {
		mux = &MuxConfig{Enabled: true, Concurrency: 8}
	}

	var settings map[string]interface{}
	var streamSettings *StreamSettings

	switch cfg.Protocol {
	case "ss":
		method, password := extractSIP002(cfg.Raw)
		if method == "" {
			method = "none"
		}
		settings = map[string]interface{}{
			"servers": []interface{}{
				map[string]interface{}{
					"address":  cfg.Server,
					"port":     cfg.Port,
					"method":   method,
					"password": password,
				},
			},
		}
		return []OutboundConfig{
			{
				Protocol: "shadowsocks",
				Settings: marshalRaw(settings),
				Mux:      mux,
			},
		}

	case "vmess":
		v := extractVMessRaw(cfg.Raw)
		id, _ := v["id"].(string)
		if id == "" {
			id = "00000000-0000-0000-0000-000000000000"
		}
		aid := getInt(v, "aid")

		settings = map[string]interface{}{
			"vnext": []interface{}{
				map[string]interface{}{
					"address": cfg.Server,
					"port":    cfg.Port,
					"users": []interface{}{
						map[string]interface{}{
							"id":       id,
							"alterId":  aid,
							"security": "auto",
						},
					},
				},
			},
		}

		net, _ := v["net"].(string)
		headerType, _ := v["type"].(string)
		path, _ := v["path"].(string)
		host, _ := v["host"].(string)
		tlsVal, _ := v["tls"].(string)
		sni, _ := v["sni"].(string)
		fp, _ := v["fp"].(string)
		alpn, _ := v["alpn"].(string)
		if sec, ok := v["security"].(string); ok && sec != "" {
			tlsVal = sec
		}
		pbk, _ := v["pbk"].(string)
		sid, _ := v["sid"].(string)
		spx, _ := v["spx"].(string)

		streamSettings = buildStreamSettings(net, headerType, path, host, tlsVal, sni, fp, alpn, pbk, sid, spx)

	case "vless":
		uuid, encryption, flow, params := extractVLessParams(cfg.Raw)
		if encryption == "" {
			encryption = "none"
		}
		user := map[string]interface{}{
			"id":         uuid,
			"encryption": encryption,
		}
		if flow != "" {
			user["flow"] = flow
		}
		settings = map[string]interface{}{
			"vnext": []interface{}{
				map[string]interface{}{
					"address": cfg.Server,
					"port":    cfg.Port,
					"users":   []interface{}{user},
				},
			},
		}

		streamSettings = buildStreamSettings(
			params["type"],
			"",
			params["path"],
			params["host"],
			params["security"],
			params["sni"],
			params["fp"],
			params["alpn"],
			params["pbk"],
			params["sid"],
			params["spx"],
		)

	case "trojan":
		password, flow, params := extractTrojanParams(cfg.Raw)
		server := map[string]interface{}{
			"address":  cfg.Server,
			"port":     cfg.Port,
			"password": password,
		}
		if flow != "" {
			server["flow"] = flow
		}
		settings = map[string]interface{}{
			"servers": []interface{}{server},
		}

		streamSettings = buildStreamSettings(
			params["type"],
			"",
			params["path"],
			params["host"],
			params["security"],
			params["sni"],
			params["fp"],
			params["alpn"],
			params["pbk"],
			params["sid"],
			params["spx"],
		)

	case "socks5":
		username, password := extractSocksParams(cfg.Raw)
		server := map[string]interface{}{
			"address": cfg.Server,
			"port":    cfg.Port,
		}
		if username != "" {
			server["users"] = []interface{}{
				map[string]interface{}{
					"user": username,
					"pass": password,
				},
			}
		}
		settings = map[string]interface{}{
			"servers": []interface{}{server},
		}

	case "hysteria2", "tuic", "wireguard":
		slog.Warn("protocol not supported as xray outbound, using freedom fallback",
			"protocol", cfg.Protocol)
		settings = map[string]interface{}{
			"domainStrategy": "UseIP",
		}
		return []OutboundConfig{
			{
				Protocol: "freedom",
				Settings: marshalRaw(settings),
			},
		}

	default:
		settings = map[string]interface{}{
			"domainStrategy": "UseIP",
		}
		return []OutboundConfig{
			{
				Protocol: "freedom",
				Settings: marshalRaw(settings),
			},
		}
	}

	obMux := mux
	// A socks5 outbound tunnels raw bytes through a plain SOCKS proxy to the
	// real target: nothing at the far end decodes smux frames, so mux would
	// corrupt every stream (mirrors the freedom-fallback no-mux rule).
	if cfg.Protocol == "socks5" {
		obMux = nil
	}
	ob := OutboundConfig{
		Protocol:       getXrayProtocol(cfg.Protocol),
		Settings:       marshalRaw(settings),
		StreamSettings: streamSettings,
		Mux:            obMux,
	}
	return []OutboundConfig{ob}
}

func getXrayProtocol(proto string) string {
	switch proto {
	case "ss":
		return "shadowsocks"
	case "vmess":
		return "vmess"
	case "vless":
		return "vless"
	case "trojan":
		return "trojan"
	case "socks5":
		return "socks"
	default:
		return "freedom"
	}
}

func buildStreamSettings(network, headerType, path, host, security, sni, fp, alpn, pbk, sid, spx string) *StreamSettings {
	ss := &StreamSettings{}

	switch network {
	case "ws", "websocket":
		ss.Network = "ws"
		ws := &WSSettings{}
		if path != "" {
			ws.Path = path
		}
		if host != "" {
			ws.Headers = map[string]string{"Host": host}
		}
		ss.WSSettings = ws
	case "grpc", "gun":
		ss.Network = "grpc"
		ss.GRPCSettings = &GRPCSettings{ServiceName: path}
	case "xhttp":
		ss.Network = "xhttp"
	case "tcp", "http", "":
		ss.Network = "tcp"
		if headerType == "http" && host != "" {
			ss.TCPSettings = &TCPSettings{
				Header: &TCPHeader{
					Type: "http",
					Request: &HTTPRequest{
						Version: "1.1",
						Method:  "GET",
						Path:    []string{"/"},
						Headers: map[string][]string{
							"Host": {host},
						},
					},
				},
			}
		}
	default:
		ss.Network = "tcp"
	}

	switch security {
	case "tls":
		ss.Security = "tls"
		tls := &TLSSettings{}
		if sni != "" {
			tls.ServerName = sni
		}
		if fp != "" {
			tls.Fingerprint = fp
		}
		if alpn != "" {
			tls.ALPN = strings.Split(alpn, ",")
		}
		ss.TLSSettings = tls
	case "reality":
		ss.Security = "reality"
		ss.RealitySettings = &RealitySettings{
			ServerName:  sni,
			Fingerprint: fp,
			PublicKey:   pbk,
			ShortID:     sid,
			SpiderX:     spx,
		}
	}

	return ss
}

func extractSIP002(raw string) (method, password string) {
	const prefix = "ss://"
	rest := raw[len(prefix):]

	if idx := strings.IndexByte(rest, '#'); idx >= 0 {
		rest = rest[:idx]
	}
	if idx := strings.IndexByte(rest, '?'); idx >= 0 {
		rest = rest[:idx]
	}

	userinfoB64, _, found := strings.Cut(rest, "@")
	if found {
		userinfo, err := proxycfg.Base64Decode(userinfoB64)
		if err != nil {
			return "", ""
		}
		method, password, _ = strings.Cut(userinfo, ":")
		return method, password
	}

	decoded, err := proxycfg.Base64Decode(rest)
	if err != nil {
		return "", ""
	}
	userinfo, _, _ := strings.Cut(decoded, "@")
	method, password, _ = strings.Cut(userinfo, ":")
	return method, password
}

func extractVMessRaw(raw string) map[string]interface{} {
	const prefix = "vmess://"
	rest := raw[len(prefix):]

	if idx := strings.IndexByte(rest, '#'); idx >= 0 {
		rest = rest[:idx]
	}
	if idx := strings.IndexByte(rest, '?'); idx >= 0 {
		rest = rest[:idx]
	}

	data, err := proxycfg.Base64Decode(rest)
	if err != nil {
		return nil
	}

	var v map[string]interface{}
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		return nil
	}
	return v
}

func extractVLessParams(raw string) (uuid, encryption, flow string, params map[string]string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", nil
	}
	if u.User != nil {
		uuid = u.User.Username()
	}
	q := u.Query()
	encryption = q.Get("encryption")
	flow = q.Get("flow")
	params = map[string]string{
		"security": q.Get("security"),
		"type":     q.Get("type"),
		"path":     q.Get("path"),
		"host":     q.Get("host"),
		"sni":      q.Get("sni"),
		"fp":       q.Get("fp"),
		"alpn":     q.Get("alpn"),
		"pbk":      q.Get("pbk"),
		"sid":      q.Get("sid"),
		"spx":      q.Get("spx"),
	}
	return
}

func extractTrojanParams(raw string) (password, flow string, params map[string]string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", nil
	}
	if u.User != nil {
		password, _ = u.User.Password()
		if password == "" {
			password = u.User.Username()
		}
	}
	q := u.Query()
	flow = q.Get("flow")
	params = map[string]string{
		"security": q.Get("security"),
		"type":     q.Get("type"),
		"path":     q.Get("path"),
		"host":     q.Get("host"),
		"sni":      q.Get("sni"),
		"fp":       q.Get("fp"),
		"alpn":     q.Get("alpn"),
		"pbk":      q.Get("pbk"),
		"sid":      q.Get("sid"),
		"spx":      q.Get("spx"),
	}
	return
}

func extractSocksParams(raw string) (username, password string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", ""
	}
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	return
}

func marshalRaw(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func getInt(v map[string]interface{}, key string) int {
	switch val := v[key].(type) {
	case float64:
		return int(val)
	case string:
		n, _ := strconv.Atoi(val)
		return n
	}
	return 0
}
