package xraycfg

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"viberoxy/internal/proxycfg"
)

// xray JSON config generation: BuildXrayConfig renders one proxy config into
// an xray configuration document.

// BuildXrayConfig renders the xray JSON for one proxy config. When
// muxEnabled is omitted it defaults to true: client connections to this
// xray instance then share a single multiplexed upstream connection,
// amortizing the TLS/protocol handshake per connection. Mux is only emitted
// on proxied outbounds (never on the freedom fallback, never on socks5
// outbounds whose plain-proxy remote cannot demultiplex smux, and never when
// the sharelink carries a flow such as xtls-rprx-vision).
//
// The builders below return an error instead of rendering an outbound that
// can never work (undecodable vmess/ss payloads, xhttp missing path/host/
// mode); a config that reaches this point but cannot be rendered is a hard
// failure, not a silently broken tunnel.
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

	outbounds, err := buildOutbound(cfg, mux)
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
		Outbounds: outbounds,
	}

	return json.MarshalIndent(conf, "", "  ")
}

func buildOutbound(cfg *proxycfg.ProxyConfig, muxEnabled bool) ([]OutboundConfig, error) {
	mux := (*MuxConfig)(nil)
	if muxEnabled {
		mux = &MuxConfig{Enabled: true, Concurrency: 8}
	}

	var settings map[string]interface{}
	var streamSettings *StreamSettings
	// flow is the sharelink's flow parameter (e.g. xtls-rprx-vision).
	// A non-empty flow and mux are mutually exclusive (F-11): vision-style
	// flows operate per real connection while smux folds many streams onto
	// one upstream connection, so xray cannot honour both at once.
	var flow string
	var err error

	switch cfg.Protocol {
	case "ss":
		method, password, err := extractSIP002(cfg.Raw)
		if err != nil {
			return nil, err
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
		}, nil

	case "vmess":
		v, err := extractVMessRaw(cfg.Raw)
		if err != nil {
			return nil, err
		}
		id, _ := v["id"].(string)
		if id == "" {
			return nil, errors.New("vmess: missing user id")
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
		mode, _ := v["mode"].(string)
		if sec, ok := v["security"].(string); ok && sec != "" {
			tlsVal = sec
		}
		pbk, _ := v["pbk"].(string)
		sid, _ := v["sid"].(string)
		spx, _ := v["spx"].(string)

		streamSettings, err = buildStreamSettings(net, headerType, path, host, tlsVal, sni, fp, alpn, pbk, sid, spx, mode)
		if err != nil {
			return nil, err
		}

	case "vless":
		var uuid, encryption string
		var params map[string]string
		uuid, encryption, flow, params = extractVLessParams(cfg.Raw)
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

		streamSettings, err = buildStreamSettings(
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
			params["mode"],
		)
		if err != nil {
			return nil, err
		}

	case "trojan":
		var password string
		var params map[string]string
		password, flow, params = extractTrojanParams(cfg.Raw)
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

		streamSettings, err = buildStreamSettings(
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
			params["mode"],
		)
		if err != nil {
			return nil, err
		}

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
		}, nil

	default:
		settings = map[string]interface{}{
			"domainStrategy": "UseIP",
		}
		return []OutboundConfig{
			{
				Protocol: "freedom",
				Settings: marshalRaw(settings),
			},
		}, nil
	}

	obMux := mux
	// A socks5 outbound tunnels raw bytes through a plain SOCKS proxy to the
	// real target: nothing at the far end decodes smux frames, so mux would
	// corrupt every stream (mirrors the freedom-fallback no-mux rule).
	if cfg.Protocol == "socks5" {
		obMux = nil
	}
	// Same rule for flows: mux must never ride together with a non-empty
	// flow (xtls-rprx-vision and friends) — see the flow comment above.
	if flow != "" {
		obMux = nil
	}
	ob := OutboundConfig{
		Protocol:       getXrayProtocol(cfg.Protocol),
		Settings:       marshalRaw(settings),
		StreamSettings: streamSettings,
		Mux:            obMux,
	}
	return []OutboundConfig{ob}, nil
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

// buildStreamSettings renders the streamSettings block. It returns an error
// for transports that cannot be rendered into a working outbound: xhttp
// needs path, host and mode (mirrors proxycfg's parse-time checkXHTTP, so a
// directly-built config is rejected exactly like a parsed one).
func buildStreamSettings(network, headerType, path, host, security, sni, fp, alpn, pbk, sid, spx, mode string) (*StreamSettings, error) {
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
		if path == "" {
			return nil, errors.New("xhttp missing path")
		}
		if host == "" {
			return nil, errors.New("xhttp missing host")
		}
		if mode == "" {
			return nil, errors.New("xhttp missing mode")
		}
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

	return ss, nil
}

// extractSIP002 pulls method and password out of an ss:// sharelink. It
// returns an error when the userinfo cannot be decoded into a usable
// method:password pair — rendering that as method=none would produce an
// outbound that can never authenticate.
func extractSIP002(raw string) (method, password string, err error) {
	const prefix = "ss://"
	if !strings.HasPrefix(raw, prefix) {
		return "", "", errors.New("ss: missing ss:// prefix")
	}
	rest := raw[len(prefix):]

	if idx := strings.IndexByte(rest, '#'); idx >= 0 {
		rest = rest[:idx]
	}
	if idx := strings.IndexByte(rest, '?'); idx >= 0 {
		rest = rest[:idx]
	}

	userinfo := rest
	if userinfoB64, _, found := strings.Cut(rest, "@"); found {
		decoded, decErr := proxycfg.Base64Decode(userinfoB64)
		if decErr != nil {
			return "", "", fmt.Errorf("ss: invalid base64 userinfo: %w", decErr)
		}
		userinfo = decoded
	} else {
		decoded, decErr := proxycfg.Base64Decode(rest)
		if decErr != nil {
			return "", "", fmt.Errorf("ss: invalid base64 payload: %w", decErr)
		}
		userinfo, _, _ = strings.Cut(decoded, "@")
	}

	method, password, ok := strings.Cut(userinfo, ":")
	if !ok || method == "" {
		return "", "", errors.New("ss: userinfo missing method:password")
	}
	return method, password, nil
}

// extractVMessRaw decodes the base64+JSON payload of a vmess:// sharelink.
// Undecodable or empty payloads are errors: falling through would render an
// outbound with a zero UUID that can never authenticate.
func extractVMessRaw(raw string) (map[string]interface{}, error) {
	const prefix = "vmess://"
	if !strings.HasPrefix(raw, prefix) {
		return nil, errors.New("vmess: missing vmess:// prefix")
	}
	rest := raw[len(prefix):]

	if idx := strings.IndexByte(rest, '#'); idx >= 0 {
		rest = rest[:idx]
	}
	if idx := strings.IndexByte(rest, '?'); idx >= 0 {
		rest = rest[:idx]
	}

	data, err := proxycfg.Base64Decode(rest)
	if err != nil {
		return nil, fmt.Errorf("vmess: invalid base64 payload: %w", err)
	}

	var v map[string]interface{}
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		return nil, fmt.Errorf("vmess: invalid json payload: %w", err)
	}
	if len(v) == 0 {
		return nil, errors.New("vmess: empty json payload")
	}
	return v, nil
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
		"mode":     q.Get("mode"),
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
		"mode":     q.Get("mode"),
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
