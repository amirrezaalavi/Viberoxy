package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type XrayConfig struct {
	Log       LogConfig        `json:"log"`
	Inbounds  []InboundConfig  `json:"inbounds"`
	Outbounds []OutboundConfig `json:"outbounds"`
}

type LogConfig struct {
	LogLevel string `json:"loglevel"`
}

type InboundConfig struct {
	Port     int             `json:"port"`
	Listen   string          `json:"listen"`
	Protocol string          `json:"protocol"`
	Settings json.RawMessage `json:"settings"`
}

type OutboundConfig struct {
	Protocol       string          `json:"protocol"`
	Settings       json.RawMessage `json:"settings"`
	StreamSettings *StreamSettings `json:"streamSettings,omitempty"`
	Mux            *MuxConfig      `json:"mux,omitempty"`
}

type MuxConfig struct {
	Enabled     bool `json:"enabled"`
	Concurrency int  `json:"concurrency,omitempty"`
}

type StreamSettings struct {
	Network         string           `json:"network"`
	Security        string           `json:"security"`
	TLSSettings     *TLSSettings     `json:"tlsSettings,omitempty"`
	WSSettings      *WSSettings      `json:"wsSettings,omitempty"`
	GRPCSettings    *GRPCSettings    `json:"grpcSettings,omitempty"`
	TCPSettings     *TCPSettings     `json:"tcpSettings,omitempty"`
	RealitySettings *RealitySettings `json:"realitySettings,omitempty"`
}

type TLSSettings struct {
	ServerName    string   `json:"serverName,omitempty"`
	Fingerprint   string   `json:"fingerprint,omitempty"`
	ALPN          []string `json:"alpn,omitempty"`
	AllowInsecure bool     `json:"allowInsecure,omitempty"`
}

type WSSettings struct {
	Path    string            `json:"path,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type GRPCSettings struct {
	ServiceName string `json:"serviceName,omitempty"`
}

type TCPSettings struct {
	Header *TCPHeader `json:"header,omitempty"`
}

type TCPHeader struct {
	Type    string        `json:"type"`
	Request *HTTPRequest  `json:"request,omitempty"`
}

type HTTPRequest struct {
	Version string              `json:"version,omitempty"`
	Method  string              `json:"method,omitempty"`
	Path    []string            `json:"path,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
}

type RealitySettings struct {
	ServerName  string `json:"serverName,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	PublicKey   string `json:"publicKey,omitempty"`
	ShortID     string `json:"shortId,omitempty"`
	SpiderX     string `json:"spiderX,omitempty"`
}

// BuildXrayConfig renders the xray JSON for one proxy config. When
// muxEnabled is omitted it defaults to true: client connections to this
// xray instance then share a single multiplexed upstream connection,
// amortizing the TLS/protocol handshake per connection. Mux is only emitted
// on proxied outbounds (never on the freedom fallback).
func BuildXrayConfig(cfg *ProxyConfig, inboundPort int, muxEnabled ...bool) ([]byte, error) {
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

func buildOutbound(cfg *ProxyConfig, muxEnabled bool) []OutboundConfig {
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

		streamSettings = buildStreamSettings(net, headerType, path, host, tlsVal, sni, fp, alpn)

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

	ob := OutboundConfig{
		Protocol:       getXrayProtocol(cfg.Protocol),
		Settings:       marshalRaw(settings),
		StreamSettings: streamSettings,
		Mux:            mux,
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

func buildStreamSettings(network, headerType, path, host, security, sni, fp, alpn string) *StreamSettings {
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
		userinfo, err := base64Decode(userinfoB64)
		if err != nil {
			return "", ""
		}
		method, password, _ = strings.Cut(userinfo, ":")
		return method, password
	}

	decoded, err := base64Decode(rest)
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

	data, err := base64Decode(rest)
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

func StartXray(cfg *ProxyConfig, inboundPort int, muxEnabled ...bool) (*exec.Cmd, string, error) {
	configBytes, err := BuildXrayConfig(cfg, inboundPort, muxEnabled...)
	if err != nil {
		return nil, "", fmt.Errorf("build xray config: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "xray-config-*.json")
	if err != nil {
		return nil, "", fmt.Errorf("create temp file: %w", err)
	}

	if _, err := tmpFile.Write(configBytes); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, "", fmt.Errorf("write config: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return nil, "", fmt.Errorf("close config: %w", err)
	}

	cmd := exec.Command("xray", "-c", tmpFile.Name())
	if err := cmd.Start(); err != nil {
		os.Remove(tmpFile.Name())
		return nil, "", fmt.Errorf("start xray: %w", err)
	}

	return cmd, tmpFile.Name(), nil
}

func StopXray(cmd *exec.Cmd, configPath string) error {
	if cmd == nil || cmd.Process == nil {
		if configPath != "" {
			os.Remove(configPath)
		}
		return nil
	}

	cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
		<-done
	}

	if configPath != "" {
		os.Remove(configPath)
	}
	return nil
}

func HealthCheckXray(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil {
		return false
	}
	return cmd.Process.Signal(syscall.Signal(0)) == nil
}
