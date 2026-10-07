package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

func TestBuildXrayConfig_Shadowsocks(t *testing.T) {
	raw := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	port := 10800
	data, err := BuildXrayConfig(cfg, port)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(xc.Inbounds) != 1 {
		t.Fatalf("expected 1 inbound, got %d", len(xc.Inbounds))
	}
	if xc.Inbounds[0].Port != port {
		t.Errorf("inbound port = %d, want %d", xc.Inbounds[0].Port, port)
	}
	if xc.Inbounds[0].Listen != "127.0.0.1" {
		t.Errorf("inbound listen = %q, want 127.0.0.1", xc.Inbounds[0].Listen)
	}
	if xc.Inbounds[0].Protocol != "socks" {
		t.Errorf("inbound protocol = %q, want socks", xc.Inbounds[0].Protocol)
	}

	if len(xc.Outbounds) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(xc.Outbounds))
	}
	if xc.Outbounds[0].Protocol != "shadowsocks" {
		t.Errorf("outbound protocol = %q, want shadowsocks", xc.Outbounds[0].Protocol)
	}

	var settings struct {
		Servers []struct {
			Address  string `json:"address"`
			Port     int    `json:"port"`
			Method   string `json:"method"`
			Password string `json:"password"`
		} `json:"servers"`
	}
	if err := json.Unmarshal(xc.Outbounds[0].Settings, &settings); err != nil {
		t.Fatalf("unmarshal settings: %v", err)
	}
	if len(settings.Servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(settings.Servers))
	}
	if settings.Servers[0].Address != "1.2.3.4" {
		t.Errorf("server address = %q, want 1.2.3.4", settings.Servers[0].Address)
	}
	if settings.Servers[0].Port != 12345 {
		t.Errorf("server port = %d, want 12345", settings.Servers[0].Port)
	}
	if settings.Servers[0].Method != "aes-128-gcm" {
		t.Errorf("method = %q, want aes-128-gcm", settings.Servers[0].Method)
	}
	if settings.Servers[0].Password != "password" {
		t.Errorf("password = %q, want password", settings.Servers[0].Password)
	}

	if xc.Outbounds[0].Mux == nil || !xc.Outbounds[0].Mux.Enabled {
		t.Error("expected mux with enabled=true by default")
	}
	if xc.Outbounds[0].Mux.Concurrency != 8 {
		t.Errorf("mux concurrency = %d, want 8", xc.Outbounds[0].Mux.Concurrency)
	}
}

func TestBuildXrayConfig_MuxDisabled(t *testing.T) {
	raw := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10801, false)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if xc.Outbounds[0].Mux != nil && xc.Outbounds[0].Mux.Enabled {
		t.Error("expected mux disabled when explicitly requested")
	}
}

func TestBuildXrayConfig_FreedomFallbackNoMux(t *testing.T) {
	// hysteria2 is a freedom fallback: mux must never be emitted there,
	// even when the caller requests mux on.
	raw := "hysteria2://auth@1.2.3.4:443"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10802, true)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if xc.Outbounds[0].Protocol != "freedom" {
		t.Fatalf("outbound protocol = %q, want freedom", xc.Outbounds[0].Protocol)
	}
	if xc.Outbounds[0].Mux != nil {
		t.Error("expected no mux on freedom fallback outbound")
	}
}

func TestBuildXrayConfig_Socks5OutboundNoMux(t *testing.T) {
	// A socks5 outbound tunnels raw bytes through a plain SOCKS proxy to the
	// real target. Mux wraps streams in smux frames that only an xray/v2fly
	// peer can demultiplex, so mux frames would reach the target un-decoded
	// and corrupt every stream (downloads die with EOF). Mux must never be
	// emitted on socks5 outbounds, even when the caller requests mux on.
	raw := "socks5://user:pass@1.2.3.4:1080#MySocks"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "socks5" {
		t.Fatalf("protocol = %q, want socks5", cfg.Protocol)
	}

	for _, tc := range []struct {
		name string
		mux  []bool
	}{
		{"default", nil},
		{"explicit-on", []bool{true}},
	} {
		data, err := BuildXrayConfig(cfg, 10803, tc.mux...)
		if err != nil {
			t.Fatalf("%s: BuildXrayConfig error: %v", tc.name, err)
		}
		var xc XrayConfig
		if err := json.Unmarshal(data, &xc); err != nil {
			t.Fatalf("%s: invalid JSON: %v", tc.name, err)
		}
		if len(xc.Outbounds) != 1 {
			t.Fatalf("%s: expected 1 outbound, got %d", tc.name, len(xc.Outbounds))
		}
		if xc.Outbounds[0].Protocol != "socks" {
			t.Fatalf("%s: outbound protocol = %q, want socks", tc.name, xc.Outbounds[0].Protocol)
		}
		if xc.Outbounds[0].Mux != nil {
			t.Errorf("%s: socks5 outbound has mux=%+v, want no mux (remote cannot demultiplex smux)",
				tc.name, xc.Outbounds[0].Mux)
		}
	}
}

func TestBuildXrayConfig_VMess(t *testing.T) {
	v := map[string]interface{}{
		"add":  "1.2.3.4",
		"port": 12345,
		"id":   "109d47e4-4efe-45f8-9f63-52af26e1a5e2",
		"aid":  "0",
		"net":  "tcp",
		"type": "none",
		"tls":  "",
		"path": "",
		"host": "",
	}
	b, _ := json.Marshal(v)
	raw := "vmess://" + base64.StdEncoding.EncodeToString(b) + "#MyVMess"

	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10801)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(xc.Outbounds) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(xc.Outbounds))
	}
	if xc.Outbounds[0].Protocol != "vmess" {
		t.Errorf("protocol = %q, want vmess", xc.Outbounds[0].Protocol)
	}

	var settings struct {
		Vnext []struct {
			Address string `json:"address"`
			Port    int    `json:"port"`
			Users   []struct {
				ID       string `json:"id"`
				AlterID  int    `json:"alterId"`
				Security string `json:"security"`
			} `json:"users"`
		} `json:"vnext"`
	}
	if err := json.Unmarshal(xc.Outbounds[0].Settings, &settings); err != nil {
		t.Fatalf("unmarshal settings: %v", err)
	}
	if len(settings.Vnext) != 1 {
		t.Fatalf("expected 1 vnext, got %d", len(settings.Vnext))
	}
	u := settings.Vnext[0]
	if u.Address != "1.2.3.4" {
		t.Errorf("address = %q, want 1.2.3.4", u.Address)
	}
	if u.Port != 12345 {
		t.Errorf("port = %d, want 12345", u.Port)
	}
	if len(u.Users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(u.Users))
	}
	if u.Users[0].ID != "109d47e4-4efe-45f8-9f63-52af26e1a5e2" {
		t.Errorf("id = %q, want 109d47e4...", u.Users[0].ID)
	}
	if u.Users[0].AlterID != 0 {
		t.Errorf("alterId = %d, want 0", u.Users[0].AlterID)
	}
	if u.Users[0].Security != "auto" {
		t.Errorf("security = %q, want auto", u.Users[0].Security)
	}
}

func TestBuildXrayConfig_VLess(t *testing.T) {
	raw := "vless://109d47e4-4efe-45f8-9f63-52af26e1a5e2@1.2.3.4:12345?encryption=none&security=tls&type=tcp&path=%2F&host=example.com&sni=sni.example.com&fp=chrome&alpn=h2&flow=xtls-rprx-vision#MyVLess"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10802)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(xc.Outbounds) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(xc.Outbounds))
	}
	if xc.Outbounds[0].Protocol != "vless" {
		t.Errorf("protocol = %q, want vless", xc.Outbounds[0].Protocol)
	}

	var settings struct {
		Vnext []struct {
			Address string `json:"address"`
			Port    int    `json:"port"`
			Users   []struct {
				ID         string `json:"id"`
				Encryption string `json:"encryption"`
				Flow       string `json:"flow"`
			} `json:"users"`
		} `json:"vnext"`
	}
	if err := json.Unmarshal(xc.Outbounds[0].Settings, &settings); err != nil {
		t.Fatalf("unmarshal settings: %v", err)
	}
	if len(settings.Vnext) != 1 {
		t.Fatalf("expected 1 vnext, got %d", len(settings.Vnext))
	}
	u := settings.Vnext[0].Users[0]
	if u.ID != "109d47e4-4efe-45f8-9f63-52af26e1a5e2" {
		t.Errorf("id = %q", u.ID)
	}
	if u.Encryption != "none" {
		t.Errorf("encryption = %q, want none", u.Encryption)
	}
	if u.Flow != "xtls-rprx-vision" {
		t.Errorf("flow = %q, want xtls-rprx-vision", u.Flow)
	}

	ss := xc.Outbounds[0].StreamSettings
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Security != "tls" {
		t.Errorf("security = %q, want tls", ss.Security)
	}
	if ss.TLSSettings == nil {
		t.Fatal("expected tls settings")
	}
	if ss.TLSSettings.ServerName != "sni.example.com" {
		t.Errorf("serverName = %q, want sni.example.com", ss.TLSSettings.ServerName)
	}
	if ss.TLSSettings.Fingerprint != "chrome" {
		t.Errorf("fingerprint = %q, want chrome", ss.TLSSettings.Fingerprint)
	}
	if len(ss.TLSSettings.ALPN) != 1 || ss.TLSSettings.ALPN[0] != "h2" {
		t.Errorf("alpn = %v, want [h2]", ss.TLSSettings.ALPN)
	}
}

func TestBuildXrayConfig_Trojan(t *testing.T) {
	raw := "trojan://password123@1.2.3.4:443?security=tls&type=tcp&path=%2F&host=example.com&sni=sni.example.com&fp=chrome&alpn=h2#MyTrojan"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10803)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(xc.Outbounds) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(xc.Outbounds))
	}
	if xc.Outbounds[0].Protocol != "trojan" {
		t.Errorf("protocol = %q, want trojan", xc.Outbounds[0].Protocol)
	}

	var settings struct {
		Servers []struct {
			Address  string `json:"address"`
			Port     int    `json:"port"`
			Password string `json:"password"`
		} `json:"servers"`
	}
	if err := json.Unmarshal(xc.Outbounds[0].Settings, &settings); err != nil {
		t.Fatalf("unmarshal settings: %v", err)
	}
	if len(settings.Servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(settings.Servers))
	}
	s := settings.Servers[0]
	if s.Address != "1.2.3.4" {
		t.Errorf("address = %q, want 1.2.3.4", s.Address)
	}
	if s.Port != 443 {
		t.Errorf("port = %d, want 443", s.Port)
	}
	if s.Password != "password123" {
		t.Errorf("password = %q, want password123", s.Password)
	}
}

func TestBuildXrayConfig_SOCKS5(t *testing.T) {
	raw := "socks5://user:pass@1.2.3.4:1080#MySocks"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10804)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(xc.Outbounds) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(xc.Outbounds))
	}
	if xc.Outbounds[0].Protocol != "socks" {
		t.Errorf("protocol = %q, want socks", xc.Outbounds[0].Protocol)
	}

	var settings struct {
		Servers []struct {
			Address string `json:"address"`
			Port    int    `json:"port"`
			Users   []struct {
				User string `json:"user"`
				Pass string `json:"pass"`
			} `json:"users"`
		} `json:"servers"`
	}
	if err := json.Unmarshal(xc.Outbounds[0].Settings, &settings); err != nil {
		t.Fatalf("unmarshal settings: %v", err)
	}
	if len(settings.Servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(settings.Servers))
	}
	s := settings.Servers[0]
	if s.Address != "1.2.3.4" {
		t.Errorf("address = %q, want 1.2.3.4", s.Address)
	}
	if s.Port != 1080 {
		t.Errorf("port = %d, want 1080", s.Port)
	}
	if len(s.Users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(s.Users))
	}
	if s.Users[0].User != "user" {
		t.Errorf("user = %q, want user", s.Users[0].User)
	}
	if s.Users[0].Pass != "pass" {
		t.Errorf("pass = %q, want pass", s.Users[0].Pass)
	}
}

func TestBuildXrayConfig_SOCKS5_NoAuth(t *testing.T) {
	raw := "socks5://1.2.3.4:1080#NoAuth"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10805)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	var settings struct {
		Servers []struct {
			Address string     `json:"address"`
			Port    int        `json:"port"`
			Users   []struct{} `json:"users"`
		} `json:"servers"`
	}
	if err := json.Unmarshal(xc.Outbounds[0].Settings, &settings); err != nil {
		t.Fatalf("unmarshal settings: %v", err)
	}
	if len(settings.Servers[0].Users) != 0 {
		t.Error("expected no users for SOCKS5 without auth")
	}
}

func TestBuildXrayConfig_Fallback(t *testing.T) {
	raw := "hysteria2://auth123@1.2.3.4:443#MyHy2"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10806)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if len(xc.Outbounds) != 1 {
		t.Fatalf("expected 1 outbound, got %d", len(xc.Outbounds))
	}
	if xc.Outbounds[0].Protocol != "freedom" {
		t.Errorf("protocol = %q, want freedom", xc.Outbounds[0].Protocol)
	}

	var settings struct {
		DomainStrategy string `json:"domainStrategy"`
	}
	if err := json.Unmarshal(xc.Outbounds[0].Settings, &settings); err != nil {
		t.Fatalf("unmarshal settings: %v", err)
	}
	if settings.DomainStrategy != "UseIP" {
		t.Errorf("domainStrategy = %q, want UseIP", settings.DomainStrategy)
	}
}

func TestBuildXrayConfig_Fallback_TUIC(t *testing.T) {
	raw := "tuic://uuid:pass@1.2.3.4:443#MyTUIC"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10807)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if xc.Outbounds[0].Protocol != "freedom" {
		t.Errorf("protocol = %q, want freedom", xc.Outbounds[0].Protocol)
	}
}

func TestBuildXrayConfig_Fallback_WireGuard(t *testing.T) {
	raw := "wireguard://key@1.2.3.4:51820#MyWG"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10808)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if xc.Outbounds[0].Protocol != "freedom" {
		t.Errorf("protocol = %q, want freedom", xc.Outbounds[0].Protocol)
	}
}

func TestBuildXrayConfig_WebSocketStream(t *testing.T) {
	raw := "vless://uuid@1.2.3.4:443?type=ws&path=%2Fws&host=example.com&security=none#MyWS"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10809)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	ss := xc.Outbounds[0].StreamSettings
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Network != "ws" {
		t.Errorf("network = %q, want ws", ss.Network)
	}
	if ss.WSSettings == nil {
		t.Fatal("expected ws settings")
	}
	if ss.WSSettings.Path != "/ws" {
		t.Errorf("path = %q, want /ws", ss.WSSettings.Path)
	}
	if ss.WSSettings.Headers["Host"] != "example.com" {
		t.Errorf("Host header = %q, want example.com", ss.WSSettings.Headers["Host"])
	}
}

func TestBuildXrayConfig_GRPCStream(t *testing.T) {
	raw := "vless://uuid@1.2.3.4:443?type=grpc&path=mygrpc&security=none#MyGRPC"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10810)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	ss := xc.Outbounds[0].StreamSettings
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Network != "grpc" {
		t.Errorf("network = %q, want grpc", ss.Network)
	}
	if ss.GRPCSettings == nil {
		t.Fatal("expected grpc settings")
	}
	if ss.GRPCSettings.ServiceName != "mygrpc" {
		t.Errorf("serviceName = %q, want mygrpc", ss.GRPCSettings.ServiceName)
	}
}

func TestBuildXrayConfig_TLSStream(t *testing.T) {
	raw := "vless://uuid@1.2.3.4:443?security=tls&type=tcp&sni=example.com&fp=chrome&alpn=h2,http/1.1#MyTLS"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10811)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	ss := xc.Outbounds[0].StreamSettings
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Security != "tls" {
		t.Errorf("security = %q, want tls", ss.Security)
	}
	if ss.TLSSettings == nil {
		t.Fatal("expected tls settings")
	}
	if ss.TLSSettings.ServerName != "example.com" {
		t.Errorf("serverName = %q, want example.com", ss.TLSSettings.ServerName)
	}
	if ss.TLSSettings.Fingerprint != "chrome" {
		t.Errorf("fingerprint = %q, want chrome", ss.TLSSettings.Fingerprint)
	}
	if len(ss.TLSSettings.ALPN) != 2 || ss.TLSSettings.ALPN[0] != "h2" || ss.TLSSettings.ALPN[1] != "http/1.1" {
		t.Errorf("alpn = %v, want [h2 http/1.1]", ss.TLSSettings.ALPN)
	}
}

func TestBuildXrayConfig_RealityStream(t *testing.T) {
	raw := "vless://uuid@1.2.3.4:443?security=reality&type=tcp&sni=example.com&fp=chrome&pbk=publickey&sid=1234&spx=spiderx#MyReality"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10812)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	ss := xc.Outbounds[0].StreamSettings
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Security != "reality" {
		t.Errorf("security = %q, want reality", ss.Security)
	}
	if ss.RealitySettings == nil {
		t.Fatal("expected reality settings")
	}
	if ss.RealitySettings.ServerName != "example.com" {
		t.Errorf("serverName = %q, want example.com", ss.RealitySettings.ServerName)
	}
	if ss.RealitySettings.Fingerprint != "chrome" {
		t.Errorf("fingerprint = %q, want chrome", ss.RealitySettings.Fingerprint)
	}
	if ss.RealitySettings.PublicKey != "publickey" {
		t.Errorf("publicKey = %q, want publickey", ss.RealitySettings.PublicKey)
	}
	if ss.RealitySettings.ShortID != "1234" {
		t.Errorf("shortId = %q, want 1234", ss.RealitySettings.ShortID)
	}
	if ss.RealitySettings.SpiderX != "spiderx" {
		t.Errorf("spiderX = %q, want spiderx", ss.RealitySettings.SpiderX)
	}
}

func TestBuildXrayConfig_TCPHTTPHeader(t *testing.T) {
	raw := "vmess://" + base64.StdEncoding.EncodeToString(func() []byte {
		v := map[string]interface{}{
			"add":  "1.2.3.4",
			"port": 443,
			"id":   "109d47e4-4efe-45f8-9f63-52af26e1a5e2",
			"aid":  "0",
			"net":  "tcp",
			"type": "http",
			"host": "example.com",
			"tls":  "tls",
			"sni":  "sni.example.com",
		}
		b, _ := json.Marshal(v)
		return b
	}()) + "#VMessTCPHTTP"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10813)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	ss := xc.Outbounds[0].StreamSettings
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.TCPSettings == nil {
		t.Fatal("expected tcp settings")
	}
	if ss.TCPSettings.Header == nil {
		t.Fatal("expected tcp header")
	}
	if ss.TCPSettings.Header.Type != "http" {
		t.Errorf("header type = %q, want http", ss.TCPSettings.Header.Type)
	}
	if ss.TCPSettings.Header.Request == nil {
		t.Fatal("expected request")
	}
	if ss.TCPSettings.Header.Request.Headers["Host"][0] != "example.com" {
		t.Errorf("Host header = %v, want [example.com]", ss.TCPSettings.Header.Request.Headers["Host"])
	}
}

func TestBuildXrayConfig_ValidJSON(t *testing.T) {
	tests := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#Test",
		"vmess://" + base64.StdEncoding.EncodeToString(mustMarshal(map[string]interface{}{"add": "1.2.3.4", "port": 443, "id": "109d47e4-4efe-45f8-9f63-52af26e1a5e2"})),
		"vless://uuid@1.2.3.4:443?type=tcp",
		"trojan://pass@1.2.3.4:443",
		"socks5://user:pass@1.2.3.4:1080",
		"hysteria2://auth@1.2.3.4:443",
	}
	for _, raw := range tests {
		cfg := proxycfg.ParseSingle(raw)
		if cfg == nil {
			t.Fatalf("failed to parse: %s", raw)
		}
		data, err := BuildXrayConfig(cfg, 10900)
		if err != nil {
			t.Fatalf("BuildXrayConfig error for %s: %v", raw, err)
		}
		var xc XrayConfig
		if err := json.Unmarshal(data, &xc); err != nil {
			t.Fatalf("invalid JSON for %s: %v", raw, err)
		}
		if len(xc.Inbounds) != 1 {
			t.Errorf("expected 1 inbound for %s", raw)
		}
		if len(xc.Outbounds) != 1 {
			t.Errorf("expected 1 outbound for %s", raw)
		}
	}
}

func TestHealthCheckXray(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	defer cmd.Process.Kill()
	proc := xrayproc.Wrap(cmd, "")

	if !HealthCheckXray(proc) {
		t.Error("expected health check to return true for running process")
	}

	cmd.Process.Kill()
	select {
	case <-proc.Exited:
	case <-time.After(2 * time.Second):
		t.Fatal("child was not reaped within 2s")
	}

	if HealthCheckXray(proc) {
		t.Error("expected health check to return false for killed process")
	}
}

func TestHealthCheckXray_NilCmd(t *testing.T) {
	if HealthCheckXray(nil) {
		t.Error("expected false for nil command")
	}
}

func TestStopXray(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "xray-test-config-*.json")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	tmpFile.Write([]byte("{}"))
	tmpFile.Close()
	configPath := tmpFile.Name()

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		os.Remove(configPath)
		t.Fatalf("start sleep: %v", err)
	}

	if err := StopXray(xrayproc.Wrap(cmd, configPath), configPath); err != nil {
		t.Errorf("StopXray error: %v", err)
	}

	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Error("config file was not removed")
	}

	var ws syscall.WaitStatus
	if cmd.ProcessState == nil {
		t.Error("process state is nil, process may still be running")
	} else {
		ws = cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ws.Signaled() {
			t.Logf("process exited with status: %v", cmd.ProcessState)
		}
	}
}

func TestStopXray_NilCmd(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "xray-test-config-*.json")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	tmpFile.Close()
	configPath := tmpFile.Name()

	if err := StopXray(nil, configPath); err != nil {
		t.Errorf("StopXray error: %v", err)
	}

	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Error("config file was not removed for nil cmd")
	}
}

func TestBuildXrayConfig_RealityTrojanAuth(t *testing.T) {
	raw := "trojan://password@1.2.3.4:443?security=reality&type=tcp&sni=example.com&fp=chrome&pbk=pubkey&sid=abcd&spx=spid#RealityTrojan"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	data, err := BuildXrayConfig(cfg, 10820)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}
	var xc XrayConfig
	if err := json.Unmarshal(data, &xc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	ss := xc.Outbounds[0].StreamSettings
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Security != "reality" {
		t.Errorf("security = %q, want reality", ss.Security)
	}
	if ss.RealitySettings == nil {
		t.Fatal("expected reality settings")
	}
	if ss.RealitySettings.PublicKey != "pubkey" {
		t.Errorf("publicKey = %q, want pubkey", ss.RealitySettings.PublicKey)
	}
	if ss.RealitySettings.ShortID != "abcd" {
		t.Errorf("shortId = %q, want abcd", ss.RealitySettings.ShortID)
	}
	if ss.RealitySettings.SpiderX != "spid" {
		t.Errorf("spiderX = %q, want spid", ss.RealitySettings.SpiderX)
	}
}

func TestBuildXrayConfig_LogLevel(t *testing.T) {
	raw := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#Test"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}

	data, err := BuildXrayConfig(cfg, 10814)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}

	if !strings.Contains(string(data), `"loglevel": "none"`) {
		t.Error("expected loglevel 'none' in config")
	}
}

func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ---------- F-11: speed-test xray instances must use the production mux setting ----------

// TestSpeedTestXrayMuxMatchesProduction pins test/prod mux parity: the temp
// xray a speed test starts gets exactly the mux flag production gives the
// real xray (proxycfg.Config.XrayMux, sourced from XRAY_MUX), and renders
// byte-identical config JSON from it.
func TestSpeedTestXrayMuxMatchesProduction(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  string // XRAY_MUX value ("" = leave unset)
	}{
		{"default", ""},
		{"explicit-on", "true"},
		{"explicit-off", "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Isolate from the ambient environment so ParseConfig's defaults
			// are what production would actually run with.
			for _, key := range []string{
				"SUBSCRIBER_URL", "FETCH_INTERVAL", "TEST_TIMEOUT", "DOWNLOAD_SIZE",
				"DOWNLOAD_ENDPOINT", "DOWNLOAD_FALLBACK", "WAN_COUNT", "WAN_BASE_PORT",
				"TEST_BASE_PORT", "PROXY_PORT", "SOCKS_PORT", "MINIMUM_SPEED",
				"METRICS_PORT", "ACCESS_LOG", "KEEPALIVE_INTERVAL", "WAN_FAIL_THRESHOLD",
				"STABILITY_PROBES", "ALLOW_DEGRADED_BOOT", "XRAY_MUX",
			} {
				unsetenv(t, key)
			}
			setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
			if tc.env != "" {
				setenv(t, "XRAY_MUX", tc.env)
			}

			prod, err := proxycfg.ParseConfig()
			if err != nil {
				t.Fatalf("ParseConfig error: %v", err)
			}

			orig := startTestXray
			var gotMux []bool
			startTestXray = func(_ *proxycfg.ProxyConfig, _ int, muxEnabled ...bool) (*xrayproc.Handle, string, error) {
				mux := true // BuildXrayConfig's default when the flag is omitted
				if len(muxEnabled) > 0 {
					mux = muxEnabled[0]
				}
				gotMux = append(gotMux, mux)
				return nil, "", errors.New("stub: this test never spawns xray")
			}
			t.Cleanup(func() { startTestXray = orig })

			// The same sharelink the T-XRAY-01 ss-tcp golden is built from.
			proxy := proxycfg.ParseSingle("ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS")
			if proxy == nil {
				t.Fatal("ParseSingle returned nil")
			}
			res := TestSpeedWithStability(proxy, 10999, time.Second, "http://example.invalid/", 0, 0)
			if res == nil || res.Error == nil {
				t.Fatal("expected the stubbed speed test to return an error result")
			}
			if len(gotMux) != 1 {
				t.Fatalf("speed-test xray started %d times, want 1", len(gotMux))
			}
			if gotMux[0] != prod.XrayMux {
				t.Errorf("speed-test xray mux=%v, production mux=%v (XRAY_MUX=%q): test and production instances must match",
					gotMux[0], prod.XrayMux, tc.env)
			}

			// Same flag must render the same bytes — parity is about the
			// actual config, not just the boolean.
			fromProd, err := BuildXrayConfig(proxy, 10800, prod.XrayMux)
			if err != nil {
				t.Fatalf("BuildXrayConfig (production flag) error: %v", err)
			}
			fromTest, err := BuildXrayConfig(proxy, 10800, gotMux[0])
			if err != nil {
				t.Fatalf("BuildXrayConfig (test flag) error: %v", err)
			}
			if !bytes.Equal(fromProd, fromTest) {
				t.Errorf("test and production xray configs differ:\nproduction:\n%s\ntest:\n%s", fromProd, fromTest)
			}

			// ...and the agreed flag must render the T-XRAY-01 golden bytes,
			// so the speed test exercises byte-for-byte what production runs.
			goldenName := "ss-tcp.mux-off.json"
			if prod.XrayMux {
				goldenName = "ss-tcp.mux-on.json"
			}
			golden, err := os.ReadFile(filepath.Join("internal", "xraycfg", "testdata", goldenName))
			if err != nil {
				t.Fatalf("read golden %s: %v", goldenName, err)
			}
			if !bytes.Equal(fromProd, golden) {
				t.Errorf("production render differs from golden %s:\ngolden:\n%s\nrender:\n%s", goldenName, golden, fromProd)
			}
		})
	}
}
