package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

func TestBuildXrayConfig_Shadowsocks(t *testing.T) {
	raw := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS"
	cfg := ParseSingle(raw)
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

	if xc.Outbounds[0].Mux == nil || xc.Outbounds[0].Mux.Enabled {
		t.Error("expected mux with enabled=false")
	}
}

func TestBuildXrayConfig_VMess(t *testing.T) {
	v := map[string]interface{}{
		"add": "1.2.3.4",
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

	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
			Address string `json:"address"`
			Port    int    `json:"port"`
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
	cfg := ParseSingle(raw)
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
		cfg := ParseSingle(raw)
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

	if !HealthCheckXray(cmd) {
		t.Error("expected health check to return true for running process")
	}

	cmd.Process.Kill()
	cmd.Wait()

	if HealthCheckXray(cmd) {
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

	if err := StopXray(cmd, configPath); err != nil {
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

func TestExtractSIP002(t *testing.T) {
	tests := []struct {
		raw              string
		wantMethod       string
		wantPassword     string
	}{
		{
			raw:          "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345",
			wantMethod:   "aes-128-gcm",
			wantPassword: "password",
		},
		{
			raw:          "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#Name",
			wantMethod:   "aes-128-gcm",
			wantPassword: "password",
		},
	}
	for _, tc := range tests {
		method, password := extractSIP002(tc.raw)
		if method != tc.wantMethod {
			t.Errorf("method = %q, want %q", method, tc.wantMethod)
		}
		if password != tc.wantPassword {
			t.Errorf("password = %q, want %q", password, tc.wantPassword)
		}
	}
}

func TestExtractSocksParams(t *testing.T) {
	username, password := extractSocksParams("socks5://user:pass@1.2.3.4:1080")
	if username != "user" {
		t.Errorf("username = %q, want user", username)
	}
	if password != "pass" {
		t.Errorf("password = %q, want pass", password)
	}

	username, password = extractSocksParams("socks5://1.2.3.4:1080")
	if username != "" {
		t.Errorf("expected empty username, got %q", username)
	}
	if password != "" {
		t.Errorf("expected empty password, got %q", password)
	}
}

func TestStreamSettings_TCPOnly(t *testing.T) {
	ss := buildStreamSettings("tcp", "", "", "", "", "", "", "")
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Network != "tcp" {
		t.Errorf("network = %q, want tcp", ss.Network)
	}
	if ss.TCPSettings != nil {
		t.Error("expected no tcp settings for plain tcp")
	}
}

func TestBuildXrayConfig_LogLevel(t *testing.T) {
	raw := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#Test"
	cfg := ParseSingle(raw)
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
