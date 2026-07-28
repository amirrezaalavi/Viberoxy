package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestParseShadowsocks(t *testing.T) {
	raw := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "ss" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "ss")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 12345 {
		t.Errorf("Port = %d, want %d", cfg.Port, 12345)
	}
	if cfg.Name != "MySS" {
		t.Errorf("Name = %q, want %q", cfg.Name, "MySS")
	}
	if cfg.Raw != raw {
		t.Errorf("Raw = %q, want %q", cfg.Raw, raw)
	}
}

func TestParseShadowsocks_Legacy(t *testing.T) {
	raw := "ss://" + base64.StdEncoding.EncodeToString([]byte("aes-128-gcm:password@1.2.3.4:12345")) + "#MySSLegacy"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "ss" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "ss")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 12345 {
		t.Errorf("Port = %d, want %d", cfg.Port, 12345)
	}
	if cfg.Name != "MySSLegacy" {
		t.Errorf("Name = %q, want %q", cfg.Name, "MySSLegacy")
	}
}

func TestParseVMess(t *testing.T) {
	v := map[string]interface{}{
		"add": "1.2.3.4",
		"port": 12345,
		"ps":   "My VMess",
		"id":   "109d47e4-4efe-45f8-9f63-52af26e1a5e2",
		"aid":  "0",
		"net":  "tcp",
		"type": "none",
		"tls":  "",
		"path": "",
		"host": "",
	}
	b, _ := json.Marshal(v)
	raw := "vmess://" + base64.StdEncoding.EncodeToString(b) + "#MyName"

	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "vmess" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "vmess")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 12345 {
		t.Errorf("Port = %d, want %d", cfg.Port, 12345)
	}
	if cfg.Name != "My VMess" {
		t.Errorf("Name = %q, want %q", cfg.Name, "My VMess")
	}
}

func TestParseVMess_PortAsString(t *testing.T) {
	v := map[string]interface{}{
		"add":  "1.2.3.4",
		"port": "54321",
		"ps":   "PortString",
	}
	b, _ := json.Marshal(v)
	raw := "vmess://" + base64.StdEncoding.EncodeToString(b)

	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 54321 {
		t.Errorf("Port = %d, want %d", cfg.Port, 54321)
	}
}

func TestParseVMess_AddressField(t *testing.T) {
	v := map[string]interface{}{
		"address": "5.6.7.8",
		"port":    8080,
		"ps":      "AddrField",
	}
	b, _ := json.Marshal(v)
	raw := "vmess://" + base64.StdEncoding.EncodeToString(b)

	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Server != "5.6.7.8" {
		t.Errorf("Server = %q, want %q", cfg.Server, "5.6.7.8")
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want %d", cfg.Port, 8080)
	}
}

func TestParseVLess(t *testing.T) {
	raw := "vless://109d47e4-4efe-45f8-9f63-52af26e1a5e2@1.2.3.4:12345?encryption=none&security=tls&type=tcp&path=%2F&host=example.com&sni=sni.example.com&fp=chrome&alpn=h2&flow=xtls-rprx-vision#My%20VLess"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "vless" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "vless")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 12345 {
		t.Errorf("Port = %d, want %d", cfg.Port, 12345)
	}
	if cfg.Name != "My VLess" {
		t.Errorf("Name = %q, want %q", cfg.Name, "My VLess")
	}
}

func TestParseTrojan(t *testing.T) {
	raw := "trojan://password123@1.2.3.4:443?security=tls&type=tcp&path=%2F&host=example.com&sni=sni.example.com&fp=chrome&alpn=h2#My%20Trojan"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "trojan" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "trojan")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 443 {
		t.Errorf("Port = %d, want %d", cfg.Port, 443)
	}
	if cfg.Name != "My Trojan" {
		t.Errorf("Name = %q, want %q", cfg.Name, "My Trojan")
	}
}

func TestParseHysteria2(t *testing.T) {
	raw := "hysteria2://auth123@1.2.3.4:443?obfs=salamander&obfs-password=obfspass&sni=example.com&alpn=h3&up=50&down=100&insecure=1#My%20Hy2"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "hysteria2" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "hysteria2")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 443 {
		t.Errorf("Port = %d, want %d", cfg.Port, 443)
	}
	if cfg.Name != "My Hy2" {
		t.Errorf("Name = %q, want %q", cfg.Name, "My Hy2")
	}
}

func TestParseHysteria2_Hy2Prefix(t *testing.T) {
	raw := "hy2://auth123@1.2.3.4:8443?sni=example.com#MyHy2Prefix"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "hysteria2" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "hysteria2")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 8443 {
		t.Errorf("Port = %d, want %d", cfg.Port, 8443)
	}
	if cfg.Name != "MyHy2Prefix" {
		t.Errorf("Name = %q, want %q", cfg.Name, "MyHy2Prefix")
	}
}

func TestParseHysteria2_DefaultPort(t *testing.T) {
	raw := "hysteria2://auth@1.2.3.4#NoPort"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Port != 443 {
		t.Errorf("Port = %d, want %d", cfg.Port, 443)
	}
	if cfg.Name != "NoPort" {
		t.Errorf("Name = %q, want %q", cfg.Name, "NoPort")
	}
}

func TestParseTUIC(t *testing.T) {
	raw := "tuic://uuid123:pass456@1.2.3.4:443?sni=example.com&alpn=h3&congestion_control=bbr&udp_relay_mode=native&reduce_rtt=true&allowInsecure=true#My%20TUIC"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "tuic" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "tuic")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 443 {
		t.Errorf("Port = %d, want %d", cfg.Port, 443)
	}
	if cfg.Name != "My TUIC" {
		t.Errorf("Name = %q, want %q", cfg.Name, "My TUIC")
	}
}

func TestParseTUIC_DefaultPort(t *testing.T) {
	raw := "tuic://uuid:pass@1.2.3.4#NoPort"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Port != 443 {
		t.Errorf("Port = %d, want %d", cfg.Port, 443)
	}
}

func TestParseWireGuard(t *testing.T) {
	raw := "wireguard://privatekey123@1.2.3.4:51820?publicKey=pubkey123&address=10.0.0.1%2F32&dns=1.1.1.1&mtu=1420&allowedIPs=0.0.0.0%2F0&reserved=0,0,0&presharedKey=prekey123#My%20WG"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "wireguard" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "wireguard")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 51820 {
		t.Errorf("Port = %d, want %d", cfg.Port, 51820)
	}
	if cfg.Name != "My WG" {
		t.Errorf("Name = %q, want %q", cfg.Name, "My WG")
	}
}

func TestParseWireGuard_DefaultPort(t *testing.T) {
	raw := "wireguard://key@1.2.3.4#NoPort"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Port != 51820 {
		t.Errorf("Port = %d, want %d", cfg.Port, 51820)
	}
}

func TestParseWireGuard_Base64JSON(t *testing.T) {
	wg := map[string]string{
		"endpoint":   "1.2.3.4:51820",
		"privateKey": "priv123",
		"publicKey":  "pub123",
		"address":    "10.0.0.1/32",
	}
	b, _ := json.Marshal(wg)
	raw := "wireguard://" + base64.StdEncoding.EncodeToString(b) + "#MyWGJSON"

	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "wireguard" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "wireguard")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 51820 {
		t.Errorf("Port = %d, want %d", cfg.Port, 51820)
	}
	if cfg.Name != "MyWGJSON" {
		t.Errorf("Name = %q, want %q", cfg.Name, "MyWGJSON")
	}
}

func TestParseWireGuard_Base64JSON_ServerField(t *testing.T) {
	wg := map[string]string{
		"server": "5.6.7.8:9876",
	}
	b, _ := json.Marshal(wg)
	raw := "wireguard://" + base64.StdEncoding.EncodeToString(b)

	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Server != "5.6.7.8" {
		t.Errorf("Server = %q, want %q", cfg.Server, "5.6.7.8")
	}
	if cfg.Port != 9876 {
		t.Errorf("Port = %d, want %d", cfg.Port, 9876)
	}
}

func TestParseWireGuard_Base64JSON_EndpointWithDefaultPort(t *testing.T) {
	wg := map[string]string{
		"endpoint": "5.6.7.8",
	}
	b, _ := json.Marshal(wg)
	raw := "wireguard://" + base64.StdEncoding.EncodeToString(b)

	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Server != "5.6.7.8" {
		t.Errorf("Server = %q, want %q", cfg.Server, "5.6.7.8")
	}
	if cfg.Port != 51820 {
		t.Errorf("Port = %d, want %d", cfg.Port, 51820)
	}
}

func TestParseSocks5(t *testing.T) {
	raw := "socks5://user:pass@1.2.3.4:1080#My%20Socks"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "socks5" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "socks5")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 1080 {
		t.Errorf("Port = %d, want %d", cfg.Port, 1080)
	}
	if cfg.Name != "My Socks" {
		t.Errorf("Name = %q, want %q", cfg.Name, "My Socks")
	}
}

func TestParseSocks5_NoAuth(t *testing.T) {
	raw := "socks5://1.2.3.4:1080#MySocksNoAuth"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "socks5" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "socks5")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 1080 {
		t.Errorf("Port = %d, want %d", cfg.Port, 1080)
	}
	if cfg.Name != "MySocksNoAuth" {
		t.Errorf("Name = %q, want %q", cfg.Name, "MySocksNoAuth")
	}
}

func TestParseSocks5_DefaultPort(t *testing.T) {
	raw := "socks5://1.2.3.4#NoPort"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Port != 1080 {
		t.Errorf("Port = %d, want %d", cfg.Port, 1080)
	}
}

func TestParseSocks4(t *testing.T) {
	raw := "socks4://user@1.2.3.4:1080#MySocks4"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "socks5" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "socks5")
	}
	if cfg.Server != "1.2.3.4" {
		t.Errorf("Server = %q, want %q", cfg.Server, "1.2.3.4")
	}
	if cfg.Port != 1080 {
		t.Errorf("Port = %d, want %d", cfg.Port, 1080)
	}
}

func TestParseSocks(t *testing.T) {
	raw := "socks://1.2.3.4:1080#MySocks"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Protocol != "socks5" {
		t.Errorf("Protocol = %q, want %q", cfg.Protocol, "socks5")
	}
}

func TestParseBase64Subscription(t *testing.T) {
	lines := []string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS",
		"trojan://pass@5.6.7.8:443#TrojanA",
		"vmess://" + base64.StdEncoding.EncodeToString(mustMarshal(map[string]interface{}{"add": "9.10.11.12", "port": 443, "ps": "VMessB"})),
		"hysteria2://auth@13.14.15.16:443#Hy2C",
		"# this is a comment",
		"",
		"  ",
		"garbage that should be skipped",
	}
	sub := ""
	for _, l := range lines {
		sub += l + "\n"
	}
	b64sub := base64.StdEncoding.EncodeToString([]byte(sub))

	configs := ParseConfigs(b64sub)
	if len(configs) != 4 {
		t.Fatalf("expected 4 configs, got %d", len(configs))
	}

	expectations := []struct {
		server string
		port   int
	}{
		{"1.2.3.4", 12345},
		{"5.6.7.8", 443},
		{"9.10.11.12", 443},
		{"13.14.15.16", 443},
	}
	for i, exp := range expectations {
		if configs[i].Server != exp.server {
			t.Errorf("config[%d] Server = %q, want %q", i, configs[i].Server, exp.server)
		}
		if configs[i].Port != exp.port {
			t.Errorf("config[%d] Port = %d, want %d", i, configs[i].Port, exp.port)
		}
	}
}

func TestParsePlainSubscription(t *testing.T) {
	body := `ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS
trojan://pass@5.6.7.8:443#TrojanA
# comment line

garbage
`
	configs := ParseConfigs(body)
	if len(configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(configs))
	}
	if configs[0].Server != "1.2.3.4" || configs[0].Port != 12345 {
		t.Errorf("config[0] = %s:%d, want 1.2.3.4:12345", configs[0].Server, configs[0].Port)
	}
	if configs[1].Server != "5.6.7.8" || configs[1].Port != 443 {
		t.Errorf("config[1] = %s:%d, want 5.6.7.8:443", configs[1].Server, configs[1].Port)
	}
}

func TestParseInvalidLines(t *testing.T) {
	configs := ParseConfigs("not a proxy uri")
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs, got %d", len(configs))
	}
}

func TestParseInvalid(t *testing.T) {
	cfg := ParseSingle("not even a uri")
	if cfg != nil {
		t.Fatal("expected nil, got config")
	}

	cfg = ParseSingle("unknown://stuff")
	if cfg != nil {
		t.Fatal("expected nil for unknown protocol")
	}

	cfg = ParseSingle("ss://broken")
	if cfg != nil {
		t.Fatal("expected nil for broken ss")
	}

	cfg = ParseSingle("vmess://not-base64")
	if cfg != nil {
		t.Fatal("expected nil for broken vmess")
	}

	cfg = ParseSingle("vmess://" + base64.StdEncoding.EncodeToString([]byte("not-json")))
	if cfg != nil {
		t.Fatal("expected nil for vmess with non-json body")
	}

	cfg = ParseSingle("vless://no-at-symbol")
	if cfg != nil {
		t.Fatal("expected nil for vless without @")
	}

	cfg = ParseSingle("wireguard://" + base64.StdEncoding.EncodeToString([]byte("not-json")))
	if cfg != nil {
		t.Fatal("expected nil for wireguard with non-json body")
	}
}

func TestParseEmpty(t *testing.T) {
	cfg := ParseSingle("")
	if cfg != nil {
		t.Fatal("expected nil for empty string")
	}

	cfg = ParseSingle("  ")
	if cfg != nil {
		t.Fatal("expected nil for whitespace")
	}

	configs := ParseConfigs("")
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs, got %d", len(configs))
	}

	configs = ParseConfigs("  \n  \n  ")
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs for whitespace-only, got %d", len(configs))
	}
}

func TestParseOnlyComments(t *testing.T) {
	configs := ParseConfigs("# comment 1\n# comment 2\n  # indented")
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs, got %d", len(configs))
	}
}

func TestExtractFragment(t *testing.T) {
	tests := []struct {
		raw      string
		before   string
		fragment string
	}{
		{"ss://b@h:p#Name", "ss://b@h:p", "Name"},
		{"ss://b@h:p", "ss://b@h:p", ""},
		{"#", "", ""},
		{"", "", ""},
		{"a#b#c", "a", "b#c"},
	}
	for _, tc := range tests {
		before, fragment := extractFragment(tc.raw)
		if before != tc.before || fragment != tc.fragment {
			t.Errorf("extractFragment(%q) = (%q, %q), want (%q, %q)",
				tc.raw, before, fragment, tc.before, tc.fragment)
		}
	}
}

func TestParseIPv6(t *testing.T) {
	raw := "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@[::1]:12345#IPv6"
	cfg := ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	if cfg.Server != "::1" {
		t.Errorf("Server = %q, want %q", cfg.Server, "::1")
	}
	if cfg.Port != 12345 {
		t.Errorf("Port = %d, want %d", cfg.Port, 12345)
	}
	if cfg.Name != "IPv6" {
		t.Errorf("Name = %q, want %q", cfg.Name, "IPv6")
	}
}

func TestParseConfigsReturnsNilForEmpty(t *testing.T) {
	result := ParseConfigs("")
	if result != nil {
		t.Fatal("expected nil, got non-nil")
	}
}

func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
