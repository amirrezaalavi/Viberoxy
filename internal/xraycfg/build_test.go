package xraycfg

import (
	"encoding/base64"
	"strings"
	"testing"
	"viberoxy/internal/proxycfg"
)

func TestExtractSIP002(t *testing.T) {
	tests := []struct {
		raw          string
		wantMethod   string
		wantPassword string
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
		method, password, err := extractSIP002(tc.raw)
		if err != nil {
			t.Fatalf("extractSIP002(%q) error: %v", tc.raw, err)
		}
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
	ss, err := buildStreamSettings("tcp", "", "", "", "", "", "", "", "", "", "", "")
	if err != nil {
		t.Fatalf("buildStreamSettings error: %v", err)
	}
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

func TestStreamSettings_XHTTP(t *testing.T) {
	// An xhttp transport without path/host/mode cannot be rendered into a
	// working outbound: it must be an error, not a bare network=xhttp block.
	if _, err := buildStreamSettings("xhttp", "", "", "", "", "", "", "", "", "", "", ""); err == nil {
		t.Fatal("expected error for xhttp without path/host/mode, got nil")
	}
	ss, err := buildStreamSettings("xhttp", "", "/x", "h.example", "", "", "", "", "", "", "", "auto")
	if err != nil {
		t.Fatalf("buildStreamSettings error for complete xhttp params: %v", err)
	}
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Network != "xhttp" {
		t.Errorf("network = %q, want xhttp", ss.Network)
	}
}

// ---------- F-11 / RT-05: a flow such as xtls-rprx-vision must never be
// combined with mux, even when the caller explicitly asks for mux ----------

func TestBuildXrayConfig_NoMuxWithVisionFlow(t *testing.T) {
	raw := "vless://11111111-1111-1111-1111-111111111111@1.2.3.4:443?encryption=none&flow=xtls-rprx-vision&security=reality&sni=example.com&fp=chrome&pbk=abc&sid=01&type=tcp#n"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	b, err := BuildXrayConfig(cfg, 10700, true) // mux explicitly ON
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}
	if strings.Contains(string(b), `"mux"`) {
		t.Fatalf("mux enabled together with flow=xtls-rprx-vision:\n%s", b)
	}
}

func TestBuildXrayConfig_MuxStillEmittedWithoutFlow(t *testing.T) {
	// Guard against over-suppression: the same shape without a flow keeps mux.
	raw := "vless://11111111-1111-1111-1111-111111111111@1.2.3.4:443?encryption=none&security=reality&sni=example.com&fp=chrome&pbk=abc&sid=01&type=tcp#n"
	cfg := proxycfg.ParseSingle(raw)
	if cfg == nil {
		t.Fatal("expected config, got nil")
	}
	b, err := BuildXrayConfig(cfg, 10700, true)
	if err != nil {
		t.Fatalf("BuildXrayConfig error: %v", err)
	}
	if !strings.Contains(string(b), `"mux"`) {
		t.Fatalf("mux missing although the config carries no flow:\n%s", b)
	}
}

// ---------- F-17 (builder half): builders must error instead of rendering a
// bogus outbound that can never work ----------

func TestBuildXrayConfig_ErrorsOnBogusVMess(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"undecodable payload", "vmess://!!!"},
		{"invalid json payload", "vmess://" + base64.StdEncoding.EncodeToString([]byte("this is not json"))},
		{"absent uuid in json", "vmess://" + base64.StdEncoding.EncodeToString([]byte(`{}`))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &proxycfg.ProxyConfig{Protocol: "vmess", Server: "1.2.3.4", Port: 443, Raw: tc.raw}
			b, err := BuildXrayConfig(cfg, 10801, true)
			if err == nil {
				t.Fatalf("expected error, got nil; rendered instead:\n%s", b)
			}
		})
	}
}

func TestBuildXrayConfig_ErrorsOnBogusSS(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"undecodable userinfo", "ss://!!@@1.2.3.4:8388"},
		{"undecodable payload", "ss://!!!"},
		{"userinfo without method:password", "ss://" + base64.StdEncoding.EncodeToString([]byte("nopassword")) + "@1.2.3.4:8388"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &proxycfg.ProxyConfig{Protocol: "ss", Server: "1.2.3.4", Port: 8388, Raw: tc.raw}
			b, err := BuildXrayConfig(cfg, 10801, true)
			if err == nil {
				t.Fatalf("expected error, got nil; rendered instead:\n%s", b)
			}
			if strings.Contains(string(b), `"method": "none"`) {
				t.Fatalf("bogus ss userinfo rendered as method=none:\n%s", b)
			}
		})
	}
}

func TestBuildXrayConfig_ErrorsOnIncompleteXHTTP(t *testing.T) {
	for _, tc := range []struct {
		name string
		qs   string
	}{
		{"missing path", "host=h.example&mode=auto"},
		{"missing host", "path=/x&mode=auto"},
		{"missing mode", "path=/x&host=h.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := "vless://uuid@1.2.3.4:443?type=xhttp&security=none&" + tc.qs
			cfg := &proxycfg.ProxyConfig{Protocol: "vless", Server: "1.2.3.4", Port: 443, Raw: raw}
			b, err := BuildXrayConfig(cfg, 10801, true)
			if err == nil {
				t.Fatalf("expected error for xhttp %s, got nil; rendered instead:\n%s", tc.name, b)
			}
		})
	}
}

func TestBuildXrayConfig_XHTTPCompleteParamsBuilds(t *testing.T) {
	raw := "vless://uuid@1.2.3.4:443?type=xhttp&security=none&path=/x&host=h.example&mode=auto"
	cfg := &proxycfg.ProxyConfig{Protocol: "vless", Server: "1.2.3.4", Port: 443, Raw: raw}
	b, err := BuildXrayConfig(cfg, 10801, true)
	if err != nil {
		t.Fatalf("BuildXrayConfig error for complete xhttp params: %v", err)
	}
	if !strings.Contains(string(b), `"network": "xhttp"`) {
		t.Fatalf("xhttp network missing from config:\n%s", b)
	}
}
