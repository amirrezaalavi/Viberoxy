package xraycfg

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"viberoxy/internal/proxycfg"
)

// updateGolden rewrites testdata/*.json. Run:
//
//	go test ./internal/xraycfg -run TestBuildXrayConfig_Golden -update
var updateGolden = flag.Bool("update", false, "rewrite xraycfg golden files")

// vmess sharelink payload for the vmess+ws golden case (fixed JSON keeps the
// golden stable byte for byte).
const vmessWSJSON = `{"add":"1.2.3.4","port":443,"id":"109d47e4-4efe-45f8-9f63-52af26e1a5e2","aid":"0","net":"ws","type":"none","path":"/ws","host":"h.example","tls":"tls","sni":"sni.example.com"}`

// T-XRAY-01: one golden JSON per protocol x transport, asserting structure,
// the no-mux-with-flow rule (F-11 / RT-05) and mux-flag parity: for every
// case the mux ON and mux OFF renders must be byte-identical whenever mux is
// suppressed, so test and production instances — which only differ in the
// flag they pass — cannot diverge.
func TestBuildXrayConfig_Golden(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		protocol string // expected outbound protocol
		network  string // expected streamSettings.network ("" = no stream settings)
		wantMux  bool   // mux present (and enabled, concurrency 8) iff true
	}{
		{
			name:     "ss-tcp",
			raw:      "ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#MySS",
			protocol: "shadowsocks",
			network:  "",
			wantMux:  true,
		},
		{
			name:     "vmess-ws",
			raw:      "vmess://" + base64.StdEncoding.EncodeToString([]byte(vmessWSJSON)) + "#MyVMessWS",
			protocol: "vmess",
			network:  "ws",
			wantMux:  true,
		},
		{
			name:     "vless-ws",
			raw:      "vless://109d47e4-4efe-45f8-9f63-52af26e1a5e2@1.2.3.4:443?encryption=none&type=ws&path=/ws&host=h.example&security=tls&sni=sni.example.com#VLessWS",
			protocol: "vless",
			network:  "ws",
			wantMux:  true,
		},
		{
			name:     "vless-grpc",
			raw:      "vless://109d47e4-4efe-45f8-9f63-52af26e1a5e2@1.2.3.4:443?encryption=none&type=grpc&path=mysvc&security=tls&sni=sni.example.com#VLessGRPC",
			protocol: "vless",
			network:  "grpc",
			wantMux:  true,
		},
		{
			name:     "vless-tcp",
			raw:      "vless://109d47e4-4efe-45f8-9f63-52af26e1a5e2@1.2.3.4:443?encryption=none&type=tcp&path=/&host=h.example&security=tls&sni=sni.example.com&fp=chrome&alpn=h2,http/1.1#VLessTCP",
			protocol: "vless",
			network:  "tcp",
			wantMux:  true,
		},
		{
			name:     "vless-reality-vision",
			raw:      "vless://109d47e4-4efe-45f8-9f63-52af26e1a5e2@1.2.3.4:443?encryption=none&flow=xtls-rprx-vision&security=reality&type=tcp&sni=example.com&fp=chrome&pbk=abc123&sid=01&spx=%2F#VLessRealityVision",
			protocol: "vless",
			network:  "tcp",
			wantMux:  false,
		},
		{
			name:     "trojan-ws",
			raw:      "trojan://password123@1.2.3.4:443?security=tls&type=ws&path=/ws&host=h.example&sni=sni.example.com#TrojanWS",
			protocol: "trojan",
			network:  "ws",
			wantMux:  true,
		},
		{
			name:     "socks5",
			raw:      "socks5://user:pass@1.2.3.4:1080#MySocks",
			protocol: "socks",
			network:  "",
			wantMux:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := proxycfg.ParseSingle(tc.raw)
			if cfg == nil {
				t.Fatalf("ParseSingle(%q) returned nil", tc.raw)
			}

			on, err := BuildXrayConfig(cfg, 10800, true)
			if err != nil {
				t.Fatalf("BuildXrayConfig(mux on) error: %v", err)
			}
			off, err := BuildXrayConfig(cfg, 10800, false)
			if err != nil {
				t.Fatalf("BuildXrayConfig(mux off) error: %v", err)
			}

			assertGolden(t, tc.name+".mux-on", on)
			assertGolden(t, tc.name+".mux-off", off)

			// Structure.
			var xc XrayConfig
			if err := json.Unmarshal(on, &xc); err != nil {
				t.Fatalf("invalid JSON: %v\n%s", err, on)
			}
			if len(xc.Inbounds) != 1 || xc.Inbounds[0].Port != 10800 ||
				xc.Inbounds[0].Listen != "127.0.0.1" || xc.Inbounds[0].Protocol != "socks" {
				t.Errorf("inbounds = %+v, want one 127.0.0.1:10800 socks inbound", xc.Inbounds)
			}
			if len(xc.Outbounds) != 1 {
				t.Fatalf("outbounds = %d, want 1", len(xc.Outbounds))
			}
			ob := xc.Outbounds[0]
			if ob.Protocol != tc.protocol {
				t.Errorf("outbound protocol = %q, want %q", ob.Protocol, tc.protocol)
			}
			if tc.network == "" {
				if ob.StreamSettings != nil {
					t.Errorf("streamSettings = %+v, want none for %q", ob.StreamSettings, tc.name)
				}
			} else {
				if ob.StreamSettings == nil {
					t.Fatalf("streamSettings missing, want network %q", tc.network)
				}
				if ob.StreamSettings.Network != tc.network {
					t.Errorf("network = %q, want %q", ob.StreamSettings.Network, tc.network)
				}
			}

			// Mux expectation (covers no-mux-with-flow for the vision case
			// and the socks5 plain-proxy rule).
			if tc.wantMux {
				if ob.Mux == nil || !ob.Mux.Enabled || ob.Mux.Concurrency != 8 {
					t.Errorf("mux = %+v, want enabled with concurrency 8", ob.Mux)
				}
			} else {
				if ob.Mux != nil {
					t.Errorf("mux = %+v, want absent (flow/socks5 suppression)", ob.Mux)
				}
				// Parity: when mux is suppressed the flag cannot leak in —
				// ON and OFF renders must be byte-identical, so a test
				// instance and a production instance always render the same
				// config for this sharelink.
				if !bytes.Equal(on, off) {
					t.Errorf("mux-suppressed config differs between mux on/off renders:\n--- mux on ---\n%s\n--- mux off ---\n%s", on, off)
				}
			}

			// Explicit F-11 property: a config carrying a flow must never
			// contain a mux entry, whatever the caller asked for.
			if strings.Contains(tc.raw, "flow=") {
				if bytes.Contains(on, []byte(`"mux"`)) {
					t.Errorf("mux present together with a flow:\n%s", on)
				}
			}
		})
	}
}

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name+".json")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run: go test ./internal/xraycfg -run TestBuildXrayConfig_Golden -update)", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("rendered config differs from golden %s:\n--- want ---\n%s\n--- got ---\n%s", path, want, got)
	}
}
