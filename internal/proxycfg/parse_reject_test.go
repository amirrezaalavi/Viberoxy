package proxycfg

import (
	"context"
	"encoding/base64"
	"log/slog"
	"strings"
	"testing"
)

// F-17: a link that parses but cannot build a working xray outbound must be
// rejected at parse time with a specific, distinguishable reason.

const testUUID = "109d47e4-4efe-45f8-9f63-52af26e1a5e2"

func vmessLink(t *testing.T, fields map[string]interface{}) string {
	t.Helper()
	return "vmess://" + base64.StdEncoding.EncodeToString(mustMarshal(fields))
}

func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// logRecord is one captured slog record.
type logRecord struct {
	msg   string
	attrs map[string]string
}

// recordHandler captures slog records (message + attrs) for assertions,
// independent of any log format's quoting/escaping rules.
type recordHandler struct {
	records *[]logRecord
}

func (h recordHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	*h.records = append(*h.records, rec)
	return nil
}

func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

// captureLogs redirects the default slog logger while fn runs and returns
// every record that was logged.
func captureLogs(t *testing.T, fn func()) []logRecord {
	t.Helper()
	var records []logRecord
	prev := slog.Default()
	slog.SetDefault(slog.New(recordHandler{records: &records}))
	defer slog.SetDefault(prev)
	fn()
	return records
}

// rejectReason asserts the link is rejected by both entry points with exactly
// the expected reason.
func rejectReason(t *testing.T, raw, want string) {
	t.Helper()
	if cfg := ParseSingle(raw); cfg != nil {
		t.Fatalf("ParseSingle(%q) = %+v, want nil", raw, cfg)
	}
	cfg, err := ParseSingleErr(raw)
	if cfg != nil {
		t.Fatalf("ParseSingleErr(%q) cfg = %+v, want nil", raw, cfg)
	}
	if err == nil {
		t.Fatalf("ParseSingleErr(%q) err = nil, want %q", raw, want)
	}
	if err.Error() != want {
		t.Errorf("ParseSingleErr(%q) reason = %q, want %q", raw, err.Error(), want)
	}
}

func TestParseRejectsMalformedLinksWithSpecificReasons(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		// vmess: base64 JSON decodes but lacks fields xray needs.
		{
			name: "vmess missing uuid",
			raw:  vmessLink(t, map[string]interface{}{"add": "1.2.3.4", "port": 443, "ps": "V"}),
			want: "vmess: missing uuid",
		},
		{
			name: "vmess missing server",
			raw:  vmessLink(t, map[string]interface{}{"id": testUUID, "port": 443, "ps": "V"}),
			want: "vmess: missing server",
		},
		{
			name: "vmess missing port",
			raw:  vmessLink(t, map[string]interface{}{"id": testUUID, "add": "1.2.3.4", "ps": "V"}),
			want: "vmess: missing port",
		},
		{
			name: "vmess invalid port",
			raw:  vmessLink(t, map[string]interface{}{"id": testUUID, "add": "1.2.3.4", "port": "not-a-port"}),
			want: "vmess: invalid port",
		},

		// ss: userinfo decodes but carries no usable method:password.
		{
			name: "ss userinfo without colon",
			raw:  "ss://" + b64("nocolon") + "@1.2.3.4:12345#S",
			want: "ss: missing method:password",
		},
		{
			name: "ss empty password",
			raw:  "ss://" + b64("aes-128-gcm:") + "@1.2.3.4:12345#S",
			want: "ss: missing method:password",
		},
		{
			name: "ss legacy userinfo without colon",
			raw:  "ss://" + b64("nocolon@1.2.3.4:12345") + "#S",
			want: "ss: missing method:password",
		},

		// vless/trojan: type=xhttp needs path, host and mode to build an
		// xhttp outbound.
		{
			name: "vless xhttp missing path",
			raw:  "vless://" + testUUID + "@1.2.3.4:12345?encryption=none&type=xhttp&host=example.com&mode=packet-up#V",
			want: "vless: xhttp missing path",
		},
		{
			name: "vless xhttp missing host",
			raw:  "vless://" + testUUID + "@1.2.3.4:12345?encryption=none&type=xhttp&path=%2Fx&mode=packet-up#V",
			want: "vless: xhttp missing host",
		},
		{
			name: "vless xhttp missing mode",
			raw:  "vless://" + testUUID + "@1.2.3.4:12345?encryption=none&type=xhttp&path=%2Fx&host=example.com#V",
			want: "vless: xhttp missing mode",
		},
		{
			name: "trojan xhttp missing path",
			raw:  "trojan://pw@1.2.3.4:443?type=xhttp&host=example.com&mode=auto#T",
			want: "trojan: xhttp missing path",
		},

		// unknown scheme
		{
			name: "unknown scheme",
			raw:  "unknownproxy://1.2.3.4:443#U",
			want: `unsupported scheme "unknownproxy"`,
		},
		{
			name: "no scheme at all",
			raw:  "not a share link",
			want: "missing scheme",
		},

		// missing server / port
		{
			name: "vless missing server",
			raw:  "vless://" + testUUID + "@:443?type=tcp#V",
			want: "vless: missing server",
		},
		{
			name: "vless missing port",
			raw:  "vless://" + testUUID + "@1.2.3.4?type=tcp#V",
			want: "vless: missing port",
		},
		{
			name: "trojan missing server",
			raw:  "trojan://pw@:443#T",
			want: "trojan: missing server",
		},
		{
			name: "ss missing server",
			raw:  "ss://" + b64("aes-128-gcm:pw") + "@:12345#S",
			want: "ss: missing server",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rejectReason(t, tc.raw, tc.want)
		})
	}
}

// Each malformation family must have its own reason: two different ways of
// being broken must never collapse into the same message.
func TestParseRejectReasonsAreDistinguishable(t *testing.T) {
	reasons := []string{}
	collect := func(raw string) {
		t.Helper()
		_, err := ParseSingleErr(raw)
		if err == nil {
			t.Fatalf("ParseSingleErr(%q) err = nil, want a rejection reason", raw)
		}
		reasons = append(reasons, err.Error())
	}

	collect(vmessLink(t, map[string]interface{}{"add": "1.2.3.4", "port": 443}))           // vmess uuid
	collect(vmessLink(t, map[string]interface{}{"id": testUUID, "port": 443}))             // vmess server
	collect(vmessLink(t, map[string]interface{}{"id": testUUID, "add": "1.2.3.4"}))        // vmess port
	collect("ss://" + b64("nocolon") + "@1.2.3.4:12345#S")                                 // ss userinfo
	collect("vless://" + testUUID + "@1.2.3.4:1?encryption=none&type=xhttp&host=h&mode=m") // xhttp path
	collect("vless://" + testUUID + "@1.2.3.4:1?encryption=none&type=xhttp&path=p&mode=m") // xhttp host
	collect("vless://" + testUUID + "@1.2.3.4:1?encryption=none&type=xhttp&path=p&host=h") // xhttp mode
	collect("weird://1.2.3.4:1")                                                           // unknown scheme
	collect("vless://" + testUUID + "@:443?type=tcp")                                      // missing server
	collect("vless://" + testUUID + "@1.2.3.4?type=tcp")                                   // missing port

	seen := map[string]bool{}
	for _, r := range reasons {
		if seen[r] {
			t.Errorf("duplicate rejection reason %q: reasons must be distinguishable per malformation", r)
		}
		seen[r] = true
	}
	if len(reasons) != 10 {
		t.Errorf("collected %d reasons, want 10", len(reasons))
	}
}

// A complete xhttp link must still parse: validation rejects only links that
// would build a broken outbound.
func TestParseAcceptsCompleteXhttpLinks(t *testing.T) {
	raw := "vless://" + testUUID + "@1.2.3.4:12345?encryption=none&type=xhttp&path=%2Fx&host=example.com&mode=packet-up#V"
	cfg, err := ParseSingleErr(raw)
	if err != nil {
		t.Fatalf("ParseSingleErr(%q) err = %v, want nil", raw, err)
	}
	if cfg == nil {
		t.Fatalf("ParseSingleErr(%q) cfg = nil, want config", raw)
	}

	tra := "trojan://pw@1.2.3.4:443?type=xhttp&path=%2Fx&host=example.com&mode=auto#T"
	if cfg := ParseSingle(tra); cfg == nil {
		t.Fatalf("ParseSingle(%q) = nil, want config", tra)
	}
}

// Mixed subscription: exactly the valid configs survive, and each rejected
// config is logged with its own reason exactly once (no spam, no silence).
func TestParseConfigsMixedSubscriptionKeepsValidAndLogsEachRejectionOnce(t *testing.T) {
	validVMess := vmessLink(t, map[string]interface{}{
		"add": "5.6.7.8", "port": 443, "id": testUUID, "ps": "V",
	})
	badVMess := vmessLink(t, map[string]interface{}{"add": "1.1.1.1", "port": 443, "ps": "NoUUID"})
	badScheme := "gnarlyproxy://2.2.2.2:1#G"

	body := strings.Join([]string{
		"ss://YWVzLTEyOC1nY206cGFzc3dvcmQ=@1.2.3.4:12345#A",
		validVMess,
		"trojan://pw@9.10.11.12:443#C",
		badVMess,
		badScheme,
	}, "\n") + "\n"

	var configs []*ProxyConfig
	records := captureLogs(t, func() {
		configs = ParseConfigs(body)
	})

	wantServers := []string{"1.2.3.4", "5.6.7.8", "9.10.11.12"}
	if len(configs) != len(wantServers) {
		t.Fatalf("ParseConfigs returned %d configs, want %d", len(configs), len(wantServers))
	}
	for i, want := range wantServers {
		if configs[i].Server != want {
			t.Errorf("configs[%d].Server = %q, want %q", i, configs[i].Server, want)
		}
	}

	// Exactly two rejection records: one per bad config, logged once each.
	const skipMsg = "skipping invalid config"
	var reasons []string
	for _, rec := range records {
		if rec.msg == skipMsg {
			reasons = append(reasons, rec.attrs["reason"])
		}
	}
	if len(reasons) != 2 {
		t.Fatalf("logged %d rejection records, want exactly 2 (one per bad config); records: %+v", len(reasons), records)
	}
	wantReasons := []string{"vmess: missing uuid", `unsupported scheme "gnarlyproxy"`}
	for _, want := range wantReasons {
		n := 0
		for _, got := range reasons {
			if got == want {
				n++
			}
		}
		if n != 1 {
			t.Errorf("reason %q logged %d times, want exactly 1; got reasons %v", want, n, reasons)
		}
	}
}
