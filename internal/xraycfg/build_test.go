package xraycfg

import "testing"

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
	ss := buildStreamSettings("tcp", "", "", "", "", "", "", "", "", "", "")
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
	ss := buildStreamSettings("xhttp", "", "", "", "", "", "", "", "", "", "")
	if ss == nil {
		t.Fatal("expected stream settings")
	}
	if ss.Network != "xhttp" {
		t.Errorf("network = %q, want xhttp", ss.Network)
	}
}
