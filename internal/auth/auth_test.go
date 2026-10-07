package auth

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func unsetenv(t *testing.T, key string) {
	t.Helper()
	orig, ok := os.LookupEnv(key)
	os.Unsetenv(key)
	t.Cleanup(func() {
		if ok {
			os.Setenv(key, orig)
		}
	})
}

func TestParseUsers(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []UserCred
		wantErr bool
	}{
		{name: "empty", in: "", want: nil},
		{name: "whitespace only", in: "   ", want: nil},
		{name: "single", in: "alice:s3cret", want: []UserCred{{User: "alice", Pass: "s3cret"}}},
		{name: "multiple", in: "alice:one,bob:two", want: []UserCred{{User: "alice", Pass: "one"}, {User: "bob", Pass: "two"}}},
		{name: "spaces around entries", in: " alice:one , bob:two ", want: []UserCred{{User: "alice", Pass: "one"}, {User: "bob", Pass: "two"}}},
		{name: "colon in password", in: "alice:pa:ss", want: []UserCred{{User: "alice", Pass: "pa:ss"}}},
		{name: "missing colon", in: "alice", wantErr: true},
		{name: "empty username", in: ":pw", wantErr: true},
		{name: "empty password", in: "alice:", wantErr: true},
		{name: "empty entry between commas", in: "alice:one,,bob:two", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUsers(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseUsers(%q) = %v, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseUsers(%q) error: %v", tt.in, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ParseUsers(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("users[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// A malformed PROXY_USERS entry must never echo the secret material back in
// the error: the error text is logged at startup.
func TestParseUsers_ErrorDoesNotLeakSecrets(t *testing.T) {
	for _, secret := range []string{"s3cr3t-no-colon", "hunter2"} {
		_, err := ParseUsers(secret)
		if err == nil {
			t.Fatalf("ParseUsers(%q) = nil error, want error", secret)
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error %q leaks the entry value", err)
		}
	}
	// An entry that has a colon but an empty password must not echo the user
	// entry either.
	_, err := ParseUsers("alice:")
	if err == nil {
		t.Fatal("ParseUsers(alice:) = nil error, want error")
	}
	if strings.Contains(err.Error(), "alice:") {
		t.Errorf("error %q echoes the raw entry", err)
	}
}

func TestCheckUsers(t *testing.T) {
	users := []UserCred{{User: "alice", Pass: "s3cret"}, {User: "bob", Pass: "other"}}
	tests := []struct {
		name string
		user string
		pass string
		want bool
	}{
		{name: "first user", user: "alice", pass: "s3cret", want: true},
		{name: "second user", user: "bob", pass: "other", want: true},
		{name: "wrong password", user: "alice", pass: "wrong", want: false},
		{name: "wrong user", user: "mallory", pass: "s3cret", want: false},
		{name: "empty", user: "", pass: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckUsers(users, tt.user, tt.pass); got != tt.want {
				t.Errorf("CheckUsers(%q, %q) = %v, want %v", tt.user, tt.pass, got, tt.want)
			}
		})
	}
	if CheckUsers(nil, "alice", "s3cret") {
		t.Error("CheckUsers with no configured users must fail")
	}
}

func TestParseBasic(t *testing.T) {
	encode := func(s string) string { return "Basic " + base64.StdEncoding.EncodeToString([]byte(s)) }

	user, pass, ok := ParseBasic(encode("alice:s3cret"))
	if !ok || user != "alice" || pass != "s3cret" {
		t.Errorf("ParseBasic(valid) = (%q, %q, %v), want (alice, s3cret, true)", user, pass, ok)
	}

	user, pass, ok = ParseBasic(encode("alice:pa:ss"))
	if !ok || user != "alice" || pass != "pa:ss" {
		t.Errorf("ParseBasic(colon pass) = (%q, %q, %v), want (alice, pa:ss, true)", user, pass, ok)
	}

	// Scheme is case-insensitive per RFC 7235.
	if _, _, ok := ParseBasic("basic " + base64.StdEncoding.EncodeToString([]byte("a:b"))); !ok {
		t.Error("ParseBasic: lowercase scheme rejected")
	}

	for _, bad := range []string{
		"",
		"Bearer dXNlcjpwYXNz",
		"Basic !!!not-base64!!!",
		"Basic " + base64.StdEncoding.EncodeToString([]byte("nocolon")),
		"Basic",
	} {
		if _, _, ok := ParseBasic(bad); ok {
			t.Errorf("ParseBasic(%q) accepted, want reject", bad)
		}
	}
}

func TestCheckBearer(t *testing.T) {
	tests := []struct {
		header string
		token  string
		want   bool
	}{
		{header: "Bearer sekret", token: "sekret", want: true},
		{header: "bearer sekret", token: "sekret", want: true},
		{header: "Bearer wrong", token: "sekret", want: false},
		{header: "Bearer ", token: "sekret", want: false},
		{header: "", token: "sekret", want: false},
		{header: "sekret", token: "sekret", want: false},
		{header: "Bearer sekret", token: "", want: false},
		{header: "Basic sekret", token: "sekret", want: false},
	}
	for _, tt := range tests {
		if got := CheckBearer(tt.header, tt.token); got != tt.want {
			t.Errorf("CheckBearer(%q, %q) = %v, want %v", tt.header, tt.token, got, tt.want)
		}
	}
}

func TestUsersFromEnv(t *testing.T) {
	unsetenv(t, "PROXY_USERS")

	users, required, err := UsersFromEnv()
	if err != nil || required || users != nil {
		t.Fatalf("unset: users=%v required=%v err=%v, want nil,false,nil", users, required, err)
	}

	t.Setenv("PROXY_USERS", "alice:s3cret,bob:pw")
	users, required, err = UsersFromEnv()
	if err != nil || !required || len(users) != 2 {
		t.Fatalf("valid: users=%v required=%v err=%v, want 2 users, required", users, required, err)
	}

	// Malformed value: still REQUIRED (fail closed) but with no usable users.
	t.Setenv("PROXY_USERS", "no-colon-here")
	users, required, err = UsersFromEnv()
	if err == nil || !required || users != nil {
		t.Fatalf("malformed: users=%v required=%v err=%v, want nil users, required, error", users, required, err)
	}
}

func TestTokenFromEnv(t *testing.T) {
	unsetenv(t, "API_TOKEN")
	if got := TokenFromEnv(); got != "" {
		t.Errorf("TokenFromEnv() = %q, want empty", got)
	}
	t.Setenv("API_TOKEN", "sekret")
	if got := TokenFromEnv(); got != "sekret" {
		t.Errorf("TokenFromEnv() = %q, want sekret", got)
	}
}

func TestAllowPrivateTargetsFromEnv(t *testing.T) {
	unsetenv(t, "ALLOW_PRIVATE_TARGETS")
	if AllowPrivateTargetsFromEnv() {
		t.Error("AllowPrivateTargetsFromEnv() = true unset, want false (blocked by default)")
	}
	for _, v := range []string{"true", "TRUE", "1"} {
		t.Setenv("ALLOW_PRIVATE_TARGETS", v)
		if !AllowPrivateTargetsFromEnv() {
			t.Errorf("ALLOW_PRIVATE_TARGETS=%q: want true", v)
		}
	}
	for _, v := range []string{"false", "0", "bogus"} {
		t.Setenv("ALLOW_PRIVATE_TARGETS", v)
		if AllowPrivateTargetsFromEnv() {
			t.Errorf("ALLOW_PRIVATE_TARGETS=%q: want false", v)
		}
	}
}

func TestBindAllowed(t *testing.T) {
	tests := []struct {
		users, token, allowPublic, want bool
	}{
		{want: false},
		{users: true, want: true},
		{token: true, want: true},
		{allowPublic: true, want: true},
	}
	for _, tt := range tests {
		if got := BindAllowed(tt.users, tt.token, tt.allowPublic); got != tt.want {
			t.Errorf("BindAllowed(%v,%v,%v) = %v, want %v", tt.users, tt.token, tt.allowPublic, got, tt.want)
		}
	}
}

func TestEnsureBindAllowed(t *testing.T) {
	unsetenv(t, "PROXY_USERS")
	unsetenv(t, "API_TOKEN")
	unsetenv(t, "ALLOW_PUBLIC")

	// Loopback addresses are always fine.
	for _, addr := range []string{"127.0.0.1", "127.0.0.53", "::1", "localhost"} {
		if err := EnsureBindAllowed(addr); err != nil {
			t.Errorf("EnsureBindAllowed(%q) = %v, want nil", addr, err)
		}
	}

	// Non-loopback without any opt-in: refused.
	for _, addr := range []string{"0.0.0.0", "::", "192.168.1.10", "10.0.0.1", ""} {
		if err := EnsureBindAllowed(addr); err == nil {
			t.Errorf("EnsureBindAllowed(%q) = nil, want refusal", addr)
		}
	}

	// Each opt-in opens the gate.
	t.Setenv("ALLOW_PUBLIC", "true")
	if err := EnsureBindAllowed("0.0.0.0"); err != nil {
		t.Errorf("ALLOW_PUBLIC=true: EnsureBindAllowed = %v, want nil", err)
	}
	t.Setenv("ALLOW_PUBLIC", "false")
	if err := EnsureBindAllowed("0.0.0.0"); err == nil {
		t.Error("ALLOW_PUBLIC=false: want refusal")
	}
	t.Setenv("ALLOW_PUBLIC", "")
	t.Setenv("PROXY_USERS", "alice:s3cret")
	if err := EnsureBindAllowed("0.0.0.0"); err != nil {
		t.Errorf("PROXY_USERS set: EnsureBindAllowed = %v, want nil", err)
	}
	t.Setenv("PROXY_USERS", "")
	t.Setenv("API_TOKEN", "sekret")
	if err := EnsureBindAllowed("0.0.0.0"); err != nil {
		t.Errorf("API_TOKEN set: EnsureBindAllowed = %v, want nil", err)
	}
}

func TestBindAllowedFromEnv(t *testing.T) {
	unsetenv(t, "PROXY_USERS")
	unsetenv(t, "API_TOKEN")
	unsetenv(t, "ALLOW_PUBLIC")
	if BindAllowedFromEnv() {
		t.Error("no opt-ins: BindAllowedFromEnv = true, want false")
	}
	t.Setenv("ALLOW_PUBLIC", "true")
	if !BindAllowedFromEnv() {
		t.Error("ALLOW_PUBLIC=true: BindAllowedFromEnv = false, want true")
	}
	t.Setenv("ALLOW_PUBLIC", "nope") // unparsable: fail closed
	if BindAllowedFromEnv() {
		t.Error("ALLOW_PUBLIC=nope: BindAllowedFromEnv = true, want false")
	}
	t.Setenv("ALLOW_PUBLIC", "")
	t.Setenv("PROXY_USERS", "alice:s3cret")
	if !BindAllowedFromEnv() {
		t.Error("PROXY_USERS set: BindAllowedFromEnv = false, want true")
	}
}

func TestTargetAllowed(t *testing.T) {
	tests := []struct {
		hostport     string
		allowPrivate bool
		want         bool
	}{
		// Loopback.
		{"127.0.0.1:8080", false, false},
		{"127.0.0.1:8080", true, true},
		{"127.53.1.9:80", false, false},
		{"[::1]:443", false, false},
		{"localhost:80", false, false},
		{"LOCALHOST:80", false, false},
		{"foo.localhost:80", false, false},
		// Private ranges.
		{"10.1.2.3:443", false, false},
		{"172.16.0.1:443", false, false},
		{"192.168.1.1:443", false, false},
		{"[fd00::1]:443", false, false},
		// Link-local (cloud metadata endpoint).
		{"169.254.169.254:80", false, false},
		{"[fe80::1]:443", false, false},
		// Unspecified.
		{"0.0.0.0:80", false, false},
		{"[::]:80", false, false},
		// IPv4 shorthand / alternate literal forms that still parse to a
		// loopback or private address at dial time.
		{"127.1:80", false, false},
		{"2130706433:80", false, false}, // decimal 127.0.0.1
		{"0177.0.0.1:80", false, false}, // octal 127.0.0.1
		{"0x7f000001:80", false, false}, // hex 127.0.0.1
		{"0x7f.0.0.1:80", false, false}, // hex octet form 127.0.0.1
		// Public destinations pass in both modes.
		{"1.2.3.4:443", false, true},
		{"1.2.3.4:443", true, true},
		{"example.com:443", false, true},
		{"example.com:443", true, true},
		// No port (authority-form default port): still evaluated.
		{"127.0.0.1", false, false},
		{"example.com", false, true},
	}
	for _, tt := range tests {
		if got := TargetAllowed(tt.hostport, tt.allowPrivate); got != tt.want {
			t.Errorf("TargetAllowed(%q, allowPrivate=%v) = %v, want %v", tt.hostport, tt.allowPrivate, got, tt.want)
		}
	}
}

func TestRequireBearer(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Empty token: no gating at all.
	open := RequireBearer("", next)
	rec := httptest.NewRecorder()
	open.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("no token configured: status = %d, want 200", rec.Code)
	}

	gated := RequireBearer("sekret", next)
	// Missing header -> 403.
	rec = httptest.NewRecorder()
	gated.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("no header: status = %d, want 403", rec.Code)
	}
	if auth := rec.Header().Get("WWW-Authenticate"); !strings.Contains(auth, "Bearer") {
		t.Errorf("WWW-Authenticate = %q, want a Bearer challenge", auth)
	}
	// Wrong token -> 403.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	gated.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("wrong token: status = %d, want 403", rec.Code)
	}
	// Correct token -> 200.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer sekret")
	gated.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("correct token: status = %d, want 200", rec.Code)
	}
}
