package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"viberoxy/internal/proxycfg"
)

// TestMeasureDirectExitIP measures the machine's own exit address against a
// local endpoint standing in for the IP-echo service.
func TestMeasureDirectExitIP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("203.0.113.7\n"))
	}))
	defer srv.Close()

	old := ProbeWANURL
	ProbeWANURL = srv.URL
	defer func() { ProbeWANURL = old }()

	if got := measureDirectExitIP(2 * time.Second); got != "203.0.113.7" {
		t.Errorf("measureDirectExitIP = %q, want %q", got, "203.0.113.7")
	}
}

// TestMeasureDirectExitIPUnavailable: an unreachable endpoint yields no
// address (the check then has nothing to compare against).
func TestMeasureDirectExitIPUnavailable(t *testing.T) {
	old := ProbeWANURL
	ProbeWANURL = "http://127.0.0.1:1/none"
	defer func() { ProbeWANURL = old }()

	if got := measureDirectExitIP(500 * time.Millisecond); got != "" {
		t.Errorf("measureDirectExitIP = %q, want empty", got)
	}
}

// TestCheckDirectEgress: a candidate egressing with this machine's own exit
// IP is reported; it is only REJECTED when REJECT_DIRECT_EXIT is on.
func TestCheckDirectEgress(t *testing.T) {
	oldDirect, oldReject := directExitIP, rejectDirectExit
	defer func() { directExitIP, rejectDirectExit = oldDirect, oldReject }()

	directExitIP = "198.51.100.9"
	rejectDirectExit = false

	matching := &TestResult{Config: &proxycfg.ProxyConfig{Server: "same.example"}, ExitIP: "198.51.100.9"}
	other := &TestResult{Config: &proxycfg.ProxyConfig{Server: "other.example"}, ExitIP: "203.0.113.5"}
	noExit := &TestResult{Config: &proxycfg.ProxyConfig{Server: "unknown.example"}}

	if checkDirectEgress("test", matching) {
		t.Error("advisory mode rejected the candidate; want the report only")
	}
	if checkDirectEgress("test", other) {
		t.Error("a different exit IP was rejected")
	}
	if checkDirectEgress("test", noExit) {
		t.Error("a candidate without an observed exit IP was rejected")
	}

	rejectDirectExit = true
	if !checkDirectEgress("test", matching) {
		t.Error("reject mode kept a candidate with this machine's own exit IP")
	}
	if checkDirectEgress("test", other) {
		t.Error("reject mode rejected a different exit IP")
	}

	// With no direct address measured there is nothing to compare against.
	directExitIP = ""
	if checkDirectEgress("test", matching) {
		t.Error("a candidate was rejected without a measured direct exit IP")
	}
}
