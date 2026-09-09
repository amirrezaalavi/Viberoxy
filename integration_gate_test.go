package main

import (
	"os"
	"os/exec"
	"testing"
)

func xrayIntegrationEnabled(value string) bool {
	return value == "1"
}

func requireXrayIntegration(t *testing.T) {
	t.Helper()
	if !xrayIntegrationEnabled(os.Getenv("VIBEROXY_INTEGRATION_TESTS")) {
		t.Skip("set VIBEROXY_INTEGRATION_TESTS=1 to run real-Xray integration tests")
	}
	if _, err := exec.LookPath("xray"); err != nil {
		t.Skip("xray not found in PATH")
	}
}

func TestXrayIntegrationEnabledRequiresExplicitOne(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{value: "", want: false},
		{value: "0", want: false},
		{value: "true", want: false},
		{value: "1", want: true},
	} {
		if got := xrayIntegrationEnabled(tc.value); got != tc.want {
			t.Errorf("xrayIntegrationEnabled(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}
