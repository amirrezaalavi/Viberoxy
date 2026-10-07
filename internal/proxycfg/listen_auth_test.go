package proxycfg

import (
	"strings"
	"testing"
)

// F-14: LISTEN_ADDR / PROXY_USERS / API_TOKEN / ALLOW_PUBLIC /
// ALLOW_PRIVATE_TARGETS parsing and the non-loopback bind gate.

func clearAuthEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"LISTEN_ADDR",
		"PROXY_USERS",
		"API_TOKEN",
		"ALLOW_PUBLIC",
		"ALLOW_PRIVATE_TARGETS",
	} {
		unsetenv(t, key)
	}
	setenv(t, "SUBSCRIBER_URL", "https://example.com/sub")
}

func TestParseConfig_NonLoopbackListenRefusedWithoutAuth(t *testing.T) {
	for _, addr := range []string{"0.0.0.0", "::", "192.168.1.10", "10.1.2.3", "169.254.1.1"} {
		t.Run("refused "+addr, func(t *testing.T) {
			clearAuthEnv(t)
			setenv(t, "LISTEN_ADDR", addr)
			_, err := ParseConfig()
			if err == nil {
				t.Fatalf("LISTEN_ADDR=%s parsed without auth, want refusal", addr)
			}
			if !strings.Contains(err.Error(), "LISTEN_ADDR") {
				t.Errorf("error = %v, want it to name LISTEN_ADDR", err)
			}
		})
	}

	// ALLOW_PUBLIC=false explicitly does not open the gate.
	t.Run("explicit false", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "LISTEN_ADDR", "0.0.0.0")
		setenv(t, "ALLOW_PUBLIC", "false")
		if _, err := ParseConfig(); err == nil {
			t.Fatal("ALLOW_PUBLIC=false: want refusal")
		}
	})

	// Loopback addresses always parse, with or without auth configured.
	for _, addr := range []string{"127.0.0.1", "127.0.0.53", "::1"} {
		t.Run("loopback "+addr, func(t *testing.T) {
			clearAuthEnv(t)
			setenv(t, "LISTEN_ADDR", addr)
			cfg, err := ParseConfig()
			if err != nil {
				t.Fatalf("ParseConfig() error: %v", err)
			}
			if cfg.ListenAddr != addr {
				t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, addr)
			}
		})
	}

	// Each authentication opt-in opens the gate.
	t.Run("PROXY_USERS opts in", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "LISTEN_ADDR", "0.0.0.0")
		setenv(t, "PROXY_USERS", "alice:s3cret")
		cfg, err := ParseConfig()
		if err != nil {
			t.Fatalf("ParseConfig() error: %v", err)
		}
		if cfg.ListenAddr != "0.0.0.0" {
			t.Errorf("ListenAddr = %q, want 0.0.0.0", cfg.ListenAddr)
		}
		if len(cfg.ProxyUsers) != 1 || cfg.ProxyUsers[0].User != "alice" {
			t.Errorf("ProxyUsers = %+v, want one alice entry", cfg.ProxyUsers)
		}
	})
	t.Run("API_TOKEN opts in", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "LISTEN_ADDR", "0.0.0.0")
		setenv(t, "API_TOKEN", "sekret")
		if _, err := ParseConfig(); err != nil {
			t.Fatalf("ParseConfig() error: %v", err)
		}
	})
	t.Run("ALLOW_PUBLIC opts in", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "LISTEN_ADDR", "0.0.0.0")
		setenv(t, "ALLOW_PUBLIC", "true")
		if _, err := ParseConfig(); err != nil {
			t.Fatalf("ParseConfig() error: %v", err)
		}
	})
}

func TestParseConfig_InvalidListenAddr(t *testing.T) {
	clearAuthEnv(t)
	setenv(t, "LISTEN_ADDR", "localhost")
	_, err := ParseConfig()
	if err == nil {
		t.Fatal("LISTEN_ADDR=localhost parsed, want hard-exit error")
	}
	if !strings.Contains(err.Error(), "LISTEN_ADDR") {
		t.Errorf("error = %v, want it to name LISTEN_ADDR", err)
	}
}

func TestParseConfig_ProxyUsers(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "PROXY_USERS", "alice:s3cret,bob:other:with:colons")
		cfg, err := ParseConfig()
		if err != nil {
			t.Fatalf("ParseConfig() error: %v", err)
		}
		if len(cfg.ProxyUsers) != 2 {
			t.Fatalf("ProxyUsers = %+v, want 2 entries", cfg.ProxyUsers)
		}
		if cfg.ProxyUsers[0].User != "alice" || cfg.ProxyUsers[0].Pass != "s3cret" {
			t.Errorf("users[0] = %+v, want alice/s3cret", cfg.ProxyUsers[0])
		}
		if cfg.ProxyUsers[1].Pass != "other:with:colons" {
			t.Errorf("users[1].Pass = %q, want colons preserved", cfg.ProxyUsers[1].Pass)
		}
	})

	for _, bad := range []string{"nocolon", ":pw", "alice:"} {
		t.Run("reject "+bad, func(t *testing.T) {
			clearAuthEnv(t)
			setenv(t, "PROXY_USERS", bad)
			_, err := ParseConfig()
			if err == nil {
				t.Fatalf("PROXY_USERS=%q parsed, want error", bad)
			}
			if !strings.Contains(err.Error(), "PROXY_USERS") {
				t.Errorf("error = %v, want it to name PROXY_USERS", err)
			}
		})
	}

	// The hard-exit error must never echo password material.
	t.Run("error does not leak secrets", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "PROXY_USERS", "s3cr3t-value-no-colon")
		_, err := ParseConfig()
		if err == nil {
			t.Fatal("want error")
		}
		if strings.Contains(err.Error(), "s3cr3t-value-no-colon") {
			t.Errorf("error %q leaks PROXY_USERS value", err)
		}
	})
}

func TestParseConfig_APIToken(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "API_TOKEN", "sekret-token")
		cfg, err := ParseConfig()
		if err != nil {
			t.Fatalf("ParseConfig() error: %v", err)
		}
		if cfg.APIToken != "sekret-token" {
			t.Errorf("APIToken = %q, want sekret-token", cfg.APIToken)
		}
	})

	t.Run("whitespace rejected without leaking", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "API_TOKEN", "has space")
		_, err := ParseConfig()
		if err == nil {
			t.Fatal("want error for whitespace in API_TOKEN")
		}
		if !strings.Contains(err.Error(), "API_TOKEN") {
			t.Errorf("error = %v, want it to name API_TOKEN", err)
		}
		if strings.Contains(err.Error(), "has space") {
			t.Errorf("error %q leaks the token value", err)
		}
	})
}

func TestParseConfig_AllowFlags(t *testing.T) {
	t.Run("valid values", func(t *testing.T) {
		clearAuthEnv(t)
		setenv(t, "ALLOW_PUBLIC", "true")
		setenv(t, "ALLOW_PRIVATE_TARGETS", "true")
		cfg, err := ParseConfig()
		if err != nil {
			t.Fatalf("ParseConfig() error: %v", err)
		}
		if !cfg.AllowPublic {
			t.Error("AllowPublic = false, want true")
		}
		if !cfg.AllowPrivateTargets {
			t.Error("AllowPrivateTargets = false, want true")
		}
	})

	for _, key := range []string{"ALLOW_PUBLIC", "ALLOW_PRIVATE_TARGETS"} {
		t.Run(key+" invalid value", func(t *testing.T) {
			clearAuthEnv(t)
			setenv(t, key, "bogus")
			_, err := ParseConfig()
			if err == nil {
				t.Fatalf("%s=bogus parsed, want error", key)
			}
			if !strings.Contains(err.Error(), key) {
				t.Errorf("error = %v, want it to name %s", err, key)
			}
		})
	}
}
