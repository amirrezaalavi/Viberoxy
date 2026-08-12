package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRouterAllProxyMode(t *testing.T) {
	r := &Router{Mode: RouteAllProxy}
	cases := []struct {
		host string
		want Route
	}{
		{"example.com", RouteWAN},
		{"ir", RouteWAN},
		{"1.2.3.4", RouteWAN},
		{"", RouteWAN},
	}
	for _, c := range cases {
		if got := r.Decide(c.host); got != c.want {
			t.Errorf("Decide(%q) = %v, want %v", c.host, got, c.want)
		}
	}
}

func TestRouterProxyDefaultDirectList(t *testing.T) {
	r := &Router{
		Mode:            RouteProxyDefault,
		directSuffixes:  []string{".ir", ".example.com", "plain.local"},
	}
	cases := []struct {
		host string
		want Route
	}{
		{"example.com", RouteDirect},   // exact match
		{"sub.example.com", RouteDirect}, // suffix match
		{"a.example.com.evil.com", RouteWAN}, // not a suffix match
		{"somedomain.ir", RouteDirect}, // TLD suffix
		{"ir", RouteDirect},            // bare suffix entry matches itself
		{"plain.local", RouteDirect},   // no dot: exact match only
		{"x.plain.local", RouteWAN},    // no dot entry: subdomains NOT matched
		{"google.com", RouteWAN},       // default proxy
		{"1.2.3.4", RouteWAN},          // IP literal not in list
	}
	for _, c := range cases {
		if got := r.Decide(c.host); got != c.want {
			t.Errorf("Decide(%q) = %v, want %v", c.host, got, c.want)
		}
	}
}

func TestRouterDirectDefaultProxyList(t *testing.T) {
	r := &Router{
		Mode:           RouteDirectDefault,
		proxySuffixes:  []string{".google.com", ".youtube.com"},
	}
	cases := []struct {
		host string
		want Route
	}{
		{"google.com", RouteWAN},
		{"www.youtube.com", RouteWAN},
		{"github.com", RouteDirect}, // default direct
		{"somedomain.ir", RouteDirect},
		{"1.2.3.4", RouteDirect},
	}
	for _, c := range cases {
		if got := r.Decide(c.host); got != c.want {
			t.Errorf("Decide(%q) = %v, want %v", c.host, got, c.want)
		}
	}
}

func TestRouterProxyDefaultDirectListWinsOnConflict(t *testing.T) {
	// In proxy-default mode, the direct list is the explicit override; if a
	// host is in both lists, direct wins.
	r := &Router{
		Mode:           RouteProxyDefault,
		directSuffixes: []string{".example.com"},
		proxySuffixes:  []string{".example.com"},
	}
	if got := r.Decide("www.example.com"); got != RouteDirect {
		t.Errorf("Decide = %v, want RouteDirect (explicit direct overrides)", got)
	}
}

func TestRouterCaseInsensitive(t *testing.T) {
	r := &Router{
		Mode:           RouteProxyDefault,
		directSuffixes: []string{".IR"},
	}
	for _, h := range []string{"Example.IR", "example.ir", "SUB.Example.IR", "Example.IR."} {
		if got := r.Decide(h); got != RouteDirect {
			t.Errorf("Decide(%q) = %v, want RouteDirect", h, got)
		}
	}
}

func TestRouterTrailingDot(t *testing.T) {
	r := &Router{
		Mode:           RouteProxyDefault,
		directSuffixes: []string{".example.com"},
	}
	if got := r.Decide("example.com."); got != RouteDirect {
		t.Errorf("Decide(example.com.) = %v, want RouteDirect (trailing dot normalized)", got)
	}
}

func TestParseSuffixList(t *testing.T) {
	got := parseSuffixList(" .ir ,  example.com ,, plain.local ")
	want := []string{".ir", ".example.com", ".plain.local"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("item %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseSuffixListEmpty(t *testing.T) {
	if got := parseSuffixList(""); len(got) != 0 {
		t.Errorf("empty list = %v, want empty", got)
	}
	if got := parseSuffixList("  , , "); len(got) != 0 {
		t.Errorf("blank list = %v, want empty", got)
	}
}

func TestLoadSuffixFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "direct.txt")
	content := "# country-local domains\n.ir\n\n# comment again\nexample.com\n  plain.local  \n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	got, err := loadSuffixFile(path)
	if err != nil {
		t.Fatalf("loadSuffixFile: %v", err)
	}
	want := []string{".ir", ".example.com", ".plain.local"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("item %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestLoadSuffixFileMissing(t *testing.T) {
	if _, err := loadSuffixFile("/nonexistent/nope.txt"); err == nil {
		t.Error("expected error for missing file")
	}
}

func TestRouteString(t *testing.T) {
	if RouteWAN.String() != "wan" {
		t.Errorf("RouteWAN.String() = %q, want wan", RouteWAN.String())
	}
	if RouteDirect.String() != "direct" {
		t.Errorf("RouteDirect.String() = %q, want direct", RouteDirect.String())
	}
	if Route(99).String() != "unknown" {
		t.Errorf("unknown route string = %q, want unknown", Route(99).String())
	}
}
