package affinity

import (
	"fmt"
	"testing"
	"viberoxy/internal/path"
)

func TestSiteKey(t *testing.T) {
	cases := []struct{ in, want string }{
		{"example.com:443", "example.com"},
		{"example.com", "example.com"},
		{"www.example.com:8443", "example.com"},
		{"EXAMPLE.COM.", "example.com"},
		{"a.b.example.com", "example.com"},
		// second-level exceptions take three labels
		{"bbc.co.uk", "bbc.co.uk"},
		{"news.bbc.co.uk", "bbc.co.uk"},
		{"x.example.co.uk", "example.co.uk"},
		{"api.example.com.au", "example.com.au"},
		{"x.example.co.jp", "example.co.jp"},
		{"x.example.com.br", "example.com.br"},
		{"x.example.co.kr", "example.co.kr"},
		{"x.example.com.tr", "example.com.tr"},
		{"x.example.com.cn", "example.com.cn"},
		{"x.example.co.za", "example.co.za"},
		// IP literals key on the IP, ports stripped
		{"1.2.3.4:80", "1.2.3.4"},
		{"1.2.3.4", "1.2.3.4"},
		{"[2001:db8::1]:443", "2001:db8::1"},
		{"[2001:DB8::1]", "2001:db8::1"},
		// single-label hosts stay themselves
		{"localhost:1080", "localhost"},
		{"", ""},
	}
	for _, c := range cases {
		if got := SiteKey(c.in); got != c.want {
			t.Errorf("SiteKey(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseNoAffinity(t *testing.T) {
	got := ParseNoAffinity(" Example.COM ,\t.internal.test;\n\n.Dotted.NET ")
	want := []string{"example.com", "internal.test", "dotted.net"}
	if len(got) != len(want) {
		t.Fatalf("ParseNoAffinity = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ParseNoAffinity[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if out := ParseNoAffinity("  , ; "); len(out) != 0 {
		t.Errorf("ParseNoAffinity(blank) = %q, want empty", out)
	}
	if out := NoAffinityFromEnv(); len(out) != 0 { // env unset in this test
		t.Errorf("NoAffinityFromEnv() with no env = %q, want empty", out)
	}
}

func TestSticky_NoAffinityDomains(t *testing.T) {
	t.Setenv("NO_AFFINITY_DOMAINS", "example.com, .internal.test")
	blocked := []string{
		"example.com:443",
		"www.example.com",
		"EXAMPLE.com",
		"deep.www.example.com",
		"internal.test:8080",
		"a.internal.test",
	}
	for _, h := range blocked {
		if Sticky(h) {
			t.Errorf("Sticky(%q) = true, want false (NO_AFFINITY_DOMAINS)", h)
		}
	}
	sticky := []string{"example.org", "notexample.com", "example.com.br", "other.test"}
	for _, h := range sticky {
		if !Sticky(h) {
			t.Errorf("Sticky(%q) = false, want true", h)
		}
	}
	if Sticky("") {
		t.Error("Sticky(\"\") = true, want false (nothing to key on)")
	}

	t.Setenv("NO_AFFINITY_DOMAINS", "")
	if !Sticky("example.com") {
		t.Error("Sticky with empty NO_AFFINITY_DOMAINS = false, want true")
	}
}

func mkPaths(n int) []*path.Path {
	paths := make([]*path.Path, n)
	for i := range paths {
		paths[i] = path.New(nil, nil, i, 0)
	}
	return paths
}

func TestPick_DeterministicAndBounded(t *testing.T) {
	paths := mkPaths(6)
	first := Pick("client-a", "example.com", paths)
	if first == nil {
		t.Fatal("Pick returned nil for a non-empty candidate set")
	}
	for i := 0; i < 100; i++ {
		if got := Pick("client-a", "example.com", paths); got != first {
			t.Fatalf("Pick is not deterministic: call %d = slot %d, want slot %d", i, got.Slot, first.Slot)
		}
	}
	if got := Pick("client-a", "example.com", nil); got != nil {
		t.Errorf("Pick over no candidates = %v, want nil", got)
	}
	if got := Pick("client-a", "example.com", paths[:1]); got != paths[0] {
		t.Errorf("Pick over one candidate = %v, want that candidate", got)
	}
	// Different keys spread over the paths instead of one winner.
	winner := map[int]int{}
	for i := 0; i < 500; i++ {
		p := Pick(fmt.Sprintf("client-%d", i), "example.com", paths)
		winner[p.Slot]++
	}
	if len(winner) < 3 {
		t.Errorf("500 distinct keys landed on only %d of %d paths: hash distribution is broken", len(winner), len(paths))
	}
}

// T-AFF-02 (minimal disruption): removing one of N paths from the HRW
// candidate set may only move the keys that were on THAT path — at most
// 1/N + epsilon of all keys. HRW's defining property: everyone else keeps
// their winner.
func TestTAFF02_RemovingOnePathMovesAtMostOneOverN(t *testing.T) {
	paths := mkPaths(8)
	const keyCount = 10000

	type key struct{ client, site string }
	keys := make([]key, 0, keyCount)
	for i := 0; i < keyCount; i++ {
		keys = append(keys, key{client: fmt.Sprintf("client-%d", i%100), site: fmt.Sprintf("site-%d.test", i/100%100)})
	}

	victim := paths[3]
	before := make([]int, keyCount)
	counts := make([]int, len(paths))
	for i, k := range keys {
		w := Pick(k.client, k.site, paths)
		before[i] = w.Slot
		counts[w.Slot]++
	}

	remaining := make([]*path.Path, 0, len(paths)-1)
	for _, p := range paths {
		if p != victim {
			remaining = append(remaining, p)
		}
	}
	moved := 0
	for i, k := range keys {
		after := Pick(k.client, k.site, remaining)
		if after.Slot != before[i] {
			moved++
			if before[i] != victim.Slot {
				t.Fatalf("key %d moved off slot %d although that path is still eligible (HRW invariance broken)", i, before[i])
			}
		}
	}
	if moved == 0 {
		t.Fatalf("removing path (slot %d) moved 0 of %d keys: it never owned any, distribution broken", victim.Slot, keyCount)
	}
	bound := float64(keyCount)/float64(len(paths)) + 0.03*float64(keyCount)
	if float64(moved) > bound {
		t.Errorf("removing 1 of %d paths moved %d of %d keys (%.3f), want <= %.0f (1/N + 3%%)",
			len(paths), moved, keyCount, float64(moved)/keyCount, bound)
	}
	if counts[victim.Slot] != moved {
		t.Errorf("moved=%d but victim owned %d keys: removal must move exactly the victim's keys", moved, counts[victim.Slot])
	}
}
