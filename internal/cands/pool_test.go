package cands

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
	"viberoxy/internal/proxycfg"
)

// helper: build a ProxyConfig with a distinct Raw URI
func testCfg(name string) *proxycfg.ProxyConfig {
	return &proxycfg.ProxyConfig{
		Protocol: "vmess",
		Server:   "example.com",
		Port:     443,
		Name:     name,
		Raw:      "vmess://" + name,
	}
}

func testResult(name string, speed float64) *Entry {
	return &Entry{Config: testCfg(name), Speed: speed}
}

var errFake = errors.New("fake test error")

func TestNewPool(t *testing.T) {
	pool := NewPool(5)
	if pool == nil {
		t.Fatal("NewCandidatePool returned nil")
	}
	if pool.Len() != 0 {
		t.Errorf("expected len 0, got %d", pool.Len())
	}
	if pool.maxLen != 5 {
		t.Errorf("expected maxLen 5, got %d", pool.maxLen)
	}
}

func TestUpdateSortsBySpeedDesc(t *testing.T) {
	pool := NewPool(10)
	results := []*Entry{
		testResult("slow", 1.0),
		testResult("fast", 100.0),
		testResult("mid", 50.0),
	}
	pool.Update(results)

	list := pool.List()
	if len(list) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(list))
	}
	// Should be sorted: fast(100), mid(50), slow(1)
	expected := []string{"fast", "mid", "slow"}
	for i, name := range expected {
		if list[i].Config.Name != name {
			t.Errorf("position %d: expected %s, got %s", i, name, list[i].Config.Name)
		}
	}
}

func TestUpdateTrimsToMaxLen(t *testing.T) {
	pool := NewPool(2)
	results := []*Entry{
		testResult("a", 10.0),
		testResult("b", 20.0),
		testResult("c", 30.0),
		testResult("d", 40.0),
	}
	pool.Update(results)

	if pool.Len() != 2 {
		t.Errorf("expected pool len 2 after trim, got %d", pool.Len())
	}
	list := pool.List()
	// Top-2 by speed: d(40), c(30)
	if list[0].Config.Name != "d" || list[1].Config.Name != "c" {
		t.Errorf("expected top-2 [d, c], got [%s, %s]", list[0].Config.Name, list[1].Config.Name)
	}
}

func TestUpdateMergesAndReplaces(t *testing.T) {
	pool := NewPool(10)

	// Initial update
	pool.Update([]*Entry{
		testResult("a", 10.0),
		testResult("b", 20.0),
	})
	if pool.Len() != 2 {
		t.Fatalf("expected len 2, got %d", pool.Len())
	}

	// Second update: merge with new results, keep sorted
	pool.Update([]*Entry{
		testResult("c", 30.0),
	})
	if pool.Len() != 3 {
		t.Errorf("expected len 3 after merge, got %d", pool.Len())
	}
	list := pool.List()
	if list[0].Config.Name != "c" {
		t.Errorf("expected best=c(30), got %s(%f)", list[0].Config.Name, list[0].Speed)
	}
}

func TestBestReturnsTopNonExcluded(t *testing.T) {
	pool := NewPool(10)
	pool.Update([]*Entry{
		testResult("a", 10.0),
		testResult("b", 20.0),
		testResult("c", 30.0),
	})

	best := pool.Best(nil)
	if best == nil {
		t.Fatal("Best() returned nil")
	}
	if best.Config.Name != "c" {
		t.Errorf("expected best=c, got %s", best.Config.Name)
	}

	// Exclude the best
	pool.Exclude(best.Config.Raw)
	best = pool.Best(nil)
	if best == nil {
		t.Fatal("Best() returned nil after excluding top")
	}
	if best.Config.Name != "b" {
		t.Errorf("expected best=b after excluding c, got %s", best.Config.Name)
	}
}

func TestBestExcludesFailedResults(t *testing.T) {
	pool := NewPool(10)
	failResult := &Entry{Config: testCfg("fail"), Speed: 0, Error: errFake}
	pool.Update([]*Entry{
		failResult,
		testResult("ok", 5.0),
	})

	best := pool.Best(nil)
	if best == nil {
		t.Fatal("Best() returned nil")
	}
	if best.Config.Name != "ok" {
		t.Errorf("expected best=ok, got %s", best.Config.Name)
	}
}

func TestBestAllExcludedReturnsNil(t *testing.T) {
	pool := NewPool(10)
	pool.Update([]*Entry{
		testResult("a", 10.0),
		testResult("b", 20.0),
	})
	pool.Exclude("vmess://a")
	pool.Exclude("vmess://b")

	if pool.Best(nil) != nil {
		t.Error("expected nil when all candidates excluded")
	}
}

func TestExcludeIsIdempotent(t *testing.T) {
	pool := NewPool(5)
	pool.Update([]*Entry{testResult("a", 10.0)})

	pool.Exclude("vmess://a")
	pool.Exclude("vmess://a") // no panic, no error
	if !pool.excluded["vmess://a"] {
		t.Error("expected excluded to contain vmess://a")
	}
}

func TestListReturnsCopy(t *testing.T) {
	pool := NewPool(5)
	pool.Update([]*Entry{testResult("a", 10.0)})

	list1 := pool.List()
	list2 := pool.List()

	// Mutate list1 — list2 should be unaffected
	list1[0].Speed = 999.0
	if list2[0].Speed != 10.0 {
		t.Errorf("List() did not return a copy: list2 mutated to %f", list2[0].Speed)
	}
}

func TestConcurrencySafe(t *testing.T) {
	pool := NewPool(20)
	var wg sync.WaitGroup

	// Concurrent updates
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			results := []*Entry{
				testResult("a", float64(n)),
				testResult("b", float64(n+1)),
			}
			pool.Update(results)
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = pool.List()
			_ = pool.Best(nil)
			_ = pool.Len()
		}()
	}

	// Concurrent excludes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			pool.Exclude("vmess://a")
		}(i)
	}

	wg.Wait()

	// After all concurrent ops, pool should have at most maxLen entries
	if pool.Len() > 20 {
		t.Errorf("pool overflow: len=%d, maxLen=20", pool.Len())
	}

	// Verify sorted order
	list := pool.List()
	if !sort.SliceIsSorted(list, func(i, j int) bool {
		return list[i].Speed > list[j].Speed
	}) {
		t.Error("pool not sorted by speed desc after concurrent updates")
	}
}

// TestUpdateDedupesByRawNewestWins is the unit form of RT-09 (F-12): the
// same config re-tested across cycles must leave exactly ONE entry, the
// newest result, instead of accumulating a row per cycle.
func TestUpdateDedupesByRawNewestWins(t *testing.T) {
	pool := NewPool(10)
	for _, speed := range []float64{10, 20, 5} {
		pool.Update([]*Entry{{Config: testCfg("same"), Speed: speed}})
	}
	if n := pool.Len(); n != 1 {
		t.Errorf("pool holds %d entries for one config after 3 updates; want 1", n)
	}
	list := pool.List()
	if len(list) != 1 {
		t.Fatalf("List returned %d entries; want 1", len(list))
	}
	if list[0].Speed != 5 {
		t.Errorf("kept speed %v; want the newest result (5)", list[0].Speed)
	}
}

// T-CAND-01: entries carry TestedAt and expire after DefaultTTL (30 min).
// Determinism comes both from an explicit TestedAt on the entry and from
// the injectable clock (p.now).
func TestEntriesExpireAfterTTL(t *testing.T) {
	if DefaultTTL != 30*time.Minute {
		t.Errorf("DefaultTTL = %v, want 30m", DefaultTTL)
	}
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	pool := NewPool(10)
	pool.now = func() time.Time { return now }
	pool.Update([]*Entry{
		{Config: testCfg("fresh"), Speed: 10, TestedAt: now.Add(-DefaultTTL + time.Minute)},
		{Config: testCfg("stale"), Speed: 20, TestedAt: now.Add(-DefaultTTL - time.Minute)},
	})
	if n := pool.Len(); n != 1 {
		t.Errorf("pool holds %d entries; want 1 (the stale one must expire after %v)", n, DefaultTTL)
	}
	list := pool.List()
	if len(list) == 1 && list[0].Config.Name != "fresh" {
		t.Errorf("surviving entry = %q, want fresh", list[0].Config.Name)
	}

	// Entries without an explicit TestedAt are stamped from the pool clock
	// and expire when that clock advances past the TTL.
	auto := NewPool(10)
	auto.now = func() time.Time { return now }
	auto.Update([]*Entry{{Config: testCfg("auto"), Speed: 1}})
	if got := auto.List()[0].TestedAt; !got.Equal(now) {
		t.Errorf("TestedAt = %v, want stamped from the pool clock (%v)", got, now)
	}
	auto.now = func() time.Time { return now.Add(DefaultTTL + time.Second) }
	if n := auto.Len(); n != 0 {
		t.Errorf("pool holds %d entries past the TTL; want 0", n)
	}
}

// T-CAND-02: Best accepts an exclusion predicate and skips the configs it
// matches (DropAndReplace uses this to keep already-serving configs out).
func TestBestHonorsExclusionPredicate(t *testing.T) {
	pool := NewPool(10)
	a, b, c := testCfg("a"), testCfg("b"), testCfg("c")
	pool.Update([]*Entry{
		{Config: a, Speed: 30},
		{Config: b, Speed: 20},
		{Config: c, Speed: 10},
	})

	best := pool.Best(nil)
	if best == nil || best.Config != a {
		t.Fatalf("Best(nil) = %v, want a (top speed)", best)
	}
	best = pool.Best(func(cfg *proxycfg.ProxyConfig) bool { return cfg == a || cfg == b })
	if best == nil || best.Config != c {
		t.Errorf("Best(excluding a,b) = %v, want c", best)
	}
	if pool.Best(func(*proxycfg.ProxyConfig) bool { return true }) != nil {
		t.Error("Best(excluding everything) = non-nil, want nil")
	}
}

// T-CAND-03: a per-server cap keeps one flaky server:port (re-tested under
// rotating share links) from flooding the pool; the fastest entries win.
func TestUpdateCapsEntriesPerServer(t *testing.T) {
	pool := NewPool(50)
	total := DefaultMaxPerServer + 3
	for i := 0; i < total; i++ {
		pool.Update([]*Entry{{
			Config: &proxycfg.ProxyConfig{
				Protocol: "ss",
				Server:   "flaky.example",
				Port:     8388,
				Raw:      fmt.Sprintf("ss://flaky-%d", i),
			},
			Speed: float64(10 * (i + 1)),
		}})
	}
	if n := pool.Len(); n != DefaultMaxPerServer {
		t.Errorf("pool holds %d entries for one server:port; want the cap %d", n, DefaultMaxPerServer)
	}
	list := pool.List()
	for i, want := range []float64{80, 70, 60, 50, 40} {
		if i < len(list) && list[i].Speed != want {
			t.Errorf("entry %d speed = %v, want %v (fastest %d kept)", i, list[i].Speed, want, DefaultMaxPerServer)
		}
	}

	pool.Update([]*Entry{{
		Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "other.example", Port: 8388, Raw: "ss://other"},
		Speed:  1,
	}})
	if n := pool.Len(); n != DefaultMaxPerServer+1 {
		t.Errorf("pool holds %d entries after adding another server:port; want %d", n, DefaultMaxPerServer+1)
	}
}
