package main

import (
	"errors"
	"sort"
	"sync"
	"testing"
)

// helper: build a ProxyConfig with a distinct Raw URI
func testCfg(name string) *ProxyConfig {
	return &ProxyConfig{
		Protocol: "vmess",
		Server:   "example.com",
		Port:     443,
		Name:     name,
		Raw:     "vmess://" + name,
	}
}

func testResult(name string, speed float64) *TestResult {
	return &TestResult{Config: testCfg(name), Speed: speed}
}

var errFake = errors.New("fake test error")

func TestNewCandidatePool(t *testing.T) {
	pool := NewCandidatePool(5)
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
	pool := NewCandidatePool(10)
	results := []*TestResult{
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
	pool := NewCandidatePool(2)
	results := []*TestResult{
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
	pool := NewCandidatePool(10)

	// Initial update
	pool.Update([]*TestResult{
		testResult("a", 10.0),
		testResult("b", 20.0),
	})
	if pool.Len() != 2 {
		t.Fatalf("expected len 2, got %d", pool.Len())
	}

	// Second update: merge with new results, keep sorted
	pool.Update([]*TestResult{
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
	pool := NewCandidatePool(10)
	pool.Update([]*TestResult{
		testResult("a", 10.0),
		testResult("b", 20.0),
		testResult("c", 30.0),
	})

	best := pool.Best()
	if best == nil {
		t.Fatal("Best() returned nil")
	}
	if best.Config.Name != "c" {
		t.Errorf("expected best=c, got %s", best.Config.Name)
	}

	// Exclude the best
	pool.Exclude(best.Config.Raw)
	best = pool.Best()
	if best == nil {
		t.Fatal("Best() returned nil after excluding top")
	}
	if best.Config.Name != "b" {
		t.Errorf("expected best=b after excluding c, got %s", best.Config.Name)
	}
}

func TestBestExcludesFailedResults(t *testing.T) {
	pool := NewCandidatePool(10)
	failResult := &TestResult{Config: testCfg("fail"), Speed: 0, Error: errFake}
	pool.Update([]*TestResult{
		failResult,
		testResult("ok", 5.0),
	})

	best := pool.Best()
	if best == nil {
		t.Fatal("Best() returned nil")
	}
	if best.Config.Name != "ok" {
		t.Errorf("expected best=ok, got %s", best.Config.Name)
	}
}

func TestBestAllExcludedReturnsNil(t *testing.T) {
	pool := NewCandidatePool(10)
	pool.Update([]*TestResult{
		testResult("a", 10.0),
		testResult("b", 20.0),
	})
	pool.Exclude("vmess://a")
	pool.Exclude("vmess://b")

	if pool.Best() != nil {
		t.Error("expected nil when all candidates excluded")
	}
}

func TestExcludeIsIdempotent(t *testing.T) {
	pool := NewCandidatePool(5)
	pool.Update([]*TestResult{testResult("a", 10.0)})

	pool.Exclude("vmess://a")
	pool.Exclude("vmess://a") // no panic, no error
	if !pool.excluded["vmess://a"] {
		t.Error("expected excluded to contain vmess://a")
	}
}

func TestListReturnsCopy(t *testing.T) {
	pool := NewCandidatePool(5)
	pool.Update([]*TestResult{testResult("a", 10.0)})

	list1 := pool.List()
	list2 := pool.List()

	// Mutate list1 — list2 should be unaffected
	list1[0].Speed = 999.0
	if list2[0].Speed != 10.0 {
		t.Errorf("List() did not return a copy: list2 mutated to %f", list2[0].Speed)
	}
}

func TestConcurrencySafe(t *testing.T) {
	pool := NewCandidatePool(20)
	var wg sync.WaitGroup

	// Concurrent updates
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			results := []*TestResult{
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
			_ = pool.Best()
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