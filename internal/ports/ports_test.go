package ports

// Tests for the bounded port allocator (review finding F-13, property
// T-SWAP-02). Written first: see the RED run in the task log before
// ports.go existed.

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestNew_ValidatesRange(t *testing.T) {
	cases := []struct {
		name    string
		base, n int
		wantErr bool
	}{
		{"base zero", 0, 4, true},
		{"negative base", -10800, 4, true},
		{"base past 65535", 70000, 1, true},
		{"zero size", 10800, 0, true},
		{"negative size", 10800, -1, true},
		{"range past 65535", 65535, 2, true},
		{"base+n overflow past 65535", 65530, 10, true},
		{"single port at 65535", 65535, 1, false},
		{"range ending exactly at 65535", 65534, 2, false},
		{"whole port space", 1, 65535, false},
		{"typical test range", 10800, 20, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			a, err := New(tc.base, tc.n)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("New(%d, %d) = allocator, want error", tc.base, tc.n)
				}
				return
			}
			if err != nil {
				t.Fatalf("New(%d, %d): unexpected error: %v", tc.base, tc.n, err)
			}
			if got := a.Len(); got != tc.n {
				t.Fatalf("Len() = %d, want %d", got, tc.n)
			}
		})
	}
}

// collectAll drains an allocator without bound checks; used by tests that
// first prove the allocator can hand out its full range. The loop is bounded
// by the entire TCP port space so a free list that never empties fails the
// test instead of hanging it.
func collectAll(t *testing.T, a *Allocator) map[int]int {
	t.Helper()
	seen := map[int]int{}
	for i := 0; i <= maxPort; i++ {
		p, err := a.Acquire()
		if err != nil {
			if !errors.Is(err, ErrExhausted) {
				t.Fatalf("Acquire: got %v, want ErrExhausted", err)
			}
			return seen
		}
		seen[p]++
		if seen[p] > 1 {
			t.Fatalf("port %d handed out more than once", p)
		}
	}
	t.Fatalf("allocator never exhausted after %d acquisitions", maxPort)
	return seen
}

func TestAcquire_HandsOutFullRangeThenExhausts(t *testing.T) {
	const base, size = 30000, 4
	a, err := New(base, size)
	if err != nil {
		t.Fatal(err)
	}
	seen := collectAll(t, a)
	if len(seen) != size {
		t.Fatalf("acquired %d distinct ports, want %d: %v", len(seen), size, seen)
	}
	for p := range seen {
		if p < base || p >= base+size {
			t.Fatalf("port %d outside range [%d, %d)", p, base, base+size)
		}
	}
	if got := a.InUse(); got != size {
		t.Fatalf("InUse() = %d after draining, want %d", got, size)
	}
	if got := a.Free(); got != 0 {
		t.Fatalf("Free() = %d after draining, want 0", got)
	}
}

// TestAcquire_ExhaustionReturnsErrorNotDuplicate pins the exhaustion
// contract: once the range is checked out, Acquire fails with ErrExhausted —
// it never hands out a port that is already held. The loop is bounded well
// past the capacity so a broken free list fails the test instead of hanging.
func TestAcquire_ExhaustionReturnsErrorNotDuplicate(t *testing.T) {
	const base, size = 30004, 4
	a, err := New(base, size)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int]int{}
	errs := 0
	for i := 0; i < 64; i++ {
		p, err := a.Acquire()
		if err != nil {
			if !errors.Is(err, ErrExhausted) {
				t.Fatalf("Acquire #%d: got %v, want ErrExhausted", i, err)
			}
			errs++
			continue
		}
		if seen[p] > 0 {
			t.Fatalf("port %d handed out while already held", p)
		}
		seen[p]++
	}
	if len(seen) != size {
		t.Fatalf("acquired %d distinct ports, want %d", len(seen), size)
	}
	if want := 64 - size; errs != want {
		t.Fatalf("ErrExhausted returned %d times, want %d", errs, want)
	}
}

// TestRelease_SecondReleaseDoesNotInflateFreeList: releasing a port twice
// must not put it in the free list twice (which would later hand the same
// port to two owners).
func TestRelease_SecondReleaseDoesNotInflateFreeList(t *testing.T) {
	const base, size = 30010, 3
	a, err := New(base, size)
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Release(p); err != nil {
		t.Fatalf("first Release(%d): %v", p, err)
	}
	err = a.Release(p)
	if !errors.Is(err, ErrNotOwned) {
		t.Fatalf("second Release(%d) = %v, want ErrNotOwned", p, err)
	}
	if got := a.Free(); got != size {
		t.Fatalf("Free() = %d after double release, want %d (free list inflated)", got, size)
	}
	if got := a.InUse(); got != 0 {
		t.Fatalf("InUse() = %d after double release, want 0", got)
	}
	// Drain again: the range must come out whole and duplicate-free.
	seen := collectAll(t, a)
	if len(seen) != size {
		t.Fatalf("drained %d ports after double release, want %d: %v", len(seen), size, seen)
	}
}

// TestRelease_UnownedPortDoesNotCorruptState: releasing a port the allocator
// never handed out (still free) or a port outside the range must error and
// leave the allocator exactly as it was.
func TestRelease_UnownedPortDoesNotCorruptState(t *testing.T) {
	const base, size = 30020, 4
	t.Run("in-range but not held", func(t *testing.T) {
		a, err := New(base, size)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Release(base + 1); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("Release(%d) = %v, want ErrNotOwned", base+1, err)
		}
		if got := a.Free(); got != size {
			t.Fatalf("Free() = %d, want %d", got, size)
		}
		if got := a.InUse(); got != 0 {
			t.Fatalf("InUse() = %d, want 0", got)
		}
		// Still drains to the full, duplicate-free range.
		if seen := collectAll(t, a); len(seen) != size {
			t.Fatalf("drained %d ports, want %d", len(seen), size)
		}
	})
	t.Run("out of range", func(t *testing.T) {
		a, err := New(base, size)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range []int{base - 1, base + size, 0, 65536} {
			if err := a.Release(p); err == nil {
				t.Fatalf("Release(%d) = nil, want an error", p)
			} else if errors.Is(err, ErrNotOwned) {
				t.Fatalf("Release(%d) = ErrNotOwned, want an out-of-range error", p)
			}
		}
		if got := a.Free(); got != size {
			t.Fatalf("Free() = %d, want %d", got, size)
		}
		if seen := collectAll(t, a); len(seen) != size {
			t.Fatalf("drained %d ports, want %d", len(seen), size)
		}
	})
}

func TestLenInUseFreeAccounting(t *testing.T) {
	const base, size = 40200, 5
	a, err := New(base, size)
	if err != nil {
		t.Fatal(err)
	}
	if a.Len() != size || a.InUse() != 0 || a.Free() != size {
		t.Fatalf("fresh: Len=%d InUse=%d Free=%d, want Len=%d InUse=0 Free=%d",
			a.Len(), a.InUse(), a.Free(), size, size)
	}
	p1, err := a.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	p2, err := a.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if a.InUse() != 2 || a.Free() != 3 {
		t.Fatalf("after 2 acquires: InUse=%d Free=%d, want InUse=2 Free=3", a.InUse(), a.Free())
	}
	if err := a.Release(p1); err != nil {
		t.Fatal(err)
	}
	if a.InUse() != 1 || a.Free() != 4 {
		t.Fatalf("after 1 release: InUse=%d Free=%d, want InUse=1 Free=4", a.InUse(), a.Free())
	}
	if err := a.Release(p2); err != nil {
		t.Fatal(err)
	}
	if a.InUse() != 0 || a.Free() != size {
		t.Fatalf("after all releases: InUse=%d Free=%d, want InUse=0 Free=%d", a.InUse(), a.Free(), size)
	}
}

// TestInUseAccountingMatchesReality: more goroutines than ports race for a
// single acquire each; exactly capacity winners must emerge, and InUse must
// equal the number of real holders afterwards.
func TestInUseAccountingMatchesReality(t *testing.T) {
	const base, size, contenders = 40100, 4, 16
	a, err := New(base, size)
	if err != nil {
		t.Fatal(err)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners []int
	)
	for g := 0; g < contenders; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := a.Acquire()
			if err != nil {
				if !errors.Is(err, ErrExhausted) {
					t.Errorf("Acquire: got %v, want ErrExhausted or success", err)
				}
				return
			}
			mu.Lock()
			winners = append(winners, p)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(winners) != size {
		t.Fatalf("%d goroutines won a port, want exactly %d", len(winners), size)
	}
	seen := map[int]bool{}
	for _, p := range winners {
		if seen[p] {
			t.Fatalf("port %d won by two goroutines", p)
		}
		seen[p] = true
	}
	if got := a.InUse(); got != len(winners) {
		t.Fatalf("InUse() = %d, want %d (the actual holder count)", got, len(winners))
	}
	if got := a.Free(); got != 0 {
		t.Fatalf("Free() = %d with all ports held, want 0", got)
	}

	// Everyone gives their port back: the pool must return to full strength.
	for _, p := range winners {
		if err := a.Release(p); err != nil {
			t.Fatalf("Release(%d): %v", p, err)
		}
	}
	if a.InUse() != 0 || a.Free() != size {
		t.Fatalf("after releasing all: InUse=%d Free=%d, want InUse=0 Free=%d", a.InUse(), a.Free(), size)
	}
}

// TestT_SWAP02_NoCollisionUnder64GoroutineStress is the property test for
// review finding F-13 / T-SWAP-02: 64 goroutines run many Acquire/Release
// cycles over a 4-port allocator. Every port must have at most one holder at
// any instant — holders are tracked in a side table (port -> goroutine) so a
// buggy allocator that hands the same port to two owners trips the test even
// though each goroutine only ever Releases what it Acquired.
//
// Run with -race: the side table is itself mutex-guarded, so the race
// detector also validates the test's own bookkeeping.
func TestT_SWAP02_NoCollisionUnder64GoroutineStress(t *testing.T) {
	const (
		base       = 40000
		size       = 4
		goroutines = 64
		perG       = 300
	)
	a, err := New(base, size)
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu       sync.Mutex
		holders  = make(map[int]int) // port -> goroutine id currently holding it
		collide  []string
		badErr   []string
		acquired int
		released int
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				p, err := a.Acquire()
				if err != nil {
					if !errors.Is(err, ErrExhausted) {
						mu.Lock()
						badErr = append(badErr, fmt.Sprintf("goroutine %d: Acquire = %v, want ErrExhausted", id, err))
						mu.Unlock()
					}
					runtime.Gosched()
					continue
				}
				if p < base || p >= base+size {
					mu.Lock()
					badErr = append(badErr, fmt.Sprintf("goroutine %d: port %d outside [%d, %d)", id, p, base, base+size))
					mu.Unlock()
					continue
				}

				mu.Lock()
				if prev, ok := holders[p]; ok {
					collide = append(collide, fmt.Sprintf("port %d held at once by goroutine %d and goroutine %d", p, prev, id))
				}
				holders[p] = id
				acquired++
				mu.Unlock()

				// Widen the window in which a double-booking would show.
				for s := 0; s < 4; s++ {
					runtime.Gosched()
				}

				mu.Lock()
				if holders[p] != id {
					collide = append(collide, fmt.Sprintf("port %d stolen from goroutine %d by goroutine %d while held", p, id, holders[p]))
				}
				delete(holders, p)
				released++
				mu.Unlock()

				if err := a.Release(p); err != nil {
					mu.Lock()
					badErr = append(badErr, fmt.Sprintf("goroutine %d: Release(%d) = %v", id, p, err))
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if n := len(collide); n > 0 {
		t.Fatalf("T-SWAP-02 violated: %d collision(s); first: %s", n, collide[0])
	}
	if n := len(badErr); n > 0 {
		t.Fatalf("%d unexpected error(s); first: %s", n, badErr[0])
	}
	if acquired != released {
		t.Fatalf("acquired=%d released=%d, want equal", acquired, released)
	}
	if a.InUse() != 0 {
		t.Fatalf("InUse() = %d after stress, want 0", a.InUse())
	}
	if a.Free() != size {
		t.Fatalf("Free() = %d after stress, want %d", a.Free(), size)
	}
	if acquired == 0 {
		t.Fatal("stress performed no successful acquisitions; test is vacuous")
	}
}

// TestConcurrentAcquireReleaseStress checks steady-state churn: goroutines
// acquiring and releasing (sometimes holding across a yield) never corrupt
// the pool, and every port handed out belongs to the configured range.
func TestConcurrentAcquireReleaseStress(t *testing.T) {
	const (
		base       = 40300
		size       = 8
		goroutines = 32
		perG       = 200
	)
	a, err := New(base, size)
	if err != nil {
		t.Fatal(err)
	}

	var (
		mu     sync.Mutex
		badErr []string
	)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				p, err := a.Acquire()
				if err != nil {
					if !errors.Is(err, ErrExhausted) {
						mu.Lock()
						badErr = append(badErr, fmt.Sprintf("goroutine %d: Acquire = %v", id, err))
						mu.Unlock()
					}
					continue
				}
				if p < base || p >= base+size {
					mu.Lock()
					badErr = append(badErr, fmt.Sprintf("goroutine %d: port %d outside [%d, %d)", id, p, base, base+size))
					mu.Unlock()
				}
				if i%3 == 0 {
					runtime.Gosched()
				}
				if err := a.Release(p); err != nil {
					mu.Lock()
					badErr = append(badErr, fmt.Sprintf("goroutine %d: Release(%d) = %v", id, p, err))
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if n := len(badErr); n > 0 {
		t.Fatalf("%d error(s); first: %s", n, badErr[0])
	}
	if a.InUse() != 0 {
		t.Fatalf("InUse() = %d after stress, want 0", a.InUse())
	}
	if a.Free() != size {
		t.Fatalf("Free() = %d after stress, want %d", a.Free(), size)
	}
}

func TestNewTestPorts(t *testing.T) {
	// Speed-test ports: TEST_BASE_PORT..TEST_BASE_PORT+MAX_TEST_PER_CYCLE-ish.
	a, err := NewTestPorts(10800, 20)
	if err != nil {
		t.Fatal(err)
	}
	if a.Len() != 20 || a.InUse() != 0 {
		t.Fatalf("Len=%d InUse=%d, want Len=20 InUse=0", a.Len(), a.InUse())
	}
	seen := collectAll(t, a)
	for p := range seen {
		if p < 10800 || p > 10819 {
			t.Fatalf("port %d outside TEST_BASE_PORT range [10800, 10819]", p)
		}
	}
	if len(seen) != 20 {
		t.Fatalf("drained %d ports, want 20", len(seen))
	}
	if _, err := NewTestPorts(0, 20); err == nil {
		t.Fatal("NewTestPorts(0, 20) = nil error, want validation error")
	}
}

func TestNewSpareWANPorts(t *testing.T) {
	// Spare service ports for make-before-break swaps: the slots above the
	// WAN_COUNT live services, i.e. WAN_BASE_PORT+WAN_COUNT .. +spares-1.
	a, err := NewSpareWANPorts(10700, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	if a.Len() != 3 || a.InUse() != 0 {
		t.Fatalf("Len=%d InUse=%d, want Len=3 InUse=0", a.Len(), a.InUse())
	}
	seen := collectAll(t, a)
	want := map[int]bool{10704: true, 10705: true, 10706: true}
	if len(seen) != len(want) {
		t.Fatalf("drained %d ports, want %d: %v", len(seen), len(want), seen)
	}
	for p := range seen {
		if !want[p] {
			t.Fatalf("port %d is not a spare service port (want 10704..10706)", p)
		}
	}
	if _, err := NewSpareWANPorts(10700, -1, 3); err == nil {
		t.Fatal("NewSpareWANPorts with negative wanCount = nil error, want validation error")
	}
	if _, err := NewSpareWANPorts(65534, 4, 3); err == nil {
		t.Fatal("NewSpareWANPorts past 65535 = nil error, want validation error")
	}
}

// TestAllocatorsAreIndependent: the two intended users get disjoint pools;
// exhausting one must not affect the other.
func TestAllocatorsAreIndependent(t *testing.T) {
	testPorts, err := NewTestPorts(10800, 2)
	if err != nil {
		t.Fatal(err)
	}
	spareWANs, err := NewSpareWANPorts(10700, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if seen := collectAll(t, testPorts); len(seen) != 2 {
		t.Fatalf("test allocator drained %d ports, want 2", len(seen))
	}
	// test allocator is now exhausted; the spare one is untouched.
	if _, err := testPorts.Acquire(); !errors.Is(err, ErrExhausted) {
		t.Fatalf("exhausted test allocator Acquire = %v, want ErrExhausted", err)
	}
	if got := spareWANs.Free(); got != 2 {
		t.Fatalf("spare allocator Free() = %d, want 2", got)
	}
	if p, err := spareWANs.Acquire(); err != nil || p != 10704 {
		t.Fatalf("spare Acquire = (%d, %v), want (10704, nil)", p, err)
	}
}
