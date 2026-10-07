package main

// Tests for make-before-break drop-and-replace (review finding F-13).
//
// T-SWAP-01  a full swap under synthetic load through the front-end drops
//            zero connections (testutil FakeWAN/FakeClient harness).
// T-SWAP-02  port discipline: the shared TEST_BASE_PORT allocator is used by
//            DropAndReplace AND runCycle — exhaustion is a clean error, ports
//            come back after every test, concurrent tests never share one.
// T-SWAP-03  a candidate whose server:port already serves another slot is
//            skipped (cands Best(exclude) wiring), leaving the slot untouched.
// failure    a failed candidate test / start leaves the old WAN serving —
//            the slot is never emptied by a failed replacement.

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"viberoxy/internal/cands"
	"viberoxy/internal/path"
	"viberoxy/internal/ports"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/testutil"
	"viberoxy/internal/xrayproc"
)

// swpMarkActive puts an occupied slot on the given service port the way a
// live WAN looks: Active, routable (non-nil handle) and owning its own Path
// generation. The handle wraps an UNSTARTED command, so the post-swap
// teardown of the old occupant is a no-op and a test-owned listener behind
// the port keeps serving its in-flight connections — draining connections
// across the retired process's teardown is explicitly a later task; what
// these tests pin is that the swap itself introduces no dead window.
func swpMarkActive(t *testing.T, pool *WANPool, index int, cfg *proxycfg.ProxyConfig, servicePort int) *path.Path {
	t.Helper()
	slot := pool.Slots[index]
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.State = StateActive
	slot.Config = cfg
	slot.ServicePort = servicePort
	slot.Cmd = xrayproc.Wrap(&exec.Cmd{}, "")
	pth := path.New(cfg, slot.Cmd, index, servicePort)
	slot.Current.Store(pth)
	return pth
}

// swpDummyHandle is the stand-in replacement process (never really started).
func swpDummyHandle() *xrayproc.Handle { return xrayproc.Wrap(&exec.Cmd{}, "") }

// T-SWAP-01: a full make-before-break swap under synthetic load through the
// front-end must not fail a single connection. The old WAN (a FakeWAN) owns
// the slot's service port; the replacement (a second FakeWAN) already listens
// on the spare port the allocator hands out, so the cutover moves the slot's
// dial target from one live listener to another with no dead window.
//
// Load runs through the whole candidate-test window (RT-10's guarantee, now
// observed under traffic). The instant of the cutover itself is quiesced:
// TestCandidate pauses the workers and drains in-flight front handlers
// before it returns, and the workers resume only after DropAndReplace has
// returned. That both (a) mirrors production semantics — new dials either
// land before the cutover or after it, and connections in flight across the
// retired process's teardown belong to the later drain policy — and (b)
// orders every handler's service-port read against the swap write for -race
// (the front relay reads slot.ServicePort without the slot lock, by design
// in relay.go, which this task must not touch).
func TestSwap_TSWAP01_FullSwapUnderLoadDropsZeroConnections(t *testing.T) {
	oldWAN, err := testutil.NewFakeWAN("oldwan", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oldWAN.Close() })
	newWAN, err := testutil.NewFakeWAN("newwan", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = newWAN.Close() })

	pool := NewWANPool(1, 0)
	oldCfg := &proxycfg.ProxyConfig{Protocol: "ss", Server: "old.example", Port: 443, Raw: "ss://old"}
	oldPath := swpMarkActive(t, pool, 0, oldCfg, oldWAN.Port())
	front := startSocksServer(t, pool)

	// The spare budget is pinned to the replacement listener's port:
	// Acquire must hand out exactly the port newWAN already serves on.
	spare, err := ports.New(newWAN.Port(), 1)
	if err != nil {
		t.Fatal(err)
	}

	candidates := cands.NewPool(4)
	candidates.Update([]*cands.Entry{{
		Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "new.example", Port: 8443, Raw: "ss://new"},
		Speed:  42,
	}})

	const workers = 8
	// ~200 rps target (8 workers x one op per 40ms): an unthrottled loop
	// would burn the whole ephemeral port range in TIME_WAIT sockets and
	// make the test fail on address exhaustion instead of on the swap.
	const workerInterval = 40 * time.Millisecond
	var stateDuringTest WANState = -1
	var startPort int
	pause := make(chan struct{})
	resume := make(chan struct{})
	stop := make(chan struct{})
	// Unblock the workers on every exit path (including t.Fatal inside
	// TestCandidate, which unwinds before the normal close).
	t.Cleanup(func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	})
	paused := make(chan struct{}, workers)
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		SparePorts: spare,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			stateDuringTest = pool.GetState(0)
			// The whole test window runs under live load: the old WAN
			// must keep serving while the candidate is validated.
			time.Sleep(300 * time.Millisecond)
			// Quiesce the front-end for the cutover: pause the workers,
			// then wait until every accepted connection's handler has
			// finished (its release on the old Path happens after its
			// service-port read).
			close(pause)
			for i := 0; i < workers; i++ {
				<-paused
			}
			deadline := time.Now().Add(3 * time.Second)
			for oldPath.Inflight.Load() != 0 {
				if time.Now().After(deadline) {
					t.Fatal("front handlers did not drain before the cutover")
				}
				time.Sleep(5 * time.Millisecond)
			}
			return &TestResult{Config: cfg, Speed: 42}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, port int, _ ...bool) (*xrayproc.Handle, string, error) {
			startPort = port
			return swpDummyHandle(), "", nil
		},
	}

	var attempts, failures int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			client := testutil.NewFakeClient(fmt.Sprintf("swp%d", w))
			client.Socks5 = true
			pausedOnce := false
			for seq := int64(1); ; seq++ {
				select {
				case <-pause:
					if !pausedOnce {
						pausedOnce = true
						paused <- struct{}{}
					}
					<-resume
				default:
				}
				select {
				case <-stop:
					return
				default:
				}
				atomic.AddInt64(&attempts, 1)
				if r := client.Send(front, seq); !r.OK {
					atomic.AddInt64(&failures, 1)
				}
				time.Sleep(workerInterval)
			}
		}(w)
	}

	time.Sleep(250 * time.Millisecond) // ramp the load up before the swap
	if _, err := pool.DropAndReplace(0, opts); err != nil {
		// Do not wait for the workers here: an error before TestCandidate
		// means pause was never closed and they are still running; the
		// test is failing anyway.
		close(resume)
		t.Fatalf("DropAndReplace: %v", err)
	}
	close(resume)
	time.Sleep(300 * time.Millisecond) // load keeps flowing after the cutover
	close(stop)
	wg.Wait()

	gotAttempts, gotFailures := atomic.LoadInt64(&attempts), atomic.LoadInt64(&failures)
	if gotAttempts < 50 {
		t.Fatalf("load too light (%d attempts): cannot prove zero-dropped-connections", gotAttempts)
	}
	if gotFailures != 0 {
		t.Fatalf("%d/%d connections failed across the swap; want 0", gotFailures, gotAttempts)
	}
	if stateDuringTest != StateActive {
		t.Errorf("slot was %s while the candidate was tested; the old WAN must keep serving", stateDuringTest)
	}
	if startPort != newWAN.Port() {
		t.Errorf("replacement started on port %d, want the spare %d", startPort, newWAN.Port())
	}
	if startPort == oldWAN.Port() {
		t.Error("replacement started on the slot's own service port; the old process must keep listening there")
	}
	if got := pool.GetState(0); got != StateActive {
		t.Errorf("slot state = %v, want active", got)
	}
	if got := pool.Slots[0].ServicePort; got != newWAN.Port() {
		t.Errorf("slot service port = %d, want the replacement's spare %d", got, newWAN.Port())
	}
	if newWAN.Hits() == 0 {
		t.Error("no connection ever reached the replacement; the swap did not move traffic")
	}
	if oldPath.GetState() != path.Draining {
		t.Errorf("retired path state = %v, want draining", oldPath.GetState())
	}
	if pool.Slots[0].Current.Load() == oldPath {
		t.Error("slot still holds the old path after the swap")
	}
}

// T-SWAP-02 (exhaustion + release half): the candidate test must draw its
// port from the shared allocator. While the only port is checked out,
// DropAndReplace fails cleanly — before the test even runs, never reusing a
// port it does not hold — and the slot is untouched. Once free, the test
// gets the allocator's port and releases it afterwards (also on the
// spare-exhaustion early return).
func TestSwap_TSWAP02_DropTestsDrawFromSharedTestPorts(t *testing.T) {
	pool := NewWANPool(1, 10700)
	old := &proxycfg.ProxyConfig{Protocol: "ss", Server: "old.example", Port: 443, Raw: "ss://old"}
	activeTestSlot(t, pool, 0, old)
	oldPath := pool.Slots[0].Current.Load()
	replacement := &proxycfg.ProxyConfig{Protocol: "ss", Server: "new.example", Port: 8443, Raw: "ss://new"}
	candidates := cands.NewPool(4)
	candidates.Update([]*cands.Entry{{Config: replacement, Speed: 80}})

	testPorts, err := ports.NewTestPorts(21800, 1)
	if err != nil {
		t.Fatal(err)
	}
	sparePorts, err := ports.NewSpareWANPorts(10700, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	var tested bool
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		TestPorts:  testPorts,
		SparePorts: sparePorts,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, port int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			tested = true
			return &TestResult{Config: cfg, Speed: 60}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, port int, _ ...bool) (*xrayproc.Handle, string, error) {
			return swpDummyHandle(), "", nil
		},
	}
	// (a) Every test port checked out => clean error, no test, no start.
	heldTest, err := testPorts.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.DropAndReplace(0, opts)
	if !errors.Is(err, ErrNoReplacementCandidate) {
		t.Errorf("exhausted test ports: err = %v, want ErrNoReplacementCandidate-family", err)
	}
	if tested {
		t.Error("candidate was tested while its test port was not available")
	}
	if pool.GetState(0) != StateActive || pool.Slots[0].Current.Load() != oldPath {
		t.Error("slot was touched by an operation that failed before the test")
	}
	if err := testPorts.Release(heldTest); err != nil {
		t.Fatal(err)
	}

	// (b) Test port free but the spare budget checked out => the test runs
	// (and its port comes back afterwards), the start never happens, the
	// slot stays as it was.
	heldSpare, err := sparePorts.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	tested = false
	started := false
	opts.StartCandidate = func(cfg *proxycfg.ProxyConfig, port int, _ ...bool) (*xrayproc.Handle, string, error) {
		started = true
		return swpDummyHandle(), "", nil
	}
	_, err = pool.DropAndReplace(0, opts)
	if !errors.Is(err, ErrNoReplacementCandidate) {
		t.Errorf("exhausted spare ports: err = %v, want ErrNoReplacementCandidate-family", err)
	}
	if !tested {
		t.Error("candidate was not tested in (b)")
	}
	if started {
		t.Error("StartCandidate ran despite the exhausted spare budget")
	}
	if pool.GetState(0) != StateActive || pool.Slots[0].Current.Load() != oldPath {
		t.Error("slot must stay untouched when the replacement cannot start")
	}
	if got := testPorts.Free(); got != 1 {
		t.Errorf("test ports free = %d after an early return, want 1 (released around the test)", got)
	}
	if err := testPorts.Release(heldSpare); err == nil {
		t.Error("released a spare through the test allocator; want an error (wrong allocator)")
	}
	if err := sparePorts.Release(heldSpare); err != nil {
		t.Fatal(err)
	}

	// (c) Everything free => the test gets the allocator's port and the
	// swap goes through; the slot keeps the spare (its listener port) and
	// the test port is back in the pool.
	tested = false
	var testedPort int
	opts.StartCandidate = func(cfg *proxycfg.ProxyConfig, port int, _ ...bool) (*xrayproc.Handle, string, error) {
		return swpDummyHandle(), "", nil
	}
	opts.TestCandidate = func(cfg *proxycfg.ProxyConfig, port int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
		tested = true
		testedPort = port
		return &TestResult{Config: cfg, Speed: 60}
	}
	if _, err := pool.DropAndReplace(0, opts); err != nil {
		t.Fatalf("DropAndReplace: %v", err)
	}
	if !tested || testedPort != 21800 {
		t.Errorf("tested=%v port=%d, want the allocator's port 21800", tested, testedPort)
	}
	if got := testPorts.Free(); got != 1 {
		t.Errorf("test ports free = %d after success, want 1", got)
	}
	if got := sparePorts.InUse(); got != 1 {
		t.Errorf("spares in use = %d after success, want 1 (the slot holds its listener port)", got)
	}
	if pool.Slots[0].ServicePort != 10701 {
		t.Errorf("slot service port = %d, want the spare 10701", pool.Slots[0].ServicePort)
	}
	// ResetEmpty hands the spare back and restores the slot's own port.
	if err := pool.ResetEmpty(0); err != nil {
		t.Fatal(err)
	}
	if got := sparePorts.Free(); got != 1 {
		t.Errorf("spares free = %d after ResetEmpty, want 1", got)
	}
	if pool.Slots[0].ServicePort != 10700 {
		t.Errorf("slot service port = %d after reset, want its own 10700", pool.Slots[0].ServicePort)
	}
}

// T-SWAP-02 (integration half): runCycle's speed tests draw from the SAME
// shared allocator the drop path uses. With the allocator's first port held
// (as an API-triggered replacement test would hold it), the cycle must take
// the next port instead of colliding on TEST_BASE_PORT+0, and the port must
// come back when the cycle's test ends.
func TestSwap_TSWAP02_CycleTestsDrawFromSharedTestPorts(t *testing.T) {
	orig := startTestXray
	usedPort := -1
	startTestXray = func(_ *proxycfg.ProxyConfig, port int, _ ...bool) (*xrayproc.Handle, string, error) {
		usedPort = port
		return nil, "", errors.New("spy: no xray for this test")
	}
	t.Cleanup(func() { startTestXray = orig })

	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	raw := "ss://YWVzLTEyOC1nY206cGFzcw@1.2.3.4:443#spycfg"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(base64.StdEncoding.EncodeToString([]byte(raw + "\n"))))
	}))
	defer srv.Close()

	testBase := consecutiveFreePorts(t, 2)
	testPorts, err := ports.NewTestPorts(testBase, 2)
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWANPool(1, testBase+100)
	pool.SetPortAllocators(testPorts, nil)

	// Hold the first port the way a concurrent API replacement test would.
	held, err := testPorts.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if held != testBase {
		t.Fatalf("first acquired port = %d, want %d", held, testBase)
	}

	cfg := &proxycfg.Config{SubscriberURL: srv.URL, FetchInterval: 300, TestTimeout: 3,
		TestBasePort: testBase, WanBasePort: testBase + 100, WanCount: 1,
		DownloadSize: 1000000, MinimumSpeed: 1e9}
	runCycle(cfg, pool, cands.NewPool(10), time.Minute)

	if usedPort == -1 {
		t.Fatal("runCycle never started a speed test; cannot tell which port it used")
	}
	if usedPort == testBase {
		t.Errorf("runCycle tested on %d, the port another test holds — the shared allocator was not used", usedPort)
	}
	if usedPort != testBase+1 {
		t.Errorf("runCycle tested on port %d, want the allocator's next free port %d", usedPort, testBase+1)
	}
	if got := testPorts.Free(); got != 1 {
		t.Errorf("test ports free = %d after the cycle, want 1 (released after the test)", got)
	}
	if err := testPorts.Release(held); err != nil {
		t.Fatal(err)
	}
	if got := testPorts.Free(); got != 2 {
		t.Errorf("test ports free = %d, want 2 (full budget restored)", got)
	}
}

// T-SWAP-02 (concurrency half): concurrent DropAndReplace calls on
// DIFFERENT slots test in parallel (replaceMu is not held across the speed
// test) and never receive the same test port from the shared allocator.
func TestSwap_TSWAP02_ConcurrentDropTestsNeverShareAPort(t *testing.T) {
	pool := NewWANPool(2, 10700)
	activeTestSlot(t, pool, 0, &proxycfg.ProxyConfig{Protocol: "ss", Server: "a.example", Port: 1, Raw: "ss://a"})
	activeTestSlot(t, pool, 1, &proxycfg.ProxyConfig{Protocol: "ss", Server: "b.example", Port: 2, Raw: "ss://b"})

	testPorts, err := ports.NewTestPorts(21900, 2)
	if err != nil {
		t.Fatal(err)
	}
	sparePorts, err := ports.NewSpareWANPorts(10700, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	candidates := cands.NewPool(8)
	candidates.Update([]*cands.Entry{
		{Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "c.example", Port: 3, Raw: "ss://c"}, Speed: 90},
		{Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "d.example", Port: 4, Raw: "ss://d"}, Speed: 80},
	})

	var mu sync.Mutex
	usedPorts := map[int]int{} // test port -> how many tests ran on it
	var overlapped bool
	var inFlight int
	opts := DropAndReplaceOptions{
		Candidates: candidates,
		TestPorts:  testPorts,
		SparePorts: sparePorts,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, port int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			mu.Lock()
			usedPorts[port]++
			inFlight++
			if inFlight > 1 {
				overlapped = true
			}
			mu.Unlock()
			time.Sleep(100 * time.Millisecond) // both tests run at once
			mu.Lock()
			inFlight--
			mu.Unlock()
			return &TestResult{Config: cfg, Speed: 50}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, port int, _ ...bool) (*xrayproc.Handle, string, error) {
			return swpDummyHandle(), "", nil
		},
	}

	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = pool.DropAndReplace(i, opts)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("DropAndReplace(%d): %v", i, err)
		}
	}
	if len(usedPorts) != 2 {
		t.Errorf("distinct test ports = %v, want two different ports", usedPorts)
	}
	for port, n := range usedPorts {
		if n != 1 {
			t.Errorf("port %d was used by %d tests, want 1", port, n)
		}
	}
	if !overlapped {
		t.Error("the two speed tests never ran concurrently: replaceMu is still held across the test")
	}
	if got := testPorts.Free(); got != 2 {
		t.Errorf("test ports free = %d after both swaps, want 2", got)
	}
}

// T-SWAP-03: a candidate whose server:port already serves another active
// slot must be skipped — cross-checked against cands' Best(exclude) wiring:
// when it is the ONLY entry, no candidate remains, the slot is untouched and
// nothing is tested or started.
func TestSwap_TSWAP03_SkipsCandidateAlreadyServingAnotherSlot(t *testing.T) {
	pool := NewWANPool(2, 10700)
	dropped := &proxycfg.ProxyConfig{Protocol: "ss", Server: "old.example", Port: 443, Raw: "ss://old"}
	activeTestSlot(t, pool, 0, dropped)
	droppedPath := pool.Slots[0].Current.Load()
	busy := &proxycfg.ProxyConfig{Protocol: "ss", Server: "busy.example", Port: 8443, Raw: "ss://busy"}
	activeTestSlot(t, pool, 1, busy)

	// A different share link to the very server:port slot 1 already serves
	// — the fastest (and only) pool entry.
	busyDup := &proxycfg.ProxyConfig{Protocol: "ss", Server: "busy.example", Port: 8443, Raw: "ss://busy-dup"}
	candidates := cands.NewPool(4)
	candidates.Update([]*cands.Entry{{Config: busyDup, Speed: 100}})

	tested := false
	started := false
	_, err := pool.DropAndReplace(0, DropAndReplaceOptions{
		Candidates: candidates,
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			tested = true
			return &TestResult{Config: cfg, Speed: 42}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ ...bool) (*xrayproc.Handle, string, error) {
			started = true
			return swpDummyHandle(), "", nil
		},
	})
	if !errors.Is(err, ErrNoReplacementCandidate) {
		t.Fatalf("err = %v, want ErrNoReplacementCandidate", err)
	}
	if tested || started {
		t.Errorf("tested=%v started=%v: an already-serving server:port must be skipped at the pick, before any test", tested, started)
	}
	if pool.GetState(0) != StateActive || pool.Slots[0].Config != dropped || pool.Slots[0].Current.Load() != droppedPath {
		t.Errorf("slot 0 = %v/%v, want the old WAN untouched", pool.GetState(0), pool.Slots[0].Config)
	}

	// Cross-check: cands' Best with the same HasServerPort exclusion this
	// path wires in skips busyDup by itself — the dedupe rule, not an
	// accident of the drop flow.
	if got := candidates.Best(func(c *proxycfg.ProxyConfig) bool { return pool.HasServerPort(c.Server, c.Port) }); got != nil {
		t.Errorf("cands.Best(HasServerPort) = %s:%d, want nil (already serving slot 1)", got.Config.Server, got.Config.Port)
	}
}

// Failure path: when the candidate's test fails, the old WAN keeps serving —
// verified through the front-end — and the slot is never emptied.
func TestSwap_FailedCandidateTestLeavesOldWANServing(t *testing.T) {
	oldWAN, err := testutil.NewFakeWAN("keeper", testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = oldWAN.Close() })

	pool := NewWANPool(1, 0)
	oldCfg := &proxycfg.ProxyConfig{Protocol: "ss", Server: "old.example", Port: 443, Raw: "ss://old"}
	oldPath := swpMarkActive(t, pool, 0, oldCfg, oldWAN.Port())
	front := startSocksServer(t, pool)
	client := testutil.NewFakeClient("keep")
	client.Socks5 = true

	if r := client.Send(front, 1); !r.OK {
		t.Fatalf("pre-failure connection failed: %v", r.Err)
	}

	started := false
	_, err = pool.DropAndReplace(0, DropAndReplaceOptions{
		Candidates: func() *cands.Pool {
			cp := cands.NewPool(4)
			cp.Update([]*cands.Entry{{
				Config: &proxycfg.ProxyConfig{Protocol: "ss", Server: "new.example", Port: 8443, Raw: "ss://new"},
				Speed:  70,
			}})
			return cp
		}(),
		TestCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ time.Duration, _ string, _ int64, _ int) *TestResult {
			return &TestResult{Config: cfg, Error: errors.New("probe failed")}
		},
		StartCandidate: func(cfg *proxycfg.ProxyConfig, _ int, _ ...bool) (*xrayproc.Handle, string, error) {
			started = true
			return swpDummyHandle(), "", nil
		},
	})
	if !errors.Is(err, ErrReplacementTestFailed) {
		t.Fatalf("err = %v, want ErrReplacementTestFailed", err)
	}
	if started {
		t.Error("StartCandidate ran after a failed test")
	}
	if pool.GetState(0) != StateActive {
		t.Errorf("slot state = %v, want active (never emptied by a failed replacement)", pool.GetState(0))
	}
	if pool.Slots[0].Config != oldCfg {
		t.Errorf("slot config = %v, want the old WAN", pool.Slots[0].Config)
	}
	if pool.Slots[0].Current.Load() != oldPath {
		t.Error("slot's Path generation changed despite the failed test")
	}
	if oldPath.GetState() != path.Active {
		t.Errorf("old path state = %v, want active (still serving)", oldPath.GetState())
	}
	// The old WAN still serves traffic after the failed replacement.
	if r := client.Send(front, 2); !r.OK {
		t.Errorf("old WAN stopped serving after a failed replacement: %v", r.Err)
	}
	if oldWAN.Hits() < 2 {
		t.Errorf("old WAN hits = %d, want >= 2", oldWAN.Hits())
	}
}
