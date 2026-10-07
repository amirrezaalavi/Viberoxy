package main

// Drain-semantics tests for review finding F-02 (draining contract):
//
// T-DRAIN-01  a long transfer through a path that goes Draining mid-flight
//             completes with zero resets, while every NEW connection avoids
//             the draining WAN (last-resort fallback only) and the reap
//             never kills the slot while its inflight > 0.
// T-DRAIN-02  drain completion: inflight == 0 completes the drain, and
//             DRAIN_MAX (DefaultDrainMax, 600s) hard-kills even with
//             inflight > 0 — whichever comes first.
// T-DRAIN-03  un-drain: a HEALTH-drained slot whose canary streak recovers
//             returns to Active instead of being killed; a slot marked
//             Draining because it is being REPLACED stays draining.
// selection   property: StateDraining is never selected while any Active
//             routable path exists — GetLeastLoaded and
//             GetLeastLoadedExcluding alike; RoutableCount/readyz count
//             ACTIVE routable paths only (D-03); with no active path at
//             all the degraded fallback still hands out a draining path
//             (no blackhole).
//
// All tests use the real front-end + testutil FakeWAN harness — no
// assertions on internals the production path does not use.

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"viberoxy/internal/path"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/testutil"
)

// drainMarkActive puts an occupied slot on servicePort the way a live WAN
// looks (Active, routable, owning its own Path generation) — the selection
// and drain tests' fixture.
func drainMarkActive(t *testing.T, pool *WANPool, index int, cfg *proxycfg.ProxyConfig, servicePort int) *path.Path {
	t.Helper()
	return swpMarkActive(t, pool, index, cfg, servicePort)
}

// TestDrain_TDRAIN01_LongTransferCompletesWhileNewConnsAvoidDrainingWAN is
// the RT-01 scenario observed end to end: a transfer that STARTED while the
// WAN was Active keeps flowing after the slot goes Draining (no resets, no
// reap), while every new connection lands on the still-Active WAN.
func TestDrain_TDRAIN01_LongTransferCompletesWhileNewConnsAvoidDrainingWAN(t *testing.T) {
	const slowID = "drain-long-transfer-client-identity-abcdefghijklmnop"
	const fastID = "fastactive"

	// The slow WAN delays the first response byte AND paces the payload
	// across two chunks, so the transfer spans the whole mark-drain +
	// burst window with bytes actually in flight.
	slowWAN, err := testutil.NewFakeWAN(slowID, testutil.Good(300*time.Millisecond, 40))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slowWAN.Close() })
	fastWAN, err := testutil.NewFakeWAN(fastID, testutil.Good(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fastWAN.Close() })

	pool := NewWANPool(2, 0)
	slowPath := drainMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Protocol: "ss", Server: "slow.example", Port: 443, Raw: "ss://slow"}, slowWAN.Port())
	drainMarkActive(t, pool, 1, &proxycfg.ProxyConfig{Protocol: "ss", Server: "fast.example", Port: 443, Raw: "ss://fast"}, fastWAN.Port())
	front := startSocksServer(t, pool)

	// The long transfer starts while BOTH WANs are active: the tie on
	// inflight==0 resolves to the lowest index, so it lands on slot 0.
	transferClient := testutil.NewFakeClient(slowID)
	transferClient.Socks5 = true // the system under test is a SOCKS5 front-end
	transferClient.Timeout = 15 * time.Second
	type transferResult struct{ r testutil.Result }
	done := make(chan transferResult, 1)
	go func() { done <- transferResult{transferClient.Send(front, 1)} }()

	// Wait until the transfer really holds a reservation on slot 0 and its
	// upstream accepted the connection.
	deadline := time.Now().Add(3 * time.Second)
	for slotInflight(pool, 0) == 0 || slowWAN.Hits() == 0 {
		select {
		case res := <-done:
			t.Fatalf("transfer finished before it was admitted on slot 0: ok=%v tag=%q err=%v (slot0 inflight=%d slow hits=%d fast hits=%d)",
				res.r.OK, res.r.Tag, res.r.Err, slotInflight(pool, 0), slowWAN.Hits(), fastWAN.Hits())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("transfer never reached slot 0 (slot0 inflight=%d slow hits=%d fast hits=%d)",
				slotInflight(pool, 0), slowWAN.Hits(), fastWAN.Hits())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The WAN goes Draining while the transfer is still in flight.
	if err := pool.MarkDraining(0); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}

	// Every NEW connection must avoid the draining WAN (an Active path
	// exists) and land on the active one.
	burstClient := testutil.NewFakeClient(fastID)
	burstClient.Socks5 = true
	burstClient.Timeout = 5 * time.Second
	const burst = 12
	for i := 0; i < burst; i++ {
		r := burstClient.Send(front, int64(i+1))
		if !r.OK {
			t.Fatalf("new connection %d failed while an active WAN exists: %v", i, r.Err)
		}
		if r.Tag != fastID {
			t.Fatalf("new connection %d landed on %q: a draining slot must never receive NEW connections", i, r.Tag)
		}
	}
	if got := slowWAN.Hits(); got != 1 {
		t.Errorf("draining WAN accepted %d connections, want 1 (only the transfer that started before the drain)", got)
	}
	if got := fastWAN.Hits(); got != burst {
		t.Errorf("active WAN accepted %d connections, want %d (every new connection)", got, burst)
	}

	// The transfer's path still has a reservation, so the drain is NOT
	// complete: neither the inflight rule nor DRAIN_MAX may reap it.
	if got := slotInflight(pool, 0); got == 0 {
		t.Fatal("transfer released before the drain-completion check; the run is too fast to be meaningful")
	}
	for _, idx := range pool.DrainExpired(DefaultDrainMax) {
		if idx == 0 {
			t.Fatal("draining slot with an in-flight transfer was drain-expired: the reap must wait for inflight == 0 (or DRAIN_MAX)")
		}
	}

	// The transfer finishes through the draining path with zero resets.
	res := <-done
	if !res.r.OK {
		t.Fatalf("long transfer through the draining WAN failed (reset?): %v", res.r.Err)
	}
	if res.r.Tag != slowID {
		t.Errorf("transfer echoed by %q, want %q", res.r.Tag, slowID)
	}

	// Once the flow finishes, the drain is complete and the slot identity
	// was never reset while it served.
	deadline = time.Now().Add(3 * time.Second)
	for slotInflight(pool, 0) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("transfer reservation never released")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := pool.Slots[0].Current.Load(); got != slowPath {
		t.Errorf("slot 0 Path generation changed while draining: mid-session reset happened")
	}
	if got := pool.GetState(0); got != StateDraining {
		t.Errorf("slot 0 state = %s after the transfer, want draining (never reset mid-session)", got)
	}
	expired := pool.DrainExpired(DefaultDrainMax)
	found := false
	for _, idx := range expired {
		if idx == 0 {
			found = true
		}
	}
	if !found {
		t.Errorf("DrainExpired = %v after inflight hit 0, want the draining slot 0 (drain completes when inflight == 0)", expired)
	}
}

// TestDrain_TDRAIN02_DrainMaxHardKillsWithInflight pins both halves of the
// drain-completion rule: flows finishing completes the drain early, and
// DRAIN_MAX (default 600s) is the hard stop even with flows still in
// flight — replacing the old fixed max(60s, 2×FETCH_INTERVAL) timer.
func TestDrain_TDRAIN02_DrainMaxHardKillsWithInflight(t *testing.T) {
	if DefaultDrainMax != 600*time.Second {
		t.Errorf("DefaultDrainMax = %s, want 600s (DRAIN_MAX default)", DefaultDrainMax)
	}

	pool := NewWANPool(3, 10700)
	mkDraining := func(index int, age time.Duration, inflight int64) {
		t.Helper()
		slot := pool.Slots[index]
		slot.mu.Lock()
		slot.State = StateDraining
		slot.DrainAt = time.Now().Add(-age)
		slot.mu.Unlock()
		for i := int64(0); i < inflight; i++ {
			slot.Current.Load().Reserve()
		}
	}
	mkDraining(0, 100*time.Second, 2) // mid-drain with flows -> keep serving
	mkDraining(1, 601*time.Second, 2) // DRAIN_MAX elapsed -> hard kill despite inflight
	mkDraining(2, 100*time.Second, 0) // flows finished -> drain complete now

	got := pool.DrainExpired(DefaultDrainMax)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("DrainExpired(600s) = %v, want [1 2] (inflight==0 completes; DRAIN_MAX hard-kills; mid-drain with flows stays)", got)
	}

	// A shorter clock behaves the same way: age decides, inflight only
	// decides the early-completion half.
	short := NewWANPool(2, 10700)
	for _, tc := range []struct {
		index    int
		age      time.Duration
		inflight int64
	}{{0, 40 * time.Second, 3}, {1, 70 * time.Second, 3}} {
		slot := short.Slots[tc.index]
		slot.mu.Lock()
		slot.State = StateDraining
		slot.DrainAt = time.Now().Add(-tc.age)
		slot.mu.Unlock()
		for i := int64(0); i < tc.inflight; i++ {
			slot.Current.Load().Reserve()
		}
	}
	if got := short.DrainExpired(time.Minute); len(got) != 1 || got[0] != 1 {
		t.Errorf("DrainExpired(60s) = %v, want [1] (60s old reaped, 40s old still draining)", got)
	}
}

// TestDrain_TDRAIN03_UnDrainOnCanaryRecovery drives the production canary
// path (runCanaries) against a real SOCKS5 relay and a local endpoint:
// a HEALTH-drained slot comes back to Active after the recovery streak,
// while a slot marked Draining for REPLACEMENT stays draining through the
// same recovery — and the reap un-drains a recovered health drain instead
// of killing it.
func TestDrain_TDRAIN03_UnDrainOnCanaryRecovery(t *testing.T) {
	ln, addr := startTestSocksServer(t)
	t.Cleanup(func() { ln.Close() })
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split socks addr: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("socks port: %v", err)
	}

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(endpoint.Close)

	pool := NewWANPool(2, 0)
	drainMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Server: "health.example", Port: 443}, port)
	drainMarkActive(t, pool, 1, &proxycfg.ProxyConfig{Server: "replace.example", Port: 443}, port)
	if err := pool.MarkDrainingHealth(0); err != nil {
		t.Fatalf("MarkDrainingHealth: %v", err)
	}
	if err := pool.MarkDraining(1); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}

	cfg := &proxycfg.Config{TestTimeout: 1, WanFailThreshold: 2, KeepaliveInterval: 10}
	endpoints := []string{endpoint.URL}

	// First passing canary: streak 1 < HalfOpenSuccesses — still draining.
	runCanaries(cfg, pool, endpoints)
	if got := pool.GetState(0); got != StateDraining {
		t.Errorf("health-drained slot state after 1 canary success = %s, want draining (recovery needs the full streak)", got)
	}

	// Second consecutive success completes the recovery streak: the slot
	// returns to Active instead of waiting to be reaped.
	runCanaries(cfg, pool, endpoints)
	if got := pool.GetState(0); got != StateActive {
		t.Errorf("health-drained slot state after recovery = %s, want active (un-drain on canary recovery)", got)
	}
	if !pool.Slots[0].DrainAt.IsZero() {
		t.Error("DrainAt not cleared on un-drain")
	}
	if got := pool.Slots[0].Current.Load().GetState(); got != path.Active {
		t.Errorf("un-drained path state = %s, want active", got)
	}
	// And the un-drained slot is selectable again.
	if idx := slotOf(pool.GetLeastLoaded(2)); idx != 0 {
		t.Errorf("GetLeastLoaded after un-drain = %d, want 0 (the recovered slot is active again)", idx)
	}

	// The replacement-marked slot survives the identical recovery: its
	// drain is the retirement, not an ejection.
	if got := pool.GetState(1); got != StateDraining {
		t.Errorf("replacement-drained slot state = %s, want draining (recovery must not un-drain it)", got)
	}
	// Its drain still completes by the normal rule (inflight == 0).
	expired := pool.DrainExpired(DefaultDrainMax)
	if len(expired) != 1 || expired[0] != 1 {
		t.Errorf("DrainExpired = %v, want [1] (replacement drain completes; the un-drained slot 0 is active)", expired)
	}

	// Reap-time guard: a health-drained slot whose streak ALREADY recovered
	// is un-drained by the reap instead of being killed (inflight is 0, so
	// the plain completion rule would reap it).
	pool2 := NewWANPool(1, 0)
	drainMarkActive(t, pool2, 0, &proxycfg.ProxyConfig{Server: "guard.example", Port: 443}, port)
	if err := pool2.MarkDrainingHealth(0); err != nil {
		t.Fatalf("MarkDrainingHealth: %v", err)
	}
	cur := pool2.Slots[0].Current.Load()
	cur.NoteCanary(time.Now(), true)
	cur.NoteCanary(time.Now(), true)
	if got := pool2.DrainExpired(DefaultDrainMax); len(got) != 0 {
		t.Errorf("DrainExpired = %v, want empty: a recovered health drain must be un-drained, not killed", got)
	}
	if got := pool2.GetState(0); got != StateActive {
		t.Errorf("slot state after the reap-time recovery check = %s, want active", got)
	}
}

// TestDrain_SelectionProperty_DrainingNeverPickedWhileActiveExists is the
// F-02 selection property in the default suite: whenever any Active
// routable path exists, StateDraining is never handed out — by
// GetLeastLoaded, by GetLeastLoadedExcluding (the retry pass), by the
// degraded fallback, or by RoutableCount/readyz (D-03). Only with no
// active path at all does the fallback consider a draining slot.
func TestDrain_SelectionProperty_DrainingNeverPickedWhileActiveExists(t *testing.T) {
	pool := NewWANPool(2, 10700)
	drainPath := drainMarkActive(t, pool, 0, &proxycfg.ProxyConfig{Server: "drain.example", Port: 443}, 20000)
	activePath := drainMarkActive(t, pool, 1, &proxycfg.ProxyConfig{Server: "active.example", Port: 443}, 20001)
	// The active slot is BUSIER, so only the draining rule can keep the
	// selector off slot 0 (identical setup to RT-01).
	activePath.Reserve()
	activePath.Reserve()
	if err := pool.MarkDraining(0); err != nil {
		t.Fatalf("MarkDraining: %v", err)
	}

	// RoutableCount/readyz (D-03): only the ACTIVE routable slot counts.
	if c := pool.RoutableCount(DefaultFailThreshold); c != 1 {
		t.Errorf("RoutableCount = %d, want 1 (draining must not count toward readiness)", c)
	}

	if idx := slotOf(pool.GetLeastLoaded(2)); idx != 1 {
		t.Errorf("GetLeastLoaded(2) = %d, want 1 (draining slot 0 never selected while active exists)", idx)
	}

	// The retry exclusion pass must skip draining exactly like a tried
	// path: with the draining path in `tried`, and with an unrelated path
	// in `tried`, the active slot still wins.
	triedCases := map[string]map[*path.Path]bool{
		"nil":                nil,
		"empty":              {},
		"draining in tried":  {drainPath: true},
		"unrelated in tried": {path.NewVacant(9, 20009): true},
	}
	for name, tried := range triedCases {
		if idx := slotOf(pool.GetLeastLoadedExcluding(tried, 2)); idx != 1 {
			t.Errorf("GetLeastLoadedExcluding(%s) = %d, want 1 (draining slot excluded from the retry pass)", name, idx)
		}
	}

	// Degraded fallback: an OVER-THRESHOLD active slot still beats a
	// healthy draining slot — the fallback is last-resort for draining.
	setFails(pool, 1, 2)
	if idx := slotOf(pool.GetLeastLoaded(2)); idx != 1 {
		t.Errorf("GetLeastLoaded(2) with degraded active = %d, want 1 (fallback must prefer degraded ACTIVE over draining)", idx)
	}
	pool.RecordSuccess(1)

	// No active path at all: the fallback may hand out the draining slot
	// rather than blackhole (the documented last resort).
	if err := pool.ResetEmpty(1); err != nil {
		t.Fatalf("ResetEmpty: %v", err)
	}
	if c := pool.RoutableCount(DefaultFailThreshold); c != 0 {
		t.Errorf("RoutableCount with only a draining slot = %d, want 0 (readyz: not ready without an ACTIVE routable path)", c)
	}
	if idx := slotOf(pool.GetLeastLoaded(2)); idx != 0 {
		t.Errorf("GetLeastLoaded(2) with only a draining slot = %d, want 0 (last resort, no blackhole)", idx)
	}
}

// TestDrain_DrainMaxFromEnv pins the DRAIN_MAX knob: default 600s,
// seconds-based override, invalid values fall back to the default.
func TestDrain_DrainMaxFromEnv(t *testing.T) {
	t.Setenv("DRAIN_MAX", "")
	if got := drainMaxFromEnv(); got != 600*time.Second {
		t.Errorf("drainMaxFromEnv() unset = %s, want 600s", got)
	}
	t.Setenv("DRAIN_MAX", "120")
	if got := drainMaxFromEnv(); got != 120*time.Second {
		t.Errorf("drainMaxFromEnv() = %s, want 120s", got)
	}
	t.Setenv("DRAIN_MAX", "nonsense")
	if got := drainMaxFromEnv(); got != 600*time.Second {
		t.Errorf("drainMaxFromEnv() invalid = %s, want 600s fallback", got)
	}
	t.Setenv("DRAIN_MAX", "0")
	if got := drainMaxFromEnv(); got != 600*time.Second {
		t.Errorf("drainMaxFromEnv() zero = %s, want 600s fallback", got)
	}
}
