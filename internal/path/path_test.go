package path_test

import (
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"viberoxy/internal/path"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xrayproc"
)

// TestPath_InflightVisibilityClampsAtZero pins the pool-facing read: the raw
// Inflight counter may drift negative (a caller releasing more than it
// reserved), but Conns — what selection and the API see — never reports less
// than zero.
func TestPath_InflightVisibilityClampsAtZero(t *testing.T) {
	p := path.New(nil, nil, 0, 10700)

	p.Reserve()
	if got := p.Conns(); got != 1 {
		t.Errorf("Conns after one reserve = %d, want 1", got)
	}

	p.Release()
	p.Release() // one release too many
	if got := p.Inflight.Load(); got != -1 {
		t.Errorf("raw Inflight = %d, want -1 (accounting keeps the drift)", got)
	}
	if got := p.Conns(); got != 0 {
		t.Errorf("Conns = %d, want 0 (visible count clamps at zero)", got)
	}

	// A clamped read must not hide the real count: the drift is still there
	// and the next reservation brings the raw counter back to a sane value.
	p.Reserve()
	if got := p.Inflight.Load(); got != 0 {
		t.Errorf("raw Inflight after re-reserve = %d, want 0", got)
	}
	if got := p.Conns(); got != 0 {
		t.Errorf("Conns = %d, want 0", got)
	}

	// Nil paths read as "no occupant".
	var missing *path.Path
	if got := missing.Conns(); got != 0 {
		t.Errorf("nil.Conns() = %d, want 0", got)
	}
	missing.Reserve() // must not panic
	missing.Release()
	missing.RecordFailure()
	missing.RecordSuccess()
}

// TestPath_IDsMonotonicAndNeverReused pins generation identity: IDs increase
// monotonically across constructions (including vacant placeholders), so a
// slot's old and new occupant are always distinguishable.
func TestPath_IDsMonotonicAndNeverReused(t *testing.T) {
	const n = 64
	ids := make([]uint64, 0, n)
	for i := 0; i < n/2; i++ {
		ids = append(ids, path.New(&proxycfg.ProxyConfig{}, nil, i%4, 10700+i).ID)
		ids = append(ids, path.NewVacant(i%4, 10700+i).ID)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ID %d at position %d is not greater than predecessor %d: IDs must be monotonic and never reused", ids[i], i, ids[i-1])
		}
	}

	// Two successive occupants of the SAME slot never share an ID.
	a := path.New(nil, nil, 0, 10700)
	b := path.New(nil, nil, 0, 10700)
	if a.ID == b.ID {
		t.Errorf("successive occupants of slot 0 share ID %d", a.ID)
	}
}

// TestPath_OldPathReleaseDoesNotTouchReplacement is the object-level ABA
// invariant: reserve on a path, swap the slot to a replacement, release the
// old one — the replacement's counters must not move. (The pool-level variant,
// T-PATH-01, lives in wan_test.go: the identity guard in production is the
// relay holding the *Path, and internal/path cannot reach relay code.)
func TestPath_OldPathReleaseDoesNotTouchReplacement(t *testing.T) {
	slot := new(atomic.Pointer[path.Path])

	old := path.New(nil, nil, 0, 10700)
	slot.Store(old)
	for i := 0; i < 3; i++ {
		old.Reserve()
	}
	if got := old.Conns(); got != 3 {
		t.Fatalf("old path Conns = %d, want 3", got)
	}

	// The slot is reset and re-occupied: a new object, a new ID.
	replacement := path.New(nil, nil, 0, 10700)
	slot.Store(replacement)

	// The old handlers finish afterwards.
	for i := 0; i < 3; i++ {
		old.Release()
	}

	if got := replacement.Inflight.Load(); got != 0 {
		t.Errorf("replacement Inflight = %d, want 0 (old handlers leaked into it)", got)
	}
	if got := replacement.ConsecutiveFails(); got != 0 {
		t.Errorf("replacement fails = %d, want 0", got)
	}
	if old.ID == replacement.ID {
		t.Errorf("old and replacement share ID %d", old.ID)
	}
	if got := old.Conns(); got != 0 {
		t.Errorf("old path Conns = %d, want 0 (its own accounting still balances)", got)
	}
	if slot.Load() != replacement {
		t.Error("slot no longer points at the replacement")
	}
}

// TestPath_ConcurrentReserveRelease runs many goroutines of Reserve/Release
// against one path; under -race the counters must stay consistent and end at
// zero.
func TestPath_ConcurrentReserveRelease(t *testing.T) {
	p := path.New(&proxycfg.ProxyConfig{}, nil, 0, 10700)

	const goroutines = 16
	const perG = 500

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				p.Reserve()
				_ = p.Conns()
				p.Release()
			}
		}()
	}
	// Concurrent state/health writes must also be race-free with the counters.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				p.RecordFailure()
				_ = p.ConsecutiveFails()
				p.RecordSuccess()
				p.SetState(path.Active)
				_ = p.GetState()
			}
		}()
	}
	wg.Wait()

	if got := p.Inflight.Load(); got != 0 {
		t.Errorf("Inflight after %d goroutines = %d, want 0", goroutines, got)
	}
	if got := p.Conns(); got != 0 {
		t.Errorf("Conns after %d goroutines = %d, want 0", goroutines, got)
	}
}

// TestPath_AliveAndStop exercises the process-facing helpers: Alive follows
// the xrayproc handle, Stop terminates the child and leaves the path dead.
func TestPath_AliveAndStop(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start cmd: %v", err)
	}
	proc := xrayproc.Wrap(cmd, "")
	p := path.New(&proxycfg.ProxyConfig{Server: "1.2.3.4", Port: 443}, proc, 0, 10700)
	defer p.Stop()

	if !p.Alive() {
		t.Error("Alive = false for a running child")
	}
	if err := p.Stop(); err != nil {
		t.Errorf("Stop: %v", err)
	}
	if p.Alive() {
		t.Error("Alive = true after Stop")
	}
	if p.GetState() != path.Active {
		t.Errorf("state = %v, want active (state transitions are the pool's job)", p.GetState())
	}

	// A vacant path has no process and reports dead/not-alive.
	v := path.NewVacant(1, 10701)
	if v.Alive() {
		t.Error("vacant path reports alive")
	}
	if v.GetState() != path.Dead {
		t.Errorf("vacant state = %v, want dead", v.GetState())
	}
	if v.Cfg != nil || v.Proc != nil {
		t.Error("vacant path must carry no config/process")
	}
}
