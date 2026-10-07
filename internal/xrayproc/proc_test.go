package xrayproc

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// useFakeCommand points execCommand at a harmless stand-in for the real
// xray binary for the duration of the test, so the exact lifecycle code
// runs against a process the test fully controls.
func useFakeCommand(t *testing.T, makeCmd func(configPath string) *exec.Cmd) {
	t.Helper()
	orig := execCommand
	execCommand = makeCmd
	t.Cleanup(func() { execCommand = orig })
}

// startFake starts a handle on a fake child and guarantees it is stopped
// when the test ends.
func startFake(t *testing.T, makeCmd func(configPath string) *exec.Cmd) *Handle {
	t.Helper()
	useFakeCommand(t, makeCmd)
	h, err := Start([]byte(`{}`))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	path := h.ConfigPath()
	t.Cleanup(func() { h.Stop(path) })
	return h
}

// waitExited waits for the child to be reaped, failing the test if that
// does not happen within limit.
func waitExited(t *testing.T, h *Handle, limit time.Duration) {
	t.Helper()
	select {
	case <-h.Exited:
	case <-time.After(limit):
		t.Fatalf("child not reaped within %v (zombie left behind); Alive()=%v", limit, h.Alive())
	}
}

// waitStatus returns the wait status of the child, failing if the child
// was never waited on (i.e. is still a zombie).
func waitStatus(t *testing.T, h *Handle) syscall.WaitStatus {
	t.Helper()
	st := h.Cmd().ProcessState
	if st == nil {
		t.Fatal("ProcessState is nil: child was never waited on (still a zombie)")
	}
	ws, ok := st.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("ProcessState.Sys() is %T, not syscall.WaitStatus", st.Sys())
	}
	return ws
}

// T-PROC-01: a SIGKILLed child must be reported dead within two seconds.
// This is the F-06 repro: Signal(0)-based liveness calls an exited but
// not yet reaped child "alive" forever.
func TestT_PROC01_SigkillDetectedWithinTwoSeconds(t *testing.T) {
	h := startFake(t, func(string) *exec.Cmd { return exec.Command("sleep", "30") })

	if !h.Alive() {
		t.Fatal("Alive() = false right after Start")
	}
	if err := h.Cmd().Process.Kill(); err != nil { // SIGKILL, no chance to run a handler
		t.Fatalf("kill child: %v", err)
	}

	select {
	case <-h.Exited:
	case <-time.After(2 * time.Second):
		t.Fatalf("child exit not observed within 2s; Alive()=%v", h.Alive())
	}
	if h.Alive() {
		t.Fatal("Alive() = true after the child was killed")
	}
}

// T-PROC-02: the exited child must actually be reaped — no zombie.
func TestT_PROC02_ExitReapsChild(t *testing.T) {
	h := startFake(t, func(string) *exec.Cmd { return exec.Command("sh", "-c", "exit 0") })

	waitExited(t, h, 2*time.Second)
	waitStatus(t, h) // ProcessState is set only once Wait() has reaped the child

	// The pid must be gone from the kernel's point of view: a fresh
	// Signal(0) must fail instead of succeeding against a zombie.
	if err := h.Cmd().Process.Signal(syscall.Signal(0)); err == nil {
		t.Fatal("Signal(0) still succeeds on the exited child: it is a zombie")
	}
	if reason := h.ExitReason(); reason != nil {
		t.Fatalf("ExitReason() = %v for a clean exit, want nil", reason)
	}
}

// T-PROC-03: Stop must be SIGTERM-first, escalate to SIGKILL after the
// grace period, remove the temp config file, and be safe to call twice.
func TestT_PROC03_StopSIGTERMGraceful(t *testing.T) {
	h := startFake(t, func(string) *exec.Cmd { return exec.Command("sleep", "30") })
	path := h.ConfigPath()

	start := time.Now()
	if err := h.Stop(path); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed >= DefaultStopGrace {
		t.Fatalf("Stop took %v: SIGTERM was ignored instead of ending the child before the %v grace", elapsed, DefaultStopGrace)
	}

	ws := waitStatus(t, h)
	if !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Fatalf("child exit = %v, want terminated by SIGTERM", ws)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("temp config file %q was not removed", path)
	}
}

func TestT_PROC03_StopSIGKILLAfterGrace(t *testing.T) {
	// A child that ignores SIGTERM: Stop must escalate to SIGKILL once the
	// grace period elapses. The loop runs in the shell itself, so no
	// grandchild process can outlive the SIGKILL. "ready" on stdout is the
	// handshake that proves the trap is installed before Stop signals it,
	// otherwise the child could still die on the default disposition.
	ready := make(chan struct{})
	h := startFake(t, func(string) *exec.Cmd {
		cmd := exec.Command("sh", "-c", "trap '' TERM; echo ready; while :; do :; done")
		pr, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("stdout pipe: %v", err)
		}
		go func() {
			defer close(ready)
			line, _ := bufio.NewReader(pr).ReadString('\n')
			if line == "" {
				t.Errorf("child exited before installing its SIGTERM trap")
			}
		}()
		return cmd
	})
	path := h.ConfigPath()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("child never reported that its SIGTERM trap was installed")
	}
	h.stopGrace = 500 * time.Millisecond

	start := time.Now()
	if err := h.Stop(path); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < h.stopGrace {
		t.Fatalf("Stop returned after %v, before the %v grace period: no SIGKILL escalation", elapsed, h.stopGrace)
	}

	ws := waitStatus(t, h)
	if !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("child exit = %v, want SIGKILL after the grace period", ws)
	}
}

func TestT_PROC03_StopIdempotent(t *testing.T) {
	h := startFake(t, func(string) *exec.Cmd { return exec.Command("sleep", "30") })
	path := h.ConfigPath()

	if err := h.Stop(path); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("temp config file %q was not removed by the first Stop", path)
	}
	if err := h.Stop(path); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if err := h.Stop(""); err != nil {
		t.Fatalf("third Stop: %v", err)
	}
}

// T-PROC-04: an unexpected exit must be observable together with its
// reason, so callers can log why the child died.
func TestT_PROC04_UnexpectedExitReason(t *testing.T) {
	h := startFake(t, func(string) *exec.Cmd { return exec.Command("sh", "-c", "exit 3") })

	waitExited(t, h, 2*time.Second)

	reason := h.ExitReason()
	if reason == nil {
		t.Fatal("ExitReason() = nil for a child that exited with code 3")
	}
	var exitErr *exec.ExitError
	if !errors.As(reason, &exitErr) {
		t.Fatalf("ExitReason() = %v (%T), want *exec.ExitError", reason, reason)
	}
	if code := exitErr.ExitCode(); code != 3 {
		t.Fatalf("exit code = %d, want 3", code)
	}
	if h.Alive() {
		t.Fatal("Alive() = true after the child exited with code 3")
	}
}

// The child argv and temp config naming are contracts other tasks rely on.
func TestExecCommand_Argv(t *testing.T) {
	cmd := execCommand("/tmp/some-config.json")
	want := []string{"xray", "-c", "/tmp/some-config.json"}
	if len(cmd.Args) != len(want) {
		t.Fatalf("argv = %q, want %q", cmd.Args, want)
	}
	for i := range want {
		if cmd.Args[i] != want[i] {
			t.Fatalf("argv = %q, want %q", cmd.Args, want)
		}
	}
}

func TestStart_TempConfigNaming(t *testing.T) {
	h := startFake(t, func(string) *exec.Cmd { return exec.Command("sleep", "30") })
	base := filepath.Base(h.ConfigPath())
	if !strings.HasPrefix(base, "xray-config-") || !strings.HasSuffix(base, ".json") {
		t.Fatalf("config file %q does not match xray-config-*.json", h.ConfigPath())
	}
}
