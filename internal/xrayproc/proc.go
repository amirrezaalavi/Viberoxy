// Package xrayproc owns the lifecycle of a single xray child process:
// starting it from generated config JSON, health-checking it, and stopping
// it (graceful SIGTERM, then SIGKILL, then temp-config cleanup).
//
// Every started child gets one goroutine that runs cmd.Wait(). That is what
// makes liveness trustworthy: a crashed xray is both detected (Exited
// closes, Alive turns false) and reaped (no zombie), instead of answering
// a Signal(0) probe forever.
//
// Nothing outside this package should signal or wait on an xray process
// directly; all lifecycle operations go through a *Handle.
package xrayproc

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// DefaultStopGrace is how long Stop waits after SIGTERM before it escalates
// to SIGKILL.
const DefaultStopGrace = 5 * time.Second

// execCommand builds the child command for a given config path. It is a
// variable so tests can substitute a harmless stand-in for the real xray
// binary while exercising the exact same lifecycle code.
var execCommand = func(configPath string) *exec.Cmd {
	return exec.Command("xray", "-c", configPath)
}

// Handle is the lifecycle handle for one xray process. A nil *Handle is
// valid and behaves like "no process": Alive reports false and Stop only
// cleans up the temp config file.
type Handle struct {
	cmd        *exec.Cmd
	configPath string

	// stopGrace overrides DefaultStopGrace when positive (tests shrink it).
	stopGrace time.Duration

	// Exited is closed once the child has exited AND been waited on, so
	// observing it simultaneously observes the reap. Callers block on it
	// instead of probing liveness.
	Exited <-chan struct{}
	exited chan struct{}

	// stopMu serializes Stop and guards stopped/reaping: exactly one
	// goroutine ever owns cmd.Wait() for this child.
	stopMu  sync.Mutex
	stopped bool
	reaping bool

	mu      sync.Mutex
	exitErr error
}

// Start writes configJSON to a temp file and starts xray against it. On
// failure the temp file is removed and no handle is returned.
func Start(configJSON []byte) (*Handle, error) {
	tmpFile, err := os.CreateTemp("", "xray-config-*.json")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	if _, err := tmpFile.Write(configJSON); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("write config: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("close config: %w", err)
	}

	cmd := execCommand(tmpFile.Name())
	if err := cmd.Start(); err != nil {
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("start xray: %w", err)
	}

	h := newHandle(cmd, tmpFile.Name())
	h.reaping = true
	go h.reap()
	return h, nil
}

// Wrap adopts an already-started command as a lifecycle handle. The handle
// takes over waiting: callers must not signal or wait on the command
// themselves. A command that has already been waited on is wrapped as
// already-exited; a command that was not started yet gets no reaper, and
// Stop adopts one on demand.
func Wrap(cmd *exec.Cmd, configPath string) *Handle {
	h := newHandle(cmd, configPath)
	switch {
	case cmd == nil:
	case cmd.ProcessState != nil:
		// The caller already reaped it: the child is gone on arrival, and
		// wait-ownership went with it — nobody must Wait again.
		h.reaping = true
		close(h.exited)
	case cmd.Process != nil:
		h.reaping = true
		go h.reap()
	}
	return h
}

func newHandle(cmd *exec.Cmd, configPath string) *Handle {
	h := &Handle{cmd: cmd, configPath: configPath, exited: make(chan struct{})}
	h.Exited = h.exited
	return h
}

// reap waits for the child exactly once, records the exit reason and
// closes Exited. Waiting is also what reaps the zombie.
func (h *Handle) reap() {
	err := h.cmd.Wait()

	h.mu.Lock()
	h.exitErr = err
	h.mu.Unlock()

	close(h.exited)
}

// Cmd returns the underlying command, or nil.
func (h *Handle) Cmd() *exec.Cmd {
	if h == nil {
		return nil
	}
	return h.cmd
}

// ConfigPath returns the temp config file backing this process, or "".
func (h *Handle) ConfigPath() string {
	if h == nil {
		return ""
	}
	return h.configPath
}

// Alive reports whether the process is still running. It returns false for
// a nil handle, for a handle whose process was never started, and — the
// case that motivated Wait()-based liveness — for a child that has exited,
// whether or not anyone has reaped it yet.
func (h *Handle) Alive() bool {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return false
	}
	select {
	case <-h.Exited:
		return false
	default:
		return true
	}
}

// ExitReason reports why the child exited. It is meaningful once Exited is
// closed: nil means the child exited cleanly, non-nil carries the Wait
// error (signal, exit code, ...). It is nil while the child is running.
func (h *Handle) ExitReason() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.exitErr
}

// Stop terminates the process: SIGTERM, then SIGKILL if it is still alive
// after the grace period, then removal of the given temp config file (the
// handle's own config file if configPath is empty). It is safe on a nil
// handle and safe to call more than once; concurrent callers serialize.
func (h *Handle) Stop(configPath string) error {
	if h == nil {
		if configPath != "" {
			os.Remove(configPath)
		}
		return nil
	}
	if configPath == "" {
		configPath = h.configPath
	}

	h.stopMu.Lock()
	defer h.stopMu.Unlock()

	if h.stopped {
		if configPath != "" {
			os.Remove(configPath)
		}
		return nil
	}
	h.stopped = true

	if h.cmd != nil && h.cmd.Process != nil {
		grace := h.stopGrace
		if grace <= 0 {
			grace = DefaultStopGrace
		}
		if !h.reaping {
			// Wrap ran before the command was started, so nothing owns
			// cmd.Wait() yet: take ownership now, exactly once.
			h.reaping = true
			go h.reap()
		}

		h.cmd.Process.Signal(syscall.SIGTERM)

		select {
		case <-h.Exited:
		case <-time.After(grace):
			h.cmd.Process.Kill()
			<-h.Exited
		}
	}

	if configPath != "" {
		os.Remove(configPath)
	}
	return nil
}
