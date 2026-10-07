// Package xrayproc owns the lifecycle of a single xray child process:
// starting it from generated config JSON, health-checking it, and stopping
// it (graceful SIGTERM, then SIGKILL, then temp-config cleanup).
//
// Nothing outside this package should signal or wait on an xray process
// directly; all lifecycle operations go through a *Handle.
package xrayproc

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

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

	return &Handle{cmd: cmd, configPath: tmpFile.Name()}, nil
}

// Wrap adopts an already-started command as a lifecycle handle. Callers
// that start a command themselves (test fixtures) must wrap it exactly once
// and must not signal or wait on the command afterwards.
func Wrap(cmd *exec.Cmd, configPath string) *Handle {
	return &Handle{cmd: cmd, configPath: configPath}
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

// Alive reports whether the process is running. It returns false for a nil
// handle and for a handle whose process was never started.
func (h *Handle) Alive() bool {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return false
	}
	return h.cmd.Process.Signal(syscall.Signal(0)) == nil
}

// Stop terminates the process: SIGTERM, then SIGKILL if it is still alive
// after the grace period, then removal of the given temp config file. It
// is safe on a nil handle and safe to call more than once.
func (h *Handle) Stop(configPath string) error {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		if configPath != "" {
			os.Remove(configPath)
		}
		return nil
	}

	h.cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan error, 1)
	go func() {
		done <- h.cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		h.cmd.Process.Kill()
		<-done
	}

	if configPath != "" {
		os.Remove(configPath)
	}
	return nil
}
