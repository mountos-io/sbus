// Package bus is the client-side half of csbus: where the hub's socket
// lives, and how to reach it, spawning a detached hub on first use if none
// is listening yet.
package bus

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

const (
	sockEnvVar = "CSBUS_SOCK"
	spawnWait  = 2 * time.Second
	spawnRetry = 50 * time.Millisecond
)

// DefaultSockPath returns $CSBUS_SOCK if set, else ~/.claude/csbus.sock.
func DefaultSockPath() string {
	if v := os.Getenv(sockEnvVar); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "csbus.sock")
	}
	return filepath.Join(home, ".claude", "csbus.sock")
}

// Dial connects to the hub at sockPath, spawning a detached one if nothing
// answers there yet.
func Dial(sockPath string) (net.Conn, error) {
	if c, err := net.Dial("unix", sockPath); err == nil {
		return c, nil
	}
	if err := spawn(sockPath); err != nil {
		return nil, fmt.Errorf("start hub: %w", err)
	}

	deadline := time.Now().Add(spawnWait)
	var lastErr error
	for time.Now().Before(deadline) {
		c, err := net.Dial("unix", sockPath)
		if err == nil {
			return c, nil
		}
		lastErr = err
		time.Sleep(spawnRetry)
	}
	return nil, fmt.Errorf("hub did not come up at %s: %w", sockPath, lastErr)
}

// spawn starts this same binary as "csbus serve --sock <sockPath>",
// detached from the current process group so it outlives it.
func spawn(sockPath string) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o700); err != nil {
		return err
	}
	logFile, err := os.OpenFile(sockPath+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()

	cmd := exec.Command(self, "serve", "--sock", sockPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd.Start()
}
