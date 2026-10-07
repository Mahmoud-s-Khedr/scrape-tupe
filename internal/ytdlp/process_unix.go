//go:build unix

package ytdlp

import (
	"os/exec"
	"syscall"
	"time"
)

// Cancel the whole group, including ffmpeg and JavaScript children, rather
// than leaving descendants running after the worker loses its lease.
func Configure(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if cmd.Cancel != nil {
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	}
	cmd.WaitDelay = 5 * time.Second
}
func Stop(cmd *exec.Cmd) error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
