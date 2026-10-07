//go:build !unix

package ytdlp

import (
	"os/exec"
	"time"
)

func Configure(cmd *exec.Cmd)  { cmd.WaitDelay = 5 * time.Second }
func Stop(cmd *exec.Cmd) error { return cmd.Process.Kill() }
