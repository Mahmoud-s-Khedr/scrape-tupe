// Package ytdlp preserves bounded subprocess diagnostics for worker policy.
package ytdlp

import (
	"fmt"
	"os/exec"
	"strings"
)

const DiagnosticLimit = 16 << 10

// Diagnostics keeps the most recent output without allocating for the full log.
type Diagnostics struct{ data []byte }

func (d *Diagnostics) Write(p []byte) (int, error) {
	n := len(p)
	if n >= DiagnosticLimit {
		d.data = append(d.data[:0], p[n-DiagnosticLimit:]...)
	} else {
		if excess := len(d.data) + n - DiagnosticLimit; excess > 0 {
			d.data = d.data[excess:]
		}
		d.data = append(d.data, p...)
	}
	return n, nil
}

type ProcessError struct {
	Cause      error
	Diagnostic string
}

func (e *ProcessError) Error() string { return fmt.Sprintf("yt-dlp: %v: %s", e.Cause, e.Diagnostic) }
func (e *ProcessError) Unwrap() error { return e.Cause }
func (d *Diagnostics) Wrap(err error) error {
	if err == nil {
		return nil
	}
	return &ProcessError{Cause: err, Diagnostic: strings.TrimSpace(string(d.data))}
}

func Output(cmd *exec.Cmd) ([]byte, error) {
	Configure(cmd)
	var diagnostics Diagnostics
	cmd.Stderr = &diagnostics
	out, err := cmd.Output()
	return out, diagnostics.Wrap(err)
}
