package ytdlp

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
)

func TestSubprocessDiagnosticBoundAndExitStatus(t *testing.T) {
	_, err := Output(exec.CommandContext(context.Background(), "sh", "-c", "printf 'HTTP Error 429' >&2; exit 7"))
	var diagnostic *ProcessError
	var exited *exec.ExitError
	if !errors.As(err, &diagnostic) || !errors.As(err, &exited) || exited.ExitCode() != 7 || diagnostic.Diagnostic != "HTTP Error 429" {
		t.Fatalf("error=%v", err)
	}
	var d Diagnostics
	_, _ = d.Write([]byte(strings.Repeat("a", DiagnosticLimit*2)))
	_, _ = d.Write([]byte("blocked"))
	if len(d.data) > DiagnosticLimit || !strings.HasSuffix(string(d.data), "blocked") {
		t.Fatal("unbounded or lost diagnostic")
	}
}
