//go:build unix

package ytdlp

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func TestCancellationStopsDescendantsHoldingOutputPipes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := Output(exec.CommandContext(ctx, "sh", "-c", "sleep 60 & wait"))
	if err == nil {
		t.Fatal("cancelled process succeeded")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("descendant kept output pipes open for %s", elapsed)
	}
}
