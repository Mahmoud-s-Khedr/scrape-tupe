package envfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadProcessEnvironmentTakesPrecedenceIncludingEmpty(t *testing.T) {
	key := "SCRAPE_TUPE_ENVFILE_TEST"
	t.Setenv(key, "")
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(key+"=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Load(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv(key); got != "" {
		t.Fatalf("value = %q, want explicitly empty process value", got)
	}
}

func TestLoadSetsUnsetValue(t *testing.T) {
	key := "SCRAPE_TUPE_ENVFILE_UNSET_TEST"
	os.Unsetenv(key)
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(key+"=from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Load(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv(key); got != "from-file" {
		t.Fatalf("value = %q, want from-file", got)
	}
}
