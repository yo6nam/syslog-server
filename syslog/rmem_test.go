package syslog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeRmem(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rmem_max")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadBufferCapWarning(t *testing.T) {
	warning := readBufferCapWarning(4*1024*1024, writeRmem(t, "212992\n"))
	for _, want := range []string{"4194304", "212992", "sysctl -w net.core.rmem_max=4194304"} {
		if !strings.Contains(warning, want) {
			t.Errorf("warning must mention %q, got: %s", want, warning)
		}
	}

	if got := readBufferCapWarning(4*1024*1024, writeRmem(t, "4194304\n")); got != "" {
		t.Errorf("no warning expected when the cap equals the request: %s", got)
	}
	if got := readBufferCapWarning(1024*1024, writeRmem(t, "4194304\n")); got != "" {
		t.Errorf("no warning expected when the cap is higher than the request: %s", got)
	}
}

func TestReadBufferCapWarningIsSilentWhenItCannotCheck(t *testing.T) {
	if got := readBufferCapWarning(4*1024*1024, filepath.Join(t.TempDir(), "missing")); got != "" {
		t.Errorf("must stay silent without /proc, got: %s", got)
	}
	if got := readBufferCapWarning(4*1024*1024, writeRmem(t, "not a number")); got != "" {
		t.Errorf("must stay silent on unparsable content, got: %s", got)
	}
}
