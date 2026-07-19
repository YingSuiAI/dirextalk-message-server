package researcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadModelAPIKeyFileAcceptsOnlyARegularTrimmedSecret(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "model-key")
	if err := os.WriteFile(path, []byte("test-private-model-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := ReadModelAPIKeyFile(path)
	if err != nil || key != "test-private-model-token" {
		t.Fatalf("model key present=%t err=%v", key != "", err)
	}
	if err := os.WriteFile(path, []byte("line-one\nline-two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadModelAPIKeyFile(path); err == nil {
		t.Fatal("multiline model key must be rejected")
	}
	if _, err := ReadModelAPIKeyFile(filepath.Join(directory, strings.Repeat("a", 9000))); err == nil {
		t.Fatal("unsafe secret path must be rejected")
	}
}
