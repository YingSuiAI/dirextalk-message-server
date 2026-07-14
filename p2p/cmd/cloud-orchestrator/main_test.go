package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseConfigUsesOnlySecretFileForDatabaseURL(t *testing.T) {
	directory := t.TempDir()
	secretFile := filepath.Join(directory, "database-url")
	if err := os.WriteFile(secretFile, []byte("postgres://user:password@db.example/cloud?sslmode=require\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"CLOUD_ORCHESTRATOR_DATABASE_URL_FILE": secretFile,
		"CLOUD_ORCHESTRATOR_RESEARCHER_URL":    "https://researcher.example/v1/cloud-research",
	}
	config, err := parseConfig([]string{"--once", "--worker-id", "cloud-worker-a", "--lease", "2m", "--attempt-timeout", "90s", "--poll-interval", "3s"}, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil })
	if err != nil {
		t.Fatal(err)
	}
	if !config.once || config.workerID != "cloud-worker-a" || config.lease != 2*time.Minute || config.attemptTimeout != 90*time.Second || config.pollInterval != 3*time.Second || config.databaseURLFile != secretFile {
		t.Fatalf("config = %#v", config)
	}
	url, err := readDatabaseURL(config.databaseURLFile)
	if err != nil || url != "postgres://user:password@db.example/cloud?sslmode=require" {
		t.Fatalf("database URL = %q err=%v", url, err)
	}
}

func TestParseConfigRejectsUnsafeStartupSettings(t *testing.T) {
	env := map[string]string{
		"CLOUD_ORCHESTRATOR_DATABASE_URL_FILE": "missing",
		"CLOUD_ORCHESTRATOR_RESEARCHER_URL":    "http://researcher.example/v1/cloud-research",
	}
	if _, err := parseConfig([]string{"--lease", "1m", "--attempt-timeout", "1m"}, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("attempt timeout equal to lease must be rejected")
	}
	if _, err := parseConfig([]string{"--database-url", "postgres://must-not-be-a-flag"}, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("database URL flag must not exist")
	}
	validEndpointEnv := map[string]string{
		"CLOUD_ORCHESTRATOR_DATABASE_URL_FILE": "missing",
		"CLOUD_ORCHESTRATOR_RESEARCHER_URL":    "https://researcher.example/v1/cloud-research",
	}
	if _, err := parseConfig([]string{"--worker-id", "unsafe\nworker"}, func(key string) string { return validEndpointEnv[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("control characters in a worker id must be rejected before database access")
	}
	if _, err := parseConfig([]string{"--worker-id", strings.Repeat("a", 129)}, func(key string) string { return validEndpointEnv[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("a worker id longer than the storage claim boundary must be rejected before database access")
	}
	if _, err := readDatabaseURL(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing database URL file must be rejected")
	}
}
