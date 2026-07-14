package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseConfigUsesOnlySecretFileForDatabaseURL(t *testing.T) {
	directory := t.TempDir()
	secretFile := filepath.Join(directory, "database-url")
	keyFile := filepath.Join(directory, "node-signing-key.pem")
	if err := os.WriteFile(secretFile, []byte("postgres://user:password@db.example/cloud?sslmode=require\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeNodeSigningKey(keyFile); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"CLOUD_ORCHESTRATOR_DATABASE_URL_FILE":      secretFile,
		"CLOUD_ORCHESTRATOR_RESEARCHER_URL":         "https://researcher.example/v2/cloud-research",
		"CLOUD_ORCHESTRATOR_RESEARCHER_CA_FILE":     "ca.pem",
		"CLOUD_ORCHESTRATOR_RESEARCHER_CERT_FILE":   "client.pem",
		"CLOUD_ORCHESTRATOR_RESEARCHER_KEY_FILE":    "client.key",
		"CLOUD_ORCHESTRATOR_RESEARCHER_SERVER_NAME": "researcher.example",
		"CLOUD_ORCHESTRATOR_NODE_SIGNING_KEY_FILE":  keyFile,
	}
	config, err := parseConfig([]string{"--once", "--worker-id", "cloud-worker-a", "--lease", "2m", "--attempt-timeout", "90s", "--poll-interval", "3s"}, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil })
	if err != nil {
		t.Fatal(err)
	}
	if !config.once || config.workerID != "cloud-worker-a" || config.lease != 2*time.Minute || config.attemptTimeout != 90*time.Second || config.pollInterval != 3*time.Second || config.databaseURLFile != secretFile || config.researcherCAFile != "ca.pem" || config.researcherCertFile != "client.pem" || config.researcherKeyFile != "client.key" || config.researcherServerName != "researcher.example" || config.nodeSigningKeyFile != keyFile {
		t.Fatalf("config = %#v", config)
	}
	url, err := readDatabaseURL(config.databaseURLFile)
	if err != nil || url != "postgres://user:password@db.example/cloud?sslmode=require" {
		t.Fatalf("database URL = %q err=%v", url, err)
	}
	if key, err := readNodeSigningKey(config.nodeSigningKeyFile); err != nil || len(key) != ed25519.PrivateKeySize {
		t.Fatalf("node signing key is not readable: len=%d err=%v", len(key), err)
	}
}

func TestParseConfigRejectsUnsafeStartupSettings(t *testing.T) {
	env := map[string]string{
		"CLOUD_ORCHESTRATOR_DATABASE_URL_FILE": "missing",
		"CLOUD_ORCHESTRATOR_RESEARCHER_URL":    "http://researcher.example/v2/cloud-research",
	}
	if _, err := parseConfig([]string{"--lease", "1m", "--attempt-timeout", "1m"}, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("attempt timeout equal to lease must be rejected")
	}
	if _, err := parseConfig([]string{"--database-url", "postgres://must-not-be-a-flag"}, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("database URL flag must not exist")
	}
	validEndpointEnv := map[string]string{
		"CLOUD_ORCHESTRATOR_DATABASE_URL_FILE": "missing",
		"CLOUD_ORCHESTRATOR_RESEARCHER_URL":    "https://researcher.example/v2/cloud-research",
	}
	if _, err := parseConfig(nil, func(key string) string { return validEndpointEnv[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("orchestrator must require a dedicated mutual-TLS researcher identity")
	}
	validEndpointEnv["CLOUD_ORCHESTRATOR_RESEARCHER_CA_FILE"] = "ca.pem"
	validEndpointEnv["CLOUD_ORCHESTRATOR_RESEARCHER_CERT_FILE"] = "client.pem"
	validEndpointEnv["CLOUD_ORCHESTRATOR_RESEARCHER_KEY_FILE"] = "client.key"
	validEndpointEnv["CLOUD_ORCHESTRATOR_RESEARCHER_SERVER_NAME"] = "researcher.example"
	validEndpointEnv["CLOUD_ORCHESTRATOR_NODE_SIGNING_KEY_FILE"] = "node-key.pem"
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

func TestReadNodeSigningKeyRejectsWrongMaterial(t *testing.T) {
	directory := t.TempDir()
	wrongFile := filepath.Join(directory, "wrong.pem")
	if err := os.WriteFile(wrongFile, []byte("not-a-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readNodeSigningKey(wrongFile); err == nil {
		t.Fatal("non-PEM node signing key must be rejected")
	}
}

func TestRunIterationAttemptsEveryIndependentOutboxAfterFailures(t *testing.T) {
	researchFailure := errors.New("research unavailable")
	registrationFailure := errors.New("connection registration unavailable")
	quoteFailure := errors.New("quote unavailable")
	research := &recordingIterationRunner{processed: true, err: researchFailure}
	registration := &recordingIterationRunner{processed: true, err: registrationFailure}
	quote := &recordingIterationRunner{processed: true, err: quoteFailure}
	processed, err := runIteration(t.Context(), research, registration, quote)
	if !processed || research.calls != 1 || registration.calls != 1 || quote.calls != 1 {
		t.Fatalf("iteration = processed:%v research_calls:%d registration_calls:%d quote_calls:%d", processed, research.calls, registration.calls, quote.calls)
	}
	if !errors.Is(err, researchFailure) || !errors.Is(err, registrationFailure) || !errors.Is(err, quoteFailure) {
		t.Fatalf("iteration error = %v, want all runner failures", err)
	}
}

type recordingIterationRunner struct {
	processed bool
	err       error
	calls     int
}

func (r *recordingIterationRunner) RunOnce(context.Context) (bool, error) {
	r.calls++
	return r.processed, r.err
}

func writeNodeSigningKey(path string) error {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return err
	}
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}), 0o600)
}
