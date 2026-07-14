package main

import (
	"path/filepath"
	"testing"
)

func TestParseConfigRequiresOnlyStrictWorkerBootstrapInputs(t *testing.T) {
	directory := t.TempDir()
	environment := map[string]string{
		bootstrapManifestFileEnv: filepath.Join(directory, "manifest.json"),
		expectedConnectionIDEnv:  "connection-v2-0001",
		expectedEndpointEnv:      "https://broker.example.invalid/v2/worker-sessions",
		identityDocumentFileEnv:  filepath.Join(directory, "identity-document.b64"),
		identitySignatureFileEnv: filepath.Join(directory, "identity-signature.b64"),
		"AWS_ACCESS_KEY_ID":      "must-not-be-read",
	}
	config, err := parseConfig([]string{"--once", "--heartbeat-interval", "15s"}, func(key string) string { return environment[key] })
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if !config.once || config.heartbeatInterval.String() != "15s" || config.expectedConnection != "connection-v2-0001" {
		t.Fatalf("config = %#v", config)
	}
	delete(environment, identitySignatureFileEnv)
	if _, err := parseConfig(nil, func(key string) string { return environment[key] }); err == nil {
		t.Fatal("parseConfig() accepted a missing identity proof file")
	}
}
