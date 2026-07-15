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
	if config.trustedOCICatalogFile != "" || config.trustedOCIArchiveFile != "" {
		t.Fatalf("trusted OCI artifact transfer must be disabled by default: %#v", config)
	}
	env[trustedOCIArchiveFileEnv] = filepath.Join(directory, "trusted-oci-artifact.tar")
	if _, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("trusted OCI archive must require its controller catalog")
	}
	delete(env, trustedOCIArchiveFileEnv)
	env[trustedOCICatalogFileEnv] = filepath.Join(directory, "controller-trusted-artifact-catalog.json")
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || enabled.trustedOCICatalogFile != env[trustedOCICatalogFileEnv] {
		t.Fatalf("trusted OCI catalog config=%#v error=%v", enabled, err)
	}
	singleCatalog := env[trustedOCICatalogFileEnv]
	delete(env, trustedOCICatalogFileEnv)
	firstCatalog := filepath.Join(directory, "recipe-a", "controller-catalog.json")
	secondCatalog := filepath.Join(directory, "recipe-b", "controller-catalog.json")
	firstArchive := filepath.Join(directory, "recipe-a", "artifact.tar")
	secondArchive := filepath.Join(directory, "recipe-b", "artifact.tar")
	env[trustedOCICatalogFilesEnv] = firstCatalog + ";" + secondCatalog
	env[trustedOCIArchiveFilesEnv] = firstArchive + ";" + secondArchive
	multiple, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil })
	if err != nil || len(multiple.trustedOCICatalogFiles) != 2 || len(multiple.trustedOCIArchiveFiles) != 2 || multiple.trustedOCICatalogFiles[1] != secondCatalog || multiple.trustedOCIArchiveFiles[0] != firstArchive {
		t.Fatalf("multiple trusted OCI artifact config=%#v error=%v", multiple, err)
	}
	env[trustedOCICatalogFilesEnv] = firstCatalog + ";" + firstCatalog
	if _, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("duplicate trusted OCI catalog paths must be rejected")
	}
	delete(env, trustedOCICatalogFilesEnv)
	delete(env, trustedOCIArchiveFilesEnv)
	env[trustedOCICatalogFileEnv] = singleCatalog
	url, err := readDatabaseURL(config.databaseURLFile)
	if err != nil || url != "postgres://user:password@db.example/cloud?sslmode=require" {
		t.Fatalf("database URL = %q err=%v", url, err)
	}
	if key, err := readNodeSigningKey(config.nodeSigningKeyFile); err != nil || len(key) != ed25519.PrivateKeySize {
		t.Fatalf("node signing key is not readable: len=%d err=%v", len(key), err)
	}
	env[serviceMonitorEnabledEnv] = "true"
	if _, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("continuous service monitor must require the readiness runner")
	}
	env[serviceReadinessEnabledEnv] = "true"
	if _, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("service readiness must not be enabled without the sealed Recipe install runner")
	}
	env[recipeInstallEnabledEnv] = "true"
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || !enabled.recipeInstallEnabled || !enabled.serviceReadinessEnabled || !enabled.serviceMonitorEnabled {
		t.Fatalf("enabled Recipe/readiness config=%#v error=%v", enabled, err)
	}
	env[serviceDestroyEnabledEnv] = "true"
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || !enabled.serviceDestroyEnabled {
		t.Fatalf("enabled service destroy config=%#v error=%v", enabled, err)
	}
	env[deploymentCreateEnabledEnv] = "true"
	if _, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("deployment.create must require trusted manifest registration")
	}
	env[recipeManifestEnabledEnv] = "true"
	if _, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("deployment.create must require a verified trusted OCI archive")
	}
	env[trustedOCIArchiveFileEnv] = filepath.Join(directory, "trusted-oci-artifact.tar")
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || !enabled.deploymentCreateEnabled || !enabled.recipeManifestEnabled {
		t.Fatalf("enabled deployment execution config=%#v error=%v", enabled, err)
	}
	env[serviceOperationEnabledEnv] = "true"
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || !enabled.serviceOperationEnabled {
		t.Fatalf("enabled service operation config=%#v error=%v", enabled, err)
	}
	env[serviceBackupEnabledEnv] = "true"
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || !enabled.serviceBackupEnabled {
		t.Fatalf("enabled service backup config=%#v error=%v", enabled, err)
	}
	env[serviceRestorePlanEnabledEnv] = "true"
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || !enabled.serviceRestorePlanEnabled {
		t.Fatalf("enabled service restore plan config=%#v error=%v", enabled, err)
	}
	env[serviceRestoreEnabledEnv] = "true"
	if enabled, err := parseConfig(nil, func(key string) string { return env[key] }, func() (string, error) { return "host-a", nil }); err != nil || !enabled.serviceRestoreEnabled {
		t.Fatalf("enabled service restore config=%#v error=%v", enabled, err)
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
	validEndpointEnv[serviceDestroyEnabledEnv] = "sometimes"
	if _, err := parseConfig(nil, func(key string) string { return validEndpointEnv[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("unknown service destroy gate value must fail closed")
	}
	delete(validEndpointEnv, serviceDestroyEnabledEnv)
	validEndpointEnv[serviceRestorePlanEnabledEnv] = "sometimes"
	if _, err := parseConfig(nil, func(key string) string { return validEndpointEnv[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("unknown service restore plan gate value must fail closed")
	}
	delete(validEndpointEnv, serviceRestorePlanEnabledEnv)
	validEndpointEnv[serviceRestoreEnabledEnv] = "sometimes"
	if _, err := parseConfig(nil, func(key string) string { return validEndpointEnv[key] }, func() (string, error) { return "host-a", nil }); err == nil {
		t.Fatal("unknown service restore gate value must fail closed")
	}
	delete(validEndpointEnv, serviceRestoreEnabledEnv)
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
	deploymentFailure := errors.New("deployment unavailable")
	observationFailure := errors.New("worker observation unavailable")
	manifestFailure := errors.New("recipe manifest registration unavailable")
	executionProbeFailure := errors.New("execution probe unavailable")
	monitorFailure := errors.New("service monitor unavailable")
	readinessFailure := errors.New("service readiness unavailable")
	destroyFailure := errors.New("service destroy unavailable")
	secretObserveFailure := errors.New("service secret observation unavailable")
	research := &recordingIterationRunner{processed: true, err: researchFailure}
	registration := &recordingIterationRunner{processed: true, err: registrationFailure}
	quote := &recordingIterationRunner{processed: true, err: quoteFailure}
	deployment := &recordingIterationRunner{processed: true, err: deploymentFailure}
	observation := &recordingIterationRunner{processed: true, err: observationFailure}
	manifest := &recordingIterationRunner{processed: true, err: manifestFailure}
	executionProbe := &recordingIterationRunner{processed: true, err: executionProbeFailure}
	monitor := &recordingIterationRunner{processed: true, err: monitorFailure}
	readiness := &recordingIterationRunner{processed: true, err: readinessFailure}
	destroy := &recordingIterationRunner{processed: true, err: destroyFailure}
	secretObserver := &recordingIterationRunner{processed: true, err: secretObserveFailure}
	processed, err := runIteration(t.Context(), research, registration, quote, deployment, observation, manifest, executionProbe, nil, monitor, readiness, nil, nil, nil, nil, destroy, secretObserver)
	if !processed || research.calls != 1 || registration.calls != 1 || quote.calls != 1 || deployment.calls != 1 || observation.calls != 1 || manifest.calls != 1 || executionProbe.calls != 1 || monitor.calls != 1 || readiness.calls != 1 || destroy.calls != 1 || secretObserver.calls != 1 {
		t.Fatalf("iteration = processed:%v research_calls:%d registration_calls:%d quote_calls:%d deployment_calls:%d observation_calls:%d execution_probe_calls:%d", processed, research.calls, registration.calls, quote.calls, deployment.calls, observation.calls, executionProbe.calls)
	}
	if !errors.Is(err, researchFailure) || !errors.Is(err, registrationFailure) || !errors.Is(err, quoteFailure) || !errors.Is(err, deploymentFailure) || !errors.Is(err, observationFailure) || !errors.Is(err, manifestFailure) || !errors.Is(err, executionProbeFailure) || !errors.Is(err, monitorFailure) || !errors.Is(err, readinessFailure) || !errors.Is(err, destroyFailure) || !errors.Is(err, secretObserveFailure) {
		t.Fatalf("iteration error = %v, want all runner failures", err)
	}
}

func TestRunIterationAllowsProvisioningToRemainDisabledWhileRestrictedWorkersRun(t *testing.T) {
	research := &recordingIterationRunner{processed: true}
	registration := &recordingIterationRunner{processed: true}
	quote := &recordingIterationRunner{processed: true}
	observation := &recordingIterationRunner{processed: true}
	executionProbe := &recordingIterationRunner{processed: true}

	processed, err := runIteration(t.Context(), research, registration, quote, nil, observation, nil, executionProbe, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil || !processed {
		t.Fatalf("iteration = processed:%v err:%v", processed, err)
	}
	if research.calls != 1 || registration.calls != 1 || quote.calls != 1 || observation.calls != 1 || executionProbe.calls != 1 {
		t.Fatalf("enabled work must continue while provisioning is disabled: research=%d registration=%d quote=%d observation=%d execution_probe=%d", research.calls, registration.calls, quote.calls, observation.calls, executionProbe.calls)
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
