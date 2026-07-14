package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker"
)

func TestParseConfigUsesOnlyStrictWorkerBootstrapInputs(t *testing.T) {
	directory := t.TempDir()
	environment := map[string]string{
		bootstrapManifestFileEnv:            filepath.Join(directory, "manifest.json"),
		expectedConnectionIDEnv:             "connection-v2-0001",
		expectedEndpointEnv:                 "https://broker.example.invalid/v2/worker-sessions",
		"AWS_ACCESS_KEY_ID":                 "must-not-be-read",
		"AWS_EC2_METADATA_SERVICE_ENDPOINT": "http://metadata.example.invalid",
	}
	config, err := parseConfig([]string{"--once", "--heartbeat-interval", "15s"}, func(key string) string { return environment[key] })
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if !config.once || config.heartbeatInterval.String() != "15s" || config.expectedConnection != "connection-v2-0001" {
		t.Fatalf("config = %#v", config)
	}
	delete(environment, expectedEndpointEnv)
	if _, err := parseConfig(nil, func(key string) string { return environment[key] }); err == nil {
		t.Fatal("parseConfig() accepted a missing immutable bootstrap endpoint")
	}
}

type recordingIdentityProofProvider struct {
	proof cloudworker.InstanceIdentityProof
	err   error
	calls int
}

func (provider *recordingIdentityProofProvider) Fetch(context.Context) (cloudworker.InstanceIdentityProof, error) {
	provider.calls++
	return provider.proof, provider.err
}

type recordingWorkerSessionClient struct {
	claimProofs    []cloudworker.InstanceIdentityProof
	heartbeat      func(context.Context) error
	retryCalls     int
	closeCalls     int
	heartbeatCalls int
}

func (client *recordingWorkerSessionClient) Claim(_ context.Context, proof cloudworker.InstanceIdentityProof) error {
	client.claimProofs = append(client.claimProofs, proof)
	return nil
}

func (client *recordingWorkerSessionClient) Heartbeat(ctx context.Context) error {
	client.heartbeatCalls++
	if client.heartbeat == nil {
		return nil
	}
	return client.heartbeat(ctx)
}

func (client *recordingWorkerSessionClient) RetryPending(context.Context) error {
	client.retryCalls++
	return errors.New("retry must not run during shutdown")
}

func (client *recordingWorkerSessionClient) Close() {
	client.closeCalls++
}

func writeWorkerBootstrapConfig(t *testing.T, now time.Time, once bool, interval time.Duration) commandConfig {
	t.Helper()
	endpoint := "https://broker.example.invalid/v2/worker-sessions"
	manifest := cloudworker.BootstrapManifest{
		Schema:                 cloudworker.BootstrapManifestV1Schema,
		ConnectionID:           "connection-v2-0001",
		DeploymentID:           "deployment-v2-001",
		BootstrapSessionID:     "worker-session-v2-001",
		BootstrapEndpoint:      endpoint,
		WorkerImageDigest:      "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ArtifactManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ExpiresAt:              now.UTC().Add(5 * time.Minute).Format("2006-01-02T15:04:05.000Z"),
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal bootstrap manifest: %v", err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap-manifest.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write bootstrap manifest: %v", err)
	}
	return commandConfig{
		manifestFile:       path,
		expectedConnection: manifest.ConnectionID,
		expectedEndpoint:   endpoint,
		once:               once,
		heartbeatInterval:  interval,
	}
}

func validWorkerIdentityProof() cloudworker.InstanceIdentityProof {
	return cloudworker.InstanceIdentityProof{
		DocumentB64:  base64.StdEncoding.EncodeToString([]byte(`{"instanceId":"i-0123456789abcdef0"}`)),
		SignatureB64: base64.StdEncoding.EncodeToString([]byte("iid-signature")),
	}
}

func TestRunWithDependenciesClaimsFromIMDSProofThenHeartbeats(t *testing.T) {
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := &recordingIdentityProofProvider{proof: validWorkerIdentityProof()}
	client := &recordingWorkerSessionClient{}
	client.heartbeat = func(context.Context) error {
		cancel()
		return nil
	}
	var factoryCalls int
	err := runWithDependencies(ctx, writeWorkerBootstrapConfig(t, now, false, time.Millisecond), provider,
		func(manifest cloudworker.BootstrapManifest, config cloudworker.SessionClientConfig) (workerSessionClient, error) {
			factoryCalls++
			if manifest.ConnectionID != "connection-v2-0001" || config.ExpectedBootstrapEndpoint != manifest.BootstrapEndpoint || config.Now == nil {
				t.Fatalf("unexpected session factory input: manifest=%#v config=%#v", manifest, config)
			}
			return client, nil
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("runWithDependencies() error = %v", err)
	}
	if provider.calls != 1 || factoryCalls != 1 || len(client.claimProofs) != 1 || client.claimProofs[0] != provider.proof {
		t.Fatalf("worker bootstrap calls = provider:%d factory:%d claim:%#v", provider.calls, factoryCalls, client.claimProofs)
	}
	if client.heartbeatCalls != 1 || client.retryCalls != 0 || client.closeCalls != 1 {
		t.Fatalf("worker heartbeat lifecycle = %#v", client)
	}
}

func TestRunWithDependenciesStopsCleanlyWhenShutdownCancelsHeartbeat(t *testing.T) {
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := &recordingIdentityProofProvider{proof: validWorkerIdentityProof()}
	client := &recordingWorkerSessionClient{}
	heartbeatStarted := make(chan struct{})
	client.heartbeat = func(ctx context.Context) error {
		close(heartbeatStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	go func() {
		<-heartbeatStarted
		cancel()
	}()
	err := runWithDependencies(ctx, writeWorkerBootstrapConfig(t, now, false, time.Millisecond), provider,
		func(cloudworker.BootstrapManifest, cloudworker.SessionClientConfig) (workerSessionClient, error) {
			return client, nil
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("shutdown during heartbeat must exit cleanly, got %v", err)
	}
	if client.retryCalls != 0 || client.closeCalls != 1 {
		t.Fatalf("shutdown must not retry a canceled heartbeat: %#v", client)
	}
}

func TestRunWithDependenciesDoesNotClaimWhenIMDSProofFails(t *testing.T) {
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	provider := &recordingIdentityProofProvider{err: errors.New("IMDS unavailable")}
	client := &recordingWorkerSessionClient{}
	err := runWithDependencies(context.Background(), writeWorkerBootstrapConfig(t, now, true, time.Millisecond), provider,
		func(cloudworker.BootstrapManifest, cloudworker.SessionClientConfig) (workerSessionClient, error) {
			return client, nil
		},
		func() time.Time { return now },
	)
	if !errors.Is(err, errRunFailed) {
		t.Fatalf("IMDS failure error = %v, want %v", err, errRunFailed)
	}
	if provider.calls != 1 || len(client.claimProofs) != 0 || client.heartbeatCalls != 0 || client.closeCalls != 1 {
		t.Fatalf("failed IMDS proof must not reach Broker actions: provider=%d client=%#v", provider.calls, client)
	}
}
