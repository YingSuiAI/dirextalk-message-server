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
	retry          func(context.Context) error
	renew          func(context.Context, cloudworker.InstanceIdentityProof) error
	claimTask      func(context.Context) (cloudworker.WorkerTask, bool, error)
	retryTask      func(context.Context) error
	reportTask     func(context.Context, cloudworker.WorkerTask, cloudworker.TaskStatus, string, string, string) error
	renewProofs    []cloudworker.InstanceIdentityProof
	retryCalls     int
	renewCalls     int
	closeCalls     int
	heartbeatCalls int
	claimTaskCalls int
	retryTaskCalls int
	taskReports    []recordedTaskReport
}

type recordedTaskReport struct {
	task           cloudworker.WorkerTask
	status         cloudworker.TaskStatus
	checkpoint     string
	errorCode      string
	evidenceDigest string
}

type recordingRecipeTaskProcessor struct {
	calls int
	err   error
}

func (processor *recordingRecipeTaskProcessor) ProcessOne(context.Context) error {
	processor.calls++
	return processor.err
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

func (client *recordingWorkerSessionClient) RetryPending(ctx context.Context) error {
	client.retryCalls++
	if client.retry != nil {
		return client.retry(ctx)
	}
	return errors.New("retry must not run during shutdown")
}

func (client *recordingWorkerSessionClient) RenewIfDue(ctx context.Context, proof cloudworker.InstanceIdentityProof) error {
	client.renewCalls++
	client.renewProofs = append(client.renewProofs, proof)
	if client.renew != nil {
		return client.renew(ctx, proof)
	}
	return nil
}

func (client *recordingWorkerSessionClient) ClaimTask(ctx context.Context) (cloudworker.WorkerTask, bool, error) {
	client.claimTaskCalls++
	if client.claimTask != nil {
		return client.claimTask(ctx)
	}
	return cloudworker.WorkerTask{}, false, nil
}

func (client *recordingWorkerSessionClient) RetryPendingTask(ctx context.Context) error {
	client.retryTaskCalls++
	if client.retryTask != nil {
		return client.retryTask(ctx)
	}
	return cloudworker.ErrNoPendingTaskEvent
}

func (client *recordingWorkerSessionClient) ReportTask(ctx context.Context, task cloudworker.WorkerTask, status cloudworker.TaskStatus, checkpoint, errorCode, evidenceDigest string) error {
	client.taskReports = append(client.taskReports, recordedTaskReport{
		task:           task,
		status:         status,
		checkpoint:     checkpoint,
		errorCode:      errorCode,
		evidenceDigest: evidenceDigest,
	})
	if client.reportTask != nil {
		return client.reportTask(ctx, task, status, checkpoint, errorCode, evidenceDigest)
	}
	return nil
}

func (client *recordingWorkerSessionClient) Close() {
	client.closeCalls++
}

func writeWorkerBootstrapConfig(t *testing.T, now time.Time, once bool, interval time.Duration) commandConfig {
	return writeWorkerBootstrapConfigWithExpiry(t, now, now.Add(5*time.Minute), once, interval)
}

func writeWorkerBootstrapConfigWithExpiry(t *testing.T, now, expiresAt time.Time, once bool, interval time.Duration) commandConfig {
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
		ExpiresAt:              expiresAt.UTC().Format("2006-01-02T15:04:05.000Z"),
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

func TestWorkerCycleClaimsRecipeWorkOnlyThroughExplicitProcessorInjection(t *testing.T) {
	client := &recordingWorkerSessionClient{}
	processor := &recordingRecipeTaskProcessor{}
	if err := runWorkerCycle(context.Background(), client, validWorkerIdentityProof(), true); err != nil {
		t.Fatalf("default runWorkerCycle() error = %v", err)
	}
	if processor.calls != 0 {
		t.Fatalf("default cycle reached Recipe processor %d times", processor.calls)
	}
	if err := runWorkerCycleWithRecipe(context.Background(), client, validWorkerIdentityProof(), true, processor); err != nil {
		t.Fatalf("configured runWorkerCycleWithRecipe() error = %v", err)
	}
	if processor.calls != 1 {
		t.Fatalf("configured cycle Recipe calls = %d", processor.calls)
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
			if manifest.ConnectionID != "connection-v2-0001" || config.ExpectedBootstrapEndpoint != manifest.BootstrapEndpoint || config.Now == nil || !config.AllowExpiredBootstrap {
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

func TestRunWithDependenciesAllowsExpiredBootstrapOnlyForBrokerReauthentication(t *testing.T) {
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	provider := &recordingIdentityProofProvider{proof: validWorkerIdentityProof()}
	client := &recordingWorkerSessionClient{}
	err := runWithDependencies(context.Background(), writeWorkerBootstrapConfigWithExpiry(t, now, now.Add(-time.Minute), true, time.Millisecond), provider,
		func(_ cloudworker.BootstrapManifest, config cloudworker.SessionClientConfig) (workerSessionClient, error) {
			if !config.AllowExpiredBootstrap {
				t.Fatal("expired bootstrap was not marked for broker-side reauthentication")
			}
			return client, nil
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("expired active-session reauthentication attempt error = %v", err)
	}
	if provider.calls != 1 || len(client.claimProofs) != 1 || client.heartbeatCalls != 1 || client.closeCalls != 1 {
		t.Fatalf("expired bootstrap reauthentication lifecycle = provider:%d client:%#v", provider.calls, client)
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

func TestRunWithDependenciesRetriesTransientWorkerTransportFailuresUntilShutdown(t *testing.T) {
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := &recordingIdentityProofProvider{proof: validWorkerIdentityProof()}
	client := &recordingWorkerSessionClient{}
	client.heartbeat = func(context.Context) error {
		if client.heartbeatCalls == 2 {
			cancel()
		}
		return errors.New("temporary heartbeat outage")
	}
	client.retry = func(context.Context) error {
		return errors.New("temporary retry outage")
	}
	client.renew = func(context.Context, cloudworker.InstanceIdentityProof) error {
		return errors.New("temporary renewal outage")
	}
	err := runWithDependencies(ctx, writeWorkerBootstrapConfig(t, now, false, time.Millisecond), provider,
		func(cloudworker.BootstrapManifest, cloudworker.SessionClientConfig) (workerSessionClient, error) {
			return client, nil
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("transient worker transport failures must wait for shutdown, got %v", err)
	}
	if client.heartbeatCalls != 2 || client.retryCalls != 1 || client.renewCalls != 1 || len(client.renewProofs) != 1 || client.renewProofs[0] != provider.proof || client.closeCalls != 1 {
		t.Fatalf("transient failure lifecycle = %#v", client)
	}
}

func TestRunWithDependenciesReportsOnlyTheFixedExecutionProbe(t *testing.T) {
	now := time.Date(2026, 7, 15, 2, 0, 0, 0, time.UTC)
	baseTask := cloudworker.WorkerTask{
		Schema:                  cloudworker.WorkerTaskV1Schema,
		TaskID:                  "worker-task-v2-001",
		DeploymentID:            "deployment-v2-001",
		TaskKind:                cloudworker.TaskKindExecutionProbe,
		ExecutionManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InputDigest:             "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Attempt:                 1,
	}
	tests := []struct {
		name         string
		lastSequence uint64
		failFirst    bool
		wantError    bool
		wantReports  []recordedTaskReport
	}{
		{
			name:         "new probe emits fixed receive then transport checkpoints",
			lastSequence: 0,
			wantReports: []recordedTaskReport{
				{status: cloudworker.TaskStatusRunning, checkpoint: "execution_manifest_received", evidenceDigest: baseTask.ExecutionManifestDigest},
				{status: cloudworker.TaskStatusSucceeded, checkpoint: "task_transport_verified", evidenceDigest: baseTask.ExecutionManifestDigest},
			},
		},
		{
			name:         "reconnected probe continues after accepted running checkpoint",
			lastSequence: 1,
			wantReports: []recordedTaskReport{
				{status: cloudworker.TaskStatusSucceeded, checkpoint: "task_transport_verified", evidenceDigest: baseTask.ExecutionManifestDigest},
			},
		},
		{
			name:         "completed probe is left untouched after reconnect",
			lastSequence: 2,
		},
		{
			name:         "indeterminate running event never claims success",
			lastSequence: 0,
			failFirst:    true,
			wantError:    true,
			wantReports: []recordedTaskReport{
				{status: cloudworker.TaskStatusRunning, checkpoint: "execution_manifest_received", evidenceDigest: baseTask.ExecutionManifestDigest},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := baseTask
			task.LastSequence = test.lastSequence
			provider := &recordingIdentityProofProvider{proof: validWorkerIdentityProof()}
			client := &recordingWorkerSessionClient{}
			client.claimTask = func(context.Context) (cloudworker.WorkerTask, bool, error) {
				return task, true, nil
			}
			if test.failFirst {
				client.reportTask = func(_ context.Context, _ cloudworker.WorkerTask, _ cloudworker.TaskStatus, _ string, _ string, _ string) error {
					return errors.New("transport unavailable")
				}
			}
			err := runWithDependencies(context.Background(), writeWorkerBootstrapConfig(t, now, true, time.Millisecond), provider,
				func(cloudworker.BootstrapManifest, cloudworker.SessionClientConfig) (workerSessionClient, error) {
					return client, nil
				},
				func() time.Time { return now },
			)
			if test.wantError {
				if !errors.Is(err, errRunFailed) {
					t.Fatalf("runWithDependencies() error = %v, want %v", err, errRunFailed)
				}
			} else if err != nil {
				t.Fatalf("runWithDependencies() error = %v", err)
			}
			if client.claimTaskCalls != 1 || client.retryTaskCalls != 1 || len(client.taskReports) != len(test.wantReports) {
				t.Fatalf("task lifecycle = %#v", client)
			}
			for index, want := range test.wantReports {
				got := client.taskReports[index]
				if got.status != want.status || got.checkpoint != want.checkpoint || got.errorCode != "" || got.evidenceDigest != want.evidenceDigest || got.task.TaskKind != cloudworker.TaskKindExecutionProbe {
					t.Fatalf("task report[%d] = %#v, want %#v", index, got, want)
				}
			}
		})
	}
}
