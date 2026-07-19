package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreWorkerBootstrapQueuesPrivateExecutionProbeWithoutPublicArtifactLeak(t *testing.T) {
	now, database, store, bootstrapClaim := prepareWorkerBootstrapObservationClaim(t)
	signed := signedWorkerBootstrapObservationCommand(t, bootstrapClaim, now)
	if err := store.PersistWorkerBootstrapObservationCommand(context.Background(), bootstrapClaim, signed); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitWorkerBootstrapObservation(context.Background(), bootstrapClaim, validWorkerBootstrapObservation(bootstrapClaim, now, 2)); err != nil {
		t.Fatal(err)
	}

	var taskID, manifestDigest, inputDigest, taskStatus string
	var manifestCBOR, inputCBOR []byte
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT task_id, execution_manifest_cbor, execution_manifest_digest, input_cbor, input_digest, task_status
		FROM p2p_cloud_execution_probe_tasks WHERE deployment_id = $1
	`, bootstrapClaim.DeploymentID).Scan(&taskID, &manifestCBOR, &manifestDigest, &inputCBOR, &inputDigest, &taskStatus); err != nil {
		t.Fatal(err)
	}
	if taskID == "" || len(manifestCBOR) == 0 || len(inputCBOR) == 0 || !executionProbeDigest(manifestDigest) || !executionProbeDigest(inputDigest) || taskStatus != "unissued" {
		t.Fatalf("private execution probe task = id:%q manifest:%q input:%q status:%q", taskID, manifestDigest, inputDigest, taskStatus)
	}

	var outboxID, payload string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT outbox_id, payload_json FROM p2p_cloud_outbox WHERE aggregate_id = $1 AND kind = $2
	`, taskID, runtime.ExecutionProbeIssueRequested).Scan(&outboxID, &payload); err != nil {
		t.Fatal(err)
	}
	deploymentID, payloadTaskID, err := decodeExecutionProbeIssueOutbox(payload)
	if err != nil || deploymentID != bootstrapClaim.DeploymentID || payloadTaskID != taskID || containsAny(payload, []string{manifestDigest, inputDigest, "cbor", "recipe", "secret", "token"}) {
		t.Fatalf("execution probe outbox payload=%q deployment=%q task=%q err=%v", payload, deploymentID, payloadTaskID, err)
	}

	var jobExecution, jobOutcome, checkpoint string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT execution_status, outcome_status, checkpoint FROM p2p_cloud_jobs WHERE job_id = $1
	`, executionProbeJobID(taskID)).Scan(&jobExecution, &jobOutcome, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if jobExecution != "queued" || jobOutcome != "pending" || checkpoint != "execution_probe_queued" {
		t.Fatalf("execution probe verify job=%s/%s/%s", jobExecution, jobOutcome, checkpoint)
	}
	for table, field := range map[string]string{
		"p2p_cloud_projection_outbox": "payload_json",
		"p2p_cloud_events":            "summary_json",
	} {
		rows, err := database.DB().QueryContext(context.Background(), `
			SELECT `+field+` FROM `+table+` WHERE type = 'cloud.job.changed'
		`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var projected string
			if err := rows.Scan(&projected); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			if strings.Contains(projected, manifestDigest) || strings.Contains(projected, inputDigest) || containsAny(projected, []string{"execution_manifest_cbor", "input_cbor", "worker_session", "bootstrap_session", "secret_ref"}) {
				rows.Close()
				t.Fatalf("private execution probe material leaked into %s: %s", table, projected)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		rows.Close()
	}
	if outboxID == "" {
		t.Fatal("execution probe issue outbox id is empty")
	}
}

func TestStoreExecutionProbeVerifyJobPassesProjectionRelayWithoutArtifacts(t *testing.T) {
	_, database, _, bootstrapClaim := prepareExecutionProbeTask(t)
	var taskID, manifestDigest, inputDigest string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT task_id, execution_manifest_digest, input_digest
		FROM p2p_cloud_execution_probe_tasks WHERE deployment_id = $1
	`, bootstrapClaim.DeploymentID).Scan(&taskID, &manifestDigest, &inputDigest); err != nil {
		t.Fatal(err)
	}
	published := make([]map[string]any, 0)
	relay := cloudmodule.NewProjectionRelay(database, func(_ context.Context, _ string, eventType string, payload map[string]any) error {
		if eventType == "cloud.job.changed" {
			published = append(published, payload)
		}
		return nil
	}, cloudmodule.ProjectionRelayConfig{WorkerID: "execution-probe-projection-test"})
	for index := 0; index < 64; index++ {
		processed, err := relay.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	jobID := executionProbeJobID(taskID)
	for _, payload := range published {
		if payload["job_id"] != jobID {
			continue
		}
		if payload["kind"] != "verify" || payload["checkpoint"] != "execution_probe_queued" ||
			strings.Contains(payloadString(payload), manifestDigest) || strings.Contains(payloadString(payload), inputDigest) {
			t.Fatalf("verify Job relay payload=%#v", payload)
		}
		return
	}
	t.Fatalf("execution probe verify Job %q was not relayed", jobID)
}

func TestStoreExecutionProbeIssueObserveAndSuccessNeverMakesDeploymentReady(t *testing.T) {
	now, database, store, _ := prepareExecutionProbeTask(t)
	issue, found, err := store.ClaimExecutionProbe(context.Background(), "orchestrator-execution-probe-issue", time.Minute)
	if err != nil || !found || issue.Phase != runtime.ExecutionProbePhaseIssue {
		t.Fatalf("issue claim=%#v found=%v err=%v", issue, found, err)
	}
	if err := store.MarkExecutionProbeStarted(context.Background(), issue); err != nil {
		t.Fatal(err)
	}
	issueSigned := signedExecutionProbeTransportCommand(t, issue, now)
	if err := store.PersistExecutionProbeCommand(context.Background(), issue, issueSigned); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitExecutionProbe(context.Background(), issue, executionProbeResult(issue, now, "queued", 0)); err != nil {
		t.Fatal(err)
	}
	assertExecutionProbeJob(t, database, issue.JobID, "verifying", "pending", "execution_probe_issued")

	observeNow := now.Add(executionProbeObserveDelay + time.Millisecond)
	store.cfg.Now = func() time.Time { return observeNow }
	observe, found, err := store.ClaimExecutionProbe(context.Background(), "orchestrator-execution-probe-observe", time.Minute)
	if err != nil || !found || observe.Phase != runtime.ExecutionProbePhaseObserve {
		t.Fatalf("observe claim=%#v found=%v err=%v", observe, found, err)
	}
	observeSigned := signedExecutionProbeTransportCommand(t, observe, observeNow)
	if err := store.PersistExecutionProbeCommand(context.Background(), observe, observeSigned); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitExecutionProbe(context.Background(), observe, executionProbeResult(observe, observeNow, "running", 1)); err != nil {
		t.Fatal(err)
	}
	assertExecutionProbeJob(t, database, observe.JobID, "verifying", "pending", runtime.ExecutionProbeReceived)

	successNow := observeNow.Add(executionProbeObserveDelay + time.Millisecond)
	store.cfg.Now = func() time.Time { return successNow }
	success, found, err := store.ClaimExecutionProbe(context.Background(), "orchestrator-execution-probe-success", time.Minute)
	if err != nil || !found || success.Phase != runtime.ExecutionProbePhaseObserve {
		t.Fatalf("success claim=%#v found=%v err=%v", success, found, err)
	}
	successSigned := signedExecutionProbeTransportCommand(t, success, successNow)
	if err := store.PersistExecutionProbeCommand(context.Background(), success, successSigned); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitExecutionProbe(context.Background(), success, executionProbeResult(success, successNow, "succeeded", 2)); err != nil {
		t.Fatal(err)
	}
	assertExecutionProbeJob(t, database, success.JobID, "finished", "succeeded", runtime.ExecutionProbeTransportPassed)

	var deploymentExecution, deploymentOutcome, resourceStatus string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT execution_status, outcome_status, resource_status FROM p2p_cloud_deployments WHERE deployment_id = $1
	`, success.DeploymentID).Scan(&deploymentExecution, &deploymentOutcome, &resourceStatus); err != nil {
		t.Fatal(err)
	}
	if deploymentExecution != "verifying" || deploymentOutcome != "pending" || resourceStatus != "active" {
		t.Fatalf("task transport success changed deployment to %s/%s/%s", deploymentExecution, deploymentOutcome, resourceStatus)
	}
}

func TestStoreExecutionProbeExpiresOnlyAfterExplicitBrokerExpiry(t *testing.T) {
	now, _, store, _ := prepareExecutionProbeTask(t)
	first, found, err := store.ClaimExecutionProbe(context.Background(), "orchestrator-execution-probe-expiry", time.Minute)
	if err != nil || !found {
		t.Fatalf("first claim found=%v err=%v", found, err)
	}
	firstSigned := signedExecutionProbeTransportCommand(t, first, now)
	if err := store.PersistExecutionProbeCommand(context.Background(), first, firstSigned); err != nil {
		t.Fatal(err)
	}
	if err := store.DeferExecutionProbe(context.Background(), first, "broker_unavailable", now); err != nil {
		t.Fatal(err)
	}
	second, found, err := store.ClaimExecutionProbe(context.Background(), "orchestrator-execution-probe-replay", time.Minute)
	if err != nil || !found {
		t.Fatalf("replay claim found=%v err=%v", found, err)
	}
	if second.Command.CommandID != first.Command.CommandID || second.Command.NodeCounter != first.Command.NodeCounter || second.Command.Attempt != first.Command.Attempt {
		t.Fatalf("retry did not preserve exact command: first=%#v second=%#v", first.Command, second.Command)
	}
	if err := store.ExpireExecutionProbeCommand(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	third, found, err := store.ClaimExecutionProbe(context.Background(), "orchestrator-execution-probe-next", time.Minute)
	if err != nil || !found {
		t.Fatalf("post-expiry claim found=%v err=%v", found, err)
	}
	if third.Command.CommandID == first.Command.CommandID || third.Command.NodeCounter <= first.Command.NodeCounter || third.Command.Attempt != first.Command.Attempt+1 {
		t.Fatalf("explicit expiry did not allocate the next command: first=%#v third=%#v", first.Command, third.Command)
	}
}

func TestStoreExecutionProbeRejectsStaleOrWrongEvidence(t *testing.T) {
	now, _, store, _ := prepareExecutionProbeTask(t)
	issue, found, err := store.ClaimExecutionProbe(context.Background(), "orchestrator-execution-probe-reject", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	signed := signedExecutionProbeTransportCommand(t, issue, now)
	if err := store.PersistExecutionProbeCommand(context.Background(), issue, signed); err != nil {
		t.Fatal(err)
	}
	wrong := executionProbeResult(issue, now, "running", 1)
	badDigest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	wrong.EvidenceDigest = &badDigest
	if err := store.CommitExecutionProbe(context.Background(), issue, wrong); err == nil {
		t.Fatal("wrong execution manifest evidence was accepted")
	}
}

func prepareExecutionProbeTask(t *testing.T) (time.Time, *p2pstorage.DatabaseStore, *Store, runtime.WorkerBootstrapObservationClaim) {
	t.Helper()
	now, database, store, bootstrapClaim := prepareWorkerBootstrapObservationClaim(t)
	signed := signedWorkerBootstrapObservationCommand(t, bootstrapClaim, now)
	if err := store.PersistWorkerBootstrapObservationCommand(context.Background(), bootstrapClaim, signed); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitWorkerBootstrapObservation(context.Background(), bootstrapClaim, validWorkerBootstrapObservation(bootstrapClaim, now, 2)); err != nil {
		t.Fatal(err)
	}
	return now, database, store, bootstrapClaim
}

func signedExecutionProbeTransportCommand(t *testing.T, claim runtime.ExecutionProbeClaim, now time.Time) runtime.SignedExecutionProbeCommand {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if claim.Phase == runtime.ExecutionProbePhaseIssue {
		signed, err := transport.BuildExecutionProbeIssueCommand(claim.Command, claim.IssueRequest, now)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	signed, err := transport.BuildExecutionProbeObserveCommand(claim.Command, claim.ObserveRequest, now)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func executionProbeResult(claim runtime.ExecutionProbeClaim, now time.Time, status string, sequence int64) runtime.ExecutionProbeTaskResult {
	result := runtime.ExecutionProbeTaskResult{
		TaskID: claim.TaskID, DeploymentID: claim.DeploymentID, Status: status, Attempt: claim.TaskAttempt, LastSequence: sequence, UpdatedAt: now,
	}
	switch status {
	case "running":
		checkpoint := runtime.ExecutionProbeReceived
		evidence := claim.ExecutionManifestDigest
		result.Checkpoint, result.EvidenceDigest = &checkpoint, &evidence
	case "succeeded":
		checkpoint := runtime.ExecutionProbeTransportPassed
		evidence := claim.ExecutionManifestDigest
		result.Checkpoint, result.EvidenceDigest = &checkpoint, &evidence
	case "failed", "interrupted":
		code := "execution_probe_test_failure"
		result.ErrorCode = &code
	}
	return result
}

func assertExecutionProbeJob(t *testing.T, database *p2pstorage.DatabaseStore, jobID, execution, outcome, checkpoint string) {
	t.Helper()
	var actualExecution, actualOutcome, actualCheckpoint string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT execution_status, outcome_status, checkpoint FROM p2p_cloud_jobs WHERE job_id = $1
	`, jobID).Scan(&actualExecution, &actualOutcome, &actualCheckpoint); err != nil {
		t.Fatal(err)
	}
	if actualExecution != execution || actualOutcome != outcome || actualCheckpoint != checkpoint {
		t.Fatalf("execution probe job=%s/%s/%s, want %s/%s/%s", actualExecution, actualOutcome, actualCheckpoint, execution, outcome, checkpoint)
	}
}

func payloadString(payload map[string]any) string {
	encoded, _ := json.Marshal(payload)
	return string(encoded)
}
