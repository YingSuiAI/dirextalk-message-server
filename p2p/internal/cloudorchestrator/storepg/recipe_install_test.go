package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreRecipeInstallLeaseRecoveryAndSuccess(t *testing.T) {
	ctx := context.Background()
	now, database, store, bootstrap := prepareExecutionProbeTask(t)
	manifest, jobID, outboxID := seedApprovedRecipeInstall(t, ctx, database, bootstrap, now)

	clock := now.Add(3 * time.Minute)
	leaseNumber := 0
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string {
		leaseNumber++
		return "recipe-install-lease-" + string(rune('0'+leaseNumber))
	}

	issue, found, err := store.ClaimRecipeInstall(ctx, "recipe-install-issue-1", time.Minute)
	if err != nil || !found || issue.Phase != runtime.RecipeInstallPhaseIssue {
		t.Fatalf("issue claim=%#v found=%v err=%v", issue, found, err)
	}
	if issue.ExecutionID != manifest.ExecutionID || issue.JobID != jobID || issue.OutboxID != outboxID {
		t.Fatalf("issue binding execution=%q job=%q outbox=%q", issue.ExecutionID, issue.JobID, issue.OutboxID)
	}
	issueSigned := signedRecipeInstallCommand(t, issue, clock)
	if err := store.PersistRecipeInstallCommand(ctx, issue, issueSigned); err != nil {
		t.Fatal(err)
	}
	deferUntil := clock.Add(2 * time.Minute)
	if err := store.DeferRecipeInstall(ctx, issue, "broker_unavailable", deferUntil); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRecipeInstallStarted(ctx, issue); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale issue lease error=%v, want %v", err, ErrLeaseLost)
	}

	clock = deferUntil
	reclaimed, found, err := store.ClaimRecipeInstall(ctx, "recipe-install-issue-2", time.Minute)
	if err != nil || !found || reclaimed.Phase != runtime.RecipeInstallPhaseIssue {
		t.Fatalf("reclaimed issue=%#v found=%v err=%v", reclaimed, found, err)
	}
	if reclaimed.LeaseToken == issue.LeaseToken || reclaimed.Command.CommandID != issue.Command.CommandID ||
		reclaimed.Command.SignedEnvelope != issueSigned.EnvelopeJSON || reclaimed.Command.PayloadJSON != issueSigned.PayloadJSON ||
		reclaimed.Command.PayloadSHA256 != issueSigned.PayloadSHA256 || reclaimed.Command.RequestSHA256 != issueSigned.RequestSHA256 {
		t.Fatalf("reclaim did not replay the exact signed command: old=%#v new=%#v", issue.Command, reclaimed.Command)
	}
	wrongBinding := reclaimed
	wrongBinding.ConnectionID = "connection-wrong-binding"
	if err := store.MarkRecipeInstallStarted(ctx, wrongBinding); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong binding error=%v, want %v", err, ErrLeaseLost)
	}
	if err := store.MarkRecipeInstallStarted(ctx, reclaimed); err != nil {
		t.Fatal(err)
	}
	queued := runtime.RecipeInstallResult{
		ExecutionID: reclaimed.ExecutionID, DeploymentID: reclaimed.DeploymentID, TaskID: reclaimed.TaskID,
		Status: "queued", Attempt: reclaimed.TaskAttempt, UpdatedAt: clock,
	}
	if err := runtime.ValidateRecipeInstallResult(reclaimed, queued, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitRecipeInstall(ctx, reclaimed, queued); err != nil {
		t.Fatal(err)
	}
	assertRecipeInstallState(t, database, jobID, outboxID, "installing", "pending", "install_issued", "running", "install_issued", true)

	clock = clock.Add(2 * time.Second)
	observe, found, err := store.ClaimRecipeInstall(ctx, "recipe-install-observe", time.Minute)
	if err != nil || !found || observe.Phase != runtime.RecipeInstallPhaseObserve {
		t.Fatalf("observe claim=%#v found=%v err=%v", observe, found, err)
	}
	observeSigned := signedRecipeInstallCommand(t, observe, clock)
	if err := store.PersistRecipeInstallCommand(ctx, observe, observeSigned); err != nil {
		t.Fatal(err)
	}
	if err := store.ExpireRecipeInstallCommand(ctx, observe); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRecipeInstallStarted(ctx, observe); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired observe lease error=%v, want %v", err, ErrLeaseLost)
	}
	reclaimedObserve, found, err := store.ClaimRecipeInstall(ctx, "recipe-install-observe-retry", time.Minute)
	if err != nil || !found || reclaimedObserve.Phase != runtime.RecipeInstallPhaseObserve {
		t.Fatalf("reclaimed observe=%#v found=%v err=%v", reclaimedObserve, found, err)
	}
	if reclaimedObserve.Command.CommandID == observe.Command.CommandID || reclaimedObserve.Command.NodeCounter <= observe.Command.NodeCounter || reclaimedObserve.Command.SignedEnvelope != "" {
		t.Fatalf("expired observe command was not replaced safely: old=%#v new=%#v", observe.Command, reclaimedObserve.Command)
	}
	observe = reclaimedObserve
	observeSigned = signedRecipeInstallCommand(t, observe, clock)
	if err := store.PersistRecipeInstallCommand(ctx, observe, observeSigned); err != nil {
		t.Fatal(err)
	}
	succeeded := runtime.RecipeInstallResult{
		ExecutionID: observe.ExecutionID, DeploymentID: observe.DeploymentID, TaskID: observe.TaskID,
		Status: "succeeded", Attempt: observe.TaskAttempt, LastSequence: 2,
		LastCheckpoint: manifest.CheckpointSequence[len(manifest.CheckpointSequence)-1], UpdatedAt: clock,
	}
	if err := runtime.ValidateRecipeInstallResult(observe, succeeded, clock); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitRecipeInstall(ctx, observe, succeeded); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitRecipeInstall(ctx, observe, succeeded); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("completed observe lease error=%v, want %v", err, ErrLeaseLost)
	}
	assertRecipeInstallState(t, database, jobID, outboxID, "verifying", "pending", "readiness_queued", "running", "readiness_queued", true)

	readinessIssue, found, err := store.ClaimServiceReadiness(ctx, "service-readiness-issue", time.Minute)
	if err != nil || !found || readinessIssue.Phase != runtime.ServiceReadinessPhaseIssue ||
		readinessIssue.SemanticExpectationDigest != cloudcontracts.FixedReadinessEvidenceDigestV1 {
		t.Fatalf("readiness issue=%#v found=%v err=%v", readinessIssue, found, err)
	}
	readinessSigned := signedServiceReadinessCommand(t, readinessIssue, clock)
	if err := store.PersistServiceReadinessCommand(ctx, readinessIssue, readinessSigned); err != nil {
		t.Fatal(err)
	}
	queuedReadiness := runtime.ServiceReadinessResult{ExecutionID: readinessIssue.ExecutionID, DeploymentID: readinessIssue.DeploymentID,
		ServiceID: readinessIssue.ServiceID, TaskID: readinessIssue.TaskID, Status: "queued", Attempt: 1, UpdatedAt: clock}
	if err := store.CommitServiceReadiness(ctx, readinessIssue, queuedReadiness); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(6 * time.Second)
	readinessObserve, found, err := store.ClaimServiceReadiness(ctx, "service-readiness-observe", time.Minute)
	if err != nil || !found || readinessObserve.Phase != runtime.ServiceReadinessPhaseObserve {
		t.Fatalf("readiness observe=%#v found=%v err=%v", readinessObserve, found, err)
	}
	firstObserveCommand := readinessObserve.Command.CommandID
	readinessSigned = signedServiceReadinessCommand(t, readinessObserve, clock)
	if err := store.PersistServiceReadinessCommand(ctx, readinessObserve, readinessSigned); err != nil {
		t.Fatal(err)
	}
	if err := store.ExpireServiceReadinessCommand(ctx, readinessObserve); err != nil {
		t.Fatal(err)
	}
	readinessObserve, found, err = store.ClaimServiceReadiness(ctx, "service-readiness-observe-retry", time.Minute)
	if err != nil || !found || readinessObserve.Command.CommandID == firstObserveCommand || readinessObserve.Command.SignedEnvelope != "" {
		t.Fatalf("readiness observe retry=%#v found=%v err=%v", readinessObserve, found, err)
	}
	readinessSigned = signedServiceReadinessCommand(t, readinessObserve, clock)
	if err := store.PersistServiceReadinessCommand(ctx, readinessObserve, readinessSigned); err != nil {
		t.Fatal(err)
	}
	challenge := "sha256:" + strings.Repeat("e", 64)
	semantic := cloudcontracts.FixedReadinessEvidenceDigestV1
	stackObservation := "sha256:" + strings.Repeat("f", 64)
	succeededReadiness := runtime.ServiceReadinessResult{ExecutionID: readinessObserve.ExecutionID, DeploymentID: readinessObserve.DeploymentID,
		ServiceID: readinessObserve.ServiceID, TaskID: readinessObserve.TaskID, Status: "succeeded", Checkpoint: runtime.ServiceReadinessVerified,
		Attempt: 1, LastSequence: 1, ChallengeDigest: &challenge, SemanticEvidenceDigest: &semantic,
		StackObservationDigest: &stackObservation, UpdatedAt: clock}
	if err := store.CommitServiceReadiness(ctx, readinessObserve, succeededReadiness); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitServiceReadiness(ctx, readinessObserve, succeededReadiness); err != nil {
		t.Fatalf("idempotent readiness read-back: %v", err)
	}
	var recipeID, serviceName, serviceStatus, integrationStatus, deploymentExecution, deploymentOutcome string
	if err := database.DB().QueryRowContext(ctx, `SELECT recipe_id,name,service_status,integration_status FROM p2p_cloud_services WHERE service_id=$1`, readinessObserve.ServiceID).Scan(&recipeID, &serviceName, &serviceStatus, &integrationStatus); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(ctx, `SELECT execution_status,outcome_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, readinessObserve.DeploymentID).Scan(&deploymentExecution, &deploymentOutcome); err != nil {
		t.Fatal(err)
	}
	if recipeID == "" || serviceName == "" || serviceStatus != "experimental" || integrationStatus != "not_requested" || deploymentExecution != "finished" || deploymentOutcome != "succeeded" {
		t.Fatalf("service=%s/%s/%s/%s deployment=%s/%s", recipeID, serviceName, serviceStatus, integrationStatus, deploymentExecution, deploymentOutcome)
	}
	var serviceEvents, deploymentEvents, jobEvents int
	if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE type='cloud.service.changed' AND aggregate_id=$1`, readinessObserve.ServiceID).Scan(&serviceEvents); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE type='cloud.deployment.changed' AND aggregate_id=$1`, readinessObserve.DeploymentID).Scan(&deploymentEvents); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE type='cloud.job.changed' AND aggregate_id=$1`, jobID).Scan(&jobEvents); err != nil {
		t.Fatal(err)
	}
	if serviceEvents != 1 || deploymentEvents < 1 || jobEvents < 1 {
		t.Fatalf("events service=%d deployment=%d job=%d", serviceEvents, deploymentEvents, jobEvents)
	}
}

func TestStoreServiceReadinessFailureRetainsResources(t *testing.T) {
	ctx := context.Background()
	now, database, store, bootstrap := prepareExecutionProbeTask(t)
	manifest, jobID, _ := seedApprovedRecipeInstall(t, ctx, database, bootstrap, now)
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	inputDigest := recipeInstallInputDigest(manifestDigest)
	installTaskID := stableID("cloud_recipe_install_task_", manifest.ExecutionID, manifest.DeploymentID, manifestDigest, inputDigest)
	checkpoints, _ := json.Marshal(manifest.CheckpointSequence)
	tx, err := database.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_install_tasks(execution_id,task_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,checkpoint_sequence_json,task_status,last_sequence,last_checkpoint,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'succeeded',1,$10,$11,$11)`, manifest.ExecutionID, installTaskID, manifest.DeploymentID, manifest.PlanID, bootstrap.ConnectionID, bootstrap.InstanceID, manifestDigest, inputDigest, string(checkpoints), manifest.CheckpointSequence[len(manifest.CheckpointSequence)-1], now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	claim := runtime.RecipeInstallClaim{ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, PlanID: manifest.PlanID, ConnectionID: bootstrap.ConnectionID, InstanceID: bootstrap.InstanceID, ManifestDigest: manifestDigest, Manifest: manifest}
	if err = ensureServiceReadinessTask(ctx, tx, claim, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='verifying',outcome_status='pending',checkpoint='readiness_queued' WHERE job_id=$1`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_job_steps SET status='running',checkpoint='readiness_queued' WHERE job_id=$1 AND step_id='install'`, jobID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	clock := now.Add(3 * time.Minute)
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string { return "readiness-failure-lease" }
	issue, found, err := store.ClaimServiceReadiness(ctx, "readiness-failure-issue", time.Minute)
	if err != nil || !found {
		t.Fatalf("issue found=%v err=%v", found, err)
	}
	signed := signedServiceReadinessCommand(t, issue, clock)
	if err = store.PersistServiceReadinessCommand(ctx, issue, signed); err != nil {
		t.Fatal(err)
	}
	queued := runtime.ServiceReadinessResult{ExecutionID: issue.ExecutionID, DeploymentID: issue.DeploymentID, ServiceID: issue.ServiceID, TaskID: issue.TaskID, Status: "queued", Attempt: 1, UpdatedAt: clock}
	if err = store.CommitServiceReadiness(ctx, issue, queued); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(6 * time.Second)
	observe, found, err := store.ClaimServiceReadiness(ctx, "readiness-failure-observe", time.Minute)
	if err != nil || !found {
		t.Fatalf("observe found=%v err=%v", found, err)
	}
	signed = signedServiceReadinessCommand(t, observe, clock)
	if err = store.PersistServiceReadinessCommand(ctx, observe, signed); err != nil {
		t.Fatal(err)
	}
	code := "fixed_probe_failed"
	failed := runtime.ServiceReadinessResult{ExecutionID: observe.ExecutionID, DeploymentID: observe.DeploymentID, ServiceID: observe.ServiceID, TaskID: observe.TaskID, Status: "failed", Attempt: 1, LastSequence: 1, ErrorCode: &code, UpdatedAt: clock}
	if err = store.CommitServiceReadiness(ctx, observe, failed); err != nil {
		t.Fatal(err)
	}
	var deploymentExecution, deploymentOutcome, deploymentResource, privateResource string
	var services int
	if err = database.DB().QueryRowContext(ctx, `SELECT execution_status,outcome_status,resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, observe.DeploymentID).Scan(&deploymentExecution, &deploymentOutcome, &deploymentResource); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, observe.DeploymentID).Scan(&privateResource); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_services WHERE deployment_id=$1`, observe.DeploymentID).Scan(&services); err != nil {
		t.Fatal(err)
	}
	if deploymentExecution != "finished" || deploymentOutcome != "failed" || deploymentResource != "retained_tracked" || privateResource != "retained_tracked" || services != 0 {
		t.Fatalf("deployment=%s/%s/%s resource=%s services=%d", deploymentExecution, deploymentOutcome, deploymentResource, privateResource, services)
	}
}

func TestServiceReadinessTransitionAcceptsOnlySafeNewerAttempt(t *testing.T) {
	challenge := "sha256:" + strings.Repeat("d", 64)
	semantic := cloudcontracts.FixedReadinessEvidenceDigestV1
	stack := "sha256:" + strings.Repeat("e", 64)
	running := runtime.ServiceReadinessResult{Status: "running", Checkpoint: runtime.ServiceReadinessChallengeIssued, Attempt: 2, LastSequence: 0, ChallengeDigest: &challenge}
	if !serviceReadinessTransition("running", 1, 0, runtime.ServiceReadinessChallengeIssued, "old", "", "", "", running) {
		t.Fatal("new Worker lease attempt did not replace a sequence-zero challenge")
	}
	succeeded := runtime.ServiceReadinessResult{Status: "succeeded", Checkpoint: runtime.ServiceReadinessVerified, Attempt: 2, LastSequence: 1, ChallengeDigest: &challenge, SemanticEvidenceDigest: &semantic, StackObservationDigest: &stack}
	if !serviceReadinessTransition("running", 1, 0, runtime.ServiceReadinessChallengeIssued, "old", "", "", "", succeeded) {
		t.Fatal("new Worker lease attempt terminal evidence was rejected")
	}
	if serviceReadinessTransition("running", 1, 1, runtime.ServiceReadinessChallengeIssued, "old", "", "", "", succeeded) {
		t.Fatal("new Worker lease attempt replaced an existing event sequence")
	}
}

func seedApprovedRecipeInstall(t *testing.T, ctx context.Context, database *p2pstorage.DatabaseStore, bootstrap runtime.WorkerBootstrapObservationClaim, now time.Time) (cloudcontracts.RecipeExecutionManifestV1, string, string) {
	t.Helper()
	var planRevision, deploymentRevision int64
	if err := database.DB().QueryRowContext(ctx, `SELECT revision FROM p2p_cloud_plans WHERE plan_id=$1`, bootstrap.PlanID).Scan(&planRevision); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(ctx, `SELECT revision FROM p2p_cloud_deployments WHERE deployment_id=$1`, bootstrap.DeploymentID).Scan(&deploymentRevision); err != nil {
		t.Fatal(err)
	}
	manifest := cloudcontracts.RecipeExecutionManifestV1{
		SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema,
		ExecutionID:   "execution-recipe-install-1", DeploymentID: bootstrap.DeploymentID, PlanID: bootstrap.PlanID,
		PlanHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", PlanRevision: uint64(planRevision),
		RecipeDigest:                 "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		WorkerResourceManifestDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ArtifactDigest:               "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		ActionID:                     "install-service", RootRequired: true, TimeoutSeconds: 1200,
		CheckpointSequence: []string{"artifact_verified", "health_verified"},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifestCBOR, err := manifest.CanonicalRecipeExecutionManifestCBOR()
	if err != nil {
		t.Fatal(err)
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := now.Add(2 * time.Minute).UnixMilli()
	jobID := "job-recipe-install-1"
	outboxID := "outbox-recipe-install-1"
	if _, err := database.DB().ExecContext(ctx, `UPDATE p2p_cloud_worker_bootstrap_observations SET worker_lease_expires_at=$1 WHERE deployment_id=$2`, now.Add(time.Hour).UnixMilli(), manifest.DeploymentID); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_execution_manifests
		(execution_id,deployment_id,plan_id,plan_revision,plan_hash,cloud_connection_id,manifest_digest,manifest_cbor,manifest_json,status,revision,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'approved',3,$10,$10)
	`, manifest.ExecutionID, manifest.DeploymentID, manifest.PlanID, planRevision, manifest.PlanHash, bootstrap.ConnectionID,
		manifestDigest, manifestCBOR, string(manifestJSON), createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_execution_approvals
		(approval_id,owner_mxid,execution_id,deployment_id,deployment_revision,plan_id,plan_revision,signer_key_id,manifest_digest,approval_json,signing_payload_cbor,expires_at,status,prepare_idempotency_hash,prepare_request_digest,approve_idempotency_hash,approve_request_digest,signature,job_id,created_at,updated_at)
		VALUES('approval-recipe-install-1','@owner:example.com',$1,$2,$3,$4,$5,'device-key-recipe-install-1',$6,'{}',$7,$8,'approved','prepare-recipe-install-1','prepare-request-recipe-install-1','approve-recipe-install-1','approve-request-recipe-install-1','test-signature',$9,$10,$10)
	`, manifest.ExecutionID, manifest.DeploymentID, deploymentRevision, manifest.PlanID, planRevision, manifestDigest,
		[]byte{0xa0}, now.Add(time.Hour).UnixMilli(), jobID, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at)
		VALUES($1,$2,$3,'install','queued','pending','install_queued','',1,$4,$4)
	`, jobID, manifest.PlanID, manifest.DeploymentID, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at)
		VALUES($1,'install','queued','Recipe install queued.','install_queued','',1,$2,$2)
	`, jobID, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at)
		VALUES($1,$2,'recipe_execution',$3,$4,$5)
	`, outboxID, cloudmodule.OutboxKindRecipeExecutionInstallRequested, manifest.ExecutionID, `{"execution_id":"`+manifest.ExecutionID+`"}`, createdAt); err != nil {
		t.Fatal(err)
	}
	return manifest, jobID, outboxID
}

func signedRecipeInstallCommand(t *testing.T, claim runtime.RecipeInstallClaim, now time.Time) runtime.SignedRecipeInstallCommand {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if claim.Phase == runtime.RecipeInstallPhaseIssue {
		signed, err := transport.BuildRecipeInstallIssueCommand(claim.Command, claim.IssueRequest, now)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	signed, err := transport.BuildRecipeInstallObserveCommand(claim.Command, claim.ObserveRequest, now)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func signedServiceReadinessCommand(t *testing.T, claim runtime.ServiceReadinessClaim, now time.Time) runtime.SignedServiceReadinessCommand {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if claim.Phase == runtime.ServiceReadinessPhaseIssue {
		signed, err := transport.BuildServiceReadinessIssueCommand(claim.Command, claim.IssueRequest, now)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	signed, err := transport.BuildServiceReadinessObserveCommand(claim.Command, claim.ObserveRequest, now)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func assertRecipeInstallState(t *testing.T, database *p2pstorage.DatabaseStore, jobID, outboxID, jobExecution, jobOutcome, jobCheckpoint, stepStatus, stepCheckpoint string, outboxCompleted bool) {
	t.Helper()
	var actualJobExecution, actualJobOutcome, actualJobCheckpoint, actualStepStatus, actualStepCheckpoint string
	var completedAt int64
	if err := database.DB().QueryRowContext(context.Background(), `SELECT execution_status,outcome_status,checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, jobID).Scan(&actualJobExecution, &actualJobOutcome, &actualJobCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(context.Background(), `SELECT status,checkpoint FROM p2p_cloud_job_steps WHERE job_id=$1 AND step_id='install'`, jobID).Scan(&actualStepStatus, &actualStepCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(context.Background(), `SELECT completed_at FROM p2p_cloud_outbox WHERE outbox_id=$1`, outboxID).Scan(&completedAt); err != nil {
		t.Fatal(err)
	}
	if actualJobExecution != jobExecution || actualJobOutcome != jobOutcome || actualJobCheckpoint != jobCheckpoint ||
		actualStepStatus != stepStatus || actualStepCheckpoint != stepCheckpoint || (completedAt > 0) != outboxCompleted {
		t.Fatalf("recipe install state job=%s/%s/%s step=%s/%s completed=%d", actualJobExecution, actualJobOutcome, actualJobCheckpoint, actualStepStatus, actualStepCheckpoint, completedAt)
	}
}
