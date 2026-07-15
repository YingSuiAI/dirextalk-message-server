package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
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
	assertRecipeInstallState(t, database, jobID, outboxID, "finished", "succeeded", "install_succeeded", "finished", "install_succeeded", true)
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
