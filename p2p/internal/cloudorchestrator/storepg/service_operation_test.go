package storepg

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestStoreServiceOperationReplaysAndCommitsStoppedWithoutDestroyingResource(t *testing.T) {
	ctx := context.Background()
	now, database, store, bootstrap := prepareExecutionProbeTask(t)
	clock := now.Add(2 * time.Minute)
	store.cfg.Now = func() time.Time { return clock }
	lease := 0
	store.cfg.NewLeaseToken = func() string { lease++; return "service-operation-lease-" + string(rune('0'+lease)) }

	manifest := cloudcontracts.RecipeExecutionManifestV1{SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema, ExecutionID: "service-operation-runtime-0001", DeploymentID: bootstrap.DeploymentID, PlanID: bootstrap.PlanID, PlanHash: namedDigest("a"), PlanRevision: 1, RecipeDigest: namedDigest("b"), WorkerResourceManifestDigest: namedDigest("c"), ArtifactDigest: cloudcontracts.FixedProbeManagedArtifactDigest, ActionID: cloudcontracts.FixedProbeStopActionID, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_service_stopped"}}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifestJSON, _ := json.Marshal(manifest)
	checkpointsJSON, _ := json.Marshal(manifest.CheckpointSequence)
	inputDigest := recipeInstallInputDigest(manifestDigest)
	taskID, serviceID, jobID, outboxID := "service-operation-task-runtime-0001", "service-operation-runtime-service-0001", "service-operation-runtime-job-0001", "service-operation-runtime-outbox-0001"
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO p2p_cloud_recipes(recipe_id,name,version,digest,maturity,revision,created_at,updated_at) VALUES('service-operation-runtime-recipe-0001','Managed probe','v1',$1,'experimental',1,$2,$2)`, []any{manifest.RecipeDigest, clock.UnixMilli()}},
		{`INSERT INTO p2p_cloud_services(service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at) VALUES($1,$2,'service-operation-runtime-recipe-0001','Managed probe','active','not_requested',2,$3,$3)`, []any{serviceID, bootstrap.DeploymentID, clock.UnixMilli()}},
		{`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,$2,$3,'stop','queued','pending','service_operation_queued','',1,$4,$4)`, []any{jobID, bootstrap.PlanID, bootstrap.DeploymentID, clock.UnixMilli()}},
		{`INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at) VALUES($1,'service_operation','queued','queued','service_operation_queued','',1,$2,$2)`, []any{jobID, clock.UnixMilli()}},
		{`INSERT INTO p2p_cloud_service_operation_tasks(operation_id,approval_id,service_id,service_revision,expected_service_status,operation,execution_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,manifest_json,checkpoint_sequence_json,task_id,job_id,task_status,available_at,created_at,updated_at) VALUES($1,'approval-operation-runtime-0001',$2,2,'active','stop',$1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'queued',$13,$13,$13)`, []any{manifest.ExecutionID, serviceID, bootstrap.DeploymentID, bootstrap.PlanID, bootstrap.ConnectionID, bootstrap.InstanceID, manifestDigest, inputDigest, string(manifestJSON), string(checkpointsJSON), taskID, jobID, clock.UnixMilli()}},
		{`INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,available_at,created_at) VALUES($1,$2,'service_operation',$3,'{}',$4,$4)`, []any{outboxID, cloudmodule.OutboxKindServiceOperationRequested, manifest.ExecutionID, clock.UnixMilli()}},
	}
	for _, statement := range statements {
		if _, err = database.DB().ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed operation: %v", err)
		}
	}

	first, found, err := store.ClaimServiceOperation(ctx, "operation-runner-a", time.Minute)
	if err != nil || !found || first.Phase != runtime.RecipeInstallPhaseIssue {
		t.Fatalf("first=%#v found=%v err=%v", first, found, err)
	}
	signed := signedRecipeInstallCommand(t, first, clock)
	if err = store.PersistServiceOperationCommand(ctx, first, signed); err != nil {
		t.Fatal(err)
	}
	retryAt := clock.Add(time.Minute)
	if err = store.DeferServiceOperation(ctx, first, "broker_unavailable", retryAt); err != nil {
		t.Fatal(err)
	}
	clock = retryAt
	replayed, found, err := store.ClaimServiceOperation(ctx, "operation-runner-b", time.Minute)
	if err != nil || !found {
		t.Fatalf("replay found=%v err=%v", found, err)
	}
	if replayed.Command.CommandID != first.Command.CommandID || replayed.Command.SignedEnvelope != signed.EnvelopeJSON {
		t.Fatal("operation did not replay the exact persisted command")
	}
	if err = store.MarkServiceOperationStarted(ctx, replayed); err != nil {
		t.Fatal(err)
	}
	queued := runtime.RecipeInstallResult{ExecutionID: replayed.ExecutionID, DeploymentID: replayed.DeploymentID, TaskID: replayed.TaskID, Status: "queued", Attempt: 1, UpdatedAt: clock}
	if err = store.CommitServiceOperation(ctx, replayed, queued); err != nil {
		t.Fatal(err)
	}

	clock = clock.Add(time.Second)
	observe, found, err := store.ClaimServiceOperation(ctx, "operation-runner-observe", time.Minute)
	if err != nil || !found || observe.Phase != runtime.RecipeInstallPhaseObserve {
		t.Fatalf("observe=%#v found=%v err=%v", observe, found, err)
	}
	observeSigned := signedRecipeInstallCommand(t, observe, clock)
	if err = store.PersistServiceOperationCommand(ctx, observe, observeSigned); err != nil {
		t.Fatal(err)
	}
	succeeded := runtime.RecipeInstallResult{ExecutionID: observe.ExecutionID, DeploymentID: observe.DeploymentID, TaskID: observe.TaskID, Status: "succeeded", Attempt: 1, LastSequence: 1, LastCheckpoint: "probe_service_stopped", UpdatedAt: clock}
	if err = store.CommitServiceOperation(ctx, observe, succeeded); err != nil {
		t.Fatal(err)
	}
	var serviceStatus, resourceStatus, jobOutcome, jobCheckpoint string
	if err = database.DB().QueryRowContext(ctx, `SELECT service_status FROM p2p_cloud_services WHERE service_id=$1`, serviceID).Scan(&serviceStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, bootstrap.DeploymentID).Scan(&resourceStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT outcome_status,checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, jobID).Scan(&jobOutcome, &jobCheckpoint); err != nil {
		t.Fatal(err)
	}
	if serviceStatus != "stopped" || resourceStatus != "active" || jobOutcome != "succeeded" || jobCheckpoint != "service_operation_succeeded" {
		t.Fatalf("terminal service=%s resource=%s job=%s/%s", serviceStatus, resourceStatus, jobOutcome, jobCheckpoint)
	}
}

func namedDigest(c string) string {
	value := "sha256:"
	for i := 0; i < 64; i++ {
		value += c
	}
	return value
}
