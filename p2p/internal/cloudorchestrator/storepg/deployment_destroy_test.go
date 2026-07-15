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
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreDeploymentDestroyCompletesResidualDeploymentWithoutService(t *testing.T) {
	for _, test := range []struct {
		name, outcome, resource string
	}{
		{name: "failed orphaned deployment", outcome: "failed", resource: "orphaned"},
		{name: "canceled retained deployment", outcome: "canceled", resource: "retained_tracked"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, database, store, transport := prepareDeploymentDestroyLifecycle(t, test.outcome, test.resource)
			ctx := context.Background()
			claim, found, err := store.ClaimDeploymentDestroy(ctx, "deployment-destroy-runtime", time.Minute)
			if err != nil || !found {
				t.Fatalf("deployment destroy claim found=%v err=%v", found, err)
			}
			if claim.DeploymentExecution != "finished" || claim.DeploymentOutcome != test.outcome || claim.Approval.ResourceStatus != test.resource || claim.Request.ServiceID != "" {
				t.Fatalf("deployment destroy claim=%#v", claim)
			}
			if err = store.MarkDeploymentDestroyStarted(ctx, claim); err != nil {
				t.Fatal(err)
			}
			signed, err := transport.BuildDeploymentDestroyCommand(claim.Command, claim.Request, claim.Approval)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.PersistDeploymentDestroyCommand(ctx, claim, signed); err != nil {
				t.Fatal(err)
			}
			receipt, _ := json.Marshal(broker.DeploymentCommandReceipt{Schema: broker.ReceiptSchema, Disposition: "committed", ConnectionID: claim.ConnectionID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, Action: broker.DeploymentDestroyAction})
			result := runtime.ServiceDestroyResult{Status: "verified_destroyed", DeploymentID: claim.DeploymentID, InstanceID: claim.Request.InstanceID, VolumeIDs: claim.Request.VolumeIDs, NetworkInterfaceIDs: claim.Request.NetworkInterfaceIDs, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: string(receipt)}
			if err = store.CompleteDeploymentDestroy(ctx, claim, result); err != nil {
				t.Fatal(err)
			}
			assertDeploymentDestroyState(t, database, claim, "finished", test.outcome, "verified_destroyed", "finished", "succeeded", "verified_destroyed", "accepted")
		})
	}
}

func TestStoreDeploymentDestroyFatalProviderFailureBlocksResidualResources(t *testing.T) {
	now, database, store, builder := prepareDeploymentDestroyLifecycle(t, "failed", "orphaned")
	transport := &fatalDeploymentDestroyTransport{builder: builder}
	runner := runtime.NewDeploymentDestroyRunner(store, transport, runtime.Config{WorkerID: "deployment-destroy-fatal", Lease: time.Minute, AttemptTimeout: 30 * time.Second, Now: func() time.Time { return now }})
	handled, err := runner.RunOnce(context.Background())
	if err != nil || !handled || !transport.requested {
		t.Fatalf("fatal deployment destroy handled=%v requested=%v err=%v", handled, transport.requested, err)
	}
	claim := runtime.DeploymentDestroyClaim{DeploymentID: "deployment-destroy-runtime-1", JobID: "job-deployment-destroy-runtime-1", OutboxID: "outbox-deployment-destroy-runtime-1"}
	assertDeploymentDestroyState(t, database, claim, "finished", "failed", "blocked", "finished", "failed", "destroy_blocked", "failed")
}

func prepareDeploymentDestroyLifecycle(t *testing.T, outcome, resource string) (time.Time, *p2pstorage.DatabaseStore, *Store, *brokertransport.Transport) {
	t.Helper()
	ctx, database, closeDatabase := openMigratedStore(t)
	t.Cleanup(closeDatabase)
	createdAt := time.Date(2026, time.July, 16, 9, 0, 0, 0, time.UTC)
	devicePrivate, deviceSPKI := provisionDeviceKey(t)
	ts := createdAt.UnixMilli()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO p2p_cloud_goals(goal_id,owner_mxid,prompt,cloud_connection_id,plan_id,status,idempotency_hash,request_digest,revision,created_at,updated_at)VALUES('goal-deployment-destroy-runtime-1','@owner:example.com','destroy residual deployment','connection-deployment-destroy-runtime-1','plan-deployment-destroy-runtime-1','planned','goal-deployment-destroy-runtime-idem','goal-deployment-destroy-runtime-request',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_plans(plan_id,goal_id,cloud_connection_id,status,title,summary,recipe_digest,quote_id,plan_hash,revision,created_at,updated_at)VALUES('plan-deployment-destroy-runtime-1','goal-deployment-destroy-runtime-1','connection-deployment-destroy-runtime-1','approved','Destroy residual deployment','safe','','','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',4,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_connections(cloud_connection_id,provider,account_id,region,mode,status,revision,created_at,updated_at)VALUES('connection-deployment-destroy-runtime-1','aws','123456789012','ap-south-1','connection_stack_v2','active',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_connection_bootstraps(bootstrap_id,owner_mxid,cloud_connection_id,provider,requested_region,template_url,template_digest,source_tree_digest,stack_name,node_key_id,node_public_key_spki_base64,device_approval_key_id,device_approval_public_key_spki_base64,status,revision,idempotency_hash,request_digest,expires_at,created_at,updated_at)VALUES('bootstrap-deployment-destroy-runtime-1','@owner:example.com','connection-deployment-destroy-runtime-1','aws','ap-south-1','https://example.invalid/template','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','stack-deployment-destroy-runtime-1','node-deployment-destroy-runtime-1',$1,'device-deployment-destroy-runtime-1',$1,'active',1,'bootstrap-deployment-destroy-runtime-idem','bootstrap-deployment-destroy-runtime-request',$2,$3,$3)`, []any{deviceSPKI, createdAt.Add(time.Hour).UnixMilli(), ts}},
		{`INSERT INTO p2p_cloud_connection_brokers(cloud_connection_id,broker_command_url,broker_region,connection_generation,node_key_id,next_node_counter,created_at,updated_at)VALUES('connection-deployment-destroy-runtime-1','https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands','ap-south-1',1,'node-deployment-destroy-runtime-1',0,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_deployments(deployment_id,plan_id,cloud_connection_id,execution_status,outcome_status,resource_status,revision,created_at,updated_at)VALUES('deployment-destroy-runtime-1','plan-deployment-destroy-runtime-1','connection-deployment-destroy-runtime-1','finished',$1,$2,5,$3,$3)`, []any{outcome, resource, ts}},
		{`INSERT INTO p2p_cloud_deployment_resources(deployment_id,cloud_connection_id,request_sha256,resource_status,instance_id,volume_ids_json,network_interface_ids_json,broker_receipt_json,created_at,updated_at)VALUES('deployment-destroy-runtime-1','connection-deployment-destroy-runtime-1','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',$1,'i-0123456789abcdef0','["vol-0123456789abcdef0"]','["eni-0123456789abcdef0"]','{}',$2,$2)`, []any{resource, ts}},
	}
	for _, statement := range statements {
		if _, err := database.DB().ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed deployment destroy lifecycle: %v", err)
		}
	}
	prepared, err := database.PrepareCloudDeploymentDestroy(ctx, cloudmodule.PrepareDeploymentDestroyRequest{OwnerMXID: "@owner:example.com", DeploymentID: "deployment-destroy-runtime-1", ExpectedRevision: 5, IdempotencyHash: "prepare-deployment-destroy-runtime-1", ApprovalID: "approval-deployment-destroy-runtime-1", ChallengeID: "challenge-deployment-destroy-runtime-1", CreatedAt: ts, ExpiresAt: createdAt.Add(5 * time.Minute).UnixMilli()})
	if err != nil || !prepared.Created {
		t.Fatalf("prepare deployment destroy=%#v err=%v", prepared, err)
	}
	approvedAt := createdAt.Add(time.Minute)
	approval, err := prepared.Confirmation.Approval.Sign(devicePrivate, approvedAt)
	if err != nil {
		t.Fatal(err)
	}
	approved, err := database.ApproveCloudDeploymentDestroy(ctx, cloudmodule.ApproveDeploymentDestroyRequest{OwnerMXID: "@owner:example.com", DeploymentID: "deployment-destroy-runtime-1", ExpectedRevision: 5, IdempotencyHash: "approve-deployment-destroy-runtime-1", Approval: approval, JobID: "job-deployment-destroy-runtime-1", OutboxID: "outbox-deployment-destroy-runtime-1", DeploymentEventID: "event-deployment-destroy-runtime-1", JobEventID: "event-job-deployment-destroy-runtime-1", CreatedAt: approvedAt.UnixMilli()})
	if err != nil || !approved.Created {
		t.Fatalf("approve deployment destroy=%#v err=%v", approved, err)
	}
	store := New(database.DB(), Config{Now: func() time.Time { return approvedAt }})
	_, nodePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(nodePrivate, func() time.Time { return approvedAt })
	if err != nil {
		t.Fatal(err)
	}
	return approvedAt, database, store, transport
}

func assertDeploymentDestroyState(t *testing.T, database *p2pstorage.DatabaseStore, claim runtime.DeploymentDestroyClaim, wantExecution, wantOutcome, wantResource, wantJobExecution, wantJobOutcome, wantCheckpoint, wantCommand string) {
	t.Helper()
	var execution, outcome, publicResource, privateResource, jobExecution, jobOutcome, checkpoint, commandState string
	var completedAt int64
	var services int
	if err := database.DB().QueryRow(`SELECT execution_status,outcome_status,resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, claim.DeploymentID).Scan(&execution, &outcome, &publicResource); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, claim.DeploymentID).Scan(&privateResource); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT execution_status,outcome_status,checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, claim.JobID).Scan(&jobExecution, &jobOutcome, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT state FROM p2p_cloud_service_destroy_commands WHERE deployment_id=$1 AND service_id=''`, claim.DeploymentID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT completed_at FROM p2p_cloud_outbox WHERE outbox_id=$1`, claim.OutboxID).Scan(&completedAt); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT COUNT(*) FROM p2p_cloud_services WHERE deployment_id=$1`, claim.DeploymentID).Scan(&services); err != nil {
		t.Fatal(err)
	}
	if execution != wantExecution || outcome != wantOutcome || publicResource != wantResource || privateResource != wantResource || jobExecution != wantJobExecution || jobOutcome != wantJobOutcome || checkpoint != wantCheckpoint || commandState != wantCommand || completedAt == 0 || services != 0 {
		t.Fatalf("deployment=%s/%s/%s private=%s job=%s/%s/%s command=%s completed=%d services=%d", execution, outcome, publicResource, privateResource, jobExecution, jobOutcome, checkpoint, commandState, completedAt, services)
	}
}

type fatalDeploymentDestroyTransport struct {
	builder   *brokertransport.Transport
	requested bool
}

func (t *fatalDeploymentDestroyTransport) BuildDeploymentDestroyCommand(command runtime.ServiceDestroyCommand, request broker.DeploymentDestroyRequest, approval cloudcontracts.DeploymentDestroyApprovalV1) (runtime.SignedServiceDestroyCommand, error) {
	return t.builder.BuildDeploymentDestroyCommand(command, request, approval)
}

func (t *fatalDeploymentDestroyTransport) RequestDeploymentDestroy(context.Context, string, runtime.ServiceDestroyCommand, runtime.SignedServiceDestroyCommand, broker.DeploymentDestroyRequest, cloudcontracts.DeploymentDestroyApprovalV1) (runtime.ServiceDestroyResult, error) {
	t.requested = true
	return runtime.ServiceDestroyResult{}, errors.New("provider fatal")
}
