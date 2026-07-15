package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreServiceDestroyCompletesOnlyAfterVerifiedReadback(t *testing.T) {
	now, database, store, claim := prepareServiceDestroyClaim(t)
	signed := signedServiceDestroyCommand(t, claim, now)
	drifted := claim
	drifted.PlanID = "plan-destroy-drifted"
	if err := store.MarkServiceDestroyStarted(context.Background(), drifted); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("drifted destroy claim error=%v, want %v", err, ErrLeaseLost)
	}
	if err := store.MarkServiceDestroyStarted(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistServiceDestroyCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(broker.DeploymentCommandReceipt{Schema: broker.ReceiptSchema, Disposition: "committed", ConnectionID: claim.ConnectionID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, Action: broker.DeploymentDestroyAction})
	result := runtime.ServiceDestroyResult{Status: "verified_destroyed", DeploymentID: claim.DeploymentID, InstanceID: claim.Request.InstanceID, VolumeIDs: claim.Request.VolumeIDs, NetworkInterfaceIDs: claim.Request.NetworkInterfaceIDs, CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: string(receipt)}
	if err := store.CompleteServiceDestroy(context.Background(), claim, result); err != nil {
		t.Fatal(err)
	}
	var service, publicResource, privateResource, execution, outcome, checkpoint, commandState string
	if err := database.DB().QueryRow(`SELECT service_status FROM p2p_cloud_services WHERE service_id=$1`, claim.ServiceID).Scan(&service); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT execution_status,outcome_status,resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, claim.DeploymentID).Scan(&execution, &outcome, &publicResource); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, claim.DeploymentID).Scan(&privateResource); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, claim.JobID).Scan(&checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT state FROM p2p_cloud_service_destroy_commands WHERE command_id=$1`, claim.Command.CommandID).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if service != "destroyed" || execution != "finished" || outcome != "succeeded" || publicResource != "verified_destroyed" || privateResource != "verified_destroyed" || checkpoint != "verified_destroyed" || commandState != "accepted" {
		t.Fatalf("destroy state service=%s deployment=%s/%s/%s private=%s checkpoint=%s command=%s", service, execution, outcome, publicResource, privateResource, checkpoint, commandState)
	}
	var projection string
	if err := database.DB().QueryRow(`SELECT payload_json FROM p2p_cloud_projection_outbox WHERE type='cloud.deployment.changed' ORDER BY created_at DESC,projection_id DESC LIMIT 1`).Scan(&projection); err != nil {
		t.Fatal(err)
	}
	if containsAny(projection, []string{"i-", "vol-", "eni-", "signature", "approval"}) {
		t.Fatalf("destroy projection leaked private resources: %s", projection)
	}
}

func TestStoreServiceDestroyReplaysIndeterminateCommandAndBlocksUnverifiedFailure(t *testing.T) {
	now, database, store, claim := prepareServiceDestroyClaim(t)
	signed := signedServiceDestroyCommand(t, claim, now)
	if err := store.PersistServiceDestroyCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	if err := store.DeferServiceDestroy(context.Background(), claim, "deployment_destroy_in_progress", now); err != nil {
		t.Fatal(err)
	}
	retry, found, err := store.ClaimServiceDestroy(context.Background(), "destroy-retry", time.Minute)
	if err != nil || !found {
		t.Fatalf("retry found=%v err=%v", found, err)
	}
	if retry.Command.CommandID != claim.Command.CommandID || retry.Command.NodeCounter != claim.Command.NodeCounter || retry.Command.SignedEnvelope != signed.EnvelopeJSON {
		t.Fatalf("destroy retry drifted first=%#v retry=%#v", claim.Command, retry.Command)
	}
	if err = store.FailServiceDestroy(context.Background(), retry, "access_denied"); err != nil {
		t.Fatal(err)
	}
	var service, publicResource, privateResource, checkpoint string
	if err = database.DB().QueryRow(`SELECT service_status FROM p2p_cloud_services WHERE service_id=$1`, claim.ServiceID).Scan(&service); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, claim.DeploymentID).Scan(&publicResource); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, claim.DeploymentID).Scan(&privateResource); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRow(`SELECT checkpoint FROM p2p_cloud_jobs WHERE job_id=$1`, claim.JobID).Scan(&checkpoint); err != nil {
		t.Fatal(err)
	}
	if service != "degraded" || publicResource != "blocked" || privateResource != "blocked" || checkpoint != "destroy_blocked" {
		t.Fatalf("blocked state service=%s public=%s private=%s checkpoint=%s", service, publicResource, privateResource, checkpoint)
	}
}

func prepareServiceDestroyClaim(t *testing.T) (time.Time, *p2pstorage.DatabaseStore, *Store, runtime.ServiceDestroyClaim) {
	t.Helper()
	ctx, database, closeDatabase := openMigratedStore(t)
	t.Cleanup(closeDatabase)
	now := time.Date(2026, time.July, 15, 9, 0, 0, 0, time.UTC)
	_, devicePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	target := cloudcontracts.ServiceDestroyTargetV1{ServiceID: "service-destroy-runtime-1", ServiceRevision: 2, DeploymentID: "deployment-destroy-runtime-1", DeploymentRevision: 5, CloudConnectionID: "connection-destroy-runtime-1", RecipeID: "recipe-destroy-runtime-1", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}}
	approval, err := cloudcontracts.NewServiceDestroyApprovalV1(target, "approval-destroy-runtime-1", "challenge-destroy-runtime-1", "device-key-runtime-1", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approval, err = approval.Sign(devicePrivate, now)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := approval
	unsigned.Signature = ""
	approvalJSON, _ := json.Marshal(unsigned)
	volumes, _ := json.Marshal(target.VolumeIDs)
	interfaces, _ := json.Marshal(target.NetworkInterfaceIDs)
	ts := now.UnixMilli()
	statements := []struct {
		q string
		a []any
	}{
		{`INSERT INTO p2p_cloud_goals(goal_id,owner_mxid,prompt,cloud_connection_id,plan_id,status,idempotency_hash,request_digest,revision,created_at,updated_at)VALUES('goal-destroy-runtime-1','@owner:example.com','destroy','connection-destroy-runtime-1','plan-destroy-runtime-1','planned','goal-destroy-runtime-idem','goal-destroy-runtime-request',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_plans(plan_id,goal_id,cloud_connection_id,status,title,summary,recipe_digest,quote_id,plan_hash,revision,created_at,updated_at)VALUES('plan-destroy-runtime-1','goal-destroy-runtime-1','connection-destroy-runtime-1','approved','Destroy','safe',$1,'quote-destroy-runtime-1','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',4,$2,$2)`, []any{target.RecipeDigest, ts}},
		{`INSERT INTO p2p_cloud_connections(cloud_connection_id,provider,account_id,region,mode,status,revision,created_at,updated_at)VALUES('connection-destroy-runtime-1','aws','123456789012','ap-south-1','connection_stack_v2','active',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_connection_brokers(cloud_connection_id,broker_command_url,broker_region,connection_generation,node_key_id,next_node_counter,created_at,updated_at)VALUES('connection-destroy-runtime-1','https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands','ap-south-1',1,'node-runtime-1',0,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_recipes(recipe_id,name,version,digest,maturity,revision,created_at,updated_at)VALUES('recipe-destroy-runtime-1','Destroy','v1',$1,'experimental',1,$2,$2)`, []any{target.RecipeDigest, ts}},
		{`INSERT INTO p2p_cloud_deployments(deployment_id,plan_id,cloud_connection_id,execution_status,outcome_status,resource_status,revision,created_at,updated_at)VALUES('deployment-destroy-runtime-1','plan-destroy-runtime-1','connection-destroy-runtime-1','finished','succeeded','destroying',6,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_services(service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at)VALUES('service-destroy-runtime-1','deployment-destroy-runtime-1','recipe-destroy-runtime-1','Destroy','destroying','not_requested',3,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_deployment_resources(deployment_id,cloud_connection_id,request_sha256,resource_status,instance_id,volume_ids_json,network_interface_ids_json,broker_receipt_json,created_at,updated_at)VALUES('deployment-destroy-runtime-1','connection-destroy-runtime-1','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','destroying',$1,$2,$3,'{}',$4,$4)`, []any{target.InstanceID, string(volumes), string(interfaces), ts}},
		{`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at)VALUES('job-destroy-runtime-1','plan-destroy-runtime-1','deployment-destroy-runtime-1','destroy','queued','pending','destroy_queued','',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at)VALUES('job-destroy-runtime-1','destroy','queued','queued','destroy_queued','',1,$1,$1)`, []any{ts}},
		{`INSERT INTO p2p_cloud_service_destroy_approvals(approval_id,challenge_id,owner_mxid,service_id,service_revision,deployment_id,deployment_revision,cloud_connection_id,recipe_id,recipe_digest,instance_id,volume_ids_json,network_interface_ids_json,signer_key_id,approval_json,signing_payload,service_json,deployment_json,status,prepare_idempotency_hash,prepare_request_digest,approve_idempotency_hash,approve_request_digest,signature,job_id,expires_at,created_at,updated_at)VALUES($1,$2,'@owner:example.com',$3,2,$4,5,$5,$6,$7,$8,$9,$10,$11,$12,$13,'{}','{}','approved','prepare-runtime-idem','prepare-runtime-request','approve-runtime-idem','approve-runtime-request',$14,'job-destroy-runtime-1',$15,$16,$16)`, []any{approval.ApprovalID, approval.ChallengeID, target.ServiceID, target.DeploymentID, target.CloudConnectionID, target.RecipeID, target.RecipeDigest, target.InstanceID, string(volumes), string(interfaces), approval.SignerKeyID, string(approvalJSON), mustSigningPayload(t, approval), approval.Signature, approval.ExpiresAt.UnixMilli(), ts}},
		{`INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at)VALUES('outbox-destroy-runtime-1','cloud.service.destroy.requested','service','service-destroy-runtime-1','{"service_id":"service-destroy-runtime-1"}',$1)`, []any{ts}},
	}
	for _, s := range statements {
		if _, err = database.DB().ExecContext(ctx, s.q, s.a...); err != nil {
			t.Fatalf("seed destroy runtime: %v", err)
		}
	}
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	claim, found, err := store.ClaimServiceDestroy(ctx, "destroy-runtime", time.Minute)
	if err != nil || !found {
		t.Fatalf("destroy claim found=%v err=%v", found, err)
	}
	return now, database, store, claim
}

func mustSigningPayload(t *testing.T, approval cloudcontracts.ServiceDestroyApprovalV1) []byte {
	t.Helper()
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
func signedServiceDestroyCommand(t *testing.T, claim runtime.ServiceDestroyClaim, now time.Time) runtime.SignedServiceDestroyCommand {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(key, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildServiceDestroyCommand(claim.Command, claim.Request, claim.Approval)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}
