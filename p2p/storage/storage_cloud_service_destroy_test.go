package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestDatabaseStoreServiceDestroyApprovalIsRevisionBoundAtomicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 15, 7, 0, 0, 0, time.UTC)
	privateKey, publicSPKI := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, publicSPKI, now.UnixMilli())

	prepare := cloudmodule.PrepareServiceDestroyRequest{
		OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2,
		IdempotencyHash: "prepare-destroy-idempotency", RequestDigest: "prepare-destroy-request",
		ApprovalID: "approval-destroy-0001", ChallengeID: "challenge-destroy-0001",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	}
	prepared, err := store.PrepareCloudServiceDestroy(ctx, prepare)
	if err != nil || !prepared.Created {
		t.Fatalf("prepare destroy = %#v err=%v", prepared, err)
	}
	approval := prepared.Confirmation.Approval
	if approval.Signature != "" || approval.InstanceID != "i-0123456789abcdef0" || len(approval.VolumeIDs) != 2 || approval.VolumeIDs[0] != "vol-0aaaaaaaaaaaaaaaa" || approval.ServiceRevision != 2 || approval.DeploymentRevision != 5 {
		t.Fatalf("destroy approval boundary = %#v", approval)
	}
	replay, err := store.PrepareCloudServiceDestroy(ctx, prepare)
	if err != nil || replay.Created || mustCloudConfirmationJSON(t, replay.Confirmation) != mustCloudConfirmationJSON(t, prepared.Confirmation) {
		t.Fatalf("prepare replay = %#v err=%v", replay, err)
	}
	conflict := prepare
	conflict.RequestDigest = "different-destroy-request"
	if _, err := store.PrepareCloudServiceDestroy(ctx, conflict); !errors.Is(err, cloudmodule.ErrIdempotencyConflict) {
		t.Fatalf("prepare idempotency conflict = %v", err)
	}

	signed, err := approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tampered := signed
	tampered.VolumeIDs = []string{"vol-0cccccccccccccccc"}
	bad := serviceDestroyApprovalRequest(tampered, now.Add(time.Minute).UnixMilli())
	if _, err := store.ApproveCloudServiceDestroy(ctx, bad); !errors.Is(err, cloudmodule.ErrServiceDestroyConfirmationInvalid) && !errors.Is(err, cloudmodule.ErrServiceDestroyApprovalSignature) {
		t.Fatalf("tampered destroy approval = %v", err)
	}

	request := serviceDestroyApprovalRequest(signed, now.Add(time.Minute).UnixMilli())
	approved, err := store.ApproveCloudServiceDestroy(ctx, request)
	if err != nil || !approved.Created {
		t.Fatalf("approve destroy = %#v err=%v", approved, err)
	}
	if approved.Service.Status != "destroying" || approved.Service.Revision != 3 || approved.Deployment.Resource != "destroying" || approved.Deployment.Revision != 6 || approved.Job.Kind != "destroy" || approved.Job.Checkpoint != "destroy_queued" {
		t.Fatalf("approved destroy projection = %#v", approved)
	}
	var privateStatus string
	var outboxCount, eventCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id='deployment-destroy-0001'`).Scan(&privateStatus); err != nil || privateStatus != "destroying" {
		t.Fatalf("private resource status=%q err=%v", privateStatus, err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_outbox WHERE kind=$1 AND aggregate_id='service-destroy-0001'`, cloudmodule.OutboxKindServiceDestroyRequested).Scan(&outboxCount); err != nil || outboxCount != 1 {
		t.Fatalf("destroy outbox count=%d err=%v", outboxCount, err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE event_id IN ('event-destroy-service-0001','event-destroy-deployment-0001','event-destroy-job-0001')`).Scan(&eventCount); err != nil || eventCount != 3 {
		t.Fatalf("destroy event count=%d err=%v", eventCount, err)
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_services SET service_status='destroyed',revision=4 WHERE service_id='service-destroy-0001'`); err != nil {
		t.Fatal(err)
	}
	approvedReplay, err := store.ApproveCloudServiceDestroy(ctx, request)
	if err != nil || approvedReplay.Created || mustCloudConfirmationJSON(t, approvedReplay) != mustCloudConfirmationJSON(t, cloudmodule.ApproveServiceDestroyResult{Service: approved.Service, Deployment: approved.Deployment, Job: approved.Job}) {
		t.Fatalf("approval exact replay = %#v err=%v", approvedReplay, err)
	}
}

func serviceDestroyApprovalRequest(approval cloudcontracts.ServiceDestroyApprovalV1, createdAt int64) cloudmodule.ApproveServiceDestroyRequest {
	return cloudmodule.ApproveServiceDestroyRequest{
		OwnerMXID: "@owner:example.com", ServiceID: approval.ServiceID, ExpectedRevision: int64(approval.ServiceRevision),
		IdempotencyHash: "approve-destroy-idempotency", Approval: approval,
		JobID: "job-destroy-0001", OutboxID: "outbox-destroy-0001",
		ServiceEventID: "event-destroy-service-0001", DeploymentEventID: "event-destroy-deployment-0001", JobEventID: "event-destroy-job-0001",
		CreatedAt: createdAt,
	}
}

func seedServiceDestroyState(t *testing.T, store *DatabaseStore, publicSPKI string, now int64) {
	t.Helper()
	volumes, _ := json.Marshal([]string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"})
	interfaces, _ := json.Marshal([]string{"eni-0bbbbbbbbbbbbbbbb", "eni-0aaaaaaaaaaaaaaaa"})
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO p2p_cloud_goals(goal_id,owner_mxid,prompt,cloud_connection_id,plan_id,status,idempotency_hash,request_digest,revision,created_at,updated_at) VALUES('goal-destroy-0001','@owner:example.com','destroy test','connection-destroy-0001','plan-destroy-0001','planned','goal-destroy-idempotency','goal-destroy-request',1,$1,$1)`, []any{now}},
		{`INSERT INTO p2p_cloud_plans(plan_id,goal_id,cloud_connection_id,status,title,summary,recipe_digest,quote_id,plan_hash,revision,created_at,updated_at) VALUES('plan-destroy-0001','goal-destroy-0001','connection-destroy-0001','approved','Destroy test','safe','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','quote-destroy-0001','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',4,$1,$1)`, []any{now}},
		{`INSERT INTO p2p_cloud_connections(cloud_connection_id,provider,account_id,region,mode,status,revision,created_at,updated_at) VALUES('connection-destroy-0001','aws','123456789012','ap-south-1','role','active',1,$1,$1)`, []any{now}},
		{`INSERT INTO p2p_cloud_connection_bootstraps(bootstrap_id,owner_mxid,cloud_connection_id,provider,requested_region,template_url,template_digest,source_tree_digest,stack_name,node_key_id,node_public_key_spki_base64,device_approval_key_id,device_approval_public_key_spki_base64,status,revision,idempotency_hash,request_digest,expires_at,created_at,updated_at) VALUES('bootstrap-destroy-0001','@owner:example.com','connection-destroy-0001','aws','ap-south-1','https://example.invalid/template.json','sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc','sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd','dirextalk-destroy','node-destroy-0001',$1,'device-confirmation-1',$1,'active',1,'bootstrap-destroy-idempotency','bootstrap-destroy-request',$2,$3,$3)`, []any{publicSPKI, now + int64(time.Hour/time.Millisecond), now}},
		{`INSERT INTO p2p_cloud_recipes(recipe_id,name,version,digest,maturity,revision,created_at,updated_at) VALUES('recipe-destroy-0001','Destroy test recipe','v1','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','experimental',1,$1,$1)`, []any{now}},
		{`INSERT INTO p2p_cloud_deployments(deployment_id,plan_id,cloud_connection_id,execution_status,outcome_status,resource_status,revision,created_at,updated_at) VALUES('deployment-destroy-0001','plan-destroy-0001','connection-destroy-0001','finished','succeeded','active',5,$1,$1)`, []any{now}},
		{`INSERT INTO p2p_cloud_services(service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at) VALUES('service-destroy-0001','deployment-destroy-0001','recipe-destroy-0001','Destroy test service','experimental','not_requested',2,$1,$1)`, []any{now}},
		{`INSERT INTO p2p_cloud_deployment_resources(deployment_id,cloud_connection_id,request_sha256,resource_status,instance_id,volume_ids_json,network_interface_ids_json,broker_receipt_json,created_at,updated_at) VALUES('deployment-destroy-0001','connection-destroy-0001','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','active','i-0123456789abcdef0',$1,$2,'{}',$3,$3)`, []any{string(volumes), string(interfaces), now}},
	}
	for _, statement := range statements {
		if _, err := store.DB().ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed service destroy state: %v", err)
		}
	}
}
