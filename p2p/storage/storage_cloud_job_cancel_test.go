package storage

import (
	"context"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func TestDatabaseStoreCloudJobCancelClosesExecutionWithoutDestroyingResources(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	privateKey, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudJobCancelState(t, store, publicSPKI, now.UnixMilli())

	prepared, err := store.PrepareCloudJobCancel(ctx, cloudmodule.PrepareJobCancelRequest{
		OwnerMXID: "@owner:example.com", JobID: "job-cancel-1", ExpectedRevision: 3,
		IdempotencyHash: "prepare-job-cancel-1", ApprovalID: "approval-job-cancel-1", ChallengeID: "challenge-job-cancel-1",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil || !prepared.Created {
		t.Fatalf("prepare cancel = %#v, err=%v", prepared, err)
	}
	signed, err := prepared.Confirmation.Approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	request := cloudmodule.ApproveJobCancelRequest{
		OwnerMXID: "@owner:example.com", JobID: "job-cancel-1", ExpectedRevision: 3,
		IdempotencyHash: "approve-job-cancel-1", Approval: signed,
		JobEventID: "event-job-cancel-1", DeploymentEventID: "event-deployment-cancel-1", CreatedAt: now.Add(time.Minute).UnixMilli(),
	}
	approved, err := store.ApproveCloudJobCancel(ctx, request)
	if err != nil || !approved.Created {
		t.Fatalf("approve cancel = %#v, err=%v", approved, err)
	}
	if approved.Job.Execution != "finished" || approved.Job.Outcome != "canceled" || approved.Job.Checkpoint != "job_canceled" || approved.Job.Revision != 4 {
		t.Fatalf("canceled job = %#v", approved.Job)
	}
	if approved.Deployment.Execution != "finished" || approved.Deployment.Outcome != "canceled" || approved.Deployment.Resource != "retained_tracked" || approved.Deployment.Revision != 6 {
		t.Fatalf("canceled deployment = %#v", approved.Deployment)
	}

	var stepStatus, stepCheckpoint, resourceStatus, taskStatus, leaseOwner, leaseToken, commandState string
	var completedAt int64
	if err = store.DB().QueryRowContext(ctx, `SELECT status,checkpoint FROM p2p_cloud_job_steps WHERE job_id='job-cancel-1' AND step_id='install'`).Scan(&stepStatus, &stepCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id='deployment-cancel-1'`).Scan(&resourceStatus); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, `SELECT completed_at FROM p2p_cloud_outbox WHERE outbox_id='outbox-install-cancel-1'`).Scan(&completedAt); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, `SELECT task_status,lease_owner,lease_token FROM p2p_cloud_recipe_install_tasks WHERE execution_id='execution-cancel-1'`).Scan(&taskStatus, &leaseOwner, &leaseToken); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, `SELECT state FROM p2p_cloud_recipe_install_commands WHERE command_id='command-install-cancel-1'`).Scan(&commandState); err != nil {
		t.Fatal(err)
	}
	if stepStatus != "canceled" || stepCheckpoint != "job_canceled" || resourceStatus != "retained_tracked" || completedAt == 0 || taskStatus != "interrupted" || leaseOwner != "" || leaseToken != "" || commandState != "expired" {
		t.Fatalf("cancel facts step=%s/%s resource=%s outbox_completed=%d task=%s lease=%s/%s command=%s", stepStatus, stepCheckpoint, resourceStatus, completedAt, taskStatus, leaseOwner, leaseToken, commandState)
	}

	replay, err := store.ApproveCloudJobCancel(ctx, request)
	if err != nil || replay.Created || replay.Job.Revision != approved.Job.Revision || replay.Deployment.Revision != approved.Deployment.Revision {
		t.Fatalf("approve replay = %#v, err=%v", replay, err)
	}

	var eventCount int
	if err = store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE event_id IN ('event-job-cancel-1','event-deployment-cancel-1')`).Scan(&eventCount); err != nil || eventCount != 2 {
		t.Fatalf("cancel event count=%d err=%v", eventCount, err)
	}
}

func TestDatabaseStoreCloudJobCancelRejectsProvisionAfterCommandAllocation(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 16, 9, 0, 0, 0, time.UTC)
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudJobCancelState(t, store, publicSPKI, now.UnixMilli())
	for _, query := range []string{
		`DELETE FROM p2p_cloud_recipe_install_commands WHERE deployment_id='deployment-cancel-1'`,
		`DELETE FROM p2p_cloud_recipe_install_tasks WHERE deployment_id='deployment-cancel-1'`,
		`DELETE FROM p2p_cloud_outbox WHERE outbox_id='outbox-install-cancel-1'`,
		`DELETE FROM p2p_cloud_deployment_resources WHERE deployment_id='deployment-cancel-1'`,
		`UPDATE p2p_cloud_jobs SET kind='provision',execution_status='queued',checkpoint='provision_queued' WHERE job_id='job-cancel-1'`,
		`UPDATE p2p_cloud_job_steps SET step_id='provision',status='queued',checkpoint='provision_queued' WHERE job_id='job-cancel-1'`,
		`UPDATE p2p_cloud_deployments SET execution_status='queued',resource_status='none' WHERE deployment_id='deployment-cancel-1'`,
		`INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,created_at) VALUES('outbox-provision-cancel-1','cloud.deployment.provision.requested','deployment','deployment-cancel-1','{}',1)`,
		`INSERT INTO p2p_cloud_deployment_commands(command_id,deployment_id,cloud_connection_id,plan_id,plan_revision,approval_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at) VALUES('command-provision-cancel-1','deployment-cancel-1','connection-cancel-1','plan-cancel-1',4,'plan-approval-cancel-1','request-provision-cancel-1',1,'deployment.create','node-cancel-1',1,2,'allocated',1,1)`,
	} {
		if _, err := store.DB().ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	_, err := store.PrepareCloudJobCancel(ctx, cloudmodule.PrepareJobCancelRequest{
		OwnerMXID: "@owner:example.com", JobID: "job-cancel-1", ExpectedRevision: 3,
		IdempotencyHash: "prepare-provision-cancel-1", ApprovalID: "approval-provision-cancel-1", ChallengeID: "challenge-provision-cancel-1",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != cloudmodule.ErrJobCancelNotCancellable {
		t.Fatalf("prepare provision after command allocation error=%v", err)
	}
}

func seedCloudJobCancelState(t *testing.T, store *DatabaseStore, publicSPKI string, createdAt int64) {
	t.Helper()
	ctx := context.Background()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO p2p_cloud_goals(goal_id,owner_mxid,prompt,cloud_connection_id,plan_id,status,idempotency_hash,request_digest,revision,created_at,updated_at) VALUES('goal-cancel-1','@owner:example.com','deploy','connection-cancel-1','plan-cancel-1','planned','goal-cancel-idem','goal-cancel-request',1,$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_plans(plan_id,goal_id,cloud_connection_id,status,title,summary,recipe_digest,quote_id,plan_hash,revision,created_at,updated_at) VALUES('plan-cancel-1','goal-cancel-1','connection-cancel-1','approved','Install','Install','','','plan-hash-cancel-1',4,$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_connections(cloud_connection_id,provider,account_id,region,mode,status,revision,created_at,updated_at) VALUES('connection-cancel-1','aws','123456789012','ap-south-1','role','active',1,$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_connection_bootstraps(bootstrap_id,owner_mxid,cloud_connection_id,provider,requested_region,template_url,template_digest,source_tree_digest,stack_name,node_key_id,node_public_key_spki_base64,device_approval_key_id,device_approval_public_key_spki_base64,status,revision,idempotency_hash,request_digest,expires_at,created_at,updated_at) VALUES('bootstrap-cancel-1','@owner:example.com','connection-cancel-1','aws','ap-south-1','https://example.invalid/template','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','stack-cancel-1','node-cancel-1',$1,'device-cancel-1',$1,'active',1,'bootstrap-cancel-idem','bootstrap-cancel-request',$2,$3,$3)`, []any{publicSPKI, createdAt + int64(time.Hour/time.Millisecond), createdAt}},
		{`INSERT INTO p2p_cloud_deployments(deployment_id,plan_id,cloud_connection_id,execution_status,outcome_status,resource_status,revision,created_at,updated_at) VALUES('deployment-cancel-1','plan-cancel-1','connection-cancel-1','installing','pending','active',5,$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_deployment_resources(deployment_id,cloud_connection_id,request_sha256,resource_status,instance_id,volume_ids_json,network_interface_ids_json,broker_receipt_json,created_at,updated_at) VALUES('deployment-cancel-1','connection-cancel-1','sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc','active','i-0123456789abcdef0','[]','[]','{}',$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES('job-cancel-1','plan-cancel-1','deployment-cancel-1','install','installing','pending','install_running','',3,$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_job_steps(job_id,step_id,status,summary,checkpoint,error_code,revision,created_at,updated_at) VALUES('job-cancel-1','install','running','Installing','install_running','',2,$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_recipe_install_tasks(execution_id,task_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,checkpoint_sequence_json,task_status,task_attempt,last_sequence,last_checkpoint,error_code,available_at,lease_owner,lease_token,lease_until,attempts,last_error_code,created_at,updated_at) VALUES('execution-cancel-1','task-install-cancel-1','deployment-cancel-1','plan-cancel-1','connection-cancel-1','i-0123456789abcdef0','sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd','sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','[]','running',1,0,'install_running','',0,'runner','lease-cancel-1',$1,1,'',$2,$2)`, []any{createdAt + int64(time.Minute/time.Millisecond), createdAt}},
		{`INSERT INTO p2p_cloud_recipe_install_commands(command_id,execution_id,deployment_id,task_id,cloud_connection_id,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at) VALUES('command-install-cancel-1','execution-cancel-1','deployment-cancel-1','task-install-cancel-1','connection-cancel-1','request-install-cancel-1',1,'worker.recipe_task.issue','node-cancel-1',1,1,'allocated',$1,$1)`, []any{createdAt}},
		{`INSERT INTO p2p_cloud_outbox(outbox_id,kind,aggregate_type,aggregate_id,payload_json,lease_owner,lease_token,lease_until,attempts,delivered_at,created_at,available_at,last_error_code,completed_at) VALUES('outbox-install-cancel-1',$1,'recipe_execution','execution-cancel-1','{}','runner','lease-cancel-1',$2,1,0,$3,$3,'',0)`, []any{cloudmodule.OutboxKindRecipeExecutionInstallRequested, createdAt + int64(time.Minute/time.Millisecond), createdAt}},
	}
	for _, statement := range statements {
		if _, err := store.DB().ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed job cancel state: %v", err)
		}
	}
}
