package storage

import (
	"context"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func TestDatabaseStoreDeploymentDestroyQueuesExactResidualResourceScope(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC)
	privateKey, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudJobCancelState(t, store, publicSPKI, now.UnixMilli())
	makeSeedDeploymentDestroyable(t, store, now.UnixMilli())
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_deployment_resources SET volume_ids_json='["vol-0123456789abcdef0"]',network_interface_ids_json='["eni-0123456789abcdef0"]' WHERE deployment_id='deployment-cancel-1'`); err != nil {
		t.Fatal(err)
	}

	prepared, err := store.PrepareCloudDeploymentDestroy(ctx, cloudmodule.PrepareDeploymentDestroyRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: "deployment-cancel-1", ExpectedRevision: 5,
		IdempotencyHash: "prepare-deployment-destroy-1", ApprovalID: "approval-deployment-destroy-1", ChallengeID: "challenge-deployment-destroy-1",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil || !prepared.Created {
		t.Fatalf("prepare deployment destroy=%#v err=%v", prepared, err)
	}
	if prepared.Confirmation.Approval.InstanceID != "i-0123456789abcdef0" || len(prepared.Confirmation.Approval.VolumeIDs) != 1 || len(prepared.Confirmation.Approval.NetworkInterfaceIDs) != 1 {
		t.Fatalf("approval scope=%#v", prepared.Confirmation.Approval)
	}
	if _, err = store.PrepareCloudDeploymentDestroy(ctx, cloudmodule.PrepareDeploymentDestroyRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: "deployment-cancel-1", ExpectedRevision: 4,
		IdempotencyHash: "prepare-deployment-destroy-stale", ApprovalID: "approval-deployment-destroy-stale", ChallengeID: "challenge-deployment-destroy-stale",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	}); err != cloudmodule.ErrDeploymentDestroyConflict {
		t.Fatalf("stale prepare error=%v", err)
	}
	signed, err := prepared.Confirmation.Approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tampered := prepared.Confirmation.Approval
	tampered.ChallengeID = "challenge-deployment-destroy-forged"
	tampered, err = tampered.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApproveCloudDeploymentDestroy(ctx, cloudmodule.ApproveDeploymentDestroyRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: "deployment-cancel-1", ExpectedRevision: 5,
		IdempotencyHash: "approve-deployment-destroy-tampered", Approval: tampered, JobID: "job-deployment-destroy-tampered", OutboxID: "outbox-deployment-destroy-tampered",
		DeploymentEventID: "event-deployment-destroy-tampered", JobEventID: "event-job-deployment-destroy-tampered", CreatedAt: now.Add(time.Minute).UnixMilli(),
	}); err != cloudmodule.ErrDeploymentDestroyInvalid {
		t.Fatalf("tampered challenge error=%v", err)
	}
	request := cloudmodule.ApproveDeploymentDestroyRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: "deployment-cancel-1", ExpectedRevision: 5,
		IdempotencyHash: "approve-deployment-destroy-1", Approval: signed, JobID: "job-deployment-destroy-1", OutboxID: "outbox-deployment-destroy-1",
		DeploymentEventID: "event-deployment-destroy-1", JobEventID: "event-job-deployment-destroy-1", CreatedAt: now.Add(time.Minute).UnixMilli(),
	}
	approved, err := store.ApproveCloudDeploymentDestroy(ctx, request)
	if err != nil || !approved.Created {
		t.Fatalf("approve deployment destroy=%#v err=%v", approved, err)
	}
	if approved.Deployment.Resource != "destroying" || approved.Deployment.Revision != 6 || approved.Job.Kind != "destroy" || approved.Job.Execution != "queued" || approved.Job.Outcome != "pending" {
		t.Fatalf("approved deployment destroy=%#v/%#v", approved.Deployment, approved.Job)
	}
	var resourceStatus, outboxKind, aggregateType, aggregateID string
	var serviceCount int
	if err = store.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id='deployment-cancel-1'`).Scan(&resourceStatus); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, `SELECT kind,aggregate_type,aggregate_id FROM p2p_cloud_outbox WHERE outbox_id='outbox-deployment-destroy-1'`).Scan(&outboxKind, &aggregateType, &aggregateID); err != nil {
		t.Fatal(err)
	}
	if err = store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_services WHERE deployment_id='deployment-cancel-1'`).Scan(&serviceCount); err != nil {
		t.Fatal(err)
	}
	if resourceStatus != "destroying" || outboxKind != cloudmodule.OutboxKindDeploymentDestroyRequested || aggregateType != "deployment" || aggregateID != "deployment-cancel-1" || serviceCount != 0 {
		t.Fatalf("private state resource=%s outbox=%s/%s/%s services=%d", resourceStatus, outboxKind, aggregateType, aggregateID, serviceCount)
	}
	replay, err := store.ApproveCloudDeploymentDestroy(ctx, request)
	if err != nil || replay.Created || replay.Job.JobID != approved.Job.JobID || replay.Deployment.Revision != approved.Deployment.Revision {
		t.Fatalf("approve replay=%#v err=%v", replay, err)
	}
}

func TestDatabaseStoreDeploymentDestroyRejectsServiceOwnedDeployment(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC)
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudJobCancelState(t, store, publicSPKI, now.UnixMilli())
	makeSeedDeploymentDestroyable(t, store, now.UnixMilli())
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_services(service_id,deployment_id,recipe_id,name,service_status,integration_status,revision,created_at,updated_at) VALUES('service-owned-deployment-destroy','deployment-cancel-1','recipe-owned-deployment-destroy','Owned service','degraded','not_requested',1,$1,$1)`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	_, err := store.PrepareCloudDeploymentDestroy(ctx, cloudmodule.PrepareDeploymentDestroyRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: "deployment-cancel-1", ExpectedRevision: 5,
		IdempotencyHash: "prepare-service-owned-deployment-destroy", ApprovalID: "approval-service-owned-deployment-destroy", ChallengeID: "challenge-service-owned-deployment-destroy",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != cloudmodule.ErrDeploymentDestroyInvalid {
		t.Fatalf("service-owned deployment destroy error=%v", err)
	}
}

func TestDatabaseStoreDeploymentDestroyRejectsPendingDeploymentJob(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC)
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudJobCancelState(t, store, publicSPKI, now.UnixMilli())
	_, err := store.PrepareCloudDeploymentDestroy(ctx, cloudmodule.PrepareDeploymentDestroyRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: "deployment-cancel-1", ExpectedRevision: 5,
		IdempotencyHash: "prepare-pending-deployment-destroy", ApprovalID: "approval-pending-deployment-destroy", ChallengeID: "challenge-pending-deployment-destroy",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != cloudmodule.ErrDeploymentDestroyInvalid {
		t.Fatalf("pending deployment destroy error=%v", err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_jobs SET execution_status='finished',outcome_status='failed',checkpoint='install_failed',updated_at=$1 WHERE job_id='job-cancel-1'`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	_, err = store.PrepareCloudDeploymentDestroy(ctx, cloudmodule.PrepareDeploymentDestroyRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: "deployment-cancel-1", ExpectedRevision: 5,
		IdempotencyHash: "prepare-nonterminal-deployment-destroy", ApprovalID: "approval-nonterminal-deployment-destroy", ChallengeID: "challenge-nonterminal-deployment-destroy",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != cloudmodule.ErrDeploymentDestroyInvalid {
		t.Fatalf("nonterminal deployment destroy error=%v", err)
	}
}

func makeSeedDeploymentDestroyable(t *testing.T, store *DatabaseStore, now int64) {
	t.Helper()
	ctx := context.Background()
	statements := []string{
		`UPDATE p2p_cloud_jobs SET execution_status='finished',outcome_status='canceled',checkpoint='job_canceled',updated_at=$1 WHERE job_id='job-cancel-1'`,
		`UPDATE p2p_cloud_deployments SET execution_status='finished',outcome_status='canceled',updated_at=$1 WHERE deployment_id='deployment-cancel-1'`,
	}
	for _, statement := range statements {
		if _, err := store.DB().ExecContext(ctx, statement, now); err != nil {
			t.Fatal(err)
		}
	}
}
