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

func TestDatabaseStoreManagementAcceptancePromotesOnlyExactVerifiedEvidence(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 16, 2, 0, 0, 0, time.UTC)
	private, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	seedManagementAcceptanceEvidence(t, store, now)
	prepare := cloudmodule.PrepareServiceManagementAcceptanceRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "management-prepare-idem", RequestDigest: "management-prepare-request", AcceptanceID: "acceptance-management-storage-0001", ApprovalID: "approval-management-storage-0001", ChallengeID: "challenge-management-storage-0001", ServiceEventID: "event-management-awaiting-0001", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}
	prepared, err := store.PrepareCloudServiceManagementAcceptance(ctx, prepare)
	if err != nil || !prepared.Created || prepared.Confirmation.Service.Status != "awaiting_management_acceptance" || prepared.Confirmation.Service.Revision != 3 || prepared.Confirmation.Recipe.Maturity != "awaiting_management_acceptance" || prepared.Confirmation.Recipe.Revision != 2 {
		t.Fatalf("prepare=%#v err=%v", prepared, err)
	}
	signed, err := prepared.Confirmation.Approval.Sign(private, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tampered := signed
	tampered.RestoreRevision++
	if _, err = store.ApproveCloudServiceManagementAcceptance(ctx, managementApproveRequest(tampered, now.Add(time.Minute).UnixMilli(), "management-approve-tampered")); !errors.Is(err, cloudmodule.ErrServiceManagementAcceptanceInvalid) && !errors.Is(err, cloudmodule.ErrServiceManagementAcceptanceSignature) {
		t.Fatalf("tampered evidence=%v", err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET stack_observation_digest=$1 WHERE task_id='readiness-management-install'`, "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApproveCloudServiceManagementAcceptance(ctx, managementApproveRequest(signed, now.Add(time.Minute).UnixMilli(), "management-approve-readiness-tampered")); !errors.Is(err, cloudmodule.ErrServiceManagementAcceptanceInvalid) {
		t.Fatalf("replaced readiness evidence=%v", err)
	}
	if _, err = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET stack_observation_digest=$1 WHERE task_id='readiness-management-install'`, "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"); err != nil {
		t.Fatal(err)
	}
	approved, err := store.ApproveCloudServiceManagementAcceptance(ctx, managementApproveRequest(signed, now.Add(time.Minute).UnixMilli(), "management-approve-idem"))
	if err != nil || !approved.Created || approved.Service.Status != "active" || approved.Service.Revision != 4 || approved.Recipe.Maturity != "managed" || approved.Recipe.Revision != 3 || approved.Acceptance.Status != "approved" {
		t.Fatalf("approved=%#v err=%v", approved, err)
	}
	var serviceStatus, recipeMaturity, acceptanceStatus string
	var serviceRevision, recipeRevision int64
	if err = store.DB().QueryRowContext(ctx, `SELECT service.service_status,service.revision,recipe.maturity,recipe.revision,acceptance.status FROM p2p_cloud_services service JOIN p2p_cloud_recipes recipe ON recipe.recipe_id=service.recipe_id JOIN p2p_cloud_service_management_acceptances acceptance ON acceptance.service_id=service.service_id WHERE service.service_id=$1`, prepare.ServiceID).Scan(&serviceStatus, &serviceRevision, &recipeMaturity, &recipeRevision, &acceptanceStatus); err != nil {
		t.Fatal(err)
	}
	if serviceStatus != "active" || serviceRevision != 4 || recipeMaturity != "managed" || recipeRevision != 3 || acceptanceStatus != "approved" {
		t.Fatalf("service=%s/%d recipe=%s/%d acceptance=%s", serviceStatus, serviceRevision, recipeMaturity, recipeRevision, acceptanceStatus)
	}
	tx, err := store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, managedDigest, managedMaturity, lockErr := lockCloudRecipeForConfirmation(ctx, tx, approved.Recipe.Digest)
	_ = tx.Rollback()
	if lockErr != nil || managedDigest != approved.Recipe.Digest || managedMaturity != cloudcontracts.RecipeManaged {
		t.Fatalf("managed recipe confirmation binding=%s/%s err=%v", managedDigest, managedMaturity, lockErr)
	}
	replay, err := store.ApproveCloudServiceManagementAcceptance(ctx, managementApproveRequest(signed, now.Add(time.Minute).UnixMilli(), "management-approve-idem"))
	if err != nil || replay.Created || replay.Acceptance.AcceptanceID != approved.Acceptance.AcceptanceID {
		t.Fatalf("replay=%#v err=%v", replay, err)
	}
}

func TestDatabaseStoreManagementAcceptanceRequiresRestartAfterRestore(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 16, 3, 0, 0, 0, time.UTC)
	_, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	seedManagementAcceptanceEvidence(t, store, now)
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_service_operation_tasks SET updated_at=$1 WHERE operation='restart'`, now.Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	_, err := store.PrepareCloudServiceManagementAcceptance(ctx, cloudmodule.PrepareServiceManagementAcceptanceRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "management-restart-gate-idem", RequestDigest: "management-restart-gate-request", AcceptanceID: "acceptance-management-restart-gate", ApprovalID: "approval-management-restart-gate", ChallengeID: "challenge-management-restart-gate", ServiceEventID: "event-management-restart-gate", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()})
	if !errors.Is(err, cloudmodule.ErrServiceManagementAcceptanceInvalid) {
		t.Fatalf("restore without later restart must fail, got %v", err)
	}
}

func TestDatabaseStoreManagementAcceptanceRejectsUnboundReadinessEvidence(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 16, 3, 30, 0, 0, time.UTC)
	_, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	seedManagementAcceptanceEvidence(t, store, now)
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_service_readiness_tasks SET semantic_evidence_digest='sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' WHERE purpose='install'`); err != nil {
		t.Fatal(err)
	}
	_, err := store.PrepareCloudServiceManagementAcceptance(ctx, cloudmodule.PrepareServiceManagementAcceptanceRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "management-readiness-gate-idem", RequestDigest: "management-readiness-gate-request", AcceptanceID: "acceptance-management-readiness-gate", ApprovalID: "approval-management-readiness-gate", ChallengeID: "challenge-management-readiness-gate", ServiceEventID: "event-management-readiness-gate", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()})
	if !errors.Is(err, cloudmodule.ErrServiceManagementAcceptanceInvalid) {
		t.Fatalf("unbound readiness evidence must fail, got %v", err)
	}
}

func TestDatabaseStoreManagementAcceptanceReissuesExpiredChallengeWithoutSecondMaturityTransition(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 16, 5, 0, 0, 0, time.UTC)
	_, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	seedManagementAcceptanceEvidence(t, store, now)
	first := cloudmodule.PrepareServiceManagementAcceptanceRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "management-expiry-first-idem", RequestDigest: "management-expiry-first-request", AcceptanceID: "acceptance-management-expiry-first", ApprovalID: "approval-management-expiry-first", ChallengeID: "challenge-management-expiry-first", ServiceEventID: "event-management-expiry-first", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()}
	prepared, err := store.PrepareCloudServiceManagementAcceptance(ctx, first)
	if err != nil || !prepared.ServiceChanged || prepared.Confirmation.Service.Revision != 3 || prepared.Confirmation.Recipe.Revision != 2 {
		t.Fatalf("first=%#v err=%v", prepared, err)
	}
	reissuedAt := now.Add(6 * time.Minute)
	second := cloudmodule.PrepareServiceManagementAcceptanceRequest{OwnerMXID: first.OwnerMXID, ServiceID: first.ServiceID, ExpectedRevision: 3, IdempotencyHash: "management-expiry-second-idem", RequestDigest: "management-expiry-second-request", AcceptanceID: "acceptance-management-expiry-second", ApprovalID: "approval-management-expiry-second", ChallengeID: "challenge-management-expiry-second", ServiceEventID: "event-management-expiry-second", CreatedAt: reissuedAt.UnixMilli(), ExpiresAt: reissuedAt.Add(5 * time.Minute).UnixMilli()}
	reissued, err := store.PrepareCloudServiceManagementAcceptance(ctx, second)
	if err != nil || !reissued.Created || reissued.ServiceChanged || reissued.Confirmation.Service.Revision != 3 || reissued.Confirmation.Recipe.Revision != 2 || reissued.Confirmation.Approval.AcceptanceID != second.AcceptanceID {
		t.Fatalf("reissued=%#v err=%v", reissued, err)
	}
	var expired, pending int
	if err = store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FILTER(WHERE status='expired'),COUNT(*) FILTER(WHERE status='pending') FROM p2p_cloud_service_management_acceptances WHERE service_id=$1`, first.ServiceID).Scan(&expired, &pending); err != nil || expired != 1 || pending != 1 {
		t.Fatalf("expired=%d pending=%d err=%v", expired, pending, err)
	}
}

func TestDatabaseStoreManagementAcceptanceExpiredApprovalReplayIsStable(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 16, 6, 0, 0, 0, time.UTC)
	private, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	seedManagementAcceptanceEvidence(t, store, now)
	prepared, err := store.PrepareCloudServiceManagementAcceptance(ctx, cloudmodule.PrepareServiceManagementAcceptanceRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "management-expired-approve-prepare", RequestDigest: "management-expired-approve-request", AcceptanceID: "acceptance-management-expired-approve", ApprovalID: "approval-management-expired-approve", ChallengeID: "challenge-management-expired-approve", ServiceEventID: "event-management-expired-approve", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	signed, err := prepared.Confirmation.Approval.Sign(private, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	request := managementApproveRequest(signed, now.Add(6*time.Minute).UnixMilli(), "management-expired-approve-idem")
	if _, err = store.ApproveCloudServiceManagementAcceptance(ctx, request); !errors.Is(err, cloudmodule.ErrServiceManagementAcceptanceExpired) {
		t.Fatalf("first expired approval = %v", err)
	}
	if _, err = store.ApproveCloudServiceManagementAcceptance(ctx, request); !errors.Is(err, cloudmodule.ErrServiceManagementAcceptanceExpired) {
		t.Fatalf("expired approval replay = %v", err)
	}
}

func TestDatabaseStoreManagementAcceptanceReusesManagedRecipeWithoutNewRecipeVersion(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 16, 7, 0, 0, 0, time.UTC)
	private, public := cloudConfirmationDeviceKey(t)
	seedServiceDestroyState(t, store, public, now.UnixMilli())
	seedManagementAcceptanceEvidence(t, store, now)
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at) SELECT recipe_id,2,canonical_cbor,display_json,digest,'managed',$1 FROM p2p_cloud_recipe_versions WHERE recipe_id='recipe-destroy-0001' AND revision=1`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipes SET maturity='managed',revision=2,updated_at=$1 WHERE recipe_id='recipe-destroy-0001'`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	prepared, err := store.PrepareCloudServiceManagementAcceptance(ctx, cloudmodule.PrepareServiceManagementAcceptanceRequest{OwnerMXID: "@owner:example.com", ServiceID: "service-destroy-0001", ExpectedRevision: 2, IdempotencyHash: "management-managed-reuse-prepare", RequestDigest: "management-managed-reuse-request", AcceptanceID: "acceptance-management-managed-reuse", ApprovalID: "approval-management-managed-reuse", ChallengeID: "challenge-management-managed-reuse", ServiceEventID: "event-management-managed-reuse-awaiting", CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli()})
	if err != nil || !prepared.ServiceChanged || prepared.Confirmation.Service.Status != "awaiting_management_acceptance" || prepared.Confirmation.Recipe.Maturity != "managed" || prepared.Confirmation.Recipe.Revision != 2 || prepared.Confirmation.Approval.RecipeMaturity != cloudcontracts.RecipeManaged {
		t.Fatalf("prepare managed reuse=%#v err=%v", prepared, err)
	}
	signed, err := prepared.Confirmation.Approval.Sign(private, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approved, err := store.ApproveCloudServiceManagementAcceptance(ctx, managementApproveRequest(signed, now.Add(time.Minute).UnixMilli(), "management-managed-reuse-approve"))
	if err != nil || approved.Service.Status != "active" || approved.Recipe.Maturity != "managed" || approved.Recipe.Revision != 2 {
		t.Fatalf("approve managed reuse=%#v err=%v", approved, err)
	}
	var versions int
	if err = store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_recipe_versions WHERE recipe_id='recipe-destroy-0001'`).Scan(&versions); err != nil || versions != 2 {
		t.Fatalf("managed recipe versions=%d err=%v", versions, err)
	}
}

func seedManagementAcceptanceEvidence(t *testing.T, store *DatabaseStore, now time.Time) {
	t.Helper()
	recipe, _ := cloudConfirmationFixtures(t, now, "connection-destroy-0001", "quote-unused")
	recipe.RecipeID = "recipe-destroy-0001"
	digest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := recipe.CanonicalRecipeCBOR()
	display, _ := json.Marshal(recipe)
	manifest := cloudcontracts.RecipeExecutionManifestV1{SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema, ExecutionID: "execution-management-install-0001", DeploymentID: "deployment-destroy-0001", PlanID: "plan-destroy-0001", PlanHash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PlanRevision: 4, RecipeDigest: digest, WorkerResourceManifestDigest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", ArtifactDigest: cloudcontracts.FixedProbeManagedArtifactDigest, ActionID: cloudcontracts.FixedProbeInstallActionID, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_binary_installed", "probe_service_installed", "probe_health_verified"}, VolumeSlots: []cloudcontracts.VolumeSlotV1{{SlotID: "knowledge", VolumeRef: "volume_ref:knowledge", ReadOnly: true}}, SecretSlots: []cloudcontracts.SecretSlotV1{{SlotID: "model", SecretRef: "secret_ref:model"}}}
	manifestDigest, _ := manifest.Digest()
	manifestJSON, _ := json.Marshal(manifest)
	replacements, _ := json.Marshal([]string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"})
	snaps, _ := json.Marshal([]map[string]any{{"volume_id": "vol-0aaaaaaaaaaaaaaaa", "snapshot_id": "snap-0aaaaaaaaaaaaaaaa", "state": "completed", "encrypted": true}, {"volume_id": "vol-0bbbbbbbbbbbbbbbb", "snapshot_id": "snap-0bbbbbbbbbbbbbbbb", "state": "completed", "encrypted": true}})
	statements := []struct {
		query string
		args  []any
	}{
		{`UPDATE p2p_cloud_recipes SET digest=$1 WHERE recipe_id='recipe-destroy-0001'`, []any{digest}},
		{`UPDATE p2p_cloud_plans SET recipe_digest=$1 WHERE plan_id='plan-destroy-0001'`, []any{digest}},
		{`INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at) VALUES('recipe-destroy-0001',1,$1,$2,$3,'experimental',$4)`, []any{canonical, string(display), digest, now.UnixMilli()}},
		{`INSERT INTO p2p_cloud_recipe_execution_manifests(execution_id,deployment_id,plan_id,plan_revision,plan_hash,cloud_connection_id,manifest_digest,manifest_cbor,manifest_json,status,revision,created_at,updated_at) VALUES($1,$2,$3,4,$4,'connection-destroy-0001',$5,$6,$7,'approved',2,$8,$8)`, []any{manifest.ExecutionID, manifest.DeploymentID, manifest.PlanID, manifest.PlanHash, manifestDigest, []byte{1}, string(manifestJSON), now.UnixMilli()}},
		{`INSERT INTO p2p_cloud_service_readiness_tasks(task_id,execution_id,deployment_id,service_id,cloud_connection_id,instance_id,recipe_execution_manifest_digest,install_evidence_digest,semantic_expectation_digest,task_status,purpose,job_id,semantic_evidence_digest,stack_observation_digest,created_at,updated_at) VALUES('readiness-management-install','execution-management-install-0001','deployment-destroy-0001','service-destroy-0001','connection-destroy-0001','i-0123456789abcdef0',$1,$2,$3,'succeeded','install','',$3,$4,$5,$5)`, []any{manifestDigest, "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", cloudcontracts.FixedReadinessEvidenceDigestV1, "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", now.UnixMilli()}},
		{`INSERT INTO p2p_cloud_service_backups(backup_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,plan_id,cloud_connection_id,instance_id,volume_ids_json,retention_policy,job_id,backup_status,image_id,snapshots_json,revision,created_at,updated_at) VALUES('backup-management-0001','approval-backup-management','service-destroy-0001',2,'deployment-destroy-0001',5,'plan-destroy-0001','connection-destroy-0001','i-0123456789abcdef0',$1,'manual','job-backup-management','available','ami-0123456789abcdef0',$2,2,$3,$3)`, []any{string(replacements), string(snaps), now.UnixMilli()}},
		{`INSERT INTO p2p_cloud_service_restores(restore_id,restore_plan_id,approval_id,service_id,service_revision,deployment_id,deployment_revision,backup_id,backup_revision,plan_id,cloud_connection_id,instance_id,region,availability_zone,volume_swaps_json,original_volume_retention,failure_policy,job_id,restore_status,original_volume_ids_json,replacement_volume_ids_json,revision,created_at,updated_at) VALUES('restore-management-0001','restore-plan-management','approval-restore-management','service-destroy-0001',2,'deployment-destroy-0001',5,'backup-management-0001',2,'plan-destroy-0001','connection-destroy-0001','i-0123456789abcdef0','ap-south-1','ap-south-1a','[]','manual','reattach_original','job-restore-management','succeeded',$1,$1,2,$2,$2)`, []any{string(replacements), now.UnixMilli()}},
		{`INSERT INTO p2p_cloud_jobs(job_id,plan_id,deployment_id,kind,execution_status,outcome_status,checkpoint,error_code,revision,created_at,updated_at) VALUES('job-restart-management','plan-destroy-0001','deployment-destroy-0001','restart','finished','succeeded','service_operation_succeeded','',3,$1,$1)`, []any{now.Add(time.Minute).UnixMilli()}},
		{`INSERT INTO p2p_cloud_service_operation_tasks(operation_id,approval_id,service_id,service_revision,expected_service_status,operation,execution_id,deployment_id,plan_id,cloud_connection_id,instance_id,manifest_digest,input_digest,manifest_json,checkpoint_sequence_json,task_id,job_id,task_status,created_at,updated_at) VALUES('operation-restart-management','approval-restart-management','service-destroy-0001',2,'experimental','restart','execution-restart-management','deployment-destroy-0001','plan-destroy-0001','connection-destroy-0001','i-0123456789abcdef0',$1,$2,$3,'["probe_service_restarted","probe_health_verified"]','task-restart-management','job-restart-management','succeeded',$4,$4)`, []any{manifestDigest, digest, string(manifestJSON), now.Add(time.Minute).UnixMilli()}},
	}
	for _, statement := range statements {
		if _, err = store.DB().ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed management acceptance evidence: %v", err)
		}
	}
}

func managementApproveRequest(approval cloudcontracts.ServiceManagementAcceptanceApprovalV1, created int64, idempotency string) cloudmodule.ApproveServiceManagementAcceptanceRequest {
	return cloudmodule.ApproveServiceManagementAcceptanceRequest{OwnerMXID: "@owner:example.com", ServiceID: approval.ServiceID, ExpectedRevision: int64(approval.ServiceRevision), IdempotencyHash: idempotency, Approval: approval, ServiceEventID: "event-management-approved-0001", CreatedAt: created}
}
