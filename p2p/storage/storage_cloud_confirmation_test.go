package storage

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestDatabaseStoreCloudPlanConfirmationBindsCapacitySignatureAndProvisionOutbox(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)

	now := time.Date(2026, time.July, 14, 9, 0, 0, 0, time.UTC)
	owner := "@owner:example.com"
	connectionID := "connection-confirmation-1"
	planID := "cloud-plan-confirmation-1"
	quoteID := "quote-confirmation-1"
	privateKey, publicSPKI := cloudConfirmationDeviceKey(t)
	recipe, quote := cloudConfirmationFixtures(t, now, connectionID, quoteID)
	seedCloudConfirmationState(t, store, owner, connectionID, planID, recipe, quote, publicSPKI)

	prepare := cloudmodule.PreparePlanConfirmationRequest{
		OwnerMXID: owner, PlanID: planID, ExpectedRevision: 3, QuoteID: quoteID, CandidateTier: "recommended",
		IdempotencyHash: "prepare-idempotency-hash", RequestDigest: "prepare-request-digest",
		ApprovalID: "approval-confirmation-1", ChallengeID: "challenge-confirmation-1",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	}
	confirmed, err := store.PrepareCloudPlanConfirmation(ctx, prepare)
	if err != nil || !confirmed.Created {
		t.Fatalf("prepare confirmation = %#v, err=%v", confirmed, err)
	}
	if confirmed.Confirmation.Plan.Status != cloudmodule.PlanStatusReadyForConfirmation || confirmed.Confirmation.Plan.Revision != 4 || confirmed.Confirmation.Plan.PlanHash == "" {
		t.Fatalf("prepared plan = %#v", confirmed.Confirmation.Plan)
	}
	approval := confirmed.Confirmation.Approval
	if approval.PlanID != planID || approval.PlanRevision != 4 || approval.SignerKeyID != "device-confirmation-1" || approval.Signature != "" ||
		approval.ResourceScope.InstanceType != "m7i.xlarge" || approval.ResourceScope.Architecture != cloudcontracts.ArchitectureAMD64 ||
		approval.ResourceScope.VCPU != 4 || approval.ResourceScope.MemoryMiB != 16384 || approval.ResourceScope.DiskGiB != 80 ||
		approval.NetworkScope.PublicIngress || approval.NetworkScope.EntryPoint != cloudcontracts.EntryPointNone ||
		len(approval.SecretScope) != 0 || len(approval.IntegrationScope) != 0 {
		t.Fatalf("approval scope = %#v", approval)
	}
	if strings.Contains(mustCloudConfirmationJSON(t, confirmed), "private-key") || strings.Contains(mustCloudConfirmationJSON(t, confirmed), "credential") {
		t.Fatalf("confirmation leaked secret-like material: %s", mustCloudConfirmationJSON(t, confirmed))
	}

	replay, err := store.PrepareCloudPlanConfirmation(ctx, prepare)
	if err != nil || replay.Created || replay.Confirmation.Approval.ApprovalID != approval.ApprovalID {
		t.Fatalf("prepare replay = %#v, err=%v", replay, err)
	}
	conflict := prepare
	conflict.RequestDigest = "different-request-digest"
	if _, err := store.PrepareCloudPlanConfirmation(ctx, conflict); err != cloudmodule.ErrIdempotencyConflict {
		t.Fatalf("prepare idempotency conflict = %v", err)
	}

	signed, err := approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	createdAt := now.Add(time.Minute).UnixMilli()
	approvalRequest := cloudConfirmationApprovalRequest(owner, planID, "confirmation-1", signed, createdAt)
	approvalRequest.IdempotencyHash = "approve-idempotency-hash"
	approved, err := store.ApproveCloudPlan(ctx, approvalRequest)
	if err != nil || !approved.Created {
		t.Fatalf("approve confirmation = %#v, err=%v", approved, err)
	}
	if approved.Plan.Status != cloudmodule.PlanStatusApproved || approved.Plan.Revision != 5 || approved.Deployment.Resource != "none" || approved.Job.Checkpoint != "provision_queued" {
		t.Fatalf("approved result = %#v", approved)
	}
	if strings.Contains(mustCloudConfirmationJSON(t, approved), signed.Signature) {
		t.Fatalf("approval response leaked device signature: %s", mustCloudConfirmationJSON(t, approved))
	}
	replayedApproval, err := store.ApproveCloudPlan(ctx, approvalRequest)
	if err != nil || replayedApproval.Created || replayedApproval.Deployment.DeploymentID != approvalRequest.Deployment.DeploymentID {
		t.Fatalf("approval replay = %#v, err=%v", replayedApproval, err)
	}
	approvalConflict := approvalRequest
	approvalConflict.ExpectedRevision++
	if _, err := store.ApproveCloudPlan(ctx, approvalConflict); err != cloudmodule.ErrIdempotencyConflict {
		t.Fatalf("approval idempotency conflict = %v", err)
	}

	var outboxCount, deploymentCount, signatureInEvents int
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_outbox WHERE kind = $1`, cloudmodule.OutboxKindDeploymentProvisionRequested).Scan(&outboxCount); err != nil || outboxCount != 1 {
		t.Fatalf("provision outbox count=%d err=%v", outboxCount, err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_deployments WHERE deployment_id = $1`, approvalRequest.Deployment.DeploymentID).Scan(&deploymentCount); err != nil || deploymentCount != 1 {
		t.Fatalf("deployment count=%d err=%v", deploymentCount, err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE summary_json LIKE '%' || $1 || '%'`, signed.Signature).Scan(&signatureInEvents); err != nil || signatureInEvents != 0 {
		t.Fatalf("event signature leak count=%d err=%v", signatureInEvents, err)
	}
}

func TestDatabaseStoreCloudPlanConfirmationRejectsSpotBeforeRecipeInterruptionSupport(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC)
	owner := "@owner:example.com"
	connectionID := "connection-confirmation-spot-1"
	planID := "cloud-plan-confirmation-spot-1"
	quoteID := "quote-confirmation-spot-1"
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	recipe, quote := cloudConfirmationFixtures(t, now, connectionID, quoteID)
	quote.Candidates[0].PurchaseOption = cloudcontracts.PurchaseSpot
	if err := quote.Validate(); err != nil {
		t.Fatalf("spot quote fixture: %v", err)
	}
	seedCloudConfirmationState(t, store, owner, connectionID, planID, recipe, quote, publicSPKI)

	_, err := store.PrepareCloudPlanConfirmation(ctx, cloudmodule.PreparePlanConfirmationRequest{
		OwnerMXID: owner, PlanID: planID, ExpectedRevision: 3, QuoteID: quoteID, CandidateTier: "recommended",
		IdempotencyHash: "prepare-idempotency-spot", RequestDigest: "prepare-request-spot",
		ApprovalID: "approval-confirmation-spot-1", ChallengeID: "challenge-confirmation-spot-1",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != cloudmodule.ErrPlanConfirmationInvalid {
		t.Fatalf("spot confirmation error = %v, want %v", err, cloudmodule.ErrPlanConfirmationInvalid)
	}
}

func TestDatabaseStoreCloudPlanApprovalExpiryClosesPlanAndReplays(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 14, 11, 0, 0, 0, time.UTC)
	owner := "@owner:example.com"
	connectionID := "connection-confirmation-expired-1"
	planID := "cloud-plan-confirmation-expired-1"
	quoteID := "quote-confirmation-expired-1"
	privateKey, publicSPKI := cloudConfirmationDeviceKey(t)
	recipe, quote := cloudConfirmationFixtures(t, now, connectionID, quoteID)
	seedCloudConfirmationState(t, store, owner, connectionID, planID, recipe, quote, publicSPKI)

	confirmed, err := store.PrepareCloudPlanConfirmation(ctx, cloudmodule.PreparePlanConfirmationRequest{
		OwnerMXID: owner, PlanID: planID, ExpectedRevision: 3, QuoteID: quoteID, CandidateTier: "recommended",
		IdempotencyHash: "prepare-idempotency-expired", RequestDigest: "prepare-request-expired",
		ApprovalID: "approval-confirmation-expired-1", ChallengeID: "challenge-confirmation-expired-1",
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil || !confirmed.Created {
		t.Fatalf("prepare expired fixture = %#v, err=%v", confirmed, err)
	}
	signed, err := confirmed.Confirmation.Approval.Sign(privateKey, now)
	if err != nil {
		t.Fatal(err)
	}
	request := cloudConfirmationApprovalRequest(owner, planID, "expired-1", signed, now.Add(6*time.Minute).UnixMilli())
	request.IdempotencyHash = "approve-idempotency-expired"
	if _, err := store.ApproveCloudPlan(ctx, request); err != cloudmodule.ErrPlanApprovalExpired {
		t.Fatalf("expired approval error = %v, want %v", err, cloudmodule.ErrPlanApprovalExpired)
	}
	if _, err := store.ApproveCloudPlan(ctx, request); err != cloudmodule.ErrPlanApprovalExpired {
		t.Fatalf("expired approval replay error = %v, want %v", err, cloudmodule.ErrPlanApprovalExpired)
	}

	var status string
	var revision int64
	var deploymentCount, expiredEventCount int
	if err := store.DB().QueryRowContext(ctx, `SELECT status, revision FROM p2p_cloud_plans WHERE plan_id = $1`, planID).Scan(&status, &revision); err != nil || status != cloudmodule.PlanStatusExpired || revision != 5 {
		t.Fatalf("expired plan status=%q revision=%d err=%v", status, revision, err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_deployments WHERE plan_id = $1`, planID).Scan(&deploymentCount); err != nil || deploymentCount != 0 {
		t.Fatalf("expired plan deployment count=%d err=%v", deploymentCount, err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_events WHERE aggregate_type = 'plan' AND aggregate_id = $1 AND type = 'cloud.plan.changed' AND revision = 5`, planID).Scan(&expiredEventCount); err != nil || expiredEventCount != 1 {
		t.Fatalf("expired plan event count=%d err=%v", expiredEventCount, err)
	}
}

func newCloudConfirmationStore(t *testing.T) *DatabaseStore {
	t.Helper()
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(closeDB)
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func cloudConfirmationApprovalRequest(owner, planID, suffix string, approval cloudcontracts.ApprovalV1, createdAt int64) cloudmodule.ApproveCloudPlanRequest {
	deploymentID := "deployment-" + suffix
	deployment := cloudmodule.Deployment{
		DeploymentID: deploymentID, PlanID: planID, Execution: "queued", Outcome: "pending", Resource: "none",
		Revision: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	job := cloudmodule.Job{
		JobID: "job-provision-" + suffix, PlanID: planID, DeploymentID: deploymentID, Kind: "provision",
		Execution: "queued", Outcome: "pending", Checkpoint: "provision_queued", Revision: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return cloudmodule.ApproveCloudPlanRequest{
		OwnerMXID: owner, PlanID: planID, ExpectedRevision: 4, IdempotencyHash: "approve-idempotency-" + suffix, Approval: approval,
		Deployment: deployment, Job: job,
		Outbox: cloudmodule.OutboxEntry{
			OutboxID: "outbox-provision-" + suffix, Kind: cloudmodule.OutboxKindDeploymentProvisionRequested, AggregateType: "deployment", AggregateID: deploymentID,
			PayloadJSON: `{"deployment_id":"` + deploymentID + `"}`, CreatedAt: createdAt,
		},
		PlanEventID: "event-plan-" + suffix, DeploymentEventID: "event-deployment-" + suffix, JobEventID: "event-job-" + suffix, CreatedAt: createdAt,
	}
}

func cloudConfirmationDeviceKey(t *testing.T) (ed25519.PrivateKey, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return privateKey, base64.StdEncoding.EncodeToString(der)
}

func cloudConfirmationFixtures(t *testing.T, now time.Time, connectionID, quoteID string) (cloudcontracts.RecipeV1, cloudcontracts.QuoteV1) {
	t.Helper()
	recipe := cloudcontracts.RecipeV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1,
		RecipeID:      "recipe-confirmation-1",
		Name:          "Private knowledge node",
		Maturity:      cloudcontracts.RecipeExperimental,
		Sources: []cloudcontracts.RecipeSourceV1{{
			URL: "https://github.com/example/knowledge-node", Version: "v0.0.1-test", Commit: "0123456789abcdef0123456789abcdef01234567",
			ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", License: "Apache-2.0", RetrievedAt: now, Official: true,
		}},
		Requirements: cloudcontracts.ResourceRequirementsV1{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudcontracts.ArchitectureAMD64},
		Install:      cloudcontracts.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: []string{"image-pulled"}, Steps: []cloudcontracts.InstallStepV1{{ID: "install-service", Summary: "Install the pinned official artifact", TimeoutSeconds: 900}}},
		Health: cloudcontracts.HealthContractV1{
			Liveness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/healthz"}, Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/readyz"}, Semantic: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "verify-index"},
		},
		Lifecycle: cloudcontracts.LifecycleContractV1{Start: "start-service", Stop: "stop-service", Restart: "restart-service", Upgrade: "upgrade-service", Rollback: "rollback-service", Backup: "backup-data", Restore: "restore-data", Destroy: "destroy-service"},
	}
	quote := cloudcontracts.QuoteV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, QuoteID: quoteID, CloudConnectionID: connectionID, Region: "ap-south-1", Currency: "USD", QuotedAt: now, ValidUntil: now.Add(15 * time.Minute),
		Candidates: []cloudcontracts.QuoteCandidateV1{{
			CandidateID: "candidate-recommended-confirmation", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand,
			Architecture: cloudcontracts.ArchitectureAMD64, VCPU: 4, MemoryMiB: 16384, GPUCount: 0, GPUMemoryMiB: 0,
			HourlyMinor: 20, ThirtyDayMinor: 14400, StartupUpperMinor: 0, EstimatedDiskGiB: 80, AvailabilityZones: []string{"ap-south-1a"},
		}},
		IncludedItems: []string{"ec2_linux_ondemand"}, UnincludedItems: []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"},
	}
	if err := recipe.Validate(); err != nil {
		t.Fatalf("recipe fixture: %v", err)
	}
	if err := quote.Validate(); err != nil {
		t.Fatalf("quote fixture: %v", err)
	}
	return recipe, quote
}

func seedCloudConfirmationState(t *testing.T, store *DatabaseStore, owner, connectionID, planID string, recipe cloudcontracts.RecipeV1, quote cloudcontracts.QuoteV1, publicSPKI string) {
	t.Helper()
	ctx := context.Background()
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	recipeCBOR, err := recipe.CanonicalRecipeCBOR()
	if err != nil {
		t.Fatal(err)
	}
	recipeJSON, err := json.Marshal(recipe)
	if err != nil {
		t.Fatal(err)
	}
	quoteDigest, err := quote.Digest()
	if err != nil {
		t.Fatal(err)
	}
	quoteCBOR, err := quote.CanonicalQuoteCBOR()
	if err != nil {
		t.Fatal(err)
	}
	quoteJSON, err := json.Marshal(quote)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := quote.QuotedAt.UnixMilli()
	request := cloudGoalCreateRequest("goal-confirmation-1", planID, "goal-idempotency-confirmation", "goal-request-confirmation", "event-confirmation", "outbox-confirmation")
	request.Goal.OwnerMXID = owner
	request.Goal.ConnectionID = connectionID
	request.Plan.ConnectionID = connectionID
	if _, err := store.CreateCloudGoal(ctx, request); err != nil {
		t.Fatalf("seed goal: %v", err)
	}
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO p2p_cloud_connections (cloud_connection_id, provider, account_id, region, mode, status, revision, created_at, updated_at) VALUES ($1, 'aws', '123456789012', 'ap-south-1', 'role', 'active', 1, $2, $2)`, []any{connectionID, createdAt}},
		{`INSERT INTO p2p_cloud_connection_bootstraps (bootstrap_id, owner_mxid, cloud_connection_id, provider, requested_region, template_url, template_digest, source_tree_digest, stack_name, node_key_id, node_public_key_spki_base64, device_approval_key_id, device_approval_public_key_spki_base64, status, revision, idempotency_hash, request_digest, expires_at, created_at, updated_at) VALUES ('bootstrap-confirmation-1', $1, $2, 'aws', 'ap-south-1', 'https://example.invalid/template.json', 'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 'sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', 'dirextalk-confirmation', 'node-confirmation-1', $3, 'device-confirmation-1', $4, 'active', 1, 'bootstrap-idempotency-confirmation', 'bootstrap-request-confirmation', $5, $5, $5)`, []any{owner, connectionID, publicSPKI, publicSPKI, createdAt + int64(time.Hour/time.Millisecond)}},
		{`INSERT INTO p2p_cloud_recipes (recipe_id, name, version, digest, maturity, revision, created_at, updated_at) VALUES ($1, $2, 'v1', $3, $4, 1, $5, $5)`, []any{recipe.RecipeID, recipe.Name, recipeDigest, string(recipe.Maturity), createdAt}},
		{`INSERT INTO p2p_cloud_recipe_versions (recipe_id, revision, canonical_cbor, display_json, digest, maturity, created_at) VALUES ($1, 1, $2, $3, $4, $5, $6)`, []any{recipe.RecipeID, recipeCBOR, string(recipeJSON), recipeDigest, string(recipe.Maturity), createdAt}},
		{`INSERT INTO p2p_cloud_quotes (quote_id, cloud_connection_id, region, currency, digest, canonical_cbor, display_json, quoted_at, valid_until, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $8)`, []any{quote.QuoteID, quote.CloudConnectionID, quote.Region, quote.Currency, quoteDigest, quoteCBOR, string(quoteJSON), quote.QuotedAt.UnixMilli(), quote.ValidUntil.UnixMilli()}},
		{`UPDATE p2p_cloud_plans SET cloud_connection_id = $1, status = 'quoting', recipe_digest = $2, quote_id = $3, plan_hash = '', revision = 3, updated_at = $4 WHERE plan_id = $5`, []any{connectionID, recipeDigest, quote.QuoteID, createdAt, planID}},
	}
	for _, statement := range statements {
		if _, err := store.DB().ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed confirmation state: %v", err)
		}
	}
}

func mustCloudConfirmationJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
