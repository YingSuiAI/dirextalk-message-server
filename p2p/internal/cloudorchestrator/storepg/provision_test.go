package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreDeploymentProvisionPersistsApprovedReceiptAndKeepsResourcesPrivate(t *testing.T) {
	now, database, store, claim := prepareProvisionClaim(t)
	signed := signedDeploymentCreateCommand(t, claim, now)
	if err := store.MarkDeploymentProvisionStarted(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistDeploymentCreateCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	result := validProvisionReceipt(t, claim, signed)
	if err := store.CommitDeploymentProvision(context.Background(), claim, result); err != nil {
		t.Fatal(err)
	}

	var execution, outcome, resource string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT execution_status, outcome_status, resource_status FROM p2p_cloud_deployments WHERE deployment_id = $1
	`, claim.DeploymentID).Scan(&execution, &outcome, &resource); err != nil {
		t.Fatal(err)
	}
	if execution != "provisioning" || outcome != "pending" || resource != "active" {
		t.Fatalf("deployment state = execution:%q outcome:%q resource:%q", execution, outcome, resource)
	}
	var checkpoint string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT checkpoint FROM p2p_cloud_jobs WHERE job_id = $1
	`, claim.JobID).Scan(&checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint != "worker_bootstrap_pending" {
		t.Fatalf("provision job checkpoint = %q", checkpoint)
	}
	var resourceState, instanceID, volumes, interfaces, receipt string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT resource_status, instance_id, volume_ids_json, network_interface_ids_json, broker_receipt_json
		FROM p2p_cloud_deployment_resources WHERE deployment_id = $1
	`, claim.DeploymentID).Scan(&resourceState, &instanceID, &volumes, &interfaces, &receipt); err != nil {
		t.Fatal(err)
	}
	if resourceState != "active" || instanceID != result.InstanceID || volumes != `["vol-0123456789abcdef0"]` || interfaces != `["eni-0123456789abcdef0"]` {
		t.Fatalf("private resource record = state:%q instance:%q volumes:%s interfaces:%s", resourceState, instanceID, volumes, interfaces)
	}
	if containsAny(receipt, []string{"approval_proof", "signature", "payload_b64", result.InstanceID, "vol-", "eni-"}) {
		t.Fatalf("resource audit receipt retained unsafe material: %s", receipt)
	}
	var commandState, safeCommandReceipt, envelope string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT state, receipt_json, signed_envelope_json FROM p2p_cloud_deployment_commands WHERE command_id = $1
	`, claim.Command.CommandID).Scan(&commandState, &safeCommandReceipt, &envelope); err != nil {
		t.Fatal(err)
	}
	if commandState != "accepted" || containsAny(safeCommandReceipt, []string{"approval_proof", "signature", "payload_b64", result.InstanceID, "vol-", "eni-"}) {
		t.Fatalf("deployment command audit = state:%q receipt:%s", commandState, safeCommandReceipt)
	}
	if !containsAny(envelope, []string{"approval_proof"}) {
		t.Fatal("private durable command must retain its signed device proof for exact replay")
	}
	var projection string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT payload_json FROM p2p_cloud_projection_outbox
		WHERE type = 'cloud.deployment.changed' ORDER BY created_at DESC, projection_id DESC LIMIT 1
	`).Scan(&projection); err != nil {
		t.Fatal(err)
	}
	if containsAny(projection, []string{result.InstanceID, "vol-", "eni-", "ami-", "vpc-", "subnet-", "approval", "signature", "resource_manifest"}) {
		t.Fatalf("public deployment projection leaked private placement or proof: %s", projection)
	}
}

func TestStoreDeploymentProvisionReplaysIndeterminateCommandAndOnlyExpiryAllocatesNewCounter(t *testing.T) {
	now, _, store, claim := prepareProvisionClaim(t)
	signed := signedDeploymentCreateCommand(t, claim, now)
	if err := store.MarkDeploymentProvisionStarted(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistDeploymentCreateCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	if err := store.DeferDeploymentProvision(context.Background(), claim, "broker_unavailable", now); err != nil {
		t.Fatal(err)
	}
	retry, found, err := store.ClaimDeploymentProvision(context.Background(), "orchestrator-provision-retry", time.Minute)
	if err != nil || !found {
		t.Fatalf("retry provision claim found=%v err=%v", found, err)
	}
	if retry.Command.CommandID != claim.Command.CommandID || retry.Command.NodeCounter != claim.Command.NodeCounter || retry.Command.Attempt != claim.Command.Attempt ||
		retry.Command.SignedEnvelope != signed.EnvelopeJSON || retry.Command.RequestSHA256 != signed.RequestSHA256 {
		t.Fatalf("indeterminate deployment retry drifted: first=%#v retry=%#v", claim.Command, retry.Command)
	}
	if err := store.ExpireDeploymentCreateCommand(context.Background(), retry); err != nil {
		t.Fatal(err)
	}
	next, found, err := store.ClaimDeploymentProvision(context.Background(), "orchestrator-provision-expired", time.Minute)
	if err != nil || !found {
		t.Fatalf("post-expiry provision claim found=%v err=%v", found, err)
	}
	if next.Command.CommandID == claim.Command.CommandID || next.Command.NodeCounter != claim.Command.NodeCounter+1 || next.Command.Attempt != claim.Command.Attempt+1 ||
		next.Command.SignedEnvelope != "" || next.Command.RequestSHA256 != "" {
		t.Fatalf("only explicit expiry may allocate a new deployment command: first=%#v next=%#v", claim.Command, next.Command)
	}
}

func TestStoreDeploymentProvisionPlanExpiryFailsWithoutCreatingPrivateResource(t *testing.T) {
	for _, code := range []string{runtime.DeploymentProvisionQuoteExpired, runtime.DeploymentProvisionApprovalExpired} {
		t.Run(code, func(t *testing.T) {
			now, database, store, claim := prepareProvisionClaimWithProvisionDelay(t, 14*time.Minute, 2*time.Minute)
			store.cfg.Now = func() time.Time { return claim.QuoteValidUntil }
			if !claim.QuoteValidUntil.After(now) {
				t.Fatal("test precondition: quote must initially be valid")
			}
			if err := store.FailDeploymentProvision(context.Background(), claim, code); err != nil {
				t.Fatal(err)
			}
			var execution, outcome, resource string
			if err := database.DB().QueryRowContext(context.Background(), `
				SELECT execution_status, outcome_status, resource_status FROM p2p_cloud_deployments WHERE deployment_id = $1
			`, claim.DeploymentID).Scan(&execution, &outcome, &resource); err != nil {
				t.Fatal(err)
			}
			if execution != "finished" || outcome != "failed" || resource != "none" {
				t.Fatalf("expired plan deployment state = execution:%q outcome:%q resource:%q", execution, outcome, resource)
			}
			var planStatus string
			if err := database.DB().QueryRowContext(context.Background(), `SELECT status FROM p2p_cloud_plans WHERE plan_id = $1`, claim.PlanID).Scan(&planStatus); err != nil {
				t.Fatal(err)
			}
			if planStatus != "expired" {
				t.Fatalf("expired plan status = %q", planStatus)
			}
			var resourceCount int
			if err := database.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM p2p_cloud_deployment_resources WHERE deployment_id = $1`, claim.DeploymentID).Scan(&resourceCount); err != nil {
				t.Fatal(err)
			}
			if resourceCount != 0 {
				t.Fatalf("expired plan must not create a private resource record, got %d", resourceCount)
			}
		})
	}
}

func TestStoreDeploymentProvisionRejectsDriftedValidityBound(t *testing.T) {
	for _, test := range []struct {
		name  string
		drift func(*runtime.DeploymentProvisionClaim)
	}{
		{name: "quote", drift: func(claim *runtime.DeploymentProvisionClaim) {
			claim.QuoteValidUntil = claim.QuoteValidUntil.Add(time.Millisecond)
		}},
		{name: "approval", drift: func(claim *runtime.DeploymentProvisionClaim) {
			claim.ApprovalValidUntil = claim.ApprovalValidUntil.Add(time.Millisecond)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, database, store, claim := prepareProvisionClaim(t)
			test.drift(&claim)
			if err := store.FailDeploymentProvision(context.Background(), claim, runtime.DeploymentProvisionApprovalExpired); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("drifted validity error = %v, want %v", err, ErrLeaseLost)
			}
			var planStatus string
			if err := database.DB().QueryRowContext(context.Background(), `SELECT status FROM p2p_cloud_plans WHERE plan_id = $1`, claim.PlanID).Scan(&planStatus); err != nil {
				t.Fatal(err)
			}
			if planStatus != "approved" {
				t.Fatalf("drifted claim must not expire the plan, status=%q", planStatus)
			}
		})
	}
}

func prepareProvisionClaim(t *testing.T) (time.Time, *p2pstorage.DatabaseStore, *Store, runtime.DeploymentProvisionClaim) {
	return prepareProvisionClaimWithProvisionDelay(t, time.Minute, time.Minute)
}

func prepareProvisionClaimWithProvisionDelay(t *testing.T, provisionDelay, provisionLease time.Duration) (time.Time, *p2pstorage.DatabaseStore, *Store, runtime.DeploymentProvisionClaim) {
	t.Helper()
	if provisionDelay <= 0 || provisionDelay >= 15*time.Minute {
		t.Fatal("test provision delay must fit within the fixed quote validity")
	}
	if provisionLease <= 0 || provisionLease > 5*time.Minute {
		t.Fatal("test provision lease is outside the supported range")
	}
	ctx, database, closeDatabase := openMigratedStore(t)
	t.Cleanup(closeDatabase)
	seedResearchGoal(t, ctx, database)
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	workflowStore := New(database.DB(), Config{Now: func() time.Time { return now }})
	research, found, err := workflowStore.ClaimResearchGoal(ctx, "orchestrator-provision-research", time.Minute)
	if err != nil || !found {
		t.Fatalf("research claim found=%v err=%v", found, err)
	}
	if err := workflowStore.MarkResearchStarted(ctx, research); err != nil {
		t.Fatal(err)
	}
	if err := workflowStore.CommitResearch(ctx, research, testResearchOutput(t, now)); err != nil {
		t.Fatal(err)
	}
	devicePrivate, deviceSPKI := provisionDeviceKey(t)
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_connections (
			cloud_connection_id, provider, account_id, region, mode, status, revision, created_at, updated_at
		) VALUES ('connection-1', 'aws', '123456789012', 'ap-south-1', 'connection_stack_v2', 'active', 1, $1, $1)
	`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_connection_brokers (
			cloud_connection_id, broker_command_url, broker_region, connection_generation, node_key_id,
			worker_artifact_kind, worker_ami_id, worker_vpc_id, worker_subnet_id, worker_availability_zone,
			worker_resource_manifest_digest, next_node_counter, created_at, updated_at
		) VALUES (
			'connection-1', 'https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands', 'ap-south-1', 1, 'node-key-1',
			'fixed_ami', 'ami-0123456789abcdef0', 'vpc-0123456789abcdef0', 'subnet-0123456789abcdef0', 'ap-south-1a',
			'sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', 0, $1, $1
		)
	`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_connection_bootstraps (
			bootstrap_id, owner_mxid, cloud_connection_id, provider, requested_region, template_url, template_digest,
			source_tree_digest, stack_name, node_key_id, node_public_key_spki_base64, device_approval_key_id,
			device_approval_public_key_spki_base64, status, revision, idempotency_hash, request_digest, expires_at, created_at, updated_at
		) VALUES (
			'bootstrap-provision-1', '@owner:example.com', 'connection-1', 'aws', 'ap-south-1', 'https://example.invalid/template.json',
			'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
			'sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', 'dirextalk-provision', 'node-key-1', $1,
			'device-provision-1', $1, 'active', 1, 'bootstrap-provision-idempotency', 'bootstrap-provision-request', $2, $3, $3
		)
	`, deviceSPKI, now.Add(time.Hour).UnixMilli(), now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	quoteClaim, found, err := workflowStore.ClaimQuoteRequest(ctx, "orchestrator-provision-quote", time.Minute)
	if err != nil || !found {
		t.Fatalf("quote claim found=%v err=%v", found, err)
	}
	if err := workflowStore.MarkQuoteStarted(ctx, quoteClaim); err != nil {
		t.Fatal(err)
	}
	quoteSigned := signedQuoteCommand(t, quoteClaim, now)
	if err := workflowStore.PersistQuoteCommand(ctx, quoteClaim, quoteSigned); err != nil {
		t.Fatal(err)
	}
	if err := workflowStore.CommitQuote(ctx, quoteClaim, validBrokerQuote(quoteClaim, quoteSigned)); err != nil {
		t.Fatal(err)
	}
	var planRevision int64
	var quoteID string
	if err := database.DB().QueryRowContext(ctx, `SELECT revision, quote_id FROM p2p_cloud_plans WHERE plan_id = 'plan-1'`).Scan(&planRevision, &quoteID); err != nil {
		t.Fatal(err)
	}
	provisionNow := now.Add(provisionDelay)
	confirmationExpiresAt := now.Add(5 * time.Minute)
	if !confirmationExpiresAt.After(provisionNow) {
		confirmationExpiresAt = now.Add(15*time.Minute - time.Second)
	}
	prepared, err := database.PrepareCloudPlanConfirmation(ctx, cloudmodule.PreparePlanConfirmationRequest{
		OwnerMXID: "@owner:example.com", PlanID: "plan-1", ExpectedRevision: planRevision, QuoteID: quoteID, CandidateTier: "recommended",
		IdempotencyHash: "prepare-provision-idempotency", RequestDigest: "prepare-provision-request",
		ApprovalID: "approval-provision-1", ChallengeID: "challenge-provision-1", CreatedAt: now.UnixMilli(), ExpiresAt: confirmationExpiresAt.UnixMilli(),
	})
	if err != nil || !prepared.Created {
		t.Fatalf("prepare provision confirmation = %#v err=%v", prepared, err)
	}
	signedApproval, err := prepared.Confirmation.Approval.Sign(devicePrivate, provisionNow)
	if err != nil {
		t.Fatal(err)
	}
	approvalRequest := cloudmodule.ApproveCloudPlanRequest{
		OwnerMXID: "@owner:example.com", PlanID: "plan-1", ExpectedRevision: prepared.Confirmation.Plan.Revision,
		IdempotencyHash: "approve-provision-idempotency", Approval: signedApproval,
		Deployment: cloudmodule.Deployment{
			DeploymentID: "deployment-provision-1", PlanID: "plan-1", Execution: "queued", Outcome: "pending", Resource: "none",
			Revision: 1, CreatedAt: provisionNow.UnixMilli(), UpdatedAt: provisionNow.UnixMilli(),
		},
		Job: cloudmodule.Job{
			JobID: "job-provision-1", PlanID: "plan-1", DeploymentID: "deployment-provision-1", Kind: "provision",
			Execution: "queued", Outcome: "pending", Checkpoint: "provision_queued", Revision: 1, CreatedAt: provisionNow.UnixMilli(), UpdatedAt: provisionNow.UnixMilli(),
		},
		Outbox: cloudmodule.OutboxEntry{
			OutboxID: "outbox-provision-1", Kind: cloudmodule.OutboxKindDeploymentProvisionRequested, AggregateType: "deployment", AggregateID: "deployment-provision-1",
			PayloadJSON: `{"deployment_id":"deployment-provision-1"}`, CreatedAt: provisionNow.UnixMilli(),
		},
		PlanEventID: "event-plan-provision-1", DeploymentEventID: "event-deployment-provision-1", JobEventID: "event-job-provision-1", CreatedAt: provisionNow.UnixMilli(),
	}
	approved, err := database.ApproveCloudPlan(ctx, approvalRequest)
	if err != nil || !approved.Created {
		t.Fatalf("approve provision confirmation = %#v err=%v", approved, err)
	}
	store := New(database.DB(), Config{Now: func() time.Time { return provisionNow }})
	claim, found, err := store.ClaimDeploymentProvision(ctx, "orchestrator-provision", provisionLease)
	if err != nil || !found {
		t.Fatalf("deployment provision claim found=%v err=%v", found, err)
	}
	return provisionNow, database, store, claim
}

func provisionDeviceKey(t *testing.T) (ed25519.PrivateKey, string) {
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

func signedDeploymentCreateCommand(t *testing.T, claim runtime.DeploymentProvisionClaim, now time.Time) runtime.SignedDeploymentCreateCommand {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildDeploymentCreateCommand(claim.Command, claim.Request, claim.ApprovalProofJSON, claim.QuoteValidUntil)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func validProvisionReceipt(t *testing.T, claim runtime.DeploymentProvisionClaim, signed runtime.SignedDeploymentCreateCommand) runtime.BrokerDeployment {
	t.Helper()
	result := runtime.BrokerDeployment{
		Schema: "dirextalk.aws.deployment-receipt/v1", DeploymentID: claim.DeploymentID, ConnectionID: claim.ConnectionID,
		CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, ResourceStatus: "provisioning",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"},
		ReceiptJSON: `{"raw_private_receipt":"not-stored"}`,
	}
	if err := runtime.ValidateBrokerDeployment(claim, signed, result); err != nil {
		t.Fatalf("valid provision receipt: %v", err)
	}
	return result
}
