package cloud

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"
	"time"
)

const (
	agentDestroyDeploymentID = "11111111-1111-4111-8111-111111111111"
	agentDestroyPlanID       = "22222222-2222-4222-8222-222222222222"
	agentDestroyConnectionID = "33333333-3333-4333-8333-333333333333"
	agentDestroyTaskID       = "44444444-4444-4444-8444-444444444444"
	agentDestroyOperationID  = "55555555-5555-4555-8555-555555555555"
	agentDestroyApprovalID   = "66666666-6666-4666-8666-666666666666"
)

type agentDestroyDeploymentReader struct {
	deployment Deployment
	found      bool
	err        error
	getCalls   int
}

func (reader *agentDestroyDeploymentReader) ListCloudDeployments(context.Context) ([]Deployment, error) {
	return []Deployment{reader.deployment}, reader.err
}

func (reader *agentDestroyDeploymentReader) GetCloudDeployment(_ context.Context, id string) (Deployment, bool, error) {
	reader.getCalls++
	return reader.deployment, reader.found && id == reader.deployment.DeploymentID, reader.err
}

type agentDestroyLegacyStore struct {
	Store
	prepareCalls int
	approveCalls int
}

func (store *agentDestroyLegacyStore) PrepareCloudDeploymentDestroy(context.Context, PrepareDeploymentDestroyRequest) (PrepareDeploymentDestroyResult, error) {
	store.prepareCalls++
	return PrepareDeploymentDestroyResult{Confirmation: DeploymentDestroyConfirmation{Deployment: Deployment{DeploymentID: "deployment-legacy-0001"}}}, nil
}

func (store *agentDestroyLegacyStore) ApproveCloudDeploymentDestroy(context.Context, ApproveDeploymentDestroyRequest) (ApproveDeploymentDestroyResult, error) {
	store.approveCalls++
	return ApproveDeploymentDestroyResult{}, nil
}

func TestAgentDeploymentDestroyPrepareBindsFullResourceGraphAndPreservesLegacyRouting(t *testing.T) {
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	deployment := validAgentDestroyDeploymentFixture(now)
	reader := &agentDestroyDeploymentReader{deployment: deployment, found: true}
	client := &agentControlModuleClient{destroyChallenge: validAgentDestroyChallengeFixture(now)}
	legacy := &agentDestroyLegacyStore{}
	module := New(legacy, Config{
		OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now },
		DeploymentReader: reader, AgentCloudControlClient: client,
	})

	result, actionErr := module.Handlers()[actionDeploymentsDestroyPlan](t.Context(), map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"signer_key_id": "cloud-device-test", "idempotency_key": "77777777-7777-4777-8777-777777777777",
	})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	confirmation := result.(map[string]any)["confirmation"].(map[string]any)
	approval := confirmation["approval"].(agentCloudDeploymentDestroyApprovalV1)
	if client.destroyChallengeCalls != 1 || legacy.prepareCalls != 0 || reader.getCalls != 1 ||
		client.destroyChallengeRequest.ExpectedDeployment != deployment || approval.Scope.Resources[3].Type != "security_group" ||
		confirmation["signing_payload_cbor"] == "" || confirmation["signing_payload_digest"] == "" {
		t.Fatalf("Agent destroy prepare result=%#v request=%#v local=%d reads=%d", confirmation, client.destroyChallengeRequest, legacy.prepareCalls, reader.getCalls)
	}
	blocked := deployment
	blocked.Resource, blocked.Revision, blocked.UpdatedAt = "destroy_blocked", 8, deployment.UpdatedAt+1000
	blockedChallenge := validAgentDestroyChallengeFixture(now)
	blockedChallenge.Scope.DeploymentRevision = blocked.Revision
	for index := range blockedChallenge.Scope.Resources {
		blockedChallenge.Scope.Resources[index].Status = "destroy_blocked"
	}
	reader.deployment, client.destroyChallenge = blocked, blockedChallenge
	if _, actionErr = module.Handlers()[actionDeploymentsDestroyPlan](t.Context(), map[string]any{
		"deployment_id": blocked.DeploymentID, "expected_revision": float64(blocked.Revision),
		"signer_key_id": "cloud-device-test", "idempotency_key": "12121212-1212-4212-8212-121212121212",
	}); actionErr != nil || client.destroyChallengeCalls != 2 {
		t.Fatalf("blocked Agent destroy retry err=%#v calls=%d", actionErr, client.destroyChallengeCalls)
	}

	if _, actionErr = module.Handlers()[actionDeploymentsDestroyPlan](t.Context(), map[string]any{
		"deployment_id": "deployment-legacy-0001", "expected_revision": float64(2),
		"idempotency_key": "88888888-8888-4888-8888-888888888888",
	}); actionErr != nil || legacy.prepareCalls != 1 || client.destroyChallengeCalls != 2 {
		t.Fatalf("legacy destroy route err=%#v local=%d remote=%d", actionErr, legacy.prepareCalls, client.destroyChallengeCalls)
	}

	if _, actionErr = module.Handlers()[actionDeploymentsDestroyPlan](t.Context(), map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"idempotency_key": "99999999-9999-4999-8999-999999999999",
	}); actionErr == nil || actionErr.Status != http.StatusBadRequest || client.destroyChallengeCalls != 2 {
		t.Fatalf("missing signer accepted err=%#v calls=%d", actionErr, client.destroyChallengeCalls)
	}
}

func TestAgentDeploymentDestroyApproveRecoversDurableBlockedResultWithoutClaimingDestroyed(t *testing.T) {
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	deployment := validAgentDestroyDeploymentFixture(now)
	blocked := deployment
	blocked.Resource = "destroy_blocked"
	blocked.Revision++
	blocked.UpdatedAt += 1000
	reader := &agentDestroyDeploymentReader{deployment: blocked, found: true}
	challenge := validAgentDestroyChallengeFixture(now)
	operation := validAgentDestroyOperationFixture(now, "destroy_blocked")
	client := &agentControlModuleClient{
		destroyApproveErr: ErrAgentCloudControlUnavailable,
		destroyOperation:  operation, destroyOperationFound: true,
	}
	module := New(nil, Config{Now: func() time.Time { return now }, DeploymentReader: reader, AgentCloudControlClient: client})
	approval := signedAgentDestroyApproval(challenge)
	result, actionErr := module.Handlers()[actionDeploymentsDestroyApprove](t.Context(), map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"approval": approval, "idempotency_key": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
	})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	view := result.(map[string]any)
	job := view["job"].(Job)
	gotDeployment := view["deployment"].(Deployment)
	if client.destroyApproveCalls != 0 || client.destroyOperationCalls != 1 || job.Checkpoint != "destroy_blocked" ||
		job.Outcome != "failed" || job.ErrorCode != "cloud_destroy_access_denied" || gotDeployment.Resource != "destroy_blocked" {
		t.Fatalf("blocked recovery result=%#v approve=%d operation=%d", result, client.destroyApproveCalls, client.destroyOperationCalls)
	}

	client.destroyOperationFound = false
	if _, actionErr = module.Handlers()[actionDeploymentsDestroyApprove](t.Context(), map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"approval": approval, "idempotency_key": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}); actionErr == nil || actionErr.Status != http.StatusConflict {
		t.Fatalf("unknown destroy outcome was reported as success: %#v", actionErr)
	}
}

func TestAgentDeploymentDestroyApproveProjectsOnlyDurableOperationAndChecksVerifiedReadBack(t *testing.T) {
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	deployment := validAgentDestroyDeploymentFixture(now)
	reader := &agentDestroyDeploymentReader{deployment: deployment, found: true}
	challenge := validAgentDestroyChallengeFixture(now)
	operation := validAgentDestroyOperationFixture(now, "approved")
	client := &agentControlModuleClient{destroyApproveResult: AgentCloudDeploymentDestroyResult{Operation: operation, Deployment: deployment}}
	module := New(nil, Config{Now: func() time.Time { return now }, DeploymentReader: reader, AgentCloudControlClient: client})
	params := map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"approval": signedAgentDestroyApproval(challenge), "idempotency_key": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
	}
	result, actionErr := module.Handlers()[actionDeploymentsDestroyApprove](t.Context(), params)
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	job := result.(map[string]any)["job"].(Job)
	if client.destroyApproveCalls != 1 || job.JobID != operation.OperationID || job.Execution != "queued" || job.Outcome != "pending" || job.Checkpoint != "approved" {
		t.Fatalf("approved operation projection=%#v calls=%d", job, client.destroyApproveCalls)
	}

	client.destroyApproveResult.Operation = validAgentDestroyOperationFixture(now, "verified_destroyed")
	client.destroyApproveResult.Deployment = deployment
	client.destroyApproveErr = nil
	client.destroyOperationFound = false
	params["idempotency_key"] = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	if _, actionErr = module.Handlers()[actionDeploymentsDestroyApprove](t.Context(), params); actionErr == nil || actionErr.Status != http.StatusBadGateway {
		t.Fatalf("verified operation without verified deployment was accepted: %#v", actionErr)
	}
}

func validAgentDestroyDeploymentFixture(now time.Time) Deployment {
	return Deployment{
		DeploymentID: agentDestroyDeploymentID, PlanID: agentDestroyPlanID, ConnectionID: agentDestroyConnectionID,
		Execution: "finished", Outcome: "failed", Resource: "active", Revision: 7,
		CreatedAt: now.Add(-time.Hour).UnixMilli(), UpdatedAt: now.Add(-time.Minute).UnixMilli(),
	}
}

func validAgentDestroyChallengeFixture(now time.Time) AgentCloudDeploymentDestroyChallenge {
	resourceIDs := []string{
		"10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002",
		"10000000-0000-4000-8000-000000000003", "10000000-0000-4000-8000-000000000004",
	}
	providerIDs := []string{"i-0123456789abcdef0", "vol-0123456789abcdef0", "eni-0123456789abcdef0", "sg-0123456789abcdef0"}
	types := []string{"ec2", "ebs", "eni", "security_group"}
	resources := make([]AgentCloudDestroyResourceScope, 0, len(resourceIDs))
	for index := range resourceIDs {
		dependencies := []string{}
		if index > 0 {
			dependencies = []string{resourceIDs[0]}
		}
		resources = append(resources, AgentCloudDestroyResourceScope{
			ResourceID: resourceIDs[index], Type: types[index], ProviderID: providerIDs[index], Revision: 3,
			DependsOnResourceIDs: dependencies, RetentionPolicy: "ephemeral_auto_destroy", DestroyDeadline: now.Add(time.Hour),
			AutoDestroyApproved: true, Status: "active", Region: "us-east-1",
			SpecDigest:         "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			ApprovedPlanHash:   "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			OriginalApprovalID: "77777777-7777-4777-8777-777777777777",
			ReadBack:           AgentCloudResourceReadBack{Observed: true, Exists: true, ProviderID: providerIDs[index], ObservedAt: now.Add(-time.Minute), TagDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333"},
		})
	}
	return AgentCloudDeploymentDestroyChallenge{
		OperationID: agentDestroyOperationID, ChallengeID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee", ApprovalID: agentDestroyApprovalID,
		SignerKeyID: "cloud-device-test", ExpiresAt: now.Add(5 * time.Minute), SigningPayloadCBOR: []byte{0xa1, 0x01, 0x01}, Revision: 1,
		Scope: AgentCloudDeploymentDestroyScope{
			SchemaVersion: agentCloudDeploymentDestroyScopeSchema, AgentInstanceID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", OwnerID: "dirextalk-project:example.com",
			DeploymentID: agentDestroyDeploymentID, DeploymentRevision: 7, TaskID: agentDestroyTaskID,
			PlanID: agentDestroyPlanID, PlanHash: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
			ConnectionID: agentDestroyConnectionID, Resources: resources,
		},
	}
}

func signedAgentDestroyApproval(challenge AgentCloudDeploymentDestroyChallenge) map[string]any {
	return map[string]any{
		"schema_version": agentCloudDeploymentDestroyApprovalSchema,
		"operation_id":   challenge.OperationID, "challenge_id": challenge.ChallengeID, "approval_id": challenge.ApprovalID,
		"signer_key_id": challenge.SignerKeyID, "scope": challenge.Scope,
		"expires_at": challenge.ExpiresAt.Format(time.RFC3339Nano), "revision": challenge.Revision,
		"signature": base64.RawURLEncoding.EncodeToString(make([]byte, 64)),
	}
}

func validAgentDestroyOperationFixture(now time.Time, status string) AgentCloudDestroyOperation {
	operation := AgentCloudDestroyOperation{
		OperationID: agentDestroyOperationID, OwnerID: "dirextalk-project:example.com", DeploymentID: agentDestroyDeploymentID,
		ApprovalID: agentDestroyApprovalID, ScopeDigest: "sha256:5555555555555555555555555555555555555555555555555555555555555555",
		Status: status, Revision: 2, CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
	}
	if status == "destroy_blocked" {
		operation.ErrorCode = "cloud_destroy_access_denied"
		operation.BlockedReason = "AWS denied destruction; resources remain tracked and may still incur charges."
		operation.AutomaticAttempts = 3
		operation.RequiresNewApproval = true
	} else if status == "destroying" || status == "verified_destroyed" {
		operation.AutomaticAttempts = 1
	}
	return operation
}
