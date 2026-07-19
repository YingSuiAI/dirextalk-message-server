package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

// TestCloudLifecycleAcceptance exercises the real durable runners and approval
// boundaries against one stateful provider double. It intentionally protects
// the whole externally visible lifecycle instead of each internal store step.
func TestCloudLifecycleAcceptance(t *testing.T) {
	ctx := context.Background()
	clock, database, store, provision := prepareProvisionClaim(t)
	store.cfg.Now = func() time.Time { return clock }
	store.cfg.NewLeaseToken = func() string { return fmt.Sprintf("acceptance-lease-%d", clock.UnixNano()) }

	_, nodeKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := brokertransport.New(nodeKey, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	provider := &cloudLifecycleProvider{
		Transport:  signer,
		now:        func() time.Time { return clock },
		instance:   "i-0123456789abcdef0",
		volumes:    []string{"vol-0123456789abcdef0"},
		interfaces: []string{"eni-0123456789abcdef0"},
	}
	cfg := runtime.Config{WorkerID: "acceptance-orchestrator", Lease: time.Minute, AttemptTimeout: 30 * time.Second, RetryDelay: time.Second, Now: func() time.Time { return clock }}

	created, err := runtime.NewDeploymentProvisionRunner(&preclaimedProvisionStore{Store: store, claim: provision}, provider, cfg).RunOnce(ctx)
	if err != nil || !created {
		t.Fatalf("provision handled=%v err=%v", created, err)
	}
	if !provider.active || provider.createCalls != 1 || provider.deploymentID != provision.DeploymentID {
		t.Fatalf("provider create state active=%v calls=%d deployment=%q", provider.active, provider.createCalls, provider.deploymentID)
	}

	observed, err := runtime.NewWorkerBootstrapObservationRunner(store, provider, cfg).RunOnce(ctx)
	if err != nil || !observed {
		t.Fatalf("worker observation handled=%v err=%v", observed, err)
	}

	var recipeJSON string
	if err = database.DB().QueryRowContext(ctx, `SELECT version.display_json FROM p2p_cloud_plans plan JOIN p2p_cloud_recipes recipe ON recipe.digest=plan.recipe_digest JOIN p2p_cloud_recipe_versions version ON version.recipe_id=recipe.recipe_id AND version.revision=recipe.revision WHERE plan.plan_id=$1`, provision.PlanID).Scan(&recipeJSON); err != nil {
		t.Fatal(err)
	}
	var recipe cloudcontracts.RecipeV1
	if err = json.Unmarshal([]byte(recipeJSON), &recipe); err != nil {
		t.Fatal(err)
	}
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	artifact := trustedStoreArtifact(t, recipe, recipeDigest)
	artifact.WorkerResourceManifestDigest = provision.Request.ResourceManifestDigest
	if registered, registerErr := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: artifact, RegisteredAt: clock.UnixMilli()}); registerErr != nil || !registered.Created {
		t.Fatalf("register artifact=%#v err=%v", registered, registerErr)
	}
	if handled, runErr := runtime.NewRecipeManifestRegistrationRunner(store).RunOnce(ctx); runErr != nil || !handled {
		t.Fatalf("manifest registration handled=%v err=%v", handled, runErr)
	}
	var deploymentRevision int64
	var executionID string
	if err = database.DB().QueryRowContext(ctx, `SELECT revision FROM p2p_cloud_deployments WHERE deployment_id=$1`, provision.DeploymentID).Scan(&deploymentRevision); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT execution_id FROM p2p_cloud_recipe_execution_manifests WHERE deployment_id=$1`, provision.DeploymentID).Scan(&executionID); err != nil {
		t.Fatal(err)
	}
	devicePrivate, deviceSPKI := provisionDeviceKey(t)
	if _, err = database.DB().ExecContext(ctx, `UPDATE p2p_cloud_connection_bootstraps SET device_approval_key_id='device-acceptance',device_approval_public_key_spki_base64=$1 WHERE cloud_connection_id=$2`, deviceSPKI, provision.ConnectionID); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(time.Minute)
	preparedExecution, err := database.PrepareCloudRecipeExecutionConfirmation(ctx, cloudmodule.PrepareRecipeExecutionConfirmationRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: provision.DeploymentID, ExpectedRevision: deploymentRevision,
		IdempotencyHash: "acceptance-execution-prepare-idempotency", RequestDigest: "acceptance-execution-prepare-request",
		ApprovalID: "approval-acceptance-execution", ChallengeID: "challenge-acceptance-execution",
		CreatedAt: clock.UnixMilli(), ExpiresAt: clock.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil || !preparedExecution.Created {
		t.Fatalf("prepare execution=%#v err=%v", preparedExecution, err)
	}
	signedExecution, err := preparedExecution.Confirmation.Approval.Sign(devicePrivate, clock.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	approvedExecution, err := database.ApproveCloudRecipeExecution(ctx, cloudmodule.ApproveRecipeExecutionRequest{
		OwnerMXID: "@owner:example.com", DeploymentID: provision.DeploymentID, ExpectedRevision: deploymentRevision,
		IdempotencyHash: "acceptance-execution-approve-idempotency", Approval: signedExecution,
		Job:      cloudmodule.Job{JobID: "job-acceptance-install", PlanID: provision.PlanID, DeploymentID: provision.DeploymentID, Kind: "install", Execution: "queued", Outcome: "pending", Checkpoint: "install_queued", Revision: 1, CreatedAt: clock.Add(time.Second).UnixMilli(), UpdatedAt: clock.Add(time.Second).UnixMilli()},
		OutboxID: "outbox-acceptance-install", JobEventID: "event-acceptance-install", CreatedAt: clock.Add(time.Second).UnixMilli(),
	})
	if err != nil || !approvedExecution.Created {
		t.Fatalf("approve execution=%#v err=%v", approvedExecution, err)
	}
	clock = clock.Add(2 * time.Second)

	installRunner := runtime.NewRecipeInstallRunner(store, provider, cfg)
	if handled, runErr := installRunner.RunOnce(ctx); runErr != nil || !handled {
		t.Fatalf("install issue handled=%v err=%v", handled, runErr)
	}
	clock = clock.Add(3 * time.Second)
	if handled, runErr := installRunner.RunOnce(ctx); runErr != nil || !handled {
		t.Fatalf("install observe handled=%v err=%v", handled, runErr)
	}

	readinessRunner := runtime.NewServiceReadinessRunner(store, provider, cfg)
	if handled, runErr := readinessRunner.RunOnce(ctx); runErr != nil || !handled {
		t.Fatalf("readiness issue handled=%v err=%v", handled, runErr)
	}
	clock = clock.Add(6 * time.Second)
	if handled, runErr := readinessRunner.RunOnce(ctx); runErr != nil || !handled {
		t.Fatalf("readiness observe handled=%v err=%v", handled, runErr)
	}

	var serviceID, serviceStatus, execution, outcome, publicResource, privateResource string
	var serviceRevision int64
	if err = database.DB().QueryRowContext(ctx, `SELECT service_id,service_status,revision FROM p2p_cloud_services WHERE deployment_id=$1`, provision.DeploymentID).Scan(&serviceID, &serviceStatus, &serviceRevision); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT execution_status,outcome_status,resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, provision.DeploymentID).Scan(&execution, &outcome, &publicResource); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, provision.DeploymentID).Scan(&privateResource); err != nil {
		t.Fatal(err)
	}
	if serviceStatus != "experimental" || execution != "finished" || outcome != "succeeded" || publicResource != "retained_tracked" || privateResource != "retained_tracked" {
		t.Fatalf("ready service=%s deployment=%s/%s/%s private=%s", serviceStatus, execution, outcome, publicResource, privateResource)
	}
	if provider.destroyCalls != 0 || !provider.active {
		t.Fatalf("finished/failed work must not auto-destroy: active=%v destroy_calls=%d", provider.active, provider.destroyCalls)
	}
	var destroyRequests int
	if err = database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_outbox WHERE kind=$1`, cloudmodule.OutboxKindServiceDestroyRequested).Scan(&destroyRequests); err != nil || destroyRequests != 0 {
		t.Fatalf("unapproved destroy requests=%d err=%v", destroyRequests, err)
	}

	clock = clock.Add(time.Minute)
	prepared, err := database.PrepareCloudServiceDestroy(ctx, cloudmodule.PrepareServiceDestroyRequest{
		OwnerMXID: "@owner:example.com", ServiceID: serviceID, ExpectedRevision: serviceRevision,
		IdempotencyHash: "acceptance-destroy-prepare-idempotency", RequestDigest: "acceptance-destroy-prepare-request",
		ApprovalID: "approval-acceptance-destroy", ChallengeID: "challenge-acceptance-destroy",
		CreatedAt: clock.UnixMilli(), ExpiresAt: clock.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil || !prepared.Created {
		t.Fatalf("prepare destroy=%#v err=%v", prepared, err)
	}
	signedApproval, err := prepared.Confirmation.Approval.Sign(devicePrivate, clock.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	approved, err := database.ApproveCloudServiceDestroy(ctx, cloudmodule.ApproveServiceDestroyRequest{
		OwnerMXID: "@owner:example.com", ServiceID: serviceID, ExpectedRevision: int64(signedApproval.ServiceRevision),
		IdempotencyHash: "acceptance-destroy-approve-idempotency", Approval: signedApproval,
		JobID: "job-acceptance-destroy", OutboxID: "outbox-acceptance-destroy",
		ServiceEventID: "event-acceptance-destroy-service", DeploymentEventID: "event-acceptance-destroy-deployment", JobEventID: "event-acceptance-destroy-job",
		CreatedAt: clock.Add(time.Second).UnixMilli(),
	})
	if err != nil || !approved.Created {
		t.Fatalf("approve destroy=%#v err=%v", approved, err)
	}
	clock = clock.Add(2 * time.Second)
	if handled, runErr := runtime.NewServiceDestroyRunner(store, provider, cfg).RunOnce(ctx); runErr != nil || !handled {
		t.Fatalf("destroy handled=%v err=%v", handled, runErr)
	}
	if provider.active || provider.destroyCalls != 1 {
		t.Fatalf("provider read-back active=%v destroy_calls=%d", provider.active, provider.destroyCalls)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT service_status FROM p2p_cloud_services WHERE service_id=$1`, serviceID).Scan(&serviceStatus); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployments WHERE deployment_id=$1`, provision.DeploymentID).Scan(&publicResource); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT resource_status FROM p2p_cloud_deployment_resources WHERE deployment_id=$1`, provision.DeploymentID).Scan(&privateResource); err != nil {
		t.Fatal(err)
	}
	if serviceStatus != "destroyed" || publicResource != "verified_destroyed" || privateResource != "verified_destroyed" {
		t.Fatalf("destroyed service=%s public=%s private=%s", serviceStatus, publicResource, privateResource)
	}
	if provider.installManifest != executionID {
		t.Fatalf("provider installed execution=%q want=%q", provider.installManifest, executionID)
	}
}

type preclaimedProvisionStore struct {
	*Store
	claim runtime.DeploymentProvisionClaim
	used  bool
}

func (s *preclaimedProvisionStore) ClaimDeploymentProvision(context.Context, string, time.Duration) (runtime.DeploymentProvisionClaim, bool, error) {
	if s.used {
		return runtime.DeploymentProvisionClaim{}, false, nil
	}
	s.used = true
	return s.claim, true, nil
}

type cloudLifecycleProvider struct {
	*brokertransport.Transport
	now                func() time.Time
	active             bool
	deploymentID       string
	instance           string
	volumes            []string
	interfaces         []string
	createCalls        int
	destroyCalls       int
	installManifest    string
	installCheckpoint  string
	readinessServiceID string
	readinessSemantic  string
}

func (p *cloudLifecycleProvider) RequestDeploymentCreate(_ context.Context, _ string, command runtime.DeploymentCreateCommand, signed runtime.SignedDeploymentCreateCommand, request runtime.DeploymentCreateRequest) (runtime.BrokerDeployment, error) {
	if err := request.Validate(); err != nil || command.DeploymentID != request.DeploymentID || signed.RequestSHA256 == "" {
		return runtime.BrokerDeployment{}, fmt.Errorf("fake create rejected: %v", err)
	}
	if p.deploymentID != "" && p.deploymentID != request.DeploymentID {
		return runtime.BrokerDeployment{}, fmt.Errorf("fake provider already owns deployment %s", p.deploymentID)
	}
	p.createCalls++
	p.active, p.deploymentID = true, request.DeploymentID
	return runtime.BrokerDeployment{Schema: "dirextalk.aws.deployment-receipt/v1", DeploymentID: request.DeploymentID, ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: signed.RequestSHA256, ResourceStatus: "provisioning", InstanceID: p.instance, VolumeIDs: p.volumes, NetworkInterfaceIDs: p.interfaces, ReceiptJSON: `{"provider":"fake-aws","read_back":"present"}`}, nil
}

func (p *cloudLifecycleProvider) RequestWorkerBootstrapObservation(_ context.Context, _ string, _ runtime.WorkerBootstrapObservationCommand, _ runtime.SignedWorkerBootstrapObservationCommand, request runtime.WorkerBootstrapObservationRequest) (runtime.WorkerBootstrapObservation, error) {
	if !p.active || request.DeploymentID != p.deploymentID {
		return runtime.WorkerBootstrapObservation{}, fmt.Errorf("fake worker is absent")
	}
	now := p.now()
	return runtime.WorkerBootstrapObservation{Schema: runtime.WorkerBootstrapObservationSchema, DeploymentID: request.DeploymentID, ResourceStatus: "provisioning", InstanceID: p.instance, WorkerSessionState: "active", LeaseEpoch: 1, LeaseExpiresAt: now.Add(time.Hour), LastSequence: 1, LastEventAt: now, ObservedAt: now}, nil
}

func (p *cloudLifecycleProvider) RequestRecipeInstallIssue(_ context.Context, _ string, _ runtime.RecipeInstallCommand, _ runtime.SignedRecipeInstallCommand, request runtime.RecipeInstallIssueRequest) (runtime.RecipeInstallResult, error) {
	if !p.active || request.DeploymentID != p.deploymentID || request.Validate() != nil {
		return runtime.RecipeInstallResult{}, fmt.Errorf("fake install issue rejected")
	}
	p.installManifest = request.ExecutionID
	p.installCheckpoint = request.CheckpointSequence[len(request.CheckpointSequence)-1]
	return runtime.RecipeInstallResult{ExecutionID: request.ExecutionID, DeploymentID: request.DeploymentID, TaskID: request.TaskID, Status: "queued", Attempt: 1, UpdatedAt: p.now()}, nil
}

func (p *cloudLifecycleProvider) RequestRecipeInstallObserve(_ context.Context, _ string, command runtime.RecipeInstallCommand, _ runtime.SignedRecipeInstallCommand, request runtime.RecipeInstallObserveRequest) (runtime.RecipeInstallResult, error) {
	if !p.active || request.DeploymentID != p.deploymentID || p.installManifest == "" {
		return runtime.RecipeInstallResult{}, fmt.Errorf("fake install task is absent")
	}
	return runtime.RecipeInstallResult{ExecutionID: command.ExecutionID, DeploymentID: request.DeploymentID, TaskID: request.TaskID, Status: "succeeded", LastCheckpoint: p.installCheckpoint, Attempt: 1, LastSequence: 1, UpdatedAt: p.now()}, nil
}

func (p *cloudLifecycleProvider) RequestServiceReadinessIssue(_ context.Context, _ string, _ runtime.ServiceReadinessCommand, _ runtime.SignedServiceReadinessCommand, request runtime.ServiceReadinessIssueRequest) (runtime.ServiceReadinessResult, error) {
	if !p.active || request.DeploymentID != p.deploymentID || request.Validate() != nil {
		return runtime.ServiceReadinessResult{}, fmt.Errorf("fake readiness issue rejected")
	}
	p.readinessServiceID, p.readinessSemantic = request.ServiceID, request.SemanticExpectationDigest
	return runtime.ServiceReadinessResult{ExecutionID: request.ExecutionID, DeploymentID: request.DeploymentID, ServiceID: request.ServiceID, TaskID: request.TaskID, Status: "queued", Attempt: 1, UpdatedAt: p.now()}, nil
}

func (p *cloudLifecycleProvider) RequestServiceReadinessObserve(_ context.Context, _ string, command runtime.ServiceReadinessCommand, _ runtime.SignedServiceReadinessCommand, request runtime.ServiceReadinessObserveRequest) (runtime.ServiceReadinessResult, error) {
	if !p.active || request.ServiceID != p.readinessServiceID || request.Validate() != nil {
		return runtime.ServiceReadinessResult{}, fmt.Errorf("fake readiness task is absent")
	}
	challenge := "sha256:" + strings.Repeat("e", 64)
	stack := "sha256:" + strings.Repeat("f", 64)
	semantic := p.readinessSemantic
	return runtime.ServiceReadinessResult{ExecutionID: command.ExecutionID, DeploymentID: request.DeploymentID, ServiceID: request.ServiceID, TaskID: request.TaskID, Status: "succeeded", Checkpoint: runtime.ServiceReadinessVerified, Attempt: 1, LastSequence: 1, ChallengeDigest: &challenge, SemanticEvidenceDigest: &semantic, StackObservationDigest: &stack, UpdatedAt: p.now()}, nil
}

func (p *cloudLifecycleProvider) RequestServiceDestroy(_ context.Context, _ string, command runtime.ServiceDestroyCommand, signed runtime.SignedServiceDestroyCommand, request broker.DeploymentDestroyRequest, approval cloudcontracts.ServiceDestroyApprovalV1) (runtime.ServiceDestroyResult, error) {
	if !p.active || request.DeploymentID != p.deploymentID || request.InstanceID != p.instance || approval.Signature == "" {
		return runtime.ServiceDestroyResult{}, fmt.Errorf("fake destroy rejected")
	}
	p.destroyCalls++
	p.active = false
	receipt, err := json.Marshal(broker.DeploymentCommandReceipt{Schema: broker.ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: signed.RequestSHA256, Action: broker.DeploymentDestroyAction})
	if err != nil {
		return runtime.ServiceDestroyResult{}, err
	}
	return runtime.ServiceDestroyResult{Status: "verified_destroyed", DeploymentID: request.DeploymentID, InstanceID: request.InstanceID, VolumeIDs: request.VolumeIDs, NetworkInterfaceIDs: request.NetworkInterfaceIDs, SecretRefs: request.SecretRefs, CommandID: command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: string(receipt)}, nil
}
