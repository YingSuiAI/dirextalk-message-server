package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestServiceOperationRunnerPersistsExactEnvelopeBeforeWorkerRequest(t *testing.T) {
	now := time.Date(2026, 7, 15, 14, 0, 0, 0, time.UTC)
	claim := serviceOperationTestClaim(t)
	store := &serviceOperationMemoryStore{claim: claim}
	transport := &serviceOperationMemoryTransport{now: now, store: store}
	runner := NewServiceOperationRunner(store, transport, Config{WorkerID: "service-operation-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err := runner.RunOnce(t.Context())
	if err != nil || !processed || !store.started || !store.persisted || !store.committed || !transport.requested || transport.requestBeforePersist {
		t.Fatalf("run processed=%v err=%v store=%#v transport=%#v", processed, err, store, transport)
	}
}

func serviceOperationTestClaim(t *testing.T) RecipeInstallClaim {
	t.Helper()
	d := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	manifest := cloudcontracts.RecipeExecutionManifestV1{
		SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema, ExecutionID: "service-operation-0001",
		DeploymentID: "deployment-operation-0001", PlanID: "plan-operation-0001", PlanHash: d("a"), PlanRevision: 1,
		RecipeDigest: d("b"), WorkerResourceManifestDigest: d("c"), ArtifactDigest: cloudcontracts.FixedProbeManagedArtifactDigest,
		ActionID: cloudcontracts.FixedProbeRestartActionID, RootRequired: true, TimeoutSeconds: 120,
		CheckpointSequence: []string{"probe_service_restarted", "probe_health_verified"},
		SemanticReadiness:  cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: cloudcontracts.FixedReadinessEvidenceDigestV1},
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	request := RecipeInstallIssueRequest{Schema: RecipeInstallIssueSchema, ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskID: "service-operation-task-0001", TaskKind: "recipe_execution", RecipeExecutionManifestDigest: manifestDigest, InputDigest: d("d"), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Manifest: manifest}
	requestDigest, _ := request.Digest()
	return RecipeInstallClaim{Phase: RecipeInstallPhaseIssue, OutboxID: "service-operation-outbox-0001", Kind: ServiceOperationRequested, AggregateType: "service_operation", AggregateID: manifest.ExecutionID, LeaseToken: "service-operation-lease-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, PlanID: manifest.PlanID, ConnectionID: "connection-operation-0001", Region: "us-east-1", InstanceID: "i-0123456789abcdef0", TaskID: request.TaskID, TaskAttempt: 1, ManifestDigest: manifestDigest, InputDigest: request.InputDigest, Manifest: manifest, BrokerEndpoint: "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-key-1", ExpectedGeneration: 1, JobID: "job-operation-0001", IssueRequest: request, Command: RecipeInstallCommand{CommandID: "command-operation-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskID: request.TaskID, ConnectionID: "connection-operation-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 1, NodeCounter: 20, Attempt: 1, Action: RecipeInstallIssueAction, RequestDigest: requestDigest}}
}

type serviceOperationMemoryStore struct {
	claim                         RecipeInstallClaim
	started, persisted, committed bool
}

func (s *serviceOperationMemoryStore) ClaimServiceOperation(context.Context, string, time.Duration) (RecipeInstallClaim, bool, error) {
	return s.claim, true, nil
}
func (s *serviceOperationMemoryStore) MarkServiceOperationStarted(context.Context, RecipeInstallClaim) error {
	s.started = true
	return nil
}
func (s *serviceOperationMemoryStore) PersistServiceOperationCommand(_ context.Context, _ RecipeInstallClaim, v SignedRecipeInstallCommand) error {
	s.persisted = v.EnvelopeJSON != ""
	return nil
}
func (s *serviceOperationMemoryStore) CommitServiceOperation(context.Context, RecipeInstallClaim, RecipeInstallResult) error {
	s.committed = true
	return nil
}
func (*serviceOperationMemoryStore) DeferServiceOperation(context.Context, RecipeInstallClaim, string, time.Time) error {
	return nil
}
func (*serviceOperationMemoryStore) ExpireServiceOperationCommand(context.Context, RecipeInstallClaim) error {
	return nil
}
func (*serviceOperationMemoryStore) FailServiceOperation(context.Context, RecipeInstallClaim, string) error {
	return nil
}

type serviceOperationMemoryTransport struct {
	now                             time.Time
	store                           *serviceOperationMemoryStore
	requested, requestBeforePersist bool
}

func (t *serviceOperationMemoryTransport) BuildRecipeInstallIssueCommand(RecipeInstallCommand, RecipeInstallIssueRequest, time.Time) (SignedRecipeInstallCommand, error) {
	return SignedRecipeInstallCommand{EnvelopeJSON: `{"action":"worker.recipe_task.issue"}`, PayloadJSON: `{"sealed":true}`, PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: t.now, ExpiresAt: t.now.Add(4 * time.Minute)}, nil
}
func (t *serviceOperationMemoryTransport) RequestRecipeInstallIssue(_ context.Context, _ string, _ RecipeInstallCommand, _ SignedRecipeInstallCommand, r RecipeInstallIssueRequest) (RecipeInstallResult, error) {
	t.requested = true
	t.requestBeforePersist = !t.store.persisted
	return RecipeInstallResult{ExecutionID: r.ExecutionID, DeploymentID: r.DeploymentID, TaskID: r.TaskID, Status: "queued", Attempt: 1, UpdatedAt: t.now}, nil
}
func (*serviceOperationMemoryTransport) BuildRecipeInstallObserveCommand(RecipeInstallCommand, RecipeInstallObserveRequest, time.Time) (SignedRecipeInstallCommand, error) {
	return SignedRecipeInstallCommand{}, nil
}
func (*serviceOperationMemoryTransport) RequestRecipeInstallObserve(context.Context, string, RecipeInstallCommand, SignedRecipeInstallCommand, RecipeInstallObserveRequest) (RecipeInstallResult, error) {
	return RecipeInstallResult{}, nil
}
