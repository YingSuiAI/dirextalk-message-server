package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRecipeInstallRunnerPersistsExactEnvelopeBeforeRequest(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	claim := recipeInstallTestClaim(t)
	store := &recipeInstallMemoryStore{claim: claim}
	transport := &recipeInstallMemoryTransport{now: now, store: store}
	runner := NewRecipeInstallRunner(store, transport, Config{WorkerID: "recipe-runner-1", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err := runner.RunOnce(t.Context())
	if err != nil || !processed || !store.persisted || !transport.requested || !store.committed {
		t.Fatalf("RunOnce = processed:%v err:%v store:%#v transport:%#v", processed, err, store, transport)
	}
	if transport.requestBeforePersist {
		t.Fatal("recipe task was sent before its exact envelope was persisted")
	}

	expiredStore := &recipeInstallMemoryStore{claim: claim}
	expiredTransport := &recipeInstallMemoryTransport{now: now, store: expiredStore, requestErr: RecipeInstallCommandExpired(errors.New("expired_command"))}
	expiredRunner := NewRecipeInstallRunner(expiredStore, expiredTransport, Config{WorkerID: "recipe-runner-1", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err = expiredRunner.RunOnce(t.Context())
	if err != nil || !processed || !expiredStore.expired || expiredStore.committed || expiredStore.deferred {
		t.Fatalf("expired RunOnce = processed:%v err:%v store:%#v", processed, err, expiredStore)
	}
}

func TestRecipeInstallResultRetainsSafeFailureCheckpoint(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	claim := recipeInstallTestClaim(t)
	manifest := claim.Manifest
	errorCode := "recipe_execution_failed"
	failed := RecipeInstallResult{ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, Status: "failed", Attempt: 1,
		LastSequence: 2, LastCheckpoint: manifest.CheckpointSequence[0], ErrorCode: &errorCode, UpdatedAt: now}
	if err := ValidateRecipeInstallResult(claim, failed, now); err != nil {
		t.Fatalf("safe failure checkpoint rejected: %v", err)
	}
	failed.LastCheckpoint = manifest.CheckpointSequence[len(manifest.CheckpointSequence)-1]
	if err := ValidateRecipeInstallResult(claim, failed, now); err == nil {
		t.Fatal("terminal success checkpoint accepted on failed result")
	}
}

func recipeInstallTestClaim(t *testing.T) RecipeInstallClaim {
	t.Helper()
	manifest := recipeInstallTestManifest(t)
	digest, _ := manifest.Digest()
	request := RecipeInstallIssueRequest{Schema: RecipeInstallIssueSchema, ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID,
		TaskID: "recipe-task-0001", TaskKind: "recipe_execution", RecipeExecutionManifestDigest: digest,
		InputDigest: "sha256:" + strings.Repeat("f", 64), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Manifest: manifest}
	requestDigest, _ := request.Digest()
	return RecipeInstallClaim{Phase: RecipeInstallPhaseIssue, OutboxID: "outbox-recipe-0001", Kind: RecipeInstallRequested,
		AggregateType: "recipe_execution", AggregateID: manifest.ExecutionID, LeaseToken: "lease-recipe-0001",
		ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, PlanID: manifest.PlanID, ConnectionID: "connection-recipe-0001",
		Region: "us-east-1", InstanceID: "i-0123456789abcdef0", TaskID: request.TaskID, TaskAttempt: 1,
		ManifestDigest: digest, InputDigest: request.InputDigest, Manifest: manifest,
		BrokerEndpoint: "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-key-1", ExpectedGeneration: 1, JobID: "job-recipe-0001",
		IssueRequest: request, Command: RecipeInstallCommand{CommandID: "command-recipe-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID,
			TaskID: request.TaskID, ConnectionID: "connection-recipe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 1, NodeCounter: 15, Attempt: 1, Action: RecipeInstallIssueAction, RequestDigest: requestDigest}}
}

type recipeInstallMemoryStore struct {
	claim                RecipeInstallClaim
	persisted, committed bool
	expired, deferred    bool
}

func (s *recipeInstallMemoryStore) ClaimRecipeInstall(context.Context, string, time.Duration) (RecipeInstallClaim, bool, error) {
	return s.claim, true, nil
}
func (s *recipeInstallMemoryStore) MarkRecipeInstallStarted(context.Context, RecipeInstallClaim) error {
	return nil
}
func (s *recipeInstallMemoryStore) PersistRecipeInstallCommand(_ context.Context, _ RecipeInstallClaim, signed SignedRecipeInstallCommand) error {
	s.persisted = signed.EnvelopeJSON != ""
	return nil
}
func (s *recipeInstallMemoryStore) CommitRecipeInstall(context.Context, RecipeInstallClaim, RecipeInstallResult) error {
	s.committed = true
	return nil
}
func (s *recipeInstallMemoryStore) DeferRecipeInstall(context.Context, RecipeInstallClaim, string, time.Time) error {
	s.deferred = true
	return nil
}
func (s *recipeInstallMemoryStore) ExpireRecipeInstallCommand(context.Context, RecipeInstallClaim) error {
	s.expired = true
	return nil
}
func (s *recipeInstallMemoryStore) FailRecipeInstall(context.Context, RecipeInstallClaim, string) error {
	return nil
}

type recipeInstallMemoryTransport struct {
	now                             time.Time
	store                           *recipeInstallMemoryStore
	requested, requestBeforePersist bool
	requestErr                      error
}

func (t *recipeInstallMemoryTransport) BuildRecipeInstallIssueCommand(_ RecipeInstallCommand, _ RecipeInstallIssueRequest, _ time.Time) (SignedRecipeInstallCommand, error) {
	return SignedRecipeInstallCommand{EnvelopeJSON: `{"action":"worker.recipe_task.issue"}`, PayloadJSON: `{"sealed":true}`, PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: t.now, ExpiresAt: t.now.Add(4 * time.Minute)}, nil
}
func (t *recipeInstallMemoryTransport) RequestRecipeInstallIssue(_ context.Context, _ string, _ RecipeInstallCommand, _ SignedRecipeInstallCommand, r RecipeInstallIssueRequest) (RecipeInstallResult, error) {
	t.requested = true
	t.requestBeforePersist = t.store == nil || !t.store.persisted
	if t.requestErr != nil {
		return RecipeInstallResult{}, t.requestErr
	}
	return RecipeInstallResult{ExecutionID: r.ExecutionID, DeploymentID: r.DeploymentID, TaskID: r.TaskID, Status: "queued", Attempt: 1, UpdatedAt: t.now}, nil
}
func (t *recipeInstallMemoryTransport) BuildRecipeInstallObserveCommand(RecipeInstallCommand, RecipeInstallObserveRequest, time.Time) (SignedRecipeInstallCommand, error) {
	return SignedRecipeInstallCommand{}, nil
}
func (t *recipeInstallMemoryTransport) RequestRecipeInstallObserve(context.Context, string, RecipeInstallCommand, SignedRecipeInstallCommand, RecipeInstallObserveRequest) (RecipeInstallResult, error) {
	return RecipeInstallResult{}, nil
}

func recipeInstallTestManifest(t *testing.T) cloudcontracts.RecipeExecutionManifestV1 {
	t.Helper()
	digest := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	m := cloudcontracts.RecipeExecutionManifestV1{SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema, ExecutionID: "execution-recipe-0001", DeploymentID: "deployment-recipe-0001", PlanID: "plan-recipe-0001", PlanHash: digest("a"), PlanRevision: 1, RecipeDigest: digest("b"), WorkerResourceManifestDigest: digest("c"), ArtifactDigest: digest("d"), ActionID: "install-service", RootRequired: true, TimeoutSeconds: 1200, CheckpointSequence: []string{"artifact_verified", "health_verified"}}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	return m
}
