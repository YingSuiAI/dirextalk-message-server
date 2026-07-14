package recipeexec_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestExecutorResumesMonotonicallyAndNeverReplaysCompletedAction(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	binding := recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest}
	store := &memoryStore{state: recipeexec.CheckpointState{
		Binding:    binding,
		Checkpoint: "install_complete",
		Index:      1,
	}}
	driver := &fakeDriver{run: func(_ context.Context, request recipeexec.ActionRequest, checkpoints recipeexec.CheckpointReporter) error {
		if request.Binding != binding {
			t.Fatalf("request binding = %#v, want %#v", request.Binding, binding)
		}
		if request.ResumeAfter != "install_complete" {
			t.Fatalf("ResumeAfter = %q, want install_complete", request.ResumeAfter)
		}
		if request.Artifact.ArtifactDigest != manifest.ArtifactDigest || request.ActionID != manifest.ActionID {
			t.Fatalf("driver received an unbound action request: %#v", request)
		}
		return checkpoints.Checkpoint(context.Background(), "health_verified")
	}}
	executor := recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
		Store:    store,
		Driver:   driver,
	}

	result, err := executor.Execute(context.Background(), manifest)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !result.Completed || result.LastCheckpoint != "health_verified" || !result.Resumed {
		t.Fatalf("Execute() result = %#v, want completed resumed health checkpoint", result)
	}
	if driver.calls != 1 || store.advances != 1 {
		t.Fatalf("first Execute() calls = driver:%d store:%d, want 1:1", driver.calls, store.advances)
	}

	result, err = executor.Execute(context.Background(), manifest)
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	if !result.Completed || driver.calls != 1 || store.advances != 1 {
		t.Fatalf("completed execution replayed action: result=%#v driver=%d store=%d", result, driver.calls, store.advances)
	}
}

func TestExecutorRejectsArtifactMismatchBeforeInvokingDriver(t *testing.T) {
	manifest := validManifest()
	store := &memoryStore{}
	driver := &fakeDriver{}
	executor := recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: sha256('f'), ActionIDs: []string{manifest.ActionID}}},
		Store:    store,
		Driver:   driver,
	}

	_, err := executor.Execute(context.Background(), manifest)
	if !errors.Is(err, recipeexec.ErrArtifactDigestMismatch) {
		t.Fatalf("Execute() error = %v, want ErrArtifactDigestMismatch", err)
	}
	if driver.calls != 0 || store.loads != 0 {
		t.Fatalf("artifact mismatch reached execution state: driver=%d loads=%d", driver.calls, store.loads)
	}
}

func TestExecutorAdvancesTheExactDeclaredCheckpointSequence(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	store := &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})}
	driver := &fakeDriver{run: func(_ context.Context, _ recipeexec.ActionRequest, checkpoints recipeexec.CheckpointReporter) error {
		for _, checkpoint := range manifest.CheckpointSequence {
			if err := checkpoints.Checkpoint(context.Background(), checkpoint); err != nil {
				return err
			}
		}
		return nil
	}}
	executor := recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
		Store:    store,
		Driver:   driver,
	}

	result, err := executor.Execute(context.Background(), manifest)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !result.Completed || result.LastCheckpoint != "health_verified" || result.Resumed {
		t.Fatalf("Execute() result = %#v, want a fresh completed execution", result)
	}
	if store.advances != len(manifest.CheckpointSequence) {
		t.Fatalf("checkpoint advances = %d, want %d", store.advances, len(manifest.CheckpointSequence))
	}
}

func TestExecutorRejectsSkippedCheckpoint(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	store := &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})}
	driver := &fakeDriver{run: func(_ context.Context, _ recipeexec.ActionRequest, checkpoints recipeexec.CheckpointReporter) error {
		return checkpoints.Checkpoint(context.Background(), "health_verified")
	}}
	executor := recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
		Store:    store,
		Driver:   driver,
	}

	_, err = executor.Execute(context.Background(), manifest)
	if !errors.Is(err, recipeexec.ErrCheckpointOutOfOrder) {
		t.Fatalf("Execute() error = %v, want ErrCheckpointOutOfOrder", err)
	}
	if store.advances != 0 {
		t.Fatalf("out-of-order checkpoint was persisted %d times", store.advances)
	}
}

func TestExecutorDoesNotPersistCheckpointAfterExecutionContextCanceled(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	store := &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	driver := &fakeDriver{run: func(_ context.Context, _ recipeexec.ActionRequest, checkpoints recipeexec.CheckpointReporter) error {
		cancel()
		return checkpoints.Checkpoint(context.Background(), "artifact_verified")
	}}
	executor := recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
		Store:    store,
		Driver:   driver,
	}

	_, err = executor.Execute(parent, manifest)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if store.advances != 0 {
		t.Fatalf("canceled execution persisted %d checkpoint(s)", store.advances)
	}
}

func TestExecutorSerializesConcurrentCheckpointReports(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	store := &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})}
	driver := &fakeDriver{run: func(_ context.Context, _ recipeexec.ActionRequest, checkpoints recipeexec.CheckpointReporter) error {
		start := make(chan struct{})
		results := make(chan error, 2)
		for index := 0; index < 2; index++ {
			go func() {
				<-start
				results <- checkpoints.Checkpoint(context.Background(), "artifact_verified")
			}()
		}
		close(start)
		successes, outOfOrder := 0, 0
		for index := 0; index < 2; index++ {
			err := <-results
			if err == nil {
				successes++
			} else if errors.Is(err, recipeexec.ErrCheckpointOutOfOrder) {
				outOfOrder++
			}
		}
		if successes != 1 || outOfOrder != 1 {
			return recipeexec.ErrCheckpointConflict
		}
		return nil
	}}
	executor := recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
		Store:    store,
		Driver:   driver,
	}

	_, err = executor.Execute(context.Background(), manifest)
	if !errors.Is(err, recipeexec.ErrExecutionIncomplete) {
		t.Fatalf("Execute() error = %v, want ErrExecutionIncomplete", err)
	}
	if store.advances != 1 || store.state.Checkpoint != "artifact_verified" {
		t.Fatalf("concurrent reports advanced state incorrectly: advances=%d state=%#v", store.advances, store.state)
	}
}

func TestExecutorRejectsStateFromAnotherManifestAndIncompleteDriver(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	for _, test := range []struct {
		name   string
		store  *memoryStore
		driver *fakeDriver
		want   error
	}{
		{
			name: "foreign state",
			store: &memoryStore{state: recipeexec.CheckpointState{
				Binding: recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: sha256('e')},
				Index:   -1,
			}},
			driver: &fakeDriver{},
			want:   recipeexec.ErrCheckpointBinding,
		},
		{
			name:   "driver returns without terminal checkpoint",
			store:  &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})},
			driver: &fakeDriver{},
			want:   recipeexec.ErrExecutionIncomplete,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := recipeexec.Executor{
				Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
				Store:    test.store,
				Driver:   test.driver,
			}
			_, err := executor.Execute(context.Background(), manifest)
			if !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
		})
	}
}

type fakeResolver struct {
	bundle recipeexec.Bundle
	err    error
}

func (resolver fakeResolver) Resolve(_ context.Context, _ string) (recipeexec.Bundle, error) {
	return resolver.bundle, resolver.err
}

type fakeDriver struct {
	calls int
	run   func(context.Context, recipeexec.ActionRequest, recipeexec.CheckpointReporter) error
}

func (driver *fakeDriver) Execute(ctx context.Context, request recipeexec.ActionRequest, checkpoints recipeexec.CheckpointReporter) error {
	driver.calls++
	if driver.run == nil {
		return nil
	}
	return driver.run(ctx, request, checkpoints)
}

type memoryStore struct {
	state    recipeexec.CheckpointState
	loads    int
	advances int
}

func (store *memoryStore) Load(_ context.Context, _ recipeexec.Binding) (recipeexec.CheckpointState, error) {
	store.loads++
	return store.state, nil
}

func (store *memoryStore) Advance(_ context.Context, previous, next recipeexec.CheckpointState) error {
	if store.state != previous {
		return recipeexec.ErrCheckpointConflict
	}
	store.advances++
	store.state = next
	return nil
}

func validManifest() cloudorchestrator.RecipeExecutionManifestV1 {
	return cloudorchestrator.RecipeExecutionManifestV1{
		SchemaVersion:                cloudorchestrator.RecipeExecutionManifestV1Schema,
		ExecutionID:                  "execution-worker-1",
		DeploymentID:                 "deployment-worker-1",
		PlanID:                       "plan-worker-1",
		PlanHash:                     sha256('a'),
		PlanRevision:                 1,
		RecipeDigest:                 sha256('b'),
		WorkerResourceManifestDigest: sha256('c'),
		ArtifactDigest:               sha256('d'),
		ActionID:                     "install-service",
		RootRequired:                 true,
		TimeoutSeconds:               60,
		CheckpointSequence:           []string{"artifact_verified", "install_complete", "health_verified"},
		VolumeSlots:                  []cloudorchestrator.VolumeSlotV1{{SlotID: "data", VolumeRef: "volume_ref:data-a"}},
		DataSlots:                    []cloudorchestrator.DataSlotV1{{SlotID: "dataset", DataRef: "data_ref:dataset-a", ReadOnly: true}},
		SecretSlots:                  []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:model-token-a"}},
	}
}

func sha256(character rune) string {
	return "sha256:" + strings.Repeat(string(character), 64)
}
