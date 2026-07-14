package recipeexec_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestRecipeExecutionTaskBindsTheSealedManifestAndResumesProgress(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	task := recipeexec.TaskV1{
		Schema:                        recipeexec.TaskV1Schema,
		TaskID:                        "recipe-task-1",
		ExecutionID:                   manifest.ExecutionID,
		DeploymentID:                  manifest.DeploymentID,
		TaskKind:                      recipeexec.TaskKindRecipeExecution,
		RecipeExecutionManifestDigest: manifestDigest,
		InputDigest:                   sha256('e'),
		CheckpointSequence:            append([]string(nil), manifest.CheckpointSequence...),
		LastCheckpoint:                "install_complete",
		Attempt:                       2,
		LastSequence:                  5,
	}
	if err := task.ValidateForManifest(manifest); err != nil {
		t.Fatalf("TaskV1.ValidateForManifest() error = %v", err)
	}

	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	parsed, err := recipeexec.ParseTaskV1(raw)
	if err != nil {
		t.Fatalf("ParseTaskV1() error = %v", err)
	}
	if !parsed.Equal(task) {
		t.Fatalf("parsed task = %#v, want %#v", parsed, task)
	}

	progress, err := recipeexec.NewProgress(parsed)
	if err != nil {
		t.Fatalf("NewProgress() error = %v", err)
	}
	next := recipeTaskEvent(parsed, 7, 6, recipeexec.TaskStatusSucceeded, "health_verified", "", parsed.RecipeExecutionManifestDigest)
	progress, err = progress.Advance(next, 7)
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if !progress.Terminal || progress.LastCheckpoint != "health_verified" || progress.LastSequence != 6 {
		t.Fatalf("progress = %#v", progress)
	}
	parsed.CheckpointSequence[0] = "mutated_after_progress_creation"
	if progress.Task.CheckpointSequence[0] != "artifact_verified" {
		t.Fatalf("progress retained a caller-owned checkpoint slice: %#v", progress.Task.CheckpointSequence)
	}
}

func TestRecipeExecutionTaskRejectsArbitraryMaterialAndInvalidBindings(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	valid := recipeexec.TaskV1{
		Schema:                        recipeexec.TaskV1Schema,
		TaskID:                        "recipe-task-1",
		ExecutionID:                   manifest.ExecutionID,
		DeploymentID:                  manifest.DeploymentID,
		TaskKind:                      recipeexec.TaskKindRecipeExecution,
		RecipeExecutionManifestDigest: manifestDigest,
		InputDigest:                   sha256('e'),
		CheckpointSequence:            append([]string(nil), manifest.CheckpointSequence...),
		Attempt:                       1,
		LastSequence:                  0,
	}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	base := strings.TrimSuffix(string(encoded), "}")
	for _, raw := range []string{
		base + `,"command":"curl https://worker.invalid"}`,
		base + `,"task_id":"different-task"}`,
		strings.Replace(string(encoded), `"checkpoint_sequence":["artifact_verified","install_complete","health_verified"]`, `"checkpoint_sequence":["artifact-verified","install_complete","health_verified"]`, 1),
		strings.Replace(string(encoded), `"last_checkpoint":""`, `"last_checkpoint":"not_declared"`, 1),
		strings.Replace(string(encoded), `"recipe_execution_manifest_digest":"`+manifestDigest+`"`, `"recipe_execution_manifest_digest":"`+sha256('f')+`"`, 1),
	} {
		if task, parseErr := recipeexec.ParseTaskV1([]byte(raw)); parseErr == nil {
			if err := task.ValidateForManifest(manifest); err == nil {
				t.Fatalf("accepted invalid recipe execution task: %s", raw)
			}
		}
	}
	invalidEvent := `{"schema":"dirextalk.recipe-execution-task-event/v1","task_id":"recipe-task-1","attempt":1,"lease_epoch":7,"sequence":1,"status":"running","checkpoint":"artifact_verified","error_code":null,"evidence_digest":"` + manifestDigest + `","occurred_at":"2026-07-15T02:00:00.000Z","output":"not-allowed"}`
	if _, err := recipeexec.ParseEventV1([]byte(invalidEvent)); err == nil {
		t.Fatal("ParseEventV1() accepted arbitrary executor output")
	}

	progress, err := recipeexec.NewProgress(valid)
	if err != nil {
		t.Fatalf("NewProgress() error = %v", err)
	}
	for _, event := range []recipeexec.EventV1{
		recipeTaskEvent(valid, 7, 1, recipeexec.TaskStatusRunning, "install_complete", "", valid.RecipeExecutionManifestDigest),
		recipeTaskEvent(valid, 7, 1, recipeexec.TaskStatusRunning, "artifact_verified", "", sha256('f')),
		recipeTaskEvent(valid, 1, 1, recipeexec.TaskStatusRunning, "artifact_verified", "", valid.RecipeExecutionManifestDigest),
	} {
		if _, err := progress.Advance(event, 7); err == nil {
			t.Fatalf("Advance() accepted invalid recipe progress event: %#v", event)
		}
	}
}

func TestRecipeExecutionTaskUsesTheManifestIdentifierGrammarForBindings(t *testing.T) {
	manifest := validManifest()
	manifest.ExecutionID = "e"
	manifest.DeploymentID = "d"
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	task := recipeexec.TaskV1{
		Schema:                        recipeexec.TaskV1Schema,
		TaskID:                        "recipe-task-1",
		ExecutionID:                   manifest.ExecutionID,
		DeploymentID:                  manifest.DeploymentID,
		TaskKind:                      recipeexec.TaskKindRecipeExecution,
		RecipeExecutionManifestDigest: manifestDigest,
		InputDigest:                   sha256('e'),
		CheckpointSequence:            append([]string(nil), manifest.CheckpointSequence...),
		Attempt:                       1,
	}
	if err := task.ValidateForManifest(manifest); err != nil {
		t.Fatalf("TaskV1.ValidateForManifest() rejected manifest-compatible bindings: %v", err)
	}
}

func TestRecipeExecutionTaskProgressRequiresOrderedCheckpointsAndAClosedTerminalState(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	task := recipeexec.TaskV1{
		Schema:                        recipeexec.TaskV1Schema,
		TaskID:                        "recipe-task-1",
		ExecutionID:                   manifest.ExecutionID,
		DeploymentID:                  manifest.DeploymentID,
		TaskKind:                      recipeexec.TaskKindRecipeExecution,
		RecipeExecutionManifestDigest: manifestDigest,
		InputDigest:                   sha256('e'),
		CheckpointSequence:            append([]string(nil), manifest.CheckpointSequence...),
		Attempt:                       1,
	}
	progress, err := recipeexec.NewProgress(task)
	if err != nil {
		t.Fatalf("NewProgress() error = %v", err)
	}

	for _, event := range []recipeexec.EventV1{
		recipeTaskEvent(task, 7, 1, recipeexec.TaskStatusRunning, "artifact_verified", "", task.RecipeExecutionManifestDigest),
		recipeTaskEvent(task, 7, 2, recipeexec.TaskStatusRunning, "install_complete", "", task.RecipeExecutionManifestDigest),
	} {
		progress, err = progress.Advance(event, 7)
		if err != nil {
			t.Fatalf("Advance(%#v) error = %v", event, err)
		}
	}
	if _, err := progress.Advance(recipeTaskEvent(task, 7, 3, recipeexec.TaskStatusRunning, "health_verified", "", task.RecipeExecutionManifestDigest), 7); err == nil {
		t.Fatal("Advance() accepted the terminal checkpoint as a nonterminal running event")
	}
	progress, err = progress.Advance(recipeTaskEvent(task, 7, 3, recipeexec.TaskStatusSucceeded, "health_verified", "", task.RecipeExecutionManifestDigest), 7)
	if err != nil {
		t.Fatalf("Advance(terminal success) error = %v", err)
	}
	if !progress.Terminal {
		t.Fatal("terminal checkpoint did not close the task")
	}
	if _, err := progress.Advance(recipeTaskEvent(task, 7, 4, recipeexec.TaskStatusFailed, "", "driver_failed", ""), 7); err == nil {
		t.Fatal("Advance() accepted an event after a terminal success")
	}
}

func TestExecutorExecuteTaskRequiresTheBoundTaskAndDurableResumeCheckpoint(t *testing.T) {
	manifest := validManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	task := recipeexec.TaskV1{
		Schema:                        recipeexec.TaskV1Schema,
		TaskID:                        "recipe-task-1",
		ExecutionID:                   manifest.ExecutionID,
		DeploymentID:                  manifest.DeploymentID,
		TaskKind:                      recipeexec.TaskKindRecipeExecution,
		RecipeExecutionManifestDigest: manifestDigest,
		InputDigest:                   sha256('e'),
		CheckpointSequence:            append([]string(nil), manifest.CheckpointSequence...),
		Attempt:                       1,
	}

	driver := &fakeDriver{run: func(ctx context.Context, _ recipeexec.ActionRequest, checkpoints recipeexec.CheckpointReporter) error {
		for _, checkpoint := range manifest.CheckpointSequence {
			if err := checkpoints.Checkpoint(ctx, checkpoint); err != nil {
				return err
			}
		}
		return nil
	}}
	executor := recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
		Store:    &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})},
		Driver:   driver,
	}
	if result, err := executor.ExecuteTask(context.Background(), task, manifest); err != nil || !result.Completed || driver.calls != 1 {
		t.Fatalf("ExecuteTask() = (%#v, %v), driver calls=%d", result, err, driver.calls)
	}

	mismatched := task
	mismatched.LastCheckpoint = "install_complete"
	mismatched.LastSequence = 2
	driver = &fakeDriver{}
	executor = recipeexec.Executor{
		Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}},
		Store:    &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})},
		Driver:   driver,
	}
	if _, err := executor.ExecuteTask(context.Background(), mismatched, manifest); !errors.Is(err, recipeexec.ErrTaskCheckpointBinding) {
		t.Fatalf("ExecuteTask() error = %v, want ErrTaskCheckpointBinding", err)
	}
	if driver.calls != 0 {
		t.Fatalf("driver calls = %d after a task/store checkpoint mismatch", driver.calls)
	}

	unbound := task
	unbound.RecipeExecutionManifestDigest = sha256('f')
	executor = recipeexec.Executor{
		Resolver: panicResolver{},
		Store:    &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})},
		Driver:   &fakeDriver{},
	}
	if _, err := executor.ExecuteTask(context.Background(), unbound, manifest); !errors.Is(err, recipeexec.ErrTaskManifestBinding) {
		t.Fatalf("ExecuteTask() error = %v, want ErrTaskManifestBinding", err)
	}
}

func recipeTaskEvent(task recipeexec.TaskV1, leaseEpoch, sequence uint64, status recipeexec.TaskStatus, checkpoint, errorCode, evidence string) recipeexec.EventV1 {
	return recipeexec.EventV1{
		Schema:         recipeexec.EventV1Schema,
		TaskID:         task.TaskID,
		Attempt:        task.Attempt,
		LeaseEpoch:     leaseEpoch,
		Sequence:       sequence,
		Status:         status,
		Checkpoint:     optionalRecipeTaskString(checkpoint),
		ErrorCode:      optionalRecipeTaskString(errorCode),
		EvidenceDigest: optionalRecipeTaskString(evidence),
		OccurredAt:     "2026-07-15T02:00:00.000Z",
	}
}

func optionalRecipeTaskString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

type panicResolver struct{}

func (panicResolver) Resolve(context.Context, string) (recipeexec.Bundle, error) {
	panic("resolver must not run for an unbound task")
}
