package recipeexec_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestFileSecretStagerWrites0600AndCleansEveryValue(t *testing.T) {
	root := t.TempDir()
	stager, err := recipeexec.NewFileSecretStager(root, func(path string) error {
		if path != root && filepath.Dir(path) != root {
			return errors.New("root")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery, cleanup, err := stager.Stage(context.Background(), "deployment-1", "execution-1", []recipeexec.MaterializedSecret{
		{Target: recipeexec.SecretTarget{SlotID: "token-file", FileName: "token"}, Value: []byte("file-canary")},
		{Target: recipeexec.SecretTarget{SlotID: "token-env", EnvironmentKey: "MODEL_TOKEN"}, Value: []byte("env-canary")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if delivery.StagingDirectory != filepath.Dir(delivery.EnvironmentFile) || !filepath.IsAbs(delivery.StagingDirectory) {
		t.Fatalf("staging directory is not bound: %#v", delivery)
	}
	for _, path := range []string{delivery.Files["token-file"], delivery.EnvironmentFile} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %o", filepath.Base(path), info.Mode().Perm())
		}
	}
	cleanup()
	if _, err := os.Stat(filepath.Dir(delivery.EnvironmentFile)); !os.IsNotExist(err) {
		t.Fatalf("secret directory survived cleanup: %v", err)
	}
}

func TestValidateStagedSecretDeliveryAcceptsEmptyDelivery(t *testing.T) {
	if err := recipeexec.ValidateStagedSecretDelivery(recipeexec.SecretDelivery{}); err != nil {
		t.Fatal(err)
	}
}

type captureMaterializer struct {
	requests []recipeexec.SecretMaterializeRequest
	value    []byte
	err      error
	errs     []error
}

func (m *captureMaterializer) Materialize(_ context.Context, request recipeexec.SecretMaterializeRequest) ([]byte, error) {
	m.requests = append(m.requests, request)
	if len(m.errs) > 0 {
		err := m.errs[0]
		m.errs = m.errs[1:]
		return append([]byte(nil), m.value...), err
	}
	return append([]byte(nil), m.value...), m.err
}

type captureStager struct {
	staged  []recipeexec.MaterializedSecret
	cleaned int
}

func (s *captureStager) Stage(_ context.Context, _, _ string, values []recipeexec.MaterializedSecret) (recipeexec.SecretDelivery, func(), error) {
	s.staged = values
	return recipeexec.SecretDelivery{EnvironmentFile: "trusted/environment"}, func() { s.cleaned++ }, nil
}

func TestExecutorGateOffRejectsSecretWorkload(t *testing.T) {
	manifest := validManifest()
	manifest.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:model-token-a"}}
	digest, _ := manifest.Digest()
	driver := &fakeDriver{}
	executor := recipeexec.Executor{Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}}, Store: &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: digest})}, Driver: driver}
	if _, err := executor.Execute(context.Background(), manifest); !errors.Is(err, recipeexec.ErrSecretScope) || driver.calls != 0 {
		t.Fatalf("gate-off secret workload = %v, driver calls=%d", err, driver.calls)
	}
}

func TestExecutorMaterializesTrustedTargetsAndCleansOnRestartableFailure(t *testing.T) {
	manifest := validManifest()
	manifest.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:model-token-a"}}
	digest, _ := manifest.Digest()
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-1", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: sha256('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	materializer := &captureMaterializer{value: []byte("canary"), errs: []error{recipeexec.ErrSecretMaterializePending, recipeexec.ErrSecretMaterializePending, nil}}
	stager := &captureStager{}
	driver := &fakeDriver{run: func(ctx context.Context, request recipeexec.ActionRequest, reporter recipeexec.CheckpointReporter) error {
		if len(request.Artifact.SecretTargets) != 1 || request.Artifact.SecretTargets[0].SlotID != "model-token" || len(request.SecretSlots) != 1 || request.SecretSlots[0].SlotID != "model-token" || request.SecretSlots[0].SecretRef != "" {
			t.Fatalf("driver secret scope was not de-secreted and preserved: %#v %#v", request.Artifact.SecretTargets, request.SecretSlots)
		}
		if request.ResumeAfter == "" {
			if err := reporter.Checkpoint(ctx, manifest.CheckpointSequence[0]); err != nil {
				return err
			}
			return errors.New("restartable")
		}
		for _, checkpoint := range manifest.CheckpointSequence[1:] {
			if err := reporter.Checkpoint(ctx, checkpoint); err != nil {
				return err
			}
		}
		return nil
	}}
	executor := recipeexec.Executor{Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}, SecretTargets: []recipeexec.SecretTarget{{SlotID: "model-token", EnvironmentKey: "MODEL_TOKEN"}}}}, Store: &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: digest})}, Driver: driver, RequireSecretMaterialization: true, Materializer: materializer, SecretStager: stager, SecretRetryDelay: time.Millisecond}
	_, err := executor.ExecuteTask(context.Background(), task, manifest)
	if err == nil || len(materializer.requests) != 3 || stager.cleaned != 1 {
		t.Fatalf("first execution = %v, requests=%d cleaned=%d", err, len(materializer.requests), stager.cleaned)
	}
	result, err := executor.ExecuteTask(context.Background(), task, manifest)
	if err != nil || !result.Completed || len(materializer.requests) != 4 || stager.cleaned != 2 {
		t.Fatalf("resumed execution = (%#v, %v), requests=%d cleaned=%d", result, err, len(materializer.requests), stager.cleaned)
	}
	request := materializer.requests[0]
	if request.TaskID != task.TaskID || request.ManifestDigest != digest || request.SecretRef != "secret_ref:model-token-a" {
		t.Fatalf("request not fully bound: %#v", request)
	}
}

func TestExecutorPendingSecretHonorsTimeoutAndRetriesAfterRestart(t *testing.T) {
	manifest := validManifest()
	manifest.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:dynamic-plan/model-token"}}
	digest, _ := manifest.Digest()
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-1", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: sha256('e'), CheckpointSequence: manifest.CheckpointSequence, Attempt: 1}
	driver := &fakeDriver{run: func(ctx context.Context, _ recipeexec.ActionRequest, reporter recipeexec.CheckpointReporter) error {
		for _, checkpoint := range manifest.CheckpointSequence {
			if err := reporter.Checkpoint(ctx, checkpoint); err != nil {
				return err
			}
		}
		return nil
	}}
	materializer := &captureMaterializer{err: recipeexec.ErrSecretMaterializePending}
	executor := recipeexec.Executor{Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}, SecretTargets: []recipeexec.SecretTarget{{SlotID: "model-token", FileName: "token"}}}}, Store: &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: digest})}, Driver: driver, RequireSecretMaterialization: true, Materializer: materializer, SecretStager: &captureStager{}, SecretRetryDelay: 20 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := executor.ExecuteTask(ctx, task, manifest); !errors.Is(err, context.DeadlineExceeded) || driver.calls != 0 || len(materializer.requests) != 1 {
		t.Fatalf("pending timeout err=%v driver=%d requests=%d", err, driver.calls, len(materializer.requests))
	}
	if materializer.requests[0].SecretRef != manifest.SecretSlots[0].SecretRef {
		t.Fatalf("dynamic manifest ref not used: %#v", materializer.requests[0])
	}
	materializer.err = nil
	materializer.value = []byte("uploaded-after-restart")
	if result, err := executor.ExecuteTask(context.Background(), task, manifest); err != nil || !result.Completed || driver.calls != 1 {
		t.Fatalf("restart retry result=%#v err=%v driver=%d", result, err, driver.calls)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := executor.ExecuteTask(canceled, task, manifest); !errors.Is(err, context.Canceled) || driver.calls != 1 {
		t.Fatalf("canceled execution err=%v driver=%d", err, driver.calls)
	}
}

func TestExecutorRejectsSecretScopeAndProviderFailureBeforeDriver(t *testing.T) {
	manifest := validManifest()
	manifest.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:model-token-a"}}
	digest, _ := manifest.Digest()
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-1", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: sha256('e'), CheckpointSequence: manifest.CheckpointSequence, Attempt: 1}
	for _, tc := range []struct {
		name         string
		target       recipeexec.SecretTarget
		materializer *captureMaterializer
		want         error
	}{
		{"scope", recipeexec.SecretTarget{SlotID: "other", FileName: "token"}, &captureMaterializer{value: []byte("x")}, recipeexec.ErrSecretScope},
		{"provider", recipeexec.SecretTarget{SlotID: "model-token", FileName: "token"}, &captureMaterializer{err: errors.New("denied")}, recipeexec.ErrSecretMaterialize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driver := &fakeDriver{}
			executor := recipeexec.Executor{Resolver: fakeResolver{bundle: recipeexec.Bundle{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}, SecretTargets: []recipeexec.SecretTarget{tc.target}}}, Store: &memoryStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: digest})}, Driver: driver, RequireSecretMaterialization: true, Materializer: tc.materializer, SecretStager: &captureStager{}}
			_, err := executor.ExecuteTask(context.Background(), task, manifest)
			if !errors.Is(err, tc.want) || driver.calls != 0 {
				t.Fatalf("err=%v calls=%d", err, driver.calls)
			}
		})
	}
}
