package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRecipeTaskManifestDigestMatchesOrchestratorGolden(t *testing.T) {
	manifest := RecipeExecutionManifestV1{
		SchemaVersion: RecipeExecutionManifestSchema,
		ExecutionID:   "execution-recipe-0001", DeploymentID: "deployment-recipe-0001", PlanID: "plan-recipe-0001",
		PlanHash: "sha256:" + strings.Repeat("1", 64), PlanRevision: 2,
		RecipeDigest: "sha256:" + strings.Repeat("2", 64), WorkerResourceManifestDigest: "sha256:" + strings.Repeat("3", 64), ArtifactDigest: "sha256:" + strings.Repeat("4", 64),
		ActionID: "install_service", RootRequired: true, TimeoutSeconds: 900, CheckpointSequence: []string{"artifact_verified", "health_verified"},
	}
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	const expected = "sha256:bcf144312bae6b7722b27e6b8cc397a720ae2f5b47b275f5b9b2012deeef0514"
	if digest != expected {
		t.Fatalf("recipe manifest digest=%q", digest)
	}
}

const recipeManifestDigestFixture = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRecipeTaskGoldenMatchesRootRecipeexecTaskV1AndEventV1(t *testing.T) {
	taskJSON := `{"schema":"dirextalk.recipe-execution-task/v1","task_id":"recipe-task-0001","execution_id":"execution:0001","deployment_id":"deployment-0001","task_kind":"recipe_execution","recipe_execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","checkpoint_sequence":["artifact_verified","install_complete","health_verified"],"last_checkpoint":"install_complete","attempt":2,"last_sequence":5}`
	task, err := ParseRecipeTaskV1([]byte(taskJSON))
	if err != nil {
		t.Fatalf("ParseRecipeTaskV1() error = %v", err)
	}
	encoded, err := json.Marshal(task)
	if err != nil || string(encoded) != taskJSON {
		t.Fatalf("task golden = %s, error = %v", encoded, err)
	}

	eventJSON := `{"schema":"dirextalk.recipe-execution-task-event/v1","task_id":"recipe-task-0001","attempt":2,"lease_epoch":7,"sequence":6,"status":"succeeded","checkpoint":"health_verified","error_code":null,"evidence_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","occurred_at":"2026-07-15T02:00:00.000Z"}`
	event, err := ParseRecipeTaskEventV1([]byte(eventJSON))
	if err != nil {
		t.Fatalf("ParseRecipeTaskEventV1() error = %v", err)
	}
	encoded, err = json.Marshal(event)
	if err != nil || string(encoded) != eventJSON {
		t.Fatalf("event golden = %s, error = %v", encoded, err)
	}
	if err := ValidateRecipeTaskAdvance(task, "running", event, 7); err != nil {
		t.Fatalf("ValidateRecipeTaskAdvance() error = %v", err)
	}
}

func TestRecipeTaskIssueIsCanonicalDigestOnlyAndCheckpointBound(t *testing.T) {
	manifest := recipeManifestFixture(t)
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	requestFixture := RecipeTaskIssueRequest{Schema: RecipeTaskIssueSchema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: RecipeTaskKindExecution, RecipeExecutionManifestDigest: digest, InputDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Manifest: manifest}
	raw, err := json.Marshal(requestFixture)
	if err != nil {
		t.Fatal(err)
	}
	request, err := ParseRecipeTaskIssueRequest(raw)
	if err != nil {
		t.Fatalf("ParseRecipeTaskIssueRequest() error = %v", err)
	}
	task, err := NewRecipeTaskV1(request)
	if err != nil || task.LastCheckpoint != "" || task.Attempt != 1 || task.LastSequence != 0 {
		t.Fatalf("NewRecipeTaskV1() = (%#v, %v)", task, err)
	}
	base := strings.TrimSuffix(string(raw), "}")
	for _, forbidden := range []string{
		`,"command":"curl example.invalid"}`,
		`,"url":"https://example.invalid"}`,
		`,"path":"/root/secret"}`,
		`,"log":"sensitive"}`,
		`,"secret_ref":"secret-1"}`,
	} {
		if _, err := ParseRecipeTaskIssueRequest([]byte(base + forbidden)); err == nil {
			t.Fatalf("accepted forbidden field %s", forbidden)
		}
	}
	duplicate := strings.Replace(string(raw), `"checkpoint_sequence":["artifact_verified","install_complete","health_verified"]`, `"checkpoint_sequence":["artifact_verified","artifact_verified"]`, 1)
	if _, err := ParseRecipeTaskIssueRequest([]byte(duplicate)); err == nil {
		t.Fatal("accepted duplicate checkpoint")
	}
}

func TestRecipeTaskProgressEnforcesOrderTerminalAndExactSequence(t *testing.T) {
	task := RecipeTaskV1{Schema: RecipeTaskV1Schema, TaskID: "recipe-task-0001", ExecutionID: "execution:0001", DeploymentID: "deployment-0001", TaskKind: RecipeTaskKindExecution,
		RecipeExecutionManifestDigest: recipeManifestDigestFixture, InputDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		CheckpointSequence: []string{"artifact_verified", "install_complete", "health_verified"}, Attempt: 1}
	checkpoint := func(value string) *string { return &value }
	evidence := checkpoint(recipeManifestDigestFixture)
	event := RecipeTaskEventV1{Schema: RecipeTaskEventV1Schema, TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 4, Sequence: 1, Status: "running", Checkpoint: checkpoint("artifact_verified"), EvidenceDigest: evidence, OccurredAt: "2026-07-15T02:00:00.000Z"}
	if err := ValidateRecipeTaskAdvance(task, "queued", event, 4); err != nil {
		t.Fatalf("first checkpoint error = %v", err)
	}
	wrong := event
	wrong.Sequence = 2
	if err := ValidateRecipeTaskAdvance(task, "queued", wrong, 4); err == nil {
		t.Fatal("accepted sequence gap")
	}
	wrong = event
	wrong.Checkpoint = checkpoint("install_complete")
	if err := ValidateRecipeTaskAdvance(task, "queued", wrong, 4); err == nil {
		t.Fatal("accepted checkpoint skip")
	}
	wrong = event
	wrong.Status = "succeeded"
	if err := ValidateRecipeTaskAdvance(task, "queued", wrong, 4); err == nil {
		t.Fatal("accepted early success")
	}
	wrong = event
	wrong.EvidenceDigest = checkpoint("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	if err := ValidateRecipeTaskAdvance(task, "queued", wrong, 4); err == nil {
		t.Fatal("accepted wrong manifest evidence")
	}
}

func TestRecipeTaskClaimAndReceiptUseSeparateSchemas(t *testing.T) {
	claim, err := ParseRecipeTaskClaimRequest([]byte(`{"schema":"dirextalk.recipe-execution-task-claim/v1","lease_epoch":3}`))
	if err != nil || claim.LeaseEpoch != 3 {
		t.Fatalf("ParseRecipeTaskClaimRequest() = (%#v, %v)", claim, err)
	}
	response, err := MarshalRecipeTaskClaimResponse(3, nil, nil)
	if err != nil || string(response) != `{"schema":"dirextalk.recipe-execution-task-claim-response/v1","status":"none","lease_epoch":3}` {
		t.Fatalf("claim response = %s, %v", response, err)
	}
	event := RecipeTaskEventV1{Schema: RecipeTaskEventV1Schema, TaskID: "recipe-task-0001", Attempt: 1, LeaseEpoch: 3, Sequence: 1, Status: "failed", ErrorCode: func() *string { value := "driver_failed"; return &value }(), OccurredAt: "2026-07-15T02:00:00.000Z"}
	receipt, err := NewRecipeTaskEventReceipt(event, true)
	if err != nil || receipt.Schema != RecipeTaskEventReceiptSchema || receipt.Disposition != "idempotent" {
		t.Fatalf("NewRecipeTaskEventReceipt() = (%#v, %v)", receipt, err)
	}
}

func recipeManifestFixture(t *testing.T) RecipeExecutionManifestV1 {
	t.Helper()
	return RecipeExecutionManifestV1{SchemaVersion: RecipeExecutionManifestSchema, ExecutionID: "execution:0001", DeploymentID: "deployment-0001", PlanID: "plan-00000001", PlanHash: "sha256:1111111111111111111111111111111111111111111111111111111111111111", PlanRevision: 2, RecipeDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222", WorkerResourceManifestDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333", ArtifactDigest: "sha256:4444444444444444444444444444444444444444444444444444444444444444", ActionID: "install_service", RootRequired: true, TimeoutSeconds: 900, CheckpointSequence: []string{"artifact_verified", "install_complete", "health_verified"}, VolumeSlots: []RecipeVolumeSlotV1{{SlotID: "data_volume", VolumeRef: "volume_ref:data-001", ReadOnly: false}}, DataSlots: []RecipeDataSlotV1{{SlotID: "knowledge", DataRef: "data_ref:knowledge-001", ReadOnly: true}}, SecretSlots: []RecipeSecretSlotV1{{SlotID: "model_token", SecretRef: "secret_ref:model-token-001"}}}
}
