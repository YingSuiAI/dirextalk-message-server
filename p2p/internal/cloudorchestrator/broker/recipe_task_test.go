package broker

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestRecipeTaskManifestDigestGolden(t *testing.T) {
	manifest := cloudcontracts.RecipeExecutionManifestV1{
		SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema,
		ExecutionID:   "execution-recipe-0001", DeploymentID: "deployment-recipe-0001", PlanID: "plan-recipe-0001",
		PlanHash: namedDigest('1'), PlanRevision: 2, RecipeDigest: namedDigest('2'), WorkerResourceManifestDigest: namedDigest('3'), ArtifactDigest: namedDigest('4'),
		ActionID: "install_service", RootRequired: true, TimeoutSeconds: 900, CheckpointSequence: []string{"artifact_verified", "health_verified"},
		SemanticReadiness: cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 18080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: namedDigest('6')},
	}
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	const expected = "sha256:c09697dc5ab63148622c4e0a4b0a6dc641ef421ee13d8528dbf30e11138d13f5"
	if digest != expected {
		t.Fatalf("recipe manifest digest=%q", digest)
	}
}

func TestRecipeTaskResultStrictlyMatchesConnectionStackContract(t *testing.T) {
	issue := testRecipeTaskCommand(t, RecipeTaskIssueAction)
	issued := validRecipeTaskResult(issue, "recipe_task_issued", "committed", "queued")
	if err := ValidateRecipeTaskResult(issue, issued); err != nil {
		t.Fatalf("valid issue result: %v", err)
	}
	idempotent := issued
	idempotent.Status, idempotent.Receipt.Disposition = "idempotent", "idempotent"
	if err := ValidateRecipeTaskResult(issue, idempotent); err != nil {
		t.Fatalf("valid idempotent issue result: %v", err)
	}

	issueRequest := recipeTaskIssueRequest(t, issue)
	running := validRecipeTaskResult(issue, "recipe_task_issued", "committed", "running")
	running.Task.LastSequence = 1
	running.Task.LastCheckpoint = issueRequest.CheckpointSequence[0]
	running.Task.EvidenceDigest = &issueRequest.ManifestDigest
	if err := ValidateRecipeTaskResult(issue, running); err != nil {
		t.Fatalf("valid running issue result: %v", err)
	}

	mutations := []struct {
		name   string
		mutate func(*RecipeTaskResult)
	}{
		{"status", func(r *RecipeTaskResult) { r.Status = "recipe_task_observed" }},
		{"receipt_schema", func(r *RecipeTaskResult) { r.Receipt.Schema = "wrong" }},
		{"disposition", func(r *RecipeTaskResult) { r.Receipt.Disposition = "idempotent" }},
		{"connection", func(r *RecipeTaskResult) { r.Receipt.ConnectionID = "connection-recipe-other" }},
		{"generation", func(r *RecipeTaskResult) { r.Receipt.ExpectedGeneration++ }},
		{"counter", func(r *RecipeTaskResult) { r.Receipt.NodeCounter++ }},
		{"command", func(r *RecipeTaskResult) { r.Receipt.CommandID = "command-recipe-other" }},
		{"request_hash", func(r *RecipeTaskResult) { r.Receipt.RequestSHA256 = string(bytes.Repeat([]byte{'0'}, 64)) }},
		{"action", func(r *RecipeTaskResult) { r.Receipt.Action = RecipeTaskObserveAction }},
		{"task", func(r *RecipeTaskResult) { r.Task.TaskID = "recipe-task-other" }},
		{"execution", func(r *RecipeTaskResult) { r.Task.ExecutionID = "execution-recipe-other" }},
		{"deployment", func(r *RecipeTaskResult) { r.Task.DeploymentID = "deployment-recipe-other" }},
		{"time", func(r *RecipeTaskResult) { r.Task.UpdatedAt = "2026-07-15T10:00:00Z" }},
		{"task_status", func(r *RecipeTaskResult) { r.Task.Status = "unknown" }},
		{"error", func(r *RecipeTaskResult) { value := "worker_failed"; r.Task.ErrorCode = &value }},
		{"evidence", func(r *RecipeTaskResult) { value := namedDigest('e'); r.Task.EvidenceDigest = &value }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			candidate := issued
			test.mutate(&candidate)
			if err := ValidateRecipeTaskResult(issue, candidate); err == nil {
				t.Fatal("accepted result with broken binding")
			}
		})
	}
	wrongEvidence := running
	wrong := namedDigest('f')
	wrongEvidence.Task.EvidenceDigest = &wrong
	if err := ValidateRecipeTaskResult(issue, wrongEvidence); err == nil {
		t.Fatal("issue result accepted evidence for another manifest")
	}

	observe := testRecipeTaskCommand(t, RecipeTaskObserveAction)
	observed := validRecipeTaskResult(observe, "recipe_task_observed", "committed", "running")
	observed.Task.LastSequence = 2
	observed.Task.LastCheckpoint = "artifact_verified"
	evidence := namedDigest('e')
	observed.Task.EvidenceDigest = &evidence
	if err := ValidateRecipeTaskResult(observe, observed); err != nil {
		t.Fatalf("valid observe result: %v", err)
	}
	observed.Status, observed.Receipt.Disposition = "idempotent", "idempotent"
	if err := ValidateRecipeTaskResult(observe, observed); err != nil {
		t.Fatalf("valid idempotent observe result: %v", err)
	}
}

func TestClientAcceptsExactConnectionStackRecipeTaskJSON(t *testing.T) {
	command := testRecipeTaskCommand(t, RecipeTaskIssueAction)
	request := recipeTaskIssueRequest(t, command)
	response := fmt.Sprintf(`{"status":"recipe_task_issued","receipt":{"schema":"dirextalk.aws.command-receipt/v2","disposition":"committed","connection_id":"%s","expected_generation":%d,"node_counter":%d,"command_id":"%s","request_sha256":"%s","action":"worker.recipe_task.issue"},"task":{"task_id":"%s","execution_id":"%s","deployment_id":"%s","status":"queued","attempt":1,"last_sequence":0,"last_checkpoint":"","error_code":null,"evidence_digest":null,"updated_at":"2026-07-15T10:00:15.000Z"}}`,
		command.ConnectionID, command.ExpectedGeneration, command.NodeCounter, command.CommandID, command.RequestSHA256(), request.TaskID, request.ExecutionID, request.DeploymentID)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(response))
	}))
	defer server.Close()
	client := newTestClient(t, server, DefaultMaxResponseBytes)
	result, err := client.SubmitRecipeTask(t.Context(), command)
	if err != nil || result.Task.Status != "queued" || result.Receipt.RequestSHA256 != command.RequestSHA256() {
		t.Fatalf("SubmitRecipeTask() = (%#v, %v)", result, err)
	}
}

func testRecipeTaskCommand(t *testing.T, action string) RecipeTaskCommand {
	t.Helper()
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	input := RecipeTaskCommandInput{
		ConnectionID: "connection-recipe-0001", CommandID: "command-recipe-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: 2, NodeCounter: 17, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Action: action,
		PrivateKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x41}, ed25519.SeedSize)),
	}
	if action == RecipeTaskIssueAction {
		manifest := cloudcontracts.RecipeExecutionManifestV1{
			SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema,
			ExecutionID:   "execution-recipe-0001", DeploymentID: "deployment-recipe-0001", PlanID: "plan-recipe-0001",
			PlanHash: namedDigest('1'), PlanRevision: 2, RecipeDigest: namedDigest('2'), WorkerResourceManifestDigest: namedDigest('3'), ArtifactDigest: namedDigest('4'),
			ActionID: "install_service", RootRequired: true, TimeoutSeconds: 900, CheckpointSequence: []string{"artifact_verified", "health_verified"},
			SemanticReadiness: cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 18080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: namedDigest('6')},
		}
		digest, err := manifest.Digest()
		if err != nil {
			t.Fatal(err)
		}
		input.Issue = RecipeTaskIssueRequest{Schema: RecipeTaskIssueSchema, ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID,
			TaskID: "recipe-task-0001", TaskKind: "recipe_execution", ManifestDigest: digest, InputDigest: namedDigest('5'),
			CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Manifest: manifest}
	} else {
		input.CommandID = "command-recipe-observe-0001"
		input.NodeCounter = 18
		input.Observe = RecipeTaskObserveRequest{DeploymentID: "deployment-recipe-0001", TaskID: "recipe-task-0001"}
	}
	command, err := NewRecipeTaskCommand(input)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func recipeTaskIssueRequest(t *testing.T, command RecipeTaskCommand) RecipeTaskIssueRequest {
	t.Helper()
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil {
		t.Fatal(err)
	}
	var request RecipeTaskIssueRequest
	if err := decodeStrictJSON(payload, &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func validRecipeTaskResult(command RecipeTaskCommand, status, disposition, taskStatus string) RecipeTaskResult {
	deploymentID, taskID, executionID := "deployment-recipe-0001", "recipe-task-0001", "execution-recipe-0001"
	return RecipeTaskResult{
		Status: status,
		Receipt: RecipeTaskReceipt{Schema: ReceiptSchema, Disposition: disposition, ConnectionID: command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID,
			RequestSHA256: command.RequestSHA256(), Action: command.Action},
		Task: RecipeTaskSummary{TaskID: taskID, ExecutionID: executionID, DeploymentID: deploymentID, Status: taskStatus,
			Attempt: 1, UpdatedAt: "2026-07-15T10:00:15.000Z"},
	}
}
