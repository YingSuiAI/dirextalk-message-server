package broker

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServiceReadinessResultStrictlyBindsReceiptAndEvidence(t *testing.T) {
	command := readinessCommand(t, ServiceReadinessIssueAction)
	request := readinessIssueRequest(t, command)
	queued := readinessResult(command, "service_readiness_issued", "committed", "queued")
	if err := ValidateServiceReadinessResult(command, queued); err != nil {
		t.Fatal(err)
	}
	running := readinessResult(command, "service_readiness_issued", "committed", "running")
	challenge := namedDigest('d')
	running.Task.Checkpoint = "challenge_issued"
	running.Task.ChallengeDigest = &challenge
	if err := ValidateServiceReadinessResult(command, running); err != nil {
		t.Fatalf("running sequence zero: %v", err)
	}
	succeeded := readinessResult(command, "service_readiness_issued", "committed", "succeeded")
	stack := namedDigest('e')
	succeeded.Task.LastSequence = 1
	succeeded.Task.Checkpoint = "readiness_verified"
	succeeded.Task.ChallengeDigest = &challenge
	succeeded.Task.SemanticEvidenceDigest = &request.SemanticExpectationDigest
	succeeded.Task.StackObservationDigest = &stack
	if err := ValidateServiceReadinessResult(command, succeeded); err != nil {
		t.Fatal(err)
	}
	broken := succeeded
	broken.Receipt.NodeCounter++
	if err := ValidateServiceReadinessResult(command, broken); err == nil {
		t.Fatal("accepted another command receipt")
	}
	broken = succeeded
	broken.Task.StackObservationDigest = nil
	if err := ValidateServiceReadinessResult(command, broken); err == nil {
		t.Fatal("accepted Worker-local evidence without Stack observation")
	}
}

func TestServiceReadinessClientRequiresExplicitNullableFields(t *testing.T) {
	command := readinessCommand(t, ServiceReadinessIssueAction)
	request := readinessIssueRequest(t, command)
	exact := fmt.Sprintf(`{"status":"service_readiness_issued","receipt":{"schema":"dirextalk.aws.command-receipt/v2","disposition":"committed","connection_id":"%s","expected_generation":%d,"node_counter":%d,"command_id":"%s","request_sha256":"%s","action":"worker.service_readiness.issue"},"task":{"task_id":"%s","execution_id":"%s","deployment_id":"%s","service_id":"%s","status":"queued","attempt":1,"last_sequence":0,"checkpoint":"","challenge_digest":null,"semantic_evidence_digest":null,"stack_observation_digest":null,"error_code":null,"updated_at":"2026-07-15T10:00:15.000Z"}}`, command.ConnectionID, command.ExpectedGeneration, command.NodeCounter, command.CommandID, command.RequestSHA256(), request.TaskID, request.ExecutionID, request.DeploymentID, request.ServiceID)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(exact))
	}))
	defer server.Close()
	client := newTestClient(t, server, DefaultMaxResponseBytes)
	if _, err := client.SubmitServiceReadiness(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	missing := bytes.Replace([]byte(exact), []byte(`,"stack_observation_digest":null`), nil, 1)
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(missing)
	})
	if _, err := client.SubmitServiceReadiness(t.Context(), command); err == nil {
		t.Fatal("accepted response without explicit nullable Stack digest")
	}
}

func readinessCommand(t *testing.T, action string) ServiceReadinessCommand {
	t.Helper()
	now := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	input := ServiceReadinessCommandInput{ConnectionID: "connection-ready-0001", CommandID: "command-ready-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2, NodeCounter: 17, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Action: action, PrivateKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))}
	if action == ServiceReadinessIssueAction {
		input.Issue = ServiceReadinessIssueRequest{Schema: ServiceReadinessIssueSchema, ExecutionID: "execution-ready-0001", DeploymentID: "deployment-ready-0001", ServiceID: "service-ready-0001", TaskID: "readiness-task-0001", ProbeKind: ServiceReadinessProbeKind, RecipeExecutionManifestDigest: namedDigest('a'), InstallEvidenceDigest: namedDigest('b'), SemanticExpectationDigest: namedDigest('c')}
	} else {
		input.CommandID = "command-ready-observe-0001"
		input.NodeCounter = 18
		input.Observe = ServiceReadinessObserveRequest{DeploymentID: "deployment-ready-0001", ServiceID: "service-ready-0001", TaskID: "readiness-task-0001"}
	}
	c, err := NewServiceReadinessCommand(input)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func readinessIssueRequest(t *testing.T, c ServiceReadinessCommand) ServiceReadinessIssueRequest {
	t.Helper()
	raw, err := decodeCanonicalBase64(c.PayloadB64)
	if err != nil {
		t.Fatal(err)
	}
	var r ServiceReadinessIssueRequest
	if err = decodeStrictJSON(raw, &r); err != nil {
		t.Fatal(err)
	}
	return r
}
func readinessResult(c ServiceReadinessCommand, status, disposition, taskStatus string) ServiceReadinessResult {
	return ServiceReadinessResult{Status: status, Receipt: RecipeTaskReceipt{Schema: ReceiptSchema, Disposition: disposition, ConnectionID: c.ConnectionID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, CommandID: c.CommandID, RequestSHA256: c.RequestSHA256(), Action: c.Action}, Task: ServiceReadinessSummary{ExecutionID: "execution-ready-0001", DeploymentID: "deployment-ready-0001", ServiceID: "service-ready-0001", TaskID: "readiness-task-0001", Status: taskStatus, Attempt: 1, UpdatedAt: "2026-07-15T10:00:15.000Z"}}
}
