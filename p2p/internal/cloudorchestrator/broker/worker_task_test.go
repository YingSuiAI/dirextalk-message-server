package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewWorkerTaskIssueCommandUsesCanonicalNoApprovalEnvelope(t *testing.T) {
	command := testWorkerTaskIssueCommand(t)
	const wantPayload = `{"schema":"dirextalk.worker-task-issue/v1","deployment_id":"deployment-task-0001","task_id":"task-probe-0001","task_kind":"execution_probe","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`
	const wantRequestSHA256 = "423b1a0837f5440ce9760dac3f71b9e604fe0d75f88b75a43ad47c43fb544585"
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		t.Fatalf("decode issue payload: %v", err)
	}
	if got := string(payload); got != wantPayload {
		t.Fatalf("fixed issue payload differs\n got: %s\nwant: %s", got, wantPayload)
	}
	if got := command.SignatureBase(); !bytes.HasSuffix([]byte(got), []byte("approval_binding_sha256=\napproval_challenge_id=\napproval_signature_sha256=\napproval_proof_payload_sha256=\n")) {
		t.Fatalf("issue command omitted required empty approval digest lines: %q", got)
	}
	if got := command.RequestSHA256(); got != wantRequestSHA256 {
		t.Fatalf("issue request SHA-256 = %q, want %q", got, wantRequestSHA256)
	}
	seed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	if err := command.VerifySignature(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("verify issue signature: %v", err)
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseWorkerTaskIssueCommand(raw); err != nil {
		t.Fatalf("ParseWorkerTaskIssueCommand canonical envelope: %v", err)
	}
	if _, err := ParseWorkerTaskIssueCommand(bytes.Replace(raw, []byte(`"signature_b64"`), []byte(`"approval_proof"`), 1)); err == nil {
		t.Fatal("worker.task.issue accepted an approval proof field")
	}
}

func TestNewWorkerTaskObserveCommandUsesCanonicalNoApprovalEnvelope(t *testing.T) {
	command := testWorkerTaskObserveCommand(t)
	const wantPayload = `{"deployment_id":"deployment-task-0001","task_id":"task-probe-0001"}`
	const wantRequestSHA256 = "a441a960d89f60f7ea320e6660e38711a8245d37e22fdf69fbc95ed254c91836"
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		t.Fatalf("decode observe payload: %v", err)
	}
	if got := string(payload); got != wantPayload {
		t.Fatalf("fixed observe payload differs\n got: %s\nwant: %s", got, wantPayload)
	}
	if got := command.RequestSHA256(); got != wantRequestSHA256 {
		t.Fatalf("observe request SHA-256 = %q, want %q", got, wantRequestSHA256)
	}
	seed := bytes.Repeat([]byte{0x32}, ed25519.SeedSize)
	if err := command.VerifySignature(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("verify observe signature: %v", err)
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseWorkerTaskObserveCommand(raw); err != nil {
		t.Fatalf("ParseWorkerTaskObserveCommand canonical envelope: %v", err)
	}
	if _, err := ParseWorkerTaskObserveCommand(bytes.Replace(raw, []byte(`"signature_b64"`), []byte(`"approval_proof"`), 1)); err == nil {
		t.Fatal("worker.task.observe accepted an approval proof field")
	}
}

func TestWorkerTaskResultsRequireBoundDeSecretedSummary(t *testing.T) {
	issue := testWorkerTaskIssueCommand(t)
	issued := validWorkerTaskIssueResult(issue)
	if err := ValidateWorkerTaskIssueResult(issue, issued); err != nil {
		t.Fatalf("valid issued task: %v", err)
	}
	issued.Status = "idempotent"
	issued.Receipt.Disposition = "idempotent"
	if err := ValidateWorkerTaskIssueResult(issue, issued); err != nil {
		t.Fatalf("valid idempotent issued task: %v", err)
	}
	issued.Task.Status = "running"
	issued.Task.LastSequence = 1
	issuedCheckpoint := WorkerTaskExecutionProbeReceivedCheckpoint
	issuedEvidence := namedDigest('c')
	issued.Task.Checkpoint = &issuedCheckpoint
	issued.Task.EvidenceDigest = &issuedEvidence
	if err := ValidateWorkerTaskIssueResult(issue, issued); err == nil {
		t.Fatal("worker.task.issue accepted progress evidence for another execution manifest")
	}

	observe := testWorkerTaskObserveCommand(t)
	observed := validWorkerTaskObserveResult(observe)
	if err := ValidateWorkerTaskObserveResult(observe, observed); err != nil {
		t.Fatalf("valid observed task: %v", err)
	}
	observed.Task.ErrorCode = stringPointer("worker_failed")
	if err := ValidateWorkerTaskObserveResult(observe, observed); err == nil {
		t.Fatal("running task observation accepted an error_code")
	}
	observed = validWorkerTaskObserveResult(observe)
	unsupportedCheckpoint := "execution_probe_started"
	observed.Task.Checkpoint = &unsupportedCheckpoint
	if err := ValidateWorkerTaskObserveResult(observe, observed); err == nil {
		t.Fatal("running task observation accepted an arbitrary probe checkpoint")
	}
	observed = validWorkerTaskObserveResult(observe)
	failureCode := "worker_failed"
	observed.Task.Status = "failed"
	observed.Task.LastSequence = 0
	observed.Task.Checkpoint = nil
	observed.Task.ErrorCode = &failureCode
	observed.Task.EvidenceDigest = nil
	if err := ValidateWorkerTaskObserveResult(observe, observed); err == nil {
		t.Fatal("terminal task observation accepted a zero sequence")
	}

	raw, err := json.Marshal(validWorkerTaskObserveResult(observe))
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(raw, []byte(`"updated_at":`), []byte(`"worker_token":"not-allowed","updated_at":`), 1)
	if _, err := decodeWorkerTaskObserveResultJSON(mutated); err == nil {
		t.Fatal("worker task response accepted a Worker secret field")
	}
}

func TestWorkerTaskResultsRejectReceiptAndTaskBindingDrift(t *testing.T) {
	issue := testWorkerTaskIssueCommand(t)
	issued := validWorkerTaskIssueResult(issue)
	issued.Receipt.Action = WorkerTaskObserveAction
	if err := ValidateWorkerTaskIssueResult(issue, issued); err == nil {
		t.Fatal("worker.task.issue accepted a receipt for another action")
	}

	observe := testWorkerTaskObserveCommand(t)
	observed := validWorkerTaskObserveResult(observe)
	observed.Task.TaskID = "task-probe-other-0001"
	if err := ValidateWorkerTaskObserveResult(observe, observed); err == nil {
		t.Fatal("worker.task.observe accepted a summary for another task")
	}
}

func TestClientSubmitsWorkerTaskCommandsOnlyWithBoundSummary(t *testing.T) {
	issue := testWorkerTaskIssueCommand(t)
	observe := testWorkerTaskObserveCommand(t)
	issued := validWorkerTaskIssueResult(issue)
	observed := validWorkerTaskObserveResult(observe)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/prod/v2/commands" || request.TLS == nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var envelope struct {
			Action string `json:"action"`
		}
		if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		switch envelope.Action {
		case WorkerTaskIssueAction:
			_ = json.NewEncoder(writer).Encode(issued)
		case WorkerTaskObserveAction:
			_ = json.NewEncoder(writer).Encode(observed)
		default:
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server, DefaultMaxResponseBytes)
	gotIssued, err := client.SubmitWorkerTaskIssue(t.Context(), issue)
	if err != nil {
		t.Fatalf("SubmitWorkerTaskIssue: %v", err)
	}
	if gotIssued.Status != "worker_task_issued" || gotIssued.Task.Status != "queued" || gotIssued.Receipt.RequestSHA256 != issue.RequestSHA256() {
		t.Fatalf("unexpected validated issued task: %#v", gotIssued)
	}
	gotObserved, err := client.SubmitWorkerTaskObserve(t.Context(), observe)
	if err != nil {
		t.Fatalf("SubmitWorkerTaskObserve: %v", err)
	}
	if gotObserved.Status != "worker_task_observed" || gotObserved.Task.Status != "running" || gotObserved.Receipt.RequestSHA256 != observe.RequestSHA256() {
		t.Fatalf("unexpected validated observed task: %#v", gotObserved)
	}
}

func testWorkerTaskIssueCommand(t *testing.T) WorkerTaskIssueCommand {
	t.Helper()
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	seed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	command, err := NewWorkerTaskIssueCommand(WorkerTaskIssueCommandInput{
		ConnectionID: "connection-task-0001", CommandID: "command-task-issue-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: 2, NodeCounter: 11, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute),
		Request: WorkerTaskIssueRequest{
			Schema: WorkerTaskIssueSchema, DeploymentID: "deployment-task-0001", TaskID: "task-probe-0001", TaskKind: WorkerTaskKindExecutionProbe,
			ExecutionManifestDigest: namedDigest('a'), InputDigest: namedDigest('b'),
		},
		PrivateKey: ed25519.NewKeyFromSeed(seed),
	})
	if err != nil {
		t.Fatalf("NewWorkerTaskIssueCommand: %v", err)
	}
	return command
}

func testWorkerTaskObserveCommand(t *testing.T) WorkerTaskObserveCommand {
	t.Helper()
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	seed := bytes.Repeat([]byte{0x32}, ed25519.SeedSize)
	command, err := NewWorkerTaskObserveCommand(WorkerTaskObserveCommandInput{
		ConnectionID: "connection-task-0001", CommandID: "command-task-observe-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: 2, NodeCounter: 12, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute),
		Request:    WorkerTaskObserveRequest{DeploymentID: "deployment-task-0001", TaskID: "task-probe-0001"},
		PrivateKey: ed25519.NewKeyFromSeed(seed),
	})
	if err != nil {
		t.Fatalf("NewWorkerTaskObserveCommand: %v", err)
	}
	return command
}

func validWorkerTaskIssueResult(command WorkerTaskIssueCommand) WorkerTaskIssueResult {
	issuedAt, _ := parseCanonicalInstant(command.IssuedAt)
	updatedAt := canonicalInstant(issuedAt.Add(15 * time.Second))
	return WorkerTaskIssueResult{
		Status: "worker_task_issued",
		Receipt: WorkerTaskReceipt{
			Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID,
			RequestSHA256: command.RequestSHA256(), Action: WorkerTaskIssueAction,
		},
		Task: WorkerTaskSummary{
			TaskID: "task-probe-0001", DeploymentID: "deployment-task-0001", Status: "queued", Attempt: 1, LastSequence: 0,
			Checkpoint: nil, ErrorCode: nil, EvidenceDigest: nil, UpdatedAt: updatedAt,
		},
	}
}

func validWorkerTaskObserveResult(command WorkerTaskObserveCommand) WorkerTaskObserveResult {
	issuedAt, _ := parseCanonicalInstant(command.IssuedAt)
	updatedAt := canonicalInstant(issuedAt.Add(15 * time.Second))
	checkpoint := WorkerTaskExecutionProbeReceivedCheckpoint
	evidenceDigest := namedDigest('a')
	return WorkerTaskObserveResult{
		Status: "worker_task_observed",
		Receipt: WorkerTaskReceipt{
			Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID,
			RequestSHA256: command.RequestSHA256(), Action: WorkerTaskObserveAction,
		},
		Task: WorkerTaskSummary{
			TaskID: "task-probe-0001", DeploymentID: "deployment-task-0001", Status: "running", Attempt: 1, LastSequence: 1,
			Checkpoint: &checkpoint, ErrorCode: nil, EvidenceDigest: &evidenceDigest, UpdatedAt: updatedAt,
		},
	}
}
