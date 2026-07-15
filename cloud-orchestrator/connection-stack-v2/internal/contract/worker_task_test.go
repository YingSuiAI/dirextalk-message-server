package contract

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestWorkerTaskCommandsMatchOrchestratorGoldens(t *testing.T) {
	issuePayload := `{"schema":"dirextalk.worker-task-issue/v1","deployment_id":"deployment-task-0001","task_id":"task-probe-0001","task_kind":"execution_probe","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`
	issue := signedWorkerTaskCommand(t, ActionWorkerTaskIssue, issuePayload, "command-task-issue-0001", 11, 0x31)
	if request, err := issue.WorkerTaskIssueRequest(); err != nil || request.TaskKind != WorkerTaskKindExecutionProbe {
		t.Fatalf("WorkerTaskIssueRequest() = %#v, %v", request, err)
	}
	if got, _ := issue.RequestSHA256(); got != "423b1a0837f5440ce9760dac3f71b9e604fe0d75f88b75a43ad47c43fb544585" {
		t.Fatalf("issue request hash = %q", got)
	}

	observePayload := `{"deployment_id":"deployment-task-0001","task_id":"task-probe-0001"}`
	observe := signedWorkerTaskCommand(t, ActionWorkerTaskObserve, observePayload, "command-task-observe-0001", 12, 0x32)
	if request, err := observe.WorkerTaskObserveRequest(); err != nil || request.TaskID != "task-probe-0001" {
		t.Fatalf("WorkerTaskObserveRequest() = %#v, %v", request, err)
	}
	if got, _ := observe.RequestSHA256(); got != "a441a960d89f60f7ea320e6660e38711a8245d37e22fdf69fbc95ed254c91836" {
		t.Fatalf("observe request hash = %q", got)
	}
}

func TestWorkerTaskClaimAndEventMatchCloudWorker(t *testing.T) {
	claim, err := ParseWorkerTaskClaimRequest([]byte(`{"schema":"dirextalk.worker-task-claim/v1","lease_epoch":7}`))
	if err != nil || claim.LeaseEpoch != 7 {
		t.Fatalf("ParseWorkerTaskClaimRequest() = %#v, %v", claim, err)
	}
	request := WorkerTaskIssueRequest{Schema: WorkerTaskIssueSchema, DeploymentID: "deployment-task-0001", TaskID: "task-probe-0001",
		TaskKind: WorkerTaskKindExecutionProbe, ExecutionManifestDigest: namedTaskDigest('a'), InputDigest: namedTaskDigest('b')}
	task, err := NewWorkerTask(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := MarshalWorkerTaskClaimResponse(7, &task)
	if err != nil {
		t.Fatal(err)
	}
	const wantResponse = `{"schema":"dirextalk.worker-task-claim-response/v1","status":"claimed","lease_epoch":7,"task":{"schema":"dirextalk.worker-task/v1","task_id":"task-probe-0001","deployment_id":"deployment-task-0001","task_kind":"execution_probe","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","attempt":1,"last_sequence":0}}`
	if string(response) != wantResponse {
		t.Fatalf("claim response\n got: %s\nwant: %s", response, wantResponse)
	}

	eventRaw := []byte(`{"schema":"dirextalk.worker-task-event/v1","task_id":"task-probe-0001","attempt":1,"lease_epoch":7,"sequence":1,"status":"running","checkpoint":"execution_manifest_received","error_code":null,"evidence_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","occurred_at":"2026-07-15T10:01:00.000Z"}`)
	event, err := ParseWorkerTaskEvent(eventRaw)
	if err != nil || event.ValidateFor(task) != nil {
		t.Fatalf("task event = %#v, parse %v validate %v", event, err, event.ValidateFor(task))
	}
	receipt, err := NewWorkerTaskEventReceipt(event, false)
	if err != nil || receipt.Disposition != "accepted" || receipt.Sequence != 1 {
		t.Fatalf("task event receipt = %#v, %v", receipt, err)
	}
}

func TestWorkerTaskWireRejectsArbitraryExecutionMaterial(t *testing.T) {
	for _, raw := range []string{
		`{"schema":"dirextalk.worker-task-claim/v1","lease_epoch":7,"command":"curl example.invalid"}`,
		`{"schema":"dirextalk.worker-task-event/v1","task_id":"task-probe-0001","attempt":1,"lease_epoch":7,"sequence":1,"status":"running","checkpoint":"execution_manifest_received","error_code":null,"evidence_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","occurred_at":"2026-07-15T10:01:00.000Z","log":"secret"}`,
	} {
		if strings.Contains(raw, "task-claim") {
			if _, err := ParseWorkerTaskClaimRequest([]byte(raw)); err == nil {
				t.Fatalf("claim accepted %s", raw)
			}
		} else if _, err := ParseWorkerTaskEvent([]byte(raw)); err == nil {
			t.Fatalf("event accepted %s", raw)
		}
	}
	task := WorkerTask{Schema: WorkerTaskSchema, TaskID: "task-probe-0001", DeploymentID: "deployment-task-0001", TaskKind: WorkerTaskKindExecutionProbe,
		ExecutionManifestDigest: namedTaskDigest('a'), InputDigest: namedTaskDigest('b'), Attempt: 1}
	code := "probe_started"
	digest := namedTaskDigest('a')
	event := WorkerTaskEvent{Schema: WorkerTaskEventSchema, TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 7, Sequence: 1,
		Status: WorkerTaskStatusRunning, Checkpoint: &code, EvidenceDigest: &digest, OccurredAt: "2026-07-15T10:01:00.000Z"}
	if event.ValidateFor(task) == nil {
		t.Fatal("accepted arbitrary execution checkpoint")
	}
}

func TestWorkerHeartbeatWireIsHeartbeatOnly(t *testing.T) {
	raw := []byte(`{"schema":"dirextalk.worker-event/v1","connection_id":"connection-task-0001","deployment_id":"deployment-task-0001","bootstrap_session_id":"bootstrap-task-0001","lease_epoch":7,"sequence":2,"kind":"heartbeat","occurred_at":"2026-07-15T10:01:00.000Z"}`)
	event, err := ParseWorkerHeartbeatEvent(raw)
	if err != nil {
		t.Fatalf("ParseWorkerHeartbeatEvent(): %v", err)
	}
	receipt, err := MarshalWorkerHeartbeatEventReceipt(event, false)
	if err != nil || !bytes.Contains(receipt, []byte(`"disposition":"accepted"`)) {
		t.Fatalf("heartbeat receipt = %s, %v", receipt, err)
	}
	for _, expanded := range []string{
		strings.Replace(string(raw), `"kind":"heartbeat"`, `"kind":"checkpoint","checkpoint":"installed"`, 1),
		strings.Replace(string(raw), `"occurred_at":`, `"log":"secret","occurred_at":`, 1),
	} {
		if _, err := ParseWorkerHeartbeatEvent([]byte(expanded)); err == nil {
			t.Fatalf("heartbeat accepted expanded state: %s", expanded)
		}
	}
}

func TestWorkerTaskStoredResultRejectsNestedExpansion(t *testing.T) {
	command := signedWorkerTaskCommand(t, ActionWorkerTaskObserve,
		`{"deployment_id":"deployment-task-0001","task_id":"task-probe-0001"}`,
		"command-task-result-0001", 9, 5)
	summary := WorkerTaskSummary{TaskID: "task-probe-0001", DeploymentID: "deployment-task-0001", Status: "queued",
		Attempt: 1, UpdatedAt: "2026-07-15T10:01:00.000Z"}
	raw, err := MarshalWorkerTaskResult(command, summary, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeWorkerTaskResult(command, raw); err != nil {
		t.Fatalf("decode exact result: %v", err)
	}
	expanded := bytes.Replace(raw, []byte(`"action":"worker.task.observe"`), []byte(`"action":"worker.task.observe","secret":"forbidden"`), 1)
	if _, err := DecodeWorkerTaskResult(command, expanded); err == nil {
		t.Fatal("stored result accepted nested secret field")
	}
}

func signedWorkerTaskCommand(t *testing.T, action, payload, commandID string, counter int64, seedByte byte) Command {
	t.Helper()
	digest := sha256.Sum256([]byte(payload))
	command := Command{Schema: CommandSchema, ConnectionID: "connection-task-0001", CommandID: commandID, NodeKeyID: "node-key-1",
		IssuedAt: "2026-07-15T10:00:00.000Z", ExpiresAt: "2026-07-15T10:04:00.000Z", ExpectedGeneration: 2,
		NodeCounter: counter, Action: action, PayloadB64: base64.StdEncoding.EncodeToString([]byte(payload)), PayloadSHA256: hex.EncodeToString(digest[:])}
	base, err := command.SignatureBase()
	if err != nil {
		t.Fatal(err)
	}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seedByte}, ed25519.SeedSize))
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(key, []byte(base)))
	raw, _ := json.Marshal(command)
	parsed, err := Parse(raw)
	if err != nil || parsed.VerifyNodeSignature(key.Public().(ed25519.PublicKey)) != nil {
		t.Fatalf("signed task command: %v", err)
	}
	return parsed
}

func namedTaskDigest(value byte) string { return "sha256:" + strings.Repeat(string(value), 64) }
