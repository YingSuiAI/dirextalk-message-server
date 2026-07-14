package brokertransport

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestBuildExecutionProbeIssueCommandBindsExactDigestOnlyRequest(t *testing.T) {
	transport, now := testExecutionProbeTransport(t)
	request := testExecutionProbeIssueRequest()
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	command := runtime.ExecutionProbeCommand{
		CommandID: "command-probe-issue-0001", DeploymentID: request.DeploymentID, TaskID: request.TaskID,
		ConnectionID: "connection-probe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2,
		NodeCounter: 7, Attempt: 1, Action: runtime.ExecutionProbeIssueAction, RequestDigest: digest,
	}

	signed, err := transport.BuildExecutionProbeIssueCommand(command, request, now)
	if err != nil {
		t.Fatal(err)
	}
	if signed.RequestSHA256 == signed.PayloadSHA256 || len(signed.RequestSHA256) != 64 || signed.IssuedAt != now || signed.ExpiresAt != now.Add(commandLifetime) {
		t.Fatalf("signed execution probe issue command = %#v", signed)
	}
	parsed, err := broker.ParseWorkerTaskIssueCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnectionID != command.ConnectionID || parsed.CommandID != command.CommandID || parsed.NodeKeyID != command.NodeKeyID ||
		parsed.ExpectedGeneration != command.ExpectedGeneration || parsed.NodeCounter != command.NodeCounter ||
		parsed.RequestSHA256() != signed.RequestSHA256 || parsed.PayloadSHA256 != signed.PayloadSHA256 {
		t.Fatalf("parsed execution probe issue command does not bind logical identity: %#v", parsed)
	}
	decoded, err := parsed.WorkerTaskIssueRequest()
	want := broker.WorkerTaskIssueRequest{
		Schema: request.Schema, DeploymentID: request.DeploymentID, TaskID: request.TaskID, TaskKind: request.TaskKind,
		ExecutionManifestDigest: request.ExecutionManifestDigest, InputDigest: request.InputDigest,
	}
	if err != nil || !reflect.DeepEqual(decoded, want) || signed.PayloadJSON == "" {
		t.Fatalf("decoded execution probe issue request = %#v, err=%v", decoded, err)
	}
}

func TestBuildExecutionProbeObserveCommandBindsExactTaskReference(t *testing.T) {
	transport, now := testExecutionProbeTransport(t)
	request := runtime.ExecutionProbeObserveRequest{DeploymentID: "deployment-probe-0001", TaskID: "task-probe-0001"}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	command := runtime.ExecutionProbeCommand{
		CommandID: "command-probe-observe-0001", DeploymentID: request.DeploymentID, TaskID: request.TaskID,
		ConnectionID: "connection-probe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2,
		NodeCounter: 8, Attempt: 1, Action: runtime.ExecutionProbeObserveAction, RequestDigest: digest,
	}

	signed, err := transport.BuildExecutionProbeObserveCommand(command, request, now)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := broker.ParseWorkerTaskObserveCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnectionID != command.ConnectionID || parsed.CommandID != command.CommandID || parsed.NodeCounter != command.NodeCounter ||
		parsed.RequestSHA256() != signed.RequestSHA256 || signed.IssuedAt != now || signed.ExpiresAt != now.Add(commandLifetime) {
		t.Fatalf("parsed execution probe observe command = %#v", parsed)
	}
	decoded, err := parsed.WorkerTaskObserveRequest()
	if err != nil || decoded != (broker.WorkerTaskObserveRequest{DeploymentID: request.DeploymentID, TaskID: request.TaskID}) || signed.PayloadJSON == "" {
		t.Fatalf("decoded execution probe observe request = %#v, err=%v", decoded, err)
	}
}

func TestRequestExecutionProbeRejectsChangedPersistedPayloadBeforeNetwork(t *testing.T) {
	transport, now := testExecutionProbeTransport(t)
	issueRequest := testExecutionProbeIssueRequest()
	issueDigest, err := issueRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	issueCommand := runtime.ExecutionProbeCommand{
		CommandID: "command-probe-issue-0001", DeploymentID: issueRequest.DeploymentID, TaskID: issueRequest.TaskID,
		ConnectionID: "connection-probe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2,
		NodeCounter: 7, Attempt: 1, Action: runtime.ExecutionProbeIssueAction, RequestDigest: issueDigest,
	}
	issueSigned, err := transport.BuildExecutionProbeIssueCommand(issueCommand, issueRequest, now)
	if err != nil {
		t.Fatal(err)
	}
	issueSigned.PayloadJSON += " "
	if _, err := transport.RequestExecutionProbeIssue(nil, "https://broker.example/v2/commands", issueCommand, issueSigned, issueRequest); err == nil {
		t.Fatal("changed persisted execution probe issue payload must be rejected before network use")
	}

	observeRequest := runtime.ExecutionProbeObserveRequest{DeploymentID: issueRequest.DeploymentID, TaskID: issueRequest.TaskID}
	observeDigest, err := observeRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	observeCommand := runtime.ExecutionProbeCommand{
		CommandID: "command-probe-observe-0001", DeploymentID: observeRequest.DeploymentID, TaskID: observeRequest.TaskID,
		ConnectionID: "connection-probe-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2,
		NodeCounter: 8, Attempt: 1, Action: runtime.ExecutionProbeObserveAction, RequestDigest: observeDigest,
	}
	observeSigned, err := transport.BuildExecutionProbeObserveCommand(observeCommand, observeRequest, now)
	if err != nil {
		t.Fatal(err)
	}
	observeSigned.PayloadJSON += " "
	if _, err := transport.RequestExecutionProbeObserve(nil, "https://broker.example/v2/commands", observeCommand, observeSigned, observeRequest); err == nil {
		t.Fatal("changed persisted execution probe observe payload must be rejected before network use")
	}
}

func TestExecutionProbeTaskResultMappingAndRetryClassification(t *testing.T) {
	checkpoint := runtime.ExecutionProbeTransportPassed
	evidence := "sha256:" + strings.Repeat("a", 64)
	summary := broker.WorkerTaskSummary{
		TaskID: "task-probe-0001", DeploymentID: "deployment-probe-0001", Status: "succeeded", Attempt: 1, LastSequence: 2,
		Checkpoint: &checkpoint, EvidenceDigest: &evidence, UpdatedAt: "2026-07-15T08:00:01.000Z",
	}
	got, err := runtimeExecutionProbeTaskResult(summary)
	if err != nil {
		t.Fatal(err)
	}
	if got.TaskID != summary.TaskID || got.DeploymentID != summary.DeploymentID || got.Status != summary.Status || got.Attempt != summary.Attempt ||
		got.LastSequence != summary.LastSequence || got.Checkpoint == nil || *got.Checkpoint != checkpoint || got.EvidenceDigest == nil || *got.EvidenceDigest != evidence ||
		got.UpdatedAt.Format("2006-01-02T15:04:05.000Z") != summary.UpdatedAt {
		t.Fatalf("runtime execution probe result = %#v", got)
	}

	for _, test := range []struct {
		name       string
		stackError error
		wantPrefix string
	}{
		{name: "expired", stackError: &broker.Error{Code: "expired_command", StatusCode: 409}, wantPrefix: "execution_probe_command_expired"},
		{name: "rate_limited", stackError: &broker.Error{Code: "throttled", StatusCode: 429}, wantPrefix: "broker_unavailable"},
		{name: "timeout", stackError: &broker.Error{Code: "broker_timeout"}, wantPrefix: "broker_timeout"},
		{name: "unavailable", stackError: &broker.Error{Code: "broker_unavailable", StatusCode: 409}, wantPrefix: "broker_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := classifyExecutionProbeBrokerError(test.stackError)
			if !errors.Is(got, test.stackError) || !strings.HasPrefix(got.Error(), test.wantPrefix) {
				t.Fatalf("classified execution probe error = %v, want prefix %q", got, test.wantPrefix)
			}
		})
	}
}

func testExecutionProbeTransport(t *testing.T) (*Transport, time.Time) {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return transport, now
}

func testExecutionProbeIssueRequest() runtime.ExecutionProbeIssueRequest {
	return runtime.ExecutionProbeIssueRequest{
		Schema: runtime.ExecutionProbeIssueSchema, DeploymentID: "deployment-probe-0001", TaskID: "task-probe-0001", TaskKind: runtime.ExecutionProbeTaskKind,
		ExecutionManifestDigest: "sha256:" + strings.Repeat("a", 64), InputDigest: "sha256:" + strings.Repeat("b", 64),
	}
}
