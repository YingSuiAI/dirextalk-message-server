package contract

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestServiceReadinessContractCarriesOnlyTypedLoopbackProbe(t *testing.T) {
	digest := func(value string) string { return "sha256:" + strings.Repeat(value, 64) }
	want := ServiceReadinessIssueRequest{Schema: ServiceReadinessIssueSchema, ExecutionID: "execution-ready-0001", DeploymentID: "deployment-ready-0001", ServiceID: "service-ready-0001", TaskID: "readiness-task-0001", ProbeKind: ServiceReadinessProbeKind, RecipeExecutionManifestDigest: digest("a"), InstallEvidenceDigest: digest("b"), ArtifactDigest: digest("d"), SemanticProbe: ServiceReadinessProbeV1{Scheme: "http", Port: 19191, Path: "/openclaw/semantic", ExpectedStatus: 200, BodySHA256: digest("c")}, SemanticExpectationDigest: digest("c")}
	raw, _ := json.Marshal(want)
	request, err := ParseServiceReadinessIssueRequest(raw)
	if err != nil || request.ProbeKind != ServiceReadinessProbeKind {
		t.Fatalf("ParseServiceReadinessIssueRequest() = %#v, %v", request, err)
	}
	forbidden := bytes.Replace(raw, []byte(`"probe_kind":`), []byte(`"url":"https://worker.invalid/ready","probe_kind":`), 1)
	if _, err := ParseServiceReadinessIssueRequest(forbidden); err == nil {
		t.Fatal("issue request accepted a selectable URL")
	}
	drifted := want
	drifted.SemanticProbe.Path = "https://worker.invalid/ready"
	raw, _ = json.Marshal(drifted)
	if _, err := ParseServiceReadinessIssueRequest(raw); err == nil {
		t.Fatal("issue request accepted a non-loopback probe target")
	}
}

func TestServiceReadinessChallengeIsFreshOpaqueAndAbsentFromReceipts(t *testing.T) {
	raw := bytes.Repeat([]byte{0x5a}, 32)
	challenge, err := NewServiceReadinessChallenge(raw, "2026-07-15T12:02:00.000Z")
	if err != nil || challenge.Validate() != nil || challenge.ChallengeDigest != "sha256:60bf07c488aad18fda339df07e4fbc47b4f00be71711936f18d04d352ad01890" {
		t.Fatalf("challenge = %#v, error = %v", challenge, err)
	}
	task := ServiceReadinessTaskV1{Schema: ServiceReadinessTaskSchema, TaskID: "readiness-task-0001", ExecutionID: "execution-ready-0001", DeploymentID: "deployment-ready-0001", ServiceID: "service-ready-0001", ProbeKind: ServiceReadinessProbeKind, RecipeExecutionManifestDigest: "sha256:" + strings.Repeat("a", 64), InstallEvidenceDigest: "sha256:" + strings.Repeat("b", 64), ArtifactDigest: "sha256:" + strings.Repeat("d", 64), SemanticProbe: ServiceReadinessProbeV1{Scheme: "http", Port: 19191, Path: "/knowledge/semantic", ExpectedStatus: 200, BodySHA256: "sha256:" + strings.Repeat("c", 64)}, SemanticExpectationDigest: "sha256:" + strings.Repeat("c", 64), Attempt: 1}
	response, err := MarshalServiceReadinessClaimResponse(1, &task, &challenge)
	if err != nil || !bytes.Contains(response, []byte(challenge.ChallengeB64)) {
		t.Fatalf("claim response = %s, error = %v", response, err)
	}
	event := ServiceReadinessEventV1{Schema: ServiceReadinessEventSchema, TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 1, Sequence: 1, Status: "succeeded", ChallengeDigest: &challenge.ChallengeDigest, SemanticEvidenceDigest: &task.SemanticExpectationDigest, OccurredAt: "2026-07-15T12:01:00.000Z"}
	receipt, _ := json.Marshal(NewServiceReadinessEventReceipt(event, false))
	if bytes.Contains(receipt, []byte(challenge.ChallengeB64)) || bytes.Contains(receipt, raw) {
		t.Fatalf("event receipt leaked challenge body: %s", receipt)
	}
}

func TestServiceReadinessEventIsSingleTerminalExactDocument(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	raw := []byte(`{"schema":"dirextalk.service-readiness-task-event/v1","task_id":"readiness-task-0001","attempt":1,"lease_epoch":2,"sequence":1,"status":"succeeded","challenge_digest":"` + digest + `","semantic_evidence_digest":"` + digest + `","error_code":null,"occurred_at":"2026-07-15T12:01:00.000Z"}`)
	if _, err := ParseServiceReadinessEvent(raw); err != nil {
		t.Fatalf("ParseServiceReadinessEvent() error = %v", err)
	}
	if _, err := ParseServiceReadinessEvent(bytes.Replace(raw, []byte(`"sequence":1`), []byte(`"sequence":2`), 1)); err == nil {
		t.Fatal("event accepted a non-terminal sequence")
	}
	if _, err := ParseServiceReadinessEvent(bytes.Replace(raw, []byte(`"occurred_at":`), []byte(`"command":"curl bad","occurred_at":`), 1)); err == nil {
		t.Fatal("event accepted an arbitrary command")
	}
}
