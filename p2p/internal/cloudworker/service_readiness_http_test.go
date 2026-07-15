package cloudworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/fixedprobe"
)

func TestServiceReadinessLoopReplaysTheExactChallengeBoundEventAfterResponseLoss(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	token := "short-lived-readiness-token"
	challengeBytes := bytes.Repeat([]byte{0x4a}, 32)
	challenge := ServiceReadinessChallengeV1{
		Schema: ServiceReadinessChallengeV1Schema, ChallengeBase64: base64.StdEncoding.EncodeToString(challengeBytes),
		ChallengeDigest: namedSHA256(challengeBytes), ExpiresAt: canonicalInstant(now.Add(2 * time.Minute)),
	}
	task := ServiceReadinessTaskV1{
		Schema: ServiceReadinessTaskV1Schema, TaskID: "readiness-task-0001", ExecutionID: "execution-ready-0001",
		DeploymentID: "deployment-v2-0001", ServiceID: "service-ready-0001", ProbeKind: ServiceReadinessProbeKind,
		RecipeExecutionManifestDigest: recipeDigest('a'), InstallEvidenceDigest: recipeDigest('b'),
		SemanticExpectationDigest: FixedReadinessEvidenceDigest(), Attempt: 1, LastSequence: 0,
	}
	var (
		mu         sync.Mutex
		claimCalls int
		eventRaw   [][]byte
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("Authorization") != "Bearer "+token {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/v2/worker-sessions/worker-session-v2-01/service-readiness-tasks/claim":
			var claim ServiceReadinessTaskClaimRequestV1
			if err := json.NewDecoder(request.Body).Decode(&claim); err != nil || claim.Schema != ServiceReadinessTaskClaimV1Schema || claim.LeaseEpoch != 7 {
				http.Error(writer, "claim", http.StatusBadRequest)
				return
			}
			mu.Lock()
			claimCalls++
			call := claimCalls
			mu.Unlock()
			if call == 1 {
				writeWorkerJSON(t, writer, http.StatusOK, ServiceReadinessTaskClaimResponseV1{Schema: ServiceReadinessTaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Challenge: &challenge})
				return
			}
			writeWorkerJSON(t, writer, http.StatusOK, ServiceReadinessTaskClaimResponseV1{Schema: ServiceReadinessTaskClaimResponseV1Schema, Status: "none", LeaseEpoch: 7})
		case "/v2/worker-sessions/worker-session-v2-01/service-readiness-tasks/readiness-task-0001/events":
			raw := mustReadAll(t, request.Body)
			event, err := ParseServiceReadinessTaskEventV1(raw, task, challenge, 7)
			if err != nil {
				http.Error(writer, "event", http.StatusBadRequest)
				return
			}
			mu.Lock()
			eventRaw = append(eventRaw, append([]byte(nil), raw...))
			call := len(eventRaw)
			mu.Unlock()
			if call == 1 {
				panic(http.ErrAbortHandler)
			}
			writeWorkerJSON(t, writer, http.StatusOK, ServiceReadinessTaskEventReceiptV1{Schema: ServiceReadinessTaskEventReceiptV1Schema,
				TaskID: event.TaskID, Attempt: event.Attempt, LeaseEpoch: event.LeaseEpoch, Sequence: event.Sequence, Disposition: "idempotent"})
		default:
			http.Error(writer, "path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	endpoint, _ := url.Parse(server.URL + "/v2/worker-sessions")
	bootstrap := validTestManifest(endpoint.String())
	session := &SessionClient{manifest: bootstrap, endpoint: endpoint, client: server.Client(), now: func() time.Time { return now }, state: SessionStateActive, access: token, epoch: 7}
	client, err := session.NewServiceReadinessTaskClient()
	if err != nil {
		t.Fatal(err)
	}
	probe := &readinessProbeRecorder{}
	loop, err := NewServiceReadinessTaskLoop(client, probe)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.ProcessOne(context.Background()); err == nil {
		t.Fatal("first ProcessOne() succeeded despite a lost event response")
	}
	if err := loop.ProcessOne(context.Background()); err != nil {
		t.Fatalf("retry ProcessOne() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if claimCalls != 2 || len(eventRaw) != 2 || !bytes.Equal(eventRaw[0], eventRaw[1]) {
		t.Fatalf("flow = claims:%d event bodies equal:%t (%d)", claimCalls, len(eventRaw) == 2 && bytes.Equal(eventRaw[0], eventRaw[1]), len(eventRaw))
	}
	if !reflect.DeepEqual(probe.urls, []string{fixedprobe.ReadinessURL}) {
		t.Fatalf("probe URLs = %#v", probe.urls)
	}
	event, err := ParseServiceReadinessTaskEventV1(eventRaw[0], task, challenge, 7)
	if err != nil || event.Status != ServiceReadinessTaskSucceeded || event.ChallengeDigest == nil || *event.ChallengeDigest != challenge.ChallengeDigest ||
		event.SemanticEvidenceDigest == nil || *event.SemanticEvidenceDigest != task.SemanticExpectationDigest || event.ErrorCode != nil {
		t.Fatalf("event = %#v, error = %v", event, err)
	}
}

func TestServiceReadinessProtocolRejectsUnboundOrSelectableClaims(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse("https://worker.example/v2/worker-sessions")
	manifest := validTestManifest(endpoint.String())
	challengeBytes := bytes.Repeat([]byte{0x21}, 32)
	challenge := ServiceReadinessChallengeV1{Schema: ServiceReadinessChallengeV1Schema, ChallengeBase64: base64.StdEncoding.EncodeToString(challengeBytes), ChallengeDigest: namedSHA256(challengeBytes), ExpiresAt: canonicalInstant(now.Add(time.Minute))}
	task := ServiceReadinessTaskV1{Schema: ServiceReadinessTaskV1Schema, TaskID: "readiness-task-0001", ExecutionID: "execution-ready-0001", DeploymentID: manifest.DeploymentID,
		ServiceID: "service-ready-0001", ProbeKind: ServiceReadinessProbeKind, RecipeExecutionManifestDigest: recipeDigest('a'), InstallEvidenceDigest: recipeDigest('b'), SemanticExpectationDigest: FixedReadinessEvidenceDigest(), Attempt: 1}

	tests := []struct {
		name   string
		mutate func(*ServiceReadinessTaskV1, *ServiceReadinessChallengeV1)
	}{
		{name: "other deployment", mutate: func(task *ServiceReadinessTaskV1, _ *ServiceReadinessChallengeV1) {
			task.DeploymentID = "deployment-other-0001"
		}},
		{name: "selectable probe kind", mutate: func(task *ServiceReadinessTaskV1, _ *ServiceReadinessChallengeV1) {
			task.ProbeKind = "http://127.0.0.1:9999"
		}},
		{name: "other semantic document", mutate: func(task *ServiceReadinessTaskV1, _ *ServiceReadinessChallengeV1) {
			task.SemanticExpectationDigest = recipeDigest('f')
		}},
		{name: "forged challenge digest", mutate: func(_ *ServiceReadinessTaskV1, challenge *ServiceReadinessChallengeV1) {
			challenge.ChallengeDigest = recipeDigest('f')
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidateTask, candidateChallenge := task, challenge
			test.mutate(&candidateTask, &candidateChallenge)
			raw, _ := json.Marshal(ServiceReadinessTaskClaimResponseV1{Schema: ServiceReadinessTaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &candidateTask, Challenge: &candidateChallenge})
			if _, err := ParseServiceReadinessTaskClaimResponseV1(raw, manifest, 7, now); err == nil {
				t.Fatal("ParseServiceReadinessTaskClaimResponseV1() accepted an unbound claim")
			}
		})
	}
	withURL, _ := json.Marshal(map[string]any{"schema": ServiceReadinessTaskClaimResponseV1Schema, "status": "claimed", "lease_epoch": 7, "task": task, "challenge": challenge, "url": "https://attacker.invalid"})
	if _, err := ParseServiceReadinessTaskClaimResponseV1(withURL, manifest, 7, now); err == nil {
		t.Fatal("claim response accepted an unknown selectable URL")
	}
}

func TestServiceReadinessLoopReportsOnlyAFixedFailureCode(t *testing.T) {
	claimed := testClaimedReadinessTask()
	transport := &readinessLoopTransport{claimed: claimed}
	probeErr := errors.New("sensitive local failure")
	loop := &ServiceReadinessTaskLoop{transport: transport, probe: &readinessProbeRecorder{err: probeErr}}
	if err := loop.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne() error = %v", err)
	}
	if transport.status != ServiceReadinessTaskFailed || transport.errorCode != "fixed_probe_not_ready" {
		t.Fatalf("report = (%q, %q)", transport.status, transport.errorCode)
	}
	if bytes.Contains([]byte(transport.errorCode), []byte("sensitive")) {
		t.Fatal("probe error leaked into the event")
	}
}

func TestFixedReadinessEvidenceDigestIsTheExactBodySHA256(t *testing.T) {
	sum := sha256.Sum256([]byte(fixedprobe.ReadinessBody))
	want := "sha256:" + hex.EncodeToString(sum[:])
	if got := FixedReadinessEvidenceDigest(); got != want {
		t.Fatalf("FixedReadinessEvidenceDigest() = %q, want %q", got, want)
	}
}

func TestServiceReadinessClaimAndReceiptBindTheExactLease(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse("https://worker.example/v2/worker-sessions")
	manifest := validTestManifest(endpoint.String())
	challengeBytes := bytes.Repeat([]byte{0x31}, 32)
	challenge := ServiceReadinessChallengeV1{Schema: ServiceReadinessChallengeV1Schema, ChallengeBase64: base64.StdEncoding.EncodeToString(challengeBytes), ChallengeDigest: namedSHA256(challengeBytes), ExpiresAt: canonicalInstant(now.Add(time.Minute))}
	task := ServiceReadinessTaskV1{Schema: ServiceReadinessTaskV1Schema, TaskID: "readiness-task-0001", ExecutionID: "execution-ready-0001", DeploymentID: manifest.DeploymentID, ServiceID: "service-ready-0001", ProbeKind: ServiceReadinessProbeKind,
		RecipeExecutionManifestDigest: recipeDigest('a'), InstallEvidenceDigest: recipeDigest('b'), SemanticExpectationDigest: FixedReadinessEvidenceDigest(), Attempt: 1}
	claimRaw, _ := json.Marshal(ServiceReadinessTaskClaimResponseV1{Schema: ServiceReadinessTaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 8, Task: &task, Challenge: &challenge})
	if _, err := ParseServiceReadinessTaskClaimResponseV1(claimRaw, manifest, 7, now); err == nil {
		t.Fatal("claim response from another lease was accepted")
	}

	event := ServiceReadinessTaskEventV1{Schema: ServiceReadinessTaskEventV1Schema, TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 7, Sequence: 1, Status: ServiceReadinessTaskSucceeded,
		ChallengeDigest: optionalReadinessString(challenge.ChallengeDigest), SemanticEvidenceDigest: optionalReadinessString(FixedReadinessEvidenceDigest()), OccurredAt: canonicalInstant(now)}
	receiptRaw, _ := json.Marshal(ServiceReadinessTaskEventReceiptV1{Schema: ServiceReadinessTaskEventReceiptV1Schema, TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 8, Sequence: 1, Disposition: "accepted"})
	if _, err := ParseServiceReadinessTaskEventReceiptV1(receiptRaw, event); err == nil {
		t.Fatal("event receipt from another lease was accepted")
	}
}

func TestServiceReadinessPendingEventYieldsToANewerSessionLease(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	endpoint, _ := url.Parse("https://worker.example/v2/worker-sessions")
	session := &SessionClient{manifest: validTestManifest(endpoint.String()), endpoint: endpoint, now: func() time.Time { return now }, state: SessionStateActive, access: "new-lease-token", epoch: 8}
	claimed := testClaimedReadinessTask()
	claimed.Epoch = 7
	event := ServiceReadinessTaskEventV1{Schema: ServiceReadinessTaskEventV1Schema, TaskID: claimed.Task.TaskID, Attempt: claimed.Task.Attempt, LeaseEpoch: 7, Sequence: 1, Status: ServiceReadinessTaskInterrupted, ErrorCode: optionalReadinessString("fixed_probe_interrupted"), OccurredAt: canonicalInstant(now)}
	client := &ServiceReadinessTaskClient{session: session, claimed: &claimed, pending: &event}
	if err := client.RetryPending(context.Background()); err != nil {
		t.Fatalf("RetryPending() after lease rotation error = %v", err)
	}
	if client.pending != nil || client.claimed != nil {
		t.Fatal("stale pending event prevented a fresh challenge claim")
	}
}

type readinessProbeRecorder struct {
	urls []string
	err  error
}

func (probe *readinessProbeRecorder) CheckLoopback(_ context.Context, target string) error {
	probe.urls = append(probe.urls, target)
	return probe.err
}

type readinessLoopTransport struct {
	claimed   ClaimedServiceReadinessTask
	status    ServiceReadinessTaskStatus
	errorCode string
}

func (transport *readinessLoopTransport) RetryPending(context.Context) error {
	return ErrNoPendingServiceReadinessEvent
}
func (transport *readinessLoopTransport) Claim(context.Context) (ClaimedServiceReadinessTask, bool, error) {
	return transport.claimed, true, nil
}
func (transport *readinessLoopTransport) Report(_ context.Context, _ ClaimedServiceReadinessTask, status ServiceReadinessTaskStatus, errorCode string) error {
	transport.status, transport.errorCode = status, errorCode
	return nil
}

func testClaimedReadinessTask() ClaimedServiceReadinessTask {
	challengeBytes := bytes.Repeat([]byte{0x61}, 32)
	return ClaimedServiceReadinessTask{Epoch: 1,
		Task: ServiceReadinessTaskV1{Schema: ServiceReadinessTaskV1Schema, TaskID: "readiness-task-0001", ExecutionID: "execution-ready-0001", DeploymentID: "deployment-v2-0001", ServiceID: "service-ready-0001", ProbeKind: ServiceReadinessProbeKind,
			RecipeExecutionManifestDigest: recipeDigest('a'), InstallEvidenceDigest: recipeDigest('b'), SemanticExpectationDigest: FixedReadinessEvidenceDigest(), Attempt: 1},
		Challenge: ServiceReadinessChallengeV1{Schema: ServiceReadinessChallengeV1Schema, ChallengeBase64: base64.StdEncoding.EncodeToString(challengeBytes), ChallengeDigest: namedSHA256(challengeBytes), ExpiresAt: canonicalInstant(time.Now().UTC().Add(time.Minute))}}
}
