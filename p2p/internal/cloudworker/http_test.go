package cloudworker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestSessionClientClaimsAndRetriesTheExactOutboundEvent(t *testing.T) {
	now := time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC)
	var (
		mu          sync.Mutex
		claimCalls  int
		eventSeqs   []uint64
		eventKinds  []EventKind
		claimBody   ClaimRequest
		accessToken = "short-lived-worker-token-0123456789"
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(writer, "method", http.StatusMethodNotAllowed)
			return
		}
		switch request.URL.Path {
		case "/v2/worker-sessions/worker-session-v2-01/claim":
			if request.Header.Get("Authorization") != "" {
				http.Error(writer, "claim authorization", http.StatusUnauthorized)
				return
			}
			body, err := ParseClaimRequest(mustReadAll(t, request.Body))
			if err != nil {
				http.Error(writer, "claim", http.StatusBadRequest)
				return
			}
			mu.Lock()
			claimCalls++
			claimBody = body
			mu.Unlock()
			writeWorkerJSON(t, writer, http.StatusCreated, map[string]any{
				"schema":               WorkerSessionClaimResponseV1Schema,
				"connection_id":        body.ConnectionID,
				"deployment_id":        body.DeploymentID,
				"bootstrap_session_id": body.BootstrapSessionID,
				"lease_epoch":          1,
				"lease_expires_at":     "2026-07-14T07:05:00.000Z",
				"access_token":         accessToken,
			})
		case "/v2/worker-sessions/worker-session-v2-01/events":
			if request.Header.Get("Authorization") != "Bearer "+accessToken {
				http.Error(writer, "event authorization", http.StatusUnauthorized)
				return
			}
			event, err := ParseSessionEvent(mustReadAll(t, request.Body))
			if err != nil {
				http.Error(writer, "event", http.StatusBadRequest)
				return
			}
			mu.Lock()
			eventSeqs = append(eventSeqs, event.Sequence)
			eventKinds = append(eventKinds, event.Kind)
			call := len(eventSeqs)
			mu.Unlock()
			if call == 1 {
				http.Error(writer, "temporary", http.StatusServiceUnavailable)
				return
			}
			disposition := "accepted"
			if event.Sequence == 1 {
				disposition = "idempotent"
			}
			writeWorkerJSON(t, writer, http.StatusOK, EventReceipt{
				Schema:             WorkerEventReceiptV1Schema,
				ConnectionID:       event.ConnectionID,
				DeploymentID:       event.DeploymentID,
				BootstrapSessionID: event.BootstrapSessionID,
				LeaseEpoch:         event.LeaseEpoch,
				Sequence:           event.Sequence,
				Disposition:        disposition,
			})
		default:
			http.Error(writer, "path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	endpoint := server.URL + "/v2/worker-sessions"
	client, err := NewSessionClient(validTestManifest(endpoint), SessionClientConfig{
		ExpectedConnectionID:      "connection-v2-0001",
		ExpectedBootstrapEndpoint: endpoint,
		HTTPClient:                server.Client(),
		Now:                       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewSessionClient() error = %v", err)
	}
	proof := InstanceIdentityProof{
		DocumentB64:  base64.StdEncoding.EncodeToString([]byte("instance-document")),
		SignatureB64: base64.StdEncoding.EncodeToString([]byte("instance-signature")),
	}
	if err := client.Claim(context.Background(), proof); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if err := client.Heartbeat(context.Background()); err == nil {
		t.Fatal("Heartbeat() succeeded on an indeterminate response")
	}
	if err := client.Checkpoint(context.Background(), "artifact_verified", testDigest); !errors.Is(err, ErrPendingEvent) {
		t.Fatalf("Checkpoint() error = %v, want ErrPendingEvent", err)
	}
	if err := client.RetryPending(context.Background()); err != nil {
		t.Fatalf("RetryPending() error = %v", err)
	}
	if err := client.Checkpoint(context.Background(), "artifact_verified", testDigest); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if err := client.Report(context.Background(), ReportStatusLocalReadyUnverified, "", testDigest); err != nil {
		t.Fatalf("Report() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if claimCalls != 1 {
		t.Fatalf("claim calls = %d, want 1", claimCalls)
	}
	if claimBody.ConnectionID != "connection-v2-0001" || claimBody.WorkerImageDigest != testDigest || claimBody.ArtifactManifestDigest != testDigest {
		t.Fatalf("claim body did not bind the manifest: %#v", claimBody)
	}
	if got, want := eventSeqs, []uint64{1, 1, 2, 3}; !equalUint64s(got, want) {
		t.Fatalf("event sequence = %v, want %v", got, want)
	}
	if got, want := eventKinds, []EventKind{EventKindHeartbeat, EventKindHeartbeat, EventKindCheckpoint, EventKindReport}; !equalEventKinds(got, want) {
		t.Fatalf("event kinds = %v, want %v", got, want)
	}
	snapshot := client.Snapshot()
	if snapshot.State != SessionStateActive || snapshot.LastAcknowledgedSequence != 3 || snapshot.PendingSequence != 0 {
		t.Fatalf("safe session snapshot = %#v", snapshot)
	}
}

func TestSessionClientRenewsItsIdentityBoundSessionAndResetsTelemetryEpoch(t *testing.T) {
	currentNow := time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC)
	proof := InstanceIdentityProof{
		DocumentB64:  base64.StdEncoding.EncodeToString([]byte("instance-document")),
		SignatureB64: base64.StdEncoding.EncodeToString([]byte("instance-signature")),
	}
	const (
		firstToken  = "short-lived-worker-token-0123456789"
		secondToken = "rotated-worker-token-987654321098765"
	)
	var (
		mu             sync.Mutex
		claimCalls     int
		claimProofs    []InstanceIdentityProof
		eventTokens    []string
		eventEpochs    []uint64
		eventSequences []uint64
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v2/worker-sessions/worker-session-v2-01/claim":
			claim, err := ParseClaimRequest(mustReadAll(t, request.Body))
			if err != nil {
				http.Error(writer, "claim", http.StatusBadRequest)
				return
			}
			mu.Lock()
			claimCalls++
			claimProofs = append(claimProofs, claim.InstanceIdentityProof)
			call := claimCalls
			mu.Unlock()
			response := map[string]any{
				"schema":               WorkerSessionClaimResponseV1Schema,
				"connection_id":        claim.ConnectionID,
				"deployment_id":        claim.DeploymentID,
				"bootstrap_session_id": claim.BootstrapSessionID,
				"lease_epoch":          1,
				"lease_expires_at":     "2026-07-14T07:05:00.000Z",
				"access_token":         firstToken,
			}
			if call == 2 {
				response["lease_epoch"] = 2
				response["lease_expires_at"] = "2026-07-14T07:09:00.000Z"
				response["access_token"] = secondToken
			}
			if call > 2 {
				http.Error(writer, "unexpected claim", http.StatusConflict)
				return
			}
			writeWorkerJSON(t, writer, http.StatusCreated, response)
		case "/v2/worker-sessions/worker-session-v2-01/events":
			event, err := ParseSessionEvent(mustReadAll(t, request.Body))
			if err != nil {
				http.Error(writer, "event", http.StatusBadRequest)
				return
			}
			token := request.Header.Get("Authorization")
			mu.Lock()
			eventTokens = append(eventTokens, token)
			eventEpochs = append(eventEpochs, event.LeaseEpoch)
			eventSequences = append(eventSequences, event.Sequence)
			mu.Unlock()
			if token == "Bearer "+firstToken {
				http.Error(writer, "temporary", http.StatusServiceUnavailable)
				return
			}
			if token != "Bearer "+secondToken {
				http.Error(writer, "authorization", http.StatusUnauthorized)
				return
			}
			writeWorkerJSON(t, writer, http.StatusOK, EventReceipt{
				Schema:             WorkerEventReceiptV1Schema,
				ConnectionID:       event.ConnectionID,
				DeploymentID:       event.DeploymentID,
				BootstrapSessionID: event.BootstrapSessionID,
				LeaseEpoch:         event.LeaseEpoch,
				Sequence:           event.Sequence,
				Disposition:        "accepted",
			})
		default:
			http.Error(writer, "path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	endpoint := server.URL + "/v2/worker-sessions"
	client, err := NewSessionClient(validTestManifest(endpoint), SessionClientConfig{
		ExpectedConnectionID:      "connection-v2-0001",
		ExpectedBootstrapEndpoint: endpoint,
		HTTPClient:                server.Client(),
		Now:                       func() time.Time { return currentNow },
	})
	if err != nil {
		t.Fatalf("NewSessionClient() error = %v", err)
	}
	if err := client.Claim(context.Background(), proof); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	currentNow = currentNow.Add(3 * time.Minute)
	if err := client.RenewIfDue(context.Background(), proof); err != nil {
		t.Fatalf("RenewIfDue() before the renewal window error = %v", err)
	}
	if err := client.Heartbeat(context.Background()); err == nil {
		t.Fatal("Heartbeat() succeeded on an indeterminate old-token response")
	}
	if snapshot := client.Snapshot(); snapshot.PendingSequence != 1 || snapshot.LeaseEpoch != 1 {
		t.Fatalf("pre-renewal snapshot = %#v", snapshot)
	}
	currentNow = currentNow.Add(time.Minute)
	if err := client.RenewIfDue(context.Background(), proof); err != nil {
		t.Fatalf("RenewIfDue() error = %v", err)
	}
	if snapshot := client.Snapshot(); snapshot.State != SessionStateActive || snapshot.LeaseEpoch != 2 || snapshot.LastAcknowledgedSequence != 0 || snapshot.PendingSequence != 0 {
		t.Fatalf("renewed snapshot = %#v", snapshot)
	}
	if err := client.Heartbeat(context.Background()); err != nil {
		t.Fatalf("Heartbeat() after renewal error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if claimCalls != 2 || len(claimProofs) != 2 || claimProofs[0] != proof || claimProofs[1] != proof {
		t.Fatalf("renewal claims = calls:%d proofs:%#v", claimCalls, claimProofs)
	}
	if got, want := eventTokens, []string{"Bearer " + firstToken, "Bearer " + secondToken}; !equalStringSlices(got, want) {
		t.Fatalf("event tokens = %v, want %v", got, want)
	}
	if got, want := eventEpochs, []uint64{1, 2}; !equalUint64s(got, want) {
		t.Fatalf("event epochs = %v, want %v", got, want)
	}
	if got, want := eventSequences, []uint64{1, 1}; !equalUint64s(got, want) {
		t.Fatalf("event sequences = %v, want %v", got, want)
	}
}

func TestSessionClientClaimsAndRetriesOnlyTheBoundedTaskTransportProbe(t *testing.T) {
	now := time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC)
	proof := InstanceIdentityProof{
		DocumentB64:  base64.StdEncoding.EncodeToString([]byte("instance-document")),
		SignatureB64: base64.StdEncoding.EncodeToString([]byte("instance-signature")),
	}
	task := WorkerTask{
		Schema:                  WorkerTaskV1Schema,
		TaskID:                  "worker-task-v2-001",
		DeploymentID:            "deployment-v2-0001",
		TaskKind:                TaskKindExecutionProbe,
		ExecutionManifestDigest: testDigest,
		InputDigest:             "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Attempt:                 1,
		LastSequence:            0,
	}
	const accessToken = "short-lived-worker-token-0123456789"
	var (
		mu             sync.Mutex
		claimRequests  []TaskClaimRequest
		eventBodies    []TaskEvent
		claimTaskCalls int
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v2/worker-sessions/worker-session-v2-01/claim":
			claim, err := ParseClaimRequest(mustReadAll(t, request.Body))
			if err != nil {
				http.Error(writer, "claim", http.StatusBadRequest)
				return
			}
			writeWorkerJSON(t, writer, http.StatusCreated, map[string]any{
				"schema":               WorkerSessionClaimResponseV1Schema,
				"connection_id":        claim.ConnectionID,
				"deployment_id":        claim.DeploymentID,
				"bootstrap_session_id": claim.BootstrapSessionID,
				"lease_epoch":          1,
				"lease_expires_at":     "2026-07-14T07:05:00.000Z",
				"access_token":         accessToken,
			})
		case "/v2/worker-sessions/worker-session-v2-01/tasks/claim":
			if request.Header.Get("Authorization") != "Bearer "+accessToken {
				http.Error(writer, "authorization", http.StatusUnauthorized)
				return
			}
			claim, err := ParseTaskClaimRequest(mustReadAll(t, request.Body))
			if err != nil {
				http.Error(writer, "task claim", http.StatusBadRequest)
				return
			}
			mu.Lock()
			claimRequests = append(claimRequests, claim)
			claimTaskCalls++
			call := claimTaskCalls
			mu.Unlock()
			if call == 1 {
				response := TaskClaimResponse{
					Schema:     WorkerTaskClaimResponseV1Schema,
					Status:     "claimed",
					LeaseEpoch: claim.LeaseEpoch,
					Task:       &task,
				}
				writeWorkerJSON(t, writer, http.StatusOK, response)
				return
			}
			writeWorkerJSON(t, writer, http.StatusOK, TaskClaimResponse{
				Schema:     WorkerTaskClaimResponseV1Schema,
				Status:     "none",
				LeaseEpoch: claim.LeaseEpoch,
			})
		case "/v2/worker-sessions/worker-session-v2-01/tasks/worker-task-v2-001/events":
			if request.Header.Get("Authorization") != "Bearer "+accessToken {
				http.Error(writer, "authorization", http.StatusUnauthorized)
				return
			}
			event, err := ParseTaskEvent(mustReadAll(t, request.Body))
			if err != nil || event.ValidateFor(task) != nil {
				http.Error(writer, "task event", http.StatusBadRequest)
				return
			}
			mu.Lock()
			eventBodies = append(eventBodies, event)
			call := len(eventBodies)
			mu.Unlock()
			if call == 1 {
				http.Error(writer, "temporary", http.StatusServiceUnavailable)
				return
			}
			disposition := "accepted"
			if call == 2 {
				disposition = "idempotent"
			}
			writeWorkerJSON(t, writer, http.StatusOK, TaskEventReceipt{
				Schema:      WorkerTaskEventReceiptV1Schema,
				TaskID:      event.TaskID,
				Attempt:     event.Attempt,
				LeaseEpoch:  event.LeaseEpoch,
				Sequence:    event.Sequence,
				Disposition: disposition,
			})
		default:
			http.Error(writer, "path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	endpoint := server.URL + "/v2/worker-sessions"
	client, err := NewSessionClient(validTestManifest(endpoint), SessionClientConfig{
		ExpectedConnectionID:      "connection-v2-0001",
		ExpectedBootstrapEndpoint: endpoint,
		HTTPClient:                server.Client(),
		Now:                       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewSessionClient() error = %v", err)
	}
	if err := client.Claim(context.Background(), proof); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	claimed, found, err := client.ClaimTask(context.Background())
	if err != nil || !found || claimed != task {
		t.Fatalf("ClaimTask() = %#v, %v, %v", claimed, found, err)
	}
	if err := client.ReportTask(context.Background(), claimed, TaskStatusRunning, "execution_manifest_received", "", task.ExecutionManifestDigest); err == nil {
		t.Fatal("ReportTask(running) succeeded on an indeterminate response")
	}
	if err := client.ReportTask(context.Background(), claimed, TaskStatusSucceeded, "task_transport_verified", "", task.ExecutionManifestDigest); !errors.Is(err, ErrPendingTaskEvent) {
		t.Fatalf("ReportTask(succeeded) error = %v, want ErrPendingTaskEvent", err)
	}
	if err := client.RetryPendingTask(context.Background()); err != nil {
		t.Fatalf("RetryPendingTask() error = %v", err)
	}
	if err := client.ReportTask(context.Background(), claimed, TaskStatusSucceeded, "task_transport_verified", "", task.ExecutionManifestDigest); err != nil {
		t.Fatalf("ReportTask(succeeded) error = %v", err)
	}
	if _, found, err := client.ClaimTask(context.Background()); err != nil || found {
		t.Fatalf("ClaimTask() after terminal report = found:%v err:%v", found, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(claimRequests) != 2 || claimRequests[0].Schema != WorkerTaskClaimV1Schema || claimRequests[0].LeaseEpoch != 1 {
		t.Fatalf("task claim requests = %#v", claimRequests)
	}
	if got, want := []uint64{eventBodies[0].Sequence, eventBodies[1].Sequence, eventBodies[2].Sequence}, []uint64{1, 1, 2}; !equalUint64s(got, want) {
		t.Fatalf("task event sequence = %v, want %v", got, want)
	}
	if eventBodies[0].Status != TaskStatusRunning || eventBodies[0].Checkpoint == nil || *eventBodies[0].Checkpoint != "execution_manifest_received" || eventBodies[0].ErrorCode != nil || eventBodies[0].EvidenceDigest == nil || *eventBodies[0].EvidenceDigest != task.ExecutionManifestDigest {
		t.Fatalf("first task event was not fixed probe evidence: %#v", eventBodies[0])
	}
	if eventBodies[2].Status != TaskStatusSucceeded || eventBodies[2].Checkpoint == nil || *eventBodies[2].Checkpoint != "task_transport_verified" || eventBodies[2].ErrorCode != nil || eventBodies[2].EvidenceDigest == nil || *eventBodies[2].EvidenceDigest != task.ExecutionManifestDigest {
		t.Fatalf("terminal task event was not fixed probe evidence: %#v", eventBodies[2])
	}
}

func TestSessionClientLeaseRenewalDropsOldPendingTaskEventsUntilItReclaims(t *testing.T) {
	currentNow := time.Date(2026, 7, 14, 7, 0, 0, 0, time.UTC)
	proof := InstanceIdentityProof{
		DocumentB64:  base64.StdEncoding.EncodeToString([]byte("instance-document")),
		SignatureB64: base64.StdEncoding.EncodeToString([]byte("instance-signature")),
	}
	task := WorkerTask{
		Schema:                  WorkerTaskV1Schema,
		TaskID:                  "worker-task-v2-001",
		DeploymentID:            "deployment-v2-0001",
		TaskKind:                TaskKindExecutionProbe,
		ExecutionManifestDigest: testDigest,
		InputDigest:             "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Attempt:                 1,
	}
	const (
		firstToken  = "short-lived-worker-token-0123456789"
		secondToken = "rotated-worker-token-987654321098765"
	)
	var (
		mu         sync.Mutex
		claimCalls int
		taskTokens []string
		taskClaims []string
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v2/worker-sessions/worker-session-v2-01/claim":
			claim, err := ParseClaimRequest(mustReadAll(t, request.Body))
			if err != nil {
				http.Error(writer, "claim", http.StatusBadRequest)
				return
			}
			mu.Lock()
			claimCalls++
			call := claimCalls
			mu.Unlock()
			response := map[string]any{
				"schema":               WorkerSessionClaimResponseV1Schema,
				"connection_id":        claim.ConnectionID,
				"deployment_id":        claim.DeploymentID,
				"bootstrap_session_id": claim.BootstrapSessionID,
				"lease_epoch":          1,
				"lease_expires_at":     "2026-07-14T07:02:00.000Z",
				"access_token":         firstToken,
			}
			if call == 2 {
				response["lease_epoch"] = 2
				response["lease_expires_at"] = "2026-07-14T07:05:00.000Z"
				response["access_token"] = secondToken
			}
			writeWorkerJSON(t, writer, http.StatusCreated, response)
		case "/v2/worker-sessions/worker-session-v2-01/tasks/claim":
			if _, err := ParseTaskClaimRequest(mustReadAll(t, request.Body)); err != nil {
				http.Error(writer, "task claim", http.StatusBadRequest)
				return
			}
			token := request.Header.Get("Authorization")
			mu.Lock()
			taskClaims = append(taskClaims, token)
			call := len(taskClaims)
			mu.Unlock()
			if call == 1 && token == "Bearer "+firstToken {
				writeWorkerJSON(t, writer, http.StatusOK, TaskClaimResponse{
					Schema:     WorkerTaskClaimResponseV1Schema,
					Status:     "claimed",
					LeaseEpoch: 1,
					Task:       &task,
				})
				return
			}
			if token != "Bearer "+secondToken {
				http.Error(writer, "authorization", http.StatusUnauthorized)
				return
			}
			writeWorkerJSON(t, writer, http.StatusOK, TaskClaimResponse{
				Schema:     WorkerTaskClaimResponseV1Schema,
				Status:     "none",
				LeaseEpoch: 2,
			})
		case "/v2/worker-sessions/worker-session-v2-01/tasks/worker-task-v2-001/events":
			event, err := ParseTaskEvent(mustReadAll(t, request.Body))
			if err != nil || event.ValidateFor(task) != nil {
				http.Error(writer, "task event", http.StatusBadRequest)
				return
			}
			mu.Lock()
			taskTokens = append(taskTokens, request.Header.Get("Authorization"))
			mu.Unlock()
			http.Error(writer, "temporary", http.StatusServiceUnavailable)
		default:
			http.Error(writer, "path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	endpoint := server.URL + "/v2/worker-sessions"
	client, err := NewSessionClient(validTestManifest(endpoint), SessionClientConfig{
		ExpectedConnectionID:      "connection-v2-0001",
		ExpectedBootstrapEndpoint: endpoint,
		HTTPClient:                server.Client(),
		Now:                       func() time.Time { return currentNow },
	})
	if err != nil {
		t.Fatalf("NewSessionClient() error = %v", err)
	}
	if err := client.Claim(context.Background(), proof); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	claimed, found, err := client.ClaimTask(context.Background())
	if err != nil || !found {
		t.Fatalf("ClaimTask() = %#v, %v, %v", claimed, found, err)
	}
	if err := client.ReportTask(context.Background(), claimed, TaskStatusRunning, "execution_manifest_received", "", task.ExecutionManifestDigest); err == nil {
		t.Fatal("ReportTask() succeeded on an indeterminate old-lease response")
	}
	currentNow = currentNow.Add(time.Minute)
	if err := client.RenewIfDue(context.Background(), proof); err != nil {
		t.Fatalf("RenewIfDue() error = %v", err)
	}
	if err := client.RetryPendingTask(context.Background()); !errors.Is(err, ErrNoPendingTaskEvent) {
		t.Fatalf("RetryPendingTask() after renewal error = %v, want ErrNoPendingTaskEvent", err)
	}
	if err := client.ReportTask(context.Background(), claimed, TaskStatusSucceeded, "task_transport_verified", "", task.ExecutionManifestDigest); !errors.Is(err, ErrTaskNotClaimed) {
		t.Fatalf("ReportTask() under a rotated lease error = %v, want ErrTaskNotClaimed", err)
	}
	if _, found, err := client.ClaimTask(context.Background()); err != nil || found {
		t.Fatalf("ClaimTask() after renewal = found:%v err:%v", found, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if claimCalls != 2 || !equalStringSlices(taskTokens, []string{"Bearer " + firstToken}) || !equalStringSlices(taskClaims, []string{"Bearer " + firstToken, "Bearer " + secondToken}) {
		t.Fatalf("renewed task transport = claims:%d taskTokens:%v taskClaims:%v", claimCalls, taskTokens, taskClaims)
	}
}

func mustReadAll(t *testing.T, body io.ReadCloser) []byte {
	t.Helper()
	defer body.Close()
	value, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	return value
}

func writeWorkerJSON(t *testing.T, writer http.ResponseWriter, status int, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func equalUint64s(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalEventKinds(left, right []EventKind) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
