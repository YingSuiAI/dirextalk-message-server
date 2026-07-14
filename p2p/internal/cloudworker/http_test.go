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
