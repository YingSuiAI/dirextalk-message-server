package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
)

func TestMemoryStoreLegacyInvocationReplayUsesFirstGeneratedRunRequest(t *testing.T) {
	store := NewMemoryStore()
	candidate := legacyInvocationCandidate()

	inserted, err := store.ReserveInvocation(context.Background(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	if inserted.Status != legacygateway.ReservationInserted {
		t.Fatalf("expected inserted reservation, got %#v", inserted)
	}

	replayCandidate := candidate
	replayCandidate.RequestEventID = "01890f00-0000-7000-8000-000000000099"
	replayCandidate.RequestDigest[0] ^= 0xff
	replayCandidate.CreatedAt = candidate.CreatedAt.Add(time.Hour)
	replay, err := store.ReserveInvocation(context.Background(), replayCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != legacygateway.ReservationReplay {
		t.Fatalf("expected replay reservation, got %#v", replay)
	}
	if replay.Record.RequestEventID != candidate.RequestEventID ||
		replay.Record.RequestDigest != candidate.RequestDigest ||
		!replay.Record.CreatedAt.Equal(normalizeInvocationTime(candidate.CreatedAt)) {
		t.Fatalf("replay must return the first generated run request, got %#v", replay.Record)
	}

	conflictCandidate := candidate
	conflictCandidate.SourceDigest[0] ^= 0xff
	conflict, err := store.ReserveInvocation(context.Background(), conflictCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != legacygateway.ReservationConflict {
		t.Fatalf("expected changed source facts to conflict, got %#v", conflict)
	}
}

func TestMemoryStoreLegacyInvocationTransitionsAreFencedAndTerminal(t *testing.T) {
	store := NewMemoryStore()
	candidate := legacyInvocationCandidate()
	if _, err := store.ReserveInvocation(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}

	wrongDigest := candidate.SourceDigest
	wrongDigest[0] ^= 0xff
	if _, err := store.MarkAccepted(context.Background(), candidate.MatrixRoomID, candidate.RequestID,
		wrongDigest, legacygateway.CreateRunReceipt{RequestID: candidate.RequestID}, time.Now()); !errors.Is(err, legacygateway.ErrInvocationConflict) {
		t.Fatalf("expected source digest fence conflict, got %v", err)
	}

	receipt := legacygateway.CreateRunReceipt{
		RequestID:    candidate.RequestID,
		RunID:        "01890f00-0000-7000-8000-000000000050",
		Inserted:     true,
		RoutingState: legacygateway.RoutingQueued,
	}
	accepted, err := store.MarkAccepted(context.Background(), candidate.MatrixRoomID, candidate.RequestID,
		candidate.SourceDigest, receipt, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if accepted.State != legacygateway.InvocationAccepted || accepted.RunID != receipt.RunID {
		t.Fatalf("unexpected accepted record: %#v", accepted)
	}

	if _, err := store.MarkRejected(context.Background(), candidate.MatrixRoomID, candidate.RequestID,
		candidate.SourceDigest, "router_unavailable", time.Now()); !errors.Is(err, legacygateway.ErrInvalidInvocationTransition) {
		t.Fatalf("expected accepted record to remain terminal, got %v", err)
	}
	replayed, err := store.MarkAccepted(context.Background(), candidate.MatrixRoomID, candidate.RequestID,
		candidate.SourceDigest, receipt, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.UpdatedAt.Equal(accepted.UpdatedAt) {
		t.Fatalf("accepted replay changed terminal timestamp: before=%s after=%s", accepted.UpdatedAt, replayed.UpdatedAt)
	}
}

func legacyInvocationCandidate() legacygateway.InvocationCandidate {
	return legacygateway.InvocationCandidate{
		MatrixRoomID:         "!legacy:example.test",
		RequestID:            "01890f00-0000-7000-8000-000000000010",
		MatrixInvokeEventID:  "$invoke-event",
		MatrixInputEventID:   "$input-event",
		TenantID:             "01890f00-0000-7000-8000-000000000001",
		InstallationID:       "01890f00-0000-7000-8000-000000000011",
		ConversationID:       "01890f00-0000-7000-8000-000000000012",
		RequestEventID:       "01890f00-0000-7000-8000-000000000013",
		SourceDigest:         [32]byte{1},
		IdempotencyDigest:    [32]byte{2},
		RequestDigest:        [32]byte{3},
		PreferredConnectorID: "01890f00-0000-7000-8000-000000000014",
		RequiredCapabilities: []string{"chat.streaming", "tool.read"},
		DispatchMode:         legacygateway.DispatchSingle,
		GrantVersion:         4,
		CreatedAt:            time.Date(2026, 7, 14, 8, 30, 0, 123456789, time.FixedZone("test", 8*60*60)),
	}
}
