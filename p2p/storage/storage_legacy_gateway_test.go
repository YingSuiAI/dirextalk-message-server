package storage

import (
	"context"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestDatabaseStoreLegacyInvocationRoundTrip(t *testing.T) {
	ctx := context.Background()
	connectionString, closeDatabase := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDatabase()
	databaseOptions := config.DatabaseOptions{ConnectionString: config.DataSource(connectionString)}
	store, err := NewDatabaseStore(
		ctx,
		sqlutil.NewConnectionManager(nil, databaseOptions),
		&databaseOptions,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	candidate := legacyInvocationCandidate()
	inserted, err := store.ReserveInvocation(ctx, candidate)
	if err != nil || inserted.Status != legacygateway.ReservationInserted {
		t.Fatalf("insert reservation = %#v, err=%v", inserted, err)
	}
	retry := candidate
	retry.RequestEventID = "01890f00-0000-7000-8000-000000000099"
	retry.RequestDigest[0] ^= 0xff
	retry.CreatedAt = retry.CreatedAt.Add(time.Hour)
	replayed, err := store.ReserveInvocation(ctx, retry)
	if err != nil || replayed.Status != legacygateway.ReservationReplay ||
		replayed.Record.RequestEventID != candidate.RequestEventID {
		t.Fatalf("crash replay = %#v, err=%v", replayed, err)
	}

	receipt := legacygateway.CreateRunReceipt{
		RequestID:    candidate.RequestID,
		RunID:        "01890f00-0000-7000-8000-000000000050",
		Inserted:     true,
		RoutingState: legacygateway.RoutingQueued,
	}
	accepted, err := store.MarkAccepted(
		ctx,
		candidate.MatrixRoomID,
		candidate.RequestID,
		candidate.SourceDigest,
		receipt,
		time.Now(),
	)
	if err != nil || accepted.State != legacygateway.InvocationAccepted || accepted.RunID != receipt.RunID {
		t.Fatalf("accepted invocation = %#v, err=%v", accepted, err)
	}

	delivery := legacygateway.TerminalDelivery{
		MatrixRoomID: candidate.MatrixRoomID, RequestID: candidate.RequestID, RunID: receipt.RunID,
		Cursor: "terminal-1", Kind: legacygateway.TerminalResult, EventType: legacygateway.ResultEventType,
		ContentJSON: []byte(`{"request_id":"request-1"}`), MatrixTransactionID: "terminal-txn-1",
		MatrixEventID: "$terminal-1", Phase: legacygateway.TerminalSendIntent,
	}
	delivery.Digest[0] = 1
	terminal, err := store.ReserveTerminal(ctx, delivery, time.Now())
	if err != nil || terminal.Status != legacygateway.TerminalReservationInserted {
		t.Fatalf("terminal reservation = %#v, err=%v", terminal, err)
	}
	replay, err := store.ReserveTerminal(ctx, delivery, time.Now())
	if err != nil || replay.Status != legacygateway.TerminalReservationReplay {
		t.Fatalf("terminal replay = %#v, err=%v", replay, err)
	}
	for _, transition := range [][2]legacygateway.TerminalPhase{
		{legacygateway.TerminalSendIntent, legacygateway.TerminalSent},
		{legacygateway.TerminalSent, legacygateway.TerminalCommitted},
		{legacygateway.TerminalCommitted, legacygateway.TerminalSourceACK},
	} {
		advanced, err := store.AdvanceTerminal(ctx, candidate.MatrixRoomID, candidate.RequestID, delivery.Digest,
			transition[0], transition[1], time.Now())
		if err != nil || advanced.Phase != transition[1] {
			t.Fatalf("terminal advance %s -> %s = %#v, err=%v", transition[0], transition[1], advanced, err)
		}
		replayedAdvance, err := store.AdvanceTerminal(ctx, candidate.MatrixRoomID, candidate.RequestID, delivery.Digest,
			transition[0], transition[1], time.Now())
		if err != nil || replayedAdvance.Phase != transition[1] {
			t.Fatalf("terminal advance replay %s -> %s = %#v, err=%v", transition[0], transition[1], replayedAdvance, err)
		}
	}
	if pending, err := store.PendingTerminals(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("pending terminals=%d err=%v, want none", len(pending), err)
	}
}
