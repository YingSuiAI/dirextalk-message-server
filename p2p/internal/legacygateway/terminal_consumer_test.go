package legacygateway_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestTerminalConsumerReconcilesLostMatrixResponseWithoutDuplicateEvent(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	candidate := terminalInvocationCandidate()
	if _, err := store.ReserveInvocation(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	runID := "run-1"
	if _, err := store.MarkAccepted(ctx, candidate.MatrixRoomID, candidate.RequestID, candidate.SourceDigest,
		legacygateway.CreateRunReceipt{RequestID: candidate.RequestID, RunID: runID, Inserted: true, RoutingState: legacygateway.RoutingQueued}, time.Now()); err != nil {
		t.Fatal(err)
	}

	fact := legacygateway.RunTerminalFact{
		MatrixRoomID: candidate.MatrixRoomID, RequestID: candidate.RequestID, RunID: runID,
		Cursor: "terminal-1", Kind: legacygateway.TerminalResult, ConnectorID: "connector-1", Outcome: "succeeded",
		ResultReference: []byte(`{"artifact":"result-1"}`), EvidenceReference: []byte(`{"receipt":"evidence-1"}`),
	}
	fact.Digest = legacygateway.DigestRunTerminalFact(fact)
	source := &terminalSource{fact: fact}
	sender := &lossyMatrixSender{events: make(map[string]legacygateway.MatrixTerminalEvent), loseFirstResponse: true}
	consumer, err := legacygateway.NewTerminalConsumer(store, source, sender)
	if err != nil {
		t.Fatal(err)
	}

	if err := consumer.ProcessOnce(ctx); err == nil {
		t.Fatal("first delivery should observe the simulated lost Matrix response")
	}
	if err := consumer.ProcessOnce(ctx); err != nil {
		t.Fatalf("reconcile delivery: %v", err)
	}
	if sender.calls != 2 || len(sender.events) != 1 {
		t.Fatalf("Matrix sends=%d unique events=%d, want 2 retries and 1 event", sender.calls, len(sender.events))
	}
	if source.acks != 1 || source.nextCalls != 1 {
		t.Fatalf("source next=%d ACK=%d, want one source read and one ACK", source.nextCalls, source.acks)
	}
	pending, err := store.PendingTerminals(ctx, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending terminals=%d err=%v, want none", len(pending), err)
	}
}

func TestTerminalConsumerRejectsRunBindingMismatchBeforeMatrixSend(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	candidate := terminalInvocationCandidate()
	_, _ = store.ReserveInvocation(ctx, candidate)
	_, _ = store.MarkAccepted(ctx, candidate.MatrixRoomID, candidate.RequestID, candidate.SourceDigest,
		legacygateway.CreateRunReceipt{RequestID: candidate.RequestID, RunID: "stored-run", Inserted: true, RoutingState: legacygateway.RoutingQueued}, time.Now())
	fact := legacygateway.RunTerminalFact{MatrixRoomID: candidate.MatrixRoomID, RequestID: candidate.RequestID,
		RunID: "other-run", Cursor: "terminal-2", Kind: legacygateway.TerminalError, ErrorCode: "run_failed"}
	fact.Digest = legacygateway.DigestRunTerminalFact(fact)
	sender := &lossyMatrixSender{events: make(map[string]legacygateway.MatrixTerminalEvent)}
	consumer, _ := legacygateway.NewTerminalConsumer(store, &terminalSource{fact: fact}, sender)
	if err := consumer.ProcessOnce(ctx); !errors.Is(err, legacygateway.ErrInvocationNotFound) {
		t.Fatalf("binding mismatch error=%v, want invocation not found", err)
	}
	if sender.calls != 0 {
		t.Fatalf("Matrix sends=%d, want none", sender.calls)
	}
}

type terminalSource struct {
	fact      legacygateway.RunTerminalFact
	nextCalls int
	acks      int
}

func (source *terminalSource) Next(context.Context) (legacygateway.RunTerminalFact, error) {
	source.nextCalls++
	return source.fact, nil
}

func (source *terminalSource) ACK(_ context.Context, cursor string) error {
	if cursor != source.fact.Cursor {
		return errors.New("unexpected terminal cursor")
	}
	source.acks++
	return nil
}

type lossyMatrixSender struct {
	events            map[string]legacygateway.MatrixTerminalEvent
	calls             int
	loseFirstResponse bool
}

func (sender *lossyMatrixSender) SendTerminal(_ context.Context, event legacygateway.MatrixTerminalEvent) (string, error) {
	sender.calls++
	if stored, ok := sender.events[event.EventID]; ok {
		if stored.RoomID != event.RoomID || stored.EventType != event.EventType ||
			stored.TransactionID != event.TransactionID || !bytes.Equal(stored.ContentJSON, event.ContentJSON) {
			return "", errors.New("non-identical Matrix event replay")
		}
	} else {
		sender.events[event.EventID] = event
	}
	if sender.loseFirstResponse {
		sender.loseFirstResponse = false
		return "", errors.New("simulated Matrix response loss")
	}
	return event.EventID, nil
}

func terminalInvocationCandidate() legacygateway.InvocationCandidate {
	return legacygateway.InvocationCandidate{
		MatrixRoomID: "!agent:example.test", RequestID: "request-1", MatrixInvokeEventID: "$invoke",
		MatrixInputEventID: "$input", TenantID: "tenant-1", InstallationID: "install-1",
		ConversationID: "conversation-1", RequestEventID: "request-event-1", DispatchMode: legacygateway.DispatchSingle,
		CreatedAt: time.Now(),
	}
}
