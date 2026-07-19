package storage

import (
	"context"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	agentevents "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentevents"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/google/uuid"
)

func TestDatabaseStoreAgentProjectionReplayAndRevisionWatermark(t *testing.T) {
	store := newAgentEventDatabaseStore(t)
	ctx := context.Background()
	source := agentevents.Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	aggregateID := uuid.NewString()

	first := agentProjectionCommit(source, aggregateID, 1, 1)
	result, err := store.Commit(ctx, first)
	if err != nil || !result.Inserted || result.Cursor != 1 {
		t.Fatalf("first commit = %#v err=%v", result, err)
	}
	replay, err := store.Commit(ctx, first)
	if err != nil || replay.Inserted || replay.Cursor != 1 {
		t.Fatalf("exact replay = %#v err=%v", replay, err)
	}

	stale := agentProjectionCommit(source, aggregateID, 2, 1)
	staleResult, err := store.Commit(ctx, stale)
	if err != nil || staleResult.Inserted || staleResult.Cursor != 2 {
		t.Fatalf("stale revision = %#v err=%v", staleResult, err)
	}

	newer := agentProjectionCommit(source, aggregateID, 3, 2)
	newerResult, err := store.Commit(ctx, newer)
	if err != nil || !newerResult.Inserted || newerResult.Cursor != 3 {
		t.Fatalf("new revision = %#v err=%v", newerResult, err)
	}
	assertAgentProjectionCounts(t, store, source, 3, 2, 1)
}

func TestDatabaseStoreOrdinaryEventSequenceFollowsCommittedAgentProjection(t *testing.T) {
	store := newAgentEventDatabaseStore(t)
	ctx := context.Background()
	source := agentevents.Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	ordinary := p2pEvent{
		Seq:       1,
		Type:      "room.member.projected",
		RoomID:    "!room:example.com",
		EventID:   "$ordinary-event",
		DedupeKey: "room.member.projected:$ordinary-event:@owner:example.com",
		CreatedAt: "2026-07-17T08:00:00Z",
	}

	agentRequest := agentProjectionCommit(source, uuid.NewString(), 1, 1)
	agentResult, err := store.Commit(ctx, agentRequest)
	if err != nil || !agentResult.Inserted {
		t.Fatalf("Agent projection commit = %#v err=%v", agentResult, err)
	}
	committed, err := store.ListEvents(ctx, 0, 10)
	if err != nil || len(committed) != 1 {
		t.Fatalf("events after Agent projection = %#v err=%v", committed, err)
	}
	if ordinary.Seq >= committed[0].Seq {
		t.Fatalf("test precondition failed: ordinary candidate=%d Agent seq=%d", ordinary.Seq, committed[0].Seq)
	}

	inserted, err := store.InsertEvent(ctx, ordinary)
	if err != nil || !inserted {
		t.Fatalf("ordinary event insert = %v err=%v", inserted, err)
	}
	committed, err = store.ListEvents(ctx, 0, 10)
	if err != nil || len(committed) != 2 {
		t.Fatalf("events after ordinary insert = %#v err=%v", committed, err)
	}
	if committed[0].DedupeKey != agentRequest.Projection.DedupeKey || committed[1].DedupeKey != ordinary.DedupeKey {
		t.Fatalf("event order = [%q, %q], want Agent projection before ordinary event", committed[0].DedupeKey, committed[1].DedupeKey)
	}
	if committed[1].Seq <= committed[0].Seq {
		t.Fatalf("ordinary seq=%d must follow Agent projection seq=%d", committed[1].Seq, committed[0].Seq)
	}
}

func TestDatabaseStoreAgentProjectionFailureRollsBackEventAndCursor(t *testing.T) {
	store := newAgentEventDatabaseStore(t)
	ctx := context.Background()
	source := agentevents.Source{AgentInstanceID: uuid.NewString(), CallerID: "dirextalk-project:example.com"}
	request := agentProjectionCommit(source, uuid.NewString(), 1, 1)

	for _, statement := range []string{
		`CREATE FUNCTION p2p_test_fail_agent_cursor_advance() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.after_seq > 0 THEN RAISE EXCEPTION 'injected cursor failure'; END IF; RETURN NEW; END; $$`,
		`CREATE TRIGGER p2p_test_fail_agent_cursor_advance BEFORE UPDATE ON p2p_agent_event_cursors FOR EACH ROW EXECUTE FUNCTION p2p_test_fail_agent_cursor_advance()`,
	} {
		if _, err := store.DB().ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Commit(ctx, request); err == nil {
		t.Fatal("injected cursor failure unexpectedly committed")
	}
	assertAgentProjectionCounts(t, store, source, 0, 0, 0)

	for _, statement := range []string{
		`DROP TRIGGER p2p_test_fail_agent_cursor_advance ON p2p_agent_event_cursors`,
		`DROP FUNCTION p2p_test_fail_agent_cursor_advance()`,
	} {
		if _, err := store.DB().ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.Commit(ctx, request)
	if err != nil || !result.Inserted || result.Cursor != 1 {
		t.Fatalf("retry after rollback = %#v err=%v", result, err)
	}
	assertAgentProjectionCounts(t, store, source, 1, 1, 1)
}

func newAgentEventDatabaseStore(t *testing.T) *DatabaseStore {
	t.Helper()
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(closeDB)
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func agentProjectionCommit(source agentevents.Source, aggregateID string, sourceSeq, revision int64) agentevents.CommitRequest {
	eventID := uuid.NewString()
	occurredAt := time.Date(2026, time.July, 17, 8, 0, int(sourceSeq), 0, time.UTC)
	event := agentevents.Event{
		Seq: sourceSeq, EventID: eventID, EventType: "cloud.plan.changed", AggregateType: "cloud_plan",
		AggregateID: aggregateID, Revision: revision, SummaryJSON: []byte(`{}`), OccurredAt: occurredAt,
	}
	return agentevents.CommitRequest{
		Source: source, Event: event,
		Projection: &agentevents.Projection{
			SourceEventSeq: sourceSeq, Type: event.EventType, EventID: eventID,
			DedupeKey: "agent-event:" + source.AgentInstanceID + ":" + eventID,
			Payload: map[string]any{
				"plan_id": aggregateID, "owner_id": source.CallerID, "revision": revision, "status": "planning",
			},
			CreatedAt: occurredAt,
		},
	}
}

func assertAgentProjectionCounts(t *testing.T, store *DatabaseStore, source agentevents.Source, wantCursor, wantEvents, wantRevisions int64) {
	t.Helper()
	cursor, err := store.Cursor(context.Background(), source)
	if err != nil || cursor != wantCursor {
		t.Fatalf("cursor=%d want=%d err=%v", cursor, wantCursor, err)
	}
	var events, revisions int64
	if err := store.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM p2p_events WHERE dedupe_key LIKE $1`, "agent-event:"+source.AgentInstanceID+":%").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM p2p_agent_projection_revisions WHERE agent_instance_id=$1 AND caller_id=$2`, source.AgentInstanceID, source.CallerID).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if events != wantEvents || revisions != wantRevisions {
		t.Fatalf("projection rows events=%d revisions=%d; want %d/%d", events, revisions, wantEvents, wantRevisions)
	}
}
