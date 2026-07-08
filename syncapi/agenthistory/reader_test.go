package agenthistory

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	matrixhistory "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	rstypes "github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/storage"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/types"
	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestListOrdinaryMessagesScansPastFilteredSyncEvents(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := context.Background()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		ordinary := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"body": "older ordinary message",
		})
		for i := 0; i < 5; i++ {
			room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
				"body":     fmt.Sprintf("channel post %d", i),
				"p2p_kind": "channel_post",
			})
		}

		db, closeDB := mustCreateSyncDatabase(t, dbType)
		defer closeDB()
		mustWriteSyncEvents(t, db, room.Events())

		result, err := NewReader(db, nil, alice.ID).ListOrdinaryMessages(ctx, room.ID, matrixhistory.Page{
			SnapshotTS: time.Now().Add(time.Hour).UnixMilli(),
			Limit:      1,
		})
		if err != nil {
			t.Fatalf("ListOrdinaryMessages returned error: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Fatalf("got %d messages, want 1: %#v", len(result.Messages), result.Messages)
		}
		if result.Messages[0].EventID != ordinary.EventID() {
			t.Fatalf("got event %s, want %s", result.Messages[0].EventID, ordinary.EventID())
		}
		if result.HasMore {
			t.Fatalf("got has_more=true, want false when only one ordinary message exists")
		}
	})
}

func TestListOrdinaryMessagesFiltersLocalHiddenSyncEvents(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := context.Background()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		visible := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"body": "visible ordinary message",
		})
		hidden := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"body": "locally hidden message",
		})

		db, closeDB := mustCreateSyncDatabase(t, dbType)
		defer closeDB()
		mustWriteSyncEvents(t, db, room.Events())
		if err := db.HideLocalEvents(ctx, alice.ID, room.ID, []string{hidden.EventID()}); err != nil {
			t.Fatalf("HideLocalEvents returned error: %v", err)
		}

		result, err := NewReader(db, nil, alice.ID).ListOrdinaryMessages(ctx, room.ID, matrixhistory.Page{
			SnapshotTS: time.Now().Add(time.Hour).UnixMilli(),
			Limit:      10,
		})
		if err != nil {
			t.Fatalf("ListOrdinaryMessages returned error: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Fatalf("got %d messages, want 1: %#v", len(result.Messages), result.Messages)
		}
		if result.Messages[0].EventID != visible.EventID() {
			t.Fatalf("got event %s, want %s", result.Messages[0].EventID, visible.EventID())
		}
	})
}

func TestListOrdinaryMessagesPaginatesWithStableCursor(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := context.Background()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
		older := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"body": "older ordinary message",
		}, test.WithTimestamp(base))
		newer := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"body": "newer ordinary message",
		}, test.WithTimestamp(base.Add(time.Second)))

		db, closeDB := mustCreateSyncDatabase(t, dbType)
		defer closeDB()
		mustWriteSyncEvents(t, db, room.Events())

		reader := NewReader(db, nil, alice.ID)
		first, err := reader.ListOrdinaryMessages(ctx, room.ID, matrixhistory.Page{
			SnapshotTS: base.Add(time.Hour).UnixMilli(),
			Limit:      1,
		})
		if err != nil {
			t.Fatalf("first ListOrdinaryMessages returned error: %v", err)
		}
		if len(first.Messages) != 1 || first.Messages[0].EventID != newer.EventID() || !first.HasMore {
			t.Fatalf("unexpected first page: %#v", first)
		}

		second, err := reader.ListOrdinaryMessages(ctx, room.ID, matrixhistory.Page{
			SnapshotTS: base.Add(time.Hour).UnixMilli(),
			CursorTS:   first.Messages[0].OriginServerTS,
			CursorID:   first.Messages[0].EventID,
			Limit:      1,
		})
		if err != nil {
			t.Fatalf("second ListOrdinaryMessages returned error: %v", err)
		}
		if len(second.Messages) != 1 || second.Messages[0].EventID != older.EventID() || second.HasMore {
			t.Fatalf("unexpected second page: %#v", second)
		}
	})
}

func TestListOrdinaryMessagesAppliesRoomserverHistoryVisibility(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := context.Background()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityInvited))
		hidden := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"body": "hidden before invite",
		})
		visible := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"body": "visible after invite",
		})

		db, closeDB := mustCreateSyncDatabase(t, dbType)
		defer closeDB()
		mustWriteSyncEvents(t, db, room.Events())

		rsAPI := &agentHistoryVisibilityRoomserver{
			t:      t,
			roomID: room.ID,
			userID: alice.ID,
			membershipByEventID: map[string]string{
				hidden.EventID():  spec.Leave,
				visible.EventID(): spec.Invite,
			},
		}
		result, err := NewReader(db, rsAPI, alice.ID).ListOrdinaryMessages(ctx, room.ID, matrixhistory.Page{
			SnapshotTS: time.Now().Add(time.Hour).UnixMilli(),
			Limit:      10,
		})
		if err != nil {
			t.Fatalf("ListOrdinaryMessages returned error: %v", err)
		}
		if len(result.Messages) != 1 {
			t.Fatalf("got %d messages, want 1: %#v", len(result.Messages), result.Messages)
		}
		if result.Messages[0].EventID != visible.EventID() {
			t.Fatalf("got event %s, want %s", result.Messages[0].EventID, visible.EventID())
		}
	})
}

type agentHistoryVisibilityRoomserver struct {
	roomserverAPI.SyncRoomserverAPI
	t                   *testing.T
	roomID              string
	userID              string
	membershipByEventID map[string]string
}

func (s *agentHistoryVisibilityRoomserver) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderIDFromUserID(userID)
	return &senderID, nil
}

func (s *agentHistoryVisibilityRoomserver) QueryMembershipAtEvent(ctx context.Context, roomID spec.RoomID, eventIDs []string, senderID spec.SenderID) (map[string]*rstypes.HeaderedEvent, error) {
	result := make(map[string]*rstypes.HeaderedEvent, len(eventIDs))
	if roomID.String() != s.roomID || string(senderID) != s.userID {
		return result, nil
	}
	for _, eventID := range eventIDs {
		membership := s.membershipByEventID[eventID]
		if membership == "" {
			continue
		}
		result[eventID] = mustCreateMembershipEvent(s.t, s.roomID, s.userID, membership)
	}
	return result, nil
}

func mustCreateMembershipEvent(t *testing.T, roomID, userID, membership string) *rstypes.HeaderedEvent {
	t.Helper()
	eventJSON, err := json.Marshal(map[string]any{
		"type":      spec.MRoomMember,
		"state_key": userID,
		"room_id":   roomID,
		"sender":    userID,
		"content": map[string]any{
			"membership": membership,
		},
	})
	if err != nil {
		t.Fatalf("failed to marshal membership event: %v", err)
	}
	event, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV9).NewEventFromTrustedJSON(eventJSON, false)
	if err != nil {
		t.Fatalf("failed to create membership event: %v", err)
	}
	return &rstypes.HeaderedEvent{PDU: event}
}

func mustCreateSyncDatabase(t *testing.T, dbType test.DBType) (storage.Database, func()) {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	})
	if err != nil {
		closeDB()
		t.Fatalf("NewSyncServerDatasource returned error: %v", err)
	}
	return db, closeDB
}

func mustWriteSyncEvents(t *testing.T, db storage.Database, events []*rstypes.HeaderedEvent) []types.StreamPosition {
	t.Helper()
	positions := make([]types.StreamPosition, 0, len(events))
	for _, ev := range events {
		var addStateEvents []*rstypes.HeaderedEvent
		var addStateEventIDs []string
		if ev.StateKey() != nil {
			ev.StateKeyResolved = ev.StateKey()
			addStateEvents = append(addStateEvents, ev)
			addStateEventIDs = append(addStateEventIDs, ev.EventID())
		}
		visibility := ev.Visibility
		if visibility == "" {
			visibility = gomatrixserverlib.HistoryVisibilityShared
		}
		pos, err := db.WriteEvent(context.Background(), ev, addStateEvents, addStateEventIDs, nil, nil, false, visibility)
		if err != nil {
			t.Fatalf("WriteEvent(%s) returned error: %v", ev.EventID(), err)
		}
		positions = append(positions, pos)
	}
	return positions
}
