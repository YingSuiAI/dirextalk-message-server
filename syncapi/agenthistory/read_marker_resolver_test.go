package agenthistory

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestReadMarkerPositionResolverAuthorizesBoundOwner(t *testing.T) {
	ctx := context.Background()
	owner := test.NewUser(t)
	other := test.NewUser(t)
	stranger := test.NewUser(t)

	joinedRoom := test.NewRoom(t, owner, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityJoined))
	joinedEvent := joinedRoom.CreateAndInsert(t, owner, "m.room.message", map[string]any{"body": "joined"})

	leftRoom := test.NewRoom(t, other, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityJoined))
	leftRoom.CreateAndInsert(t, other, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(owner.ID))
	leftRoom.CreateAndInsert(t, owner, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(owner.ID))
	leftRoom.CreateAndInsert(t, owner, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(owner.ID))
	afterLeaveEvent := leftRoom.CreateAndInsert(t, other, "m.room.message", map[string]any{"body": "after leave"})

	db, closeDB := mustCreateSyncDatabase(t, test.DBTypePostgres)
	defer closeDB()
	mustWriteSyncEvents(t, db, joinedRoom.Events())
	mustWriteSyncEvents(t, db, leftRoom.Events())

	tests := []struct {
		name          string
		ownerMXID     string
		requestRoomID string
		eventID       string
		rsRoomID      string
		membership    string
		wantFound     bool
	}{
		{
			name: "joined owner", ownerMXID: owner.ID, requestRoomID: joinedRoom.ID,
			eventID: joinedEvent.EventID(), rsRoomID: joinedRoom.ID, membership: spec.Join,
			wantFound: true,
		},
		{
			name: "nonmember uses bound identity", ownerMXID: stranger.ID, requestRoomID: joinedRoom.ID,
			eventID: joinedEvent.EventID(), rsRoomID: joinedRoom.ID,
		},
		{
			name: "left owner cannot mark post-leave event", ownerMXID: owner.ID, requestRoomID: leftRoom.ID,
			eventID: afterLeaveEvent.EventID(), rsRoomID: leftRoom.ID, membership: spec.Leave,
		},
		{
			name: "cross-room event binding", ownerMXID: owner.ID, requestRoomID: leftRoom.ID,
			eventID: joinedEvent.EventID(), rsRoomID: joinedRoom.ID, membership: spec.Join,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			membershipByEventID := map[string]string{}
			if tt.membership != "" {
				membershipByEventID[tt.eventID] = tt.membership
			}
			rsAPI := &agentHistoryVisibilityRoomserver{
				t: t, roomID: tt.rsRoomID, userID: tt.ownerMXID,
				membershipByEventID: membershipByEventID,
			}
			resolver := NewReadMarkerPositionResolver(db, rsAPI, tt.ownerMXID)
			topology, stream, timestamp, found, err := resolver.ResolveReadMarkerPosition(
				ctx, tt.requestRoomID, tt.eventID,
			)
			if err != nil {
				t.Fatalf("ResolveReadMarkerPosition returned error: %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("found = %v, want %v", found, tt.wantFound)
			}
			if tt.wantFound {
				if topology <= 0 || stream <= 0 || timestamp != int64(joinedEvent.OriginServerTS()) {
					t.Fatalf("authorized position = (%d, %d, %d), want joined event position/timestamp", topology, stream, timestamp)
				}
			} else if topology != 0 || stream != 0 || timestamp != 0 {
				t.Fatalf("unauthorized resolution leaked metadata: (%d, %d, %d)", topology, stream, timestamp)
			}
		})
	}
}
