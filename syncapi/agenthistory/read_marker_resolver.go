package agenthistory

import (
	"context"
	"errors"
	"strings"

	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/storage"
)

type ReadMarkerPositionResolver struct {
	db        storage.Database
	rsAPI     roomserverAPI.SyncRoomserverAPI
	ownerMXID string
}

func NewReadMarkerPositionResolver(
	db storage.Database, rsAPI roomserverAPI.SyncRoomserverAPI, ownerMXID string,
) *ReadMarkerPositionResolver {
	return &ReadMarkerPositionResolver{
		db:        db,
		rsAPI:     rsAPI,
		ownerMXID: strings.TrimSpace(ownerMXID),
	}
}

func (r *ReadMarkerPositionResolver) ResolveReadMarkerPosition(
	ctx context.Context, roomID, eventID string,
) (topologicalPosition, streamPosition, originServerTS int64, found bool, err error) {
	if r == nil || r.db == nil || r.rsAPI == nil || r.ownerMXID == "" {
		return 0, 0, 0, false, errors.New("authorized read marker resolver is unavailable")
	}
	snapshot, err := r.db.NewDatabaseSnapshot(ctx)
	if err != nil {
		return 0, 0, 0, false, err
	}
	defer snapshot.Rollback() //nolint:errcheck

	events, err := snapshot.Events(ctx, []string{eventID})
	if err != nil {
		return 0, 0, 0, false, err
	}
	if len(events) != 1 || events[0] == nil || events[0].RoomID().String() != roomID {
		return 0, 0, 0, false, nil
	}
	reader := Reader{DB: r.db, RSAPI: r.rsAPI, UserID: r.ownerMXID}
	events, err = reader.filterVisibleEvents(ctx, snapshot, roomID, events)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if len(events) != 1 {
		return 0, 0, 0, false, nil
	}
	position, stream, err := snapshot.PositionInTopology(ctx, eventID)
	if err != nil {
		return 0, 0, 0, false, err
	}
	return int64(position), int64(stream), int64(events[0].OriginServerTS()), true, nil
}
