package setup

import (
	"context"

	syncstorage "github.com/YingSuiAI/dirextalk-message-server/syncapi/storage"
)

type syncReadMarkerPositionResolver struct {
	db syncstorage.Database
}

func (r syncReadMarkerPositionResolver) ResolveReadMarkerPosition(
	ctx context.Context, roomID, eventID string,
) (topologicalPosition, streamPosition, originServerTS int64, found bool, err error) {
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
	position, stream, err := snapshot.PositionInTopology(ctx, eventID)
	if err != nil {
		return 0, 0, 0, false, err
	}
	return int64(position), int64(stream), int64(events[0].OriginServerTS()), true, nil
}
