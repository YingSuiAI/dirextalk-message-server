package p2p

import (
	"context"

	"github.com/YingSuiAI/direxio-message-server/p2p/dendrite"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/matrix-org/gomatrixserverlib"
)

func fillMissingRoomVersion(
	ctx context.Context,
	roomID string,
	queryRes *roomserverAPI.QueryLatestEventsAndStateResponse,
	defaultVersion gomatrixserverlib.RoomVersion,
	lookup func(context.Context, string) (gomatrixserverlib.RoomVersion, error),
) {
	dendrite.FillMissingRoomVersion(ctx, roomID, queryRes, defaultVersion, lookup)
}
