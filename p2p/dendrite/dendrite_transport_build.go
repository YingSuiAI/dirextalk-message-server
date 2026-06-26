package dendrite

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/eventutil"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	"github.com/YingSuiAI/direxio-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func (t *DendriteTransport) queryAndBuildEvent(
	ctx context.Context,
	proto *gomatrixserverlib.ProtoEvent,
	identity *fclient.SigningIdentity,
	eventTime time.Time,
	roomID string,
) (*types.HeaderedEvent, roomserverAPI.QueryLatestEventsAndStateResponse, error) {
	var queryRes roomserverAPI.QueryLatestEventsAndStateResponse
	event, err := eventutil.QueryAndBuildEvent(ctx, proto, identity, eventTime, t.rsAPI, &queryRes)
	if err == nil || !queryRes.RoomExists || queryRes.RoomVersion != "" {
		return event, queryRes, err
	}
	FillMissingRoomVersion(ctx, roomID, &queryRes, t.rsAPI.DefaultRoomVersion(), t.rsAPI.QueryRoomVersionForRoom)
	eventsNeeded, neededErr := gomatrixserverlib.StateNeededForProtoEvent(proto)
	if neededErr != nil {
		return nil, queryRes, neededErr
	}
	event, err = eventutil.BuildEvent(ctx, proto, identity, eventTime, &eventsNeeded, &queryRes)
	return event, queryRes, err
}

func FillMissingRoomVersion(
	ctx context.Context,
	roomID string,
	queryRes *roomserverAPI.QueryLatestEventsAndStateResponse,
	defaultVersion gomatrixserverlib.RoomVersion,
	lookup func(context.Context, string) (gomatrixserverlib.RoomVersion, error),
) {
	if queryRes == nil || queryRes.RoomVersion != "" {
		return
	}
	if lookup != nil {
		if roomVersion, err := lookup(ctx, roomID); err == nil && roomVersion != "" {
			queryRes.RoomVersion = roomVersion
			return
		}
	}
	queryRes.RoomVersion = defaultVersion
}

func matrixVisibility(value string) string {
	if strings.TrimSpace(value) == "public" {
		return "public"
	}
	return "private"
}

func matrixPreset(visibility string, direct bool) string {
	if direct {
		return spec.PresetTrustedPrivateChat
	}
	if matrixVisibility(visibility) == "public" {
		return spec.PresetPublicChat
	}
	return spec.PresetPrivateChat
}

func localpart(mxid string) string {
	if strings.HasPrefix(mxid, "@") && strings.Contains(mxid, ":") {
		return strings.TrimPrefix(mxid[:strings.LastIndex(mxid, ":")], "@")
	}
	return mxid
}

func jsonString(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return http.StatusText(http.StatusInternalServerError)
	}
	return string(raw)
}
