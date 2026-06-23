package routing

import (
	"fmt"
	"net/http"
	"strings"

	clienthttputil "github.com/YingSuiAI/direxio-message-server/clientapi/httputil"
	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
	rstypes "github.com/YingSuiAI/direxio-message-server/roomserver/types"
	"github.com/YingSuiAI/direxio-message-server/syncapi/internal"
	"github.com/YingSuiAI/direxio-message-server/syncapi/storage"
	"github.com/YingSuiAI/direxio-message-server/syncapi/types"
	userapi "github.com/YingSuiAI/direxio-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

type localDeleteRequest struct {
	EventIDs []string `json:"event_ids,omitempty"`
	Clear    bool     `json:"clear,omitempty"`
}

type localDeleteResponse struct {
	RoomID                string               `json:"room_id"`
	HiddenEventIDs        []string             `json:"hidden_event_ids,omitempty"`
	Clear                 bool                 `json:"clear,omitempty"`
	ThroughStreamPosition types.StreamPosition `json:"through_stream_pos,omitempty"`
}

func LocalDelete(
	req *http.Request,
	device *userapi.Device,
	syncDB storage.Database,
	rsAPI roomserverAPI.SyncRoomserverAPI,
	rawRoomID string,
) util.JSONResponse {
	ctx := req.Context()
	roomID, err := spec.NewRoomID(rawRoomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid room ID"),
		}
	}

	var body localDeleteRequest
	if resErr := clienthttputil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}
	if body.Clear == (len(body.EventIDs) > 0) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("provide either clear=true or a non-empty event_ids array"),
		}
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Device UserID is invalid"),
		}
	}
	membershipResp, err := getMembershipForUser(ctx, rawRoomID, device.UserID, rsAPI)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !membershipResp.RoomExists || !membershipResp.HasBeenInRoom || membershipResp.IsRoomForgotten {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("room is not available to this user"),
		}
	}

	snapshot, err := syncDB.NewDatabaseSnapshot(ctx)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	defer snapshot.Rollback() // nolint:errcheck

	if body.Clear {
		pos, posErr := snapshot.MaxStreamPositionForPDUs(ctx)
		if posErr != nil {
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if err = syncDB.ClearLocalRoom(ctx, device.UserID, roomID.String(), pos); err != nil {
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: localDeleteResponse{
				RoomID:                roomID.String(),
				Clear:                 true,
				ThroughStreamPosition: pos,
			},
		}
	}

	eventIDs := uniqueNonEmptyStrings(body.EventIDs)
	if len(eventIDs) == 0 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("event_ids must contain at least one event ID"),
		}
	}
	events, err := snapshot.Events(ctx, eventIDs)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if len(events) != len(eventIDs) {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("one or more events were not found"),
		}
	}
	if err = requireEventsInRoom(events, roomID.String()); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}
	visibleEvents, err := internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, events, nil, *userID, "local_delete")
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if len(visibleEvents) != len(events) {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("one or more events were not found or are not visible"),
		}
	}
	if err = syncDB.HideLocalEvents(ctx, device.UserID, roomID.String(), eventIDs); err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: localDeleteResponse{
			RoomID:         roomID.String(),
			HiddenEventIDs: eventIDs,
		},
	}
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func requireEventsInRoom(events []*rstypes.HeaderedEvent, roomID string) error {
	for _, event := range events {
		if event == nil {
			continue
		}
		if event.RoomID().String() != roomID {
			return fmt.Errorf("all event IDs must belong to the requested room")
		}
	}
	return nil
}
