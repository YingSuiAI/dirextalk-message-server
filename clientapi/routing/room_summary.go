// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	roomserverAPI "github.com/YingSuiAI/direxio-message-server/roomserver/api"
)

type roomSummaryResponse struct {
	CanonicalAlias    string   `json:"canonical_alias,omitempty"`
	Name              string   `json:"name,omitempty"`
	JoinedMemberCount int      `json:"num_joined_members"`
	RoomID            string   `json:"room_id"`
	Topic             string   `json:"topic,omitempty"`
	WorldReadable     bool     `json:"world_readable"`
	GuestCanJoin      bool     `json:"guest_can_join"`
	AvatarURL         string   `json:"avatar_url,omitempty"`
	JoinRule          string   `json:"join_rule,omitempty"`
	RoomType          string   `json:"room_type,omitempty"`
	AllowedRoomIDs    []string `json:"allowed_room_ids,omitempty"`
}

// GetRoomSummary implements GET /_matrix/client/v1/room_summary/{roomIdOrAlias}.
func GetRoomSummary(
	req *http.Request,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	roomIDOrAlias string,
) util.JSONResponse {
	roomID, resErr := resolveRoomIDForSummary(req, rsAPI, roomIDOrAlias)
	if resErr != nil {
		return *resErr
	}

	if _, err := rsAPI.QueryRoomVersionForRoom(req.Context(), roomID); err != nil {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("room not found"),
		}
	}

	publicRooms, err := roomserverAPI.PopulatePublicRooms(req.Context(), []string{roomID}, rsAPI)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("PopulatePublicRooms failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	summary := roomSummaryResponse{RoomID: roomID}
	if len(publicRooms) > 0 {
		publicRoom := publicRooms[0]
		summary.CanonicalAlias = publicRoom.CanonicalAlias
		summary.Name = publicRoom.Name
		summary.JoinedMemberCount = publicRoom.JoinedMembersCount
		summary.RoomID = publicRoom.RoomID
		summary.Topic = publicRoom.Topic
		summary.WorldReadable = publicRoom.WorldReadable
		summary.GuestCanJoin = publicRoom.GuestCanJoin
		summary.AvatarURL = publicRoom.AvatarURL
		summary.JoinRule = publicRoom.JoinRule
		summary.RoomType = publicRoom.RoomType
	}
	summary.RoomID = roomID

	enrichRoomSummaryWithCurrentState(req, rsAPI, roomID, &summary)

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: summary,
	}
}

func resolveRoomIDForSummary(
	req *http.Request,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	roomIDOrAlias string,
) (string, *util.JSONResponse) {
	if strings.HasPrefix(roomIDOrAlias, "#") {
		roomIDReq := roomserverAPI.GetRoomIDForAliasRequest{Alias: roomIDOrAlias}
		roomIDRes := roomserverAPI.GetRoomIDForAliasResponse{}
		if err := rsAPI.GetRoomIDForAlias(req.Context(), &roomIDReq, &roomIDRes); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("GetRoomIDForAlias failed")
			return "", &util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if roomIDRes.RoomID == "" {
			return "", &util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("room alias not found"),
			}
		}
		return roomIDRes.RoomID, nil
	}

	if _, err := spec.NewRoomID(roomIDOrAlias); err != nil {
		return "", &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("roomIdOrAlias must be a room ID or room alias"),
		}
	}
	return roomIDOrAlias, nil
}

func enrichRoomSummaryWithCurrentState(
	req *http.Request,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	roomID string,
	summary *roomSummaryResponse,
) {
	createTuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomCreate, StateKey: ""}
	joinRuleTuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomJoinRules, StateKey: ""}

	stateRes := roomserverAPI.QueryCurrentStateResponse{}
	if err := rsAPI.QueryCurrentState(req.Context(), &roomserverAPI.QueryCurrentStateRequest{
		RoomID:      roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{createTuple, joinRuleTuple},
	}, &stateRes); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("QueryCurrentState failed for room summary")
		return
	}

	if createEvent := stateRes.StateEvents[createTuple]; createEvent != nil {
		var content struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(createEvent.Content(), &content); err == nil {
			summary.RoomType = content.Type
		}
	}

	if joinRuleEvent := stateRes.StateEvents[joinRuleTuple]; joinRuleEvent != nil {
		var content gomatrixserverlib.JoinRuleContent
		if err := json.Unmarshal(joinRuleEvent.Content(), &content); err != nil {
			util.GetLogger(req.Context()).WithError(err).Warn("failed to parse join rules for room summary")
			return
		}
		if content.JoinRule != "" {
			summary.JoinRule = content.JoinRule
		}
		summary.AllowedRoomIDs = allowedRoomIDsFromJoinRule(content)
	}
}

func allowedRoomIDsFromJoinRule(content gomatrixserverlib.JoinRuleContent) []string {
	if content.JoinRule != spec.Restricted && content.JoinRule != spec.KnockRestricted {
		return nil
	}

	allowedRoomIDs := make([]string, 0, len(content.Allow))
	for _, allow := range content.Allow {
		if allow.Type != spec.MRoomMembership {
			continue
		}
		if _, err := spec.NewRoomID(allow.RoomID); err != nil {
			continue
		}
		allowedRoomIDs = append(allowedRoomIDs, allow.RoomID)
	}
	return allowedRoomIDs
}
