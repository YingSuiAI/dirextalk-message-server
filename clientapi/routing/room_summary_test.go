// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"reflect"
	"testing"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestAllowedRoomIDsFromJoinRule(t *testing.T) {
	allowedRoomIDs := allowedRoomIDsFromJoinRule(gomatrixserverlib.JoinRuleContent{
		JoinRule: spec.Restricted,
		Allow: []gomatrixserverlib.JoinRuleContentAllowRule{
			{Type: spec.MRoomMembership, RoomID: "!space:example.com"},
			{Type: "org.example.other", RoomID: "!ignored:example.com"},
			{Type: spec.MRoomMembership, RoomID: "not-a-room-id"},
		},
	})

	if want := []string{"!space:example.com"}; !reflect.DeepEqual(allowedRoomIDs, want) {
		t.Fatalf("allowed room IDs mismatch: got %v want %v", allowedRoomIDs, want)
	}
}

func TestAllowedRoomIDsFromJoinRuleOmitsNonRestrictedRooms(t *testing.T) {
	allowedRoomIDs := allowedRoomIDsFromJoinRule(gomatrixserverlib.JoinRuleContent{
		JoinRule: spec.Invite,
		Allow: []gomatrixserverlib.JoinRuleContentAllowRule{
			{Type: spec.MRoomMembership, RoomID: "!space:example.com"},
		},
	})

	if allowedRoomIDs != nil {
		t.Fatalf("expected no allowed room IDs for non-restricted room, got %v", allowedRoomIDs)
	}
}
