package dirextalkstate

import (
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
)

func TestParseRoomProfileContentNormalizesKindAndDirectDeletionFields(t *testing.T) {
	profile, err := ParseRoomProfileContent([]byte(`{
		"room_type": " io.dirextalk.room.direct ",
		"name": " Direct ",
		"dissolved": "1",
		"account_deleted": true,
		"deleted_mxid": " @owner:example.com ",
		"requester_mxid": " @owner:example.com ",
		"target_mxid": " @peer:example.com "
	}`))
	if err != nil {
		t.Fatalf("ParseRoomProfileContent returned error: %v", err)
	}
	if profile.Kind != RoomKindDirect ||
		profile.RoomType != RoomTypeDirect ||
		profile.Name != "Direct" ||
		!profile.Dissolved ||
		!profile.AccountDeleted ||
		profile.DeletedMXID != "@owner:example.com" ||
		profile.RequesterMXID != "@owner:example.com" ||
		profile.TargetMXID != "@peer:example.com" {
		t.Fatalf("unexpected room profile parse: %#v", profile)
	}
	if profile.Raw["room_type"] != " io.dirextalk.room.direct " {
		t.Fatalf("expected raw content to remain available, got %#v", profile.Raw)
	}
}

func TestParseMemberPolicyContentNormalizesStateKeyAndFields(t *testing.T) {
	stateKey := productpolicy.UserStateKey("@alice:example.com")
	policy, err := ParseMemberPolicyContent([]byte(`{
		"role": " owner ",
		"muted": "true"
	}`), &stateKey)
	if err != nil {
		t.Fatalf("ParseMemberPolicyContent returned error: %v", err)
	}
	if policy.UserID != "@alice:example.com" ||
		policy.Role != "owner" ||
		!policy.HasMuted ||
		!policy.Muted {
		t.Fatalf("unexpected member policy parse: %#v", policy)
	}
}

func TestParseJoinRequestContentUsesContentUserOrNormalizedStateKey(t *testing.T) {
	stateKey := productpolicy.UserStateKey("@state:example.com")
	fromStateKey, err := ParseJoinRequestContent([]byte(`{
		"status": " Approved ",
		"channel_id": " channel-1 "
	}`), &stateKey)
	if err != nil {
		t.Fatalf("ParseJoinRequestContent returned error: %v", err)
	}
	if fromStateKey.UserID != "@state:example.com" ||
		fromStateKey.Status != "approved" ||
		fromStateKey.ChannelID != "channel-1" {
		t.Fatalf("unexpected join request parse from state key: %#v", fromStateKey)
	}

	fromContent, err := ParseJoinRequestContent([]byte(`{
		"status": " rejected ",
		"user_id": " @content:example.com "
	}`), &stateKey)
	if err != nil {
		t.Fatalf("ParseJoinRequestContent returned error: %v", err)
	}
	if fromContent.UserID != "@content:example.com" ||
		fromContent.Status != "rejected" {
		t.Fatalf("expected content user_id to override state key, got %#v", fromContent)
	}
}
