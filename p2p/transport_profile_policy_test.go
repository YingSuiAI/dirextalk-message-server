package p2p

import (
	"context"
	"errors"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/matrix-org/gomatrixserverlib"
	"testing"
)

func TestMemberMutePublishesMemberPolicyState(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch",
		"name":       "Channel",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@alice:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "channels.member.mute", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
		"user_id":    "@alice:example.com",
	})

	memberPolicyStates := recordedStatesOfType(transport.stateEvents, DirextalkMemberPolicyEventType)
	if len(memberPolicyStates) != 1 {
		t.Fatalf("expected member policy state event, got %#v", memberPolicyStates)
	}
	state := memberPolicyStates[0]
	if state.Event.StateKey != productpolicy.UserStateKey("@alice:example.com") || state.Event.Content["role"] != "member" || state.Event.Content["muted"] != true {
		t.Fatalf("expected muted member policy state, got %#v", state)
	}
}

func TestChannelMutePublishesMemberPolicyStateForAffectedMembers(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "ch",
		"name":       "Channel",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@alice:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     ch.RoomID,
		ChannelID:  ch.ChannelID,
		UserID:     "@bob:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "channels.mute", map[string]any{
		"channel_id": ch.ChannelID,
		"room_id":    ch.RoomID,
	})

	memberPolicyStates := recordedStatesOfType(transport.stateEvents, DirextalkMemberPolicyEventType)
	if len(memberPolicyStates) != 2 {
		t.Fatalf("expected member policy state events for all non-owner members, got %#v", memberPolicyStates)
	}
	mutedByUser := map[string]RoomStateEvent{}
	for _, state := range memberPolicyStates {
		mutedByUser[state.Event.StateKey] = state.Event
	}
	for _, userID := range []string{"@alice:example.com", "@bob:example.com"} {
		state, ok := mutedByUser[productpolicy.UserStateKey(userID)]
		if !ok || state.Content["role"] != "member" || state.Content["muted"] != true {
			t.Fatalf("expected muted member policy state for %s as regular member, got %#v", userID, state)
		}
	}
}

func TestGroupMutePublishesMemberPolicyStateForAffectedMembers(t *testing.T) {
	transport := &recordingTransport{roomID: "!group:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"name": "Group",
	})
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@alice:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID:     group.RoomID,
		UserID:     "@bob:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}

	mustHandle[map[string]any](t, service, "groups.mute", map[string]any{
		"room_id": group.RoomID,
	})

	memberPolicyStates := recordedStatesOfType(transport.stateEvents, DirextalkMemberPolicyEventType)
	if len(memberPolicyStates) != 2 {
		t.Fatalf("expected member policy state events for all non-owner members, got %#v", memberPolicyStates)
	}
	mutedByUser := map[string]RoomStateEvent{}
	for _, state := range memberPolicyStates {
		mutedByUser[state.Event.StateKey] = state.Event
	}
	for _, userID := range []string{"@alice:example.com", "@bob:example.com"} {
		state, ok := mutedByUser[productpolicy.UserStateKey(userID)]
		if !ok || state.Content["role"] != "member" || state.Content["muted"] != true {
			t.Fatalf("expected muted member policy state for %s as regular member, got %#v", userID, state)
		}
	}
}

func TestProfileUpdatePublishesMemberProfileState(t *testing.T) {
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "ch", "name": "Channel"})

	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner New",
		"avatar_url":   "mxc://example.com/avatar",
	})

	if len(transport.profiles) != 1 || transport.profiles[0] != "@owner:example.com in "+ch.RoomID+" as Owner New mxc://example.com/avatar" {
		t.Fatalf("expected profile member state through transport, got %#v", transport.profiles)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": ch.ChannelID})["members"].([]memberRecord)
	owner := findMember(members, "@owner:example.com")
	if owner.DisplayName != "Owner New" || owner.AvatarURL != "mxc://example.com/avatar" {
		t.Fatalf("expected owner member profile refreshed, got %#v", owner)
	}
}

func TestProfileUpdateIgnoresSingleRoomMemberProfileFailure(t *testing.T) {
	transport := &recordingTransport{
		roomID:        "!first:example.com",
		profileErrors: map[string]error{"!first:example.com": errors.New("stale room")},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	first := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "first", "name": "First"})
	transport.roomID = "!second:example.com"
	second := mustHandle[channel](t, service, "channels.create", map[string]any{"channel_id": "second", "name": "Second"})

	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner Best Effort",
		"avatar_url":   "mxc://example.com/best-effort",
	})

	if len(transport.profiles) != 2 {
		t.Fatalf("expected both room profile updates attempted, got %#v", transport.profiles)
	}
	members := mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": first.ChannelID})["members"].([]memberRecord)
	if owner := findMember(members, "@owner:example.com"); owner.DisplayName != "Owner Best Effort" || owner.AvatarURL != "mxc://example.com/best-effort" {
		t.Fatalf("expected local member profile refreshed for failed transport room, got %#v", owner)
	}
	members = mustHandle[map[string]any](t, service, "channels.members", map[string]any{"channel_id": second.ChannelID})["members"].([]memberRecord)
	if owner := findMember(members, "@owner:example.com"); owner.DisplayName != "Owner Best Effort" || owner.AvatarURL != "mxc://example.com/best-effort" {
		t.Fatalf("expected local member profile refreshed for successful transport room, got %#v", owner)
	}
}

func TestFillMissingRoomVersionPrefersLookupBeforeDefault(t *testing.T) {
	ctx := context.Background()
	queryRes := roomserverAPI.QueryLatestEventsAndStateResponse{RoomExists: true}
	fillMissingRoomVersion(
		ctx,
		"!room:example.com",
		&queryRes,
		gomatrixserverlib.RoomVersionV10,
		func(context.Context, string) (gomatrixserverlib.RoomVersion, error) {
			return gomatrixserverlib.RoomVersionV11, nil
		},
	)
	if queryRes.RoomVersion != gomatrixserverlib.RoomVersionV11 {
		t.Fatalf("expected lookup room version, got %q", queryRes.RoomVersion)
	}

	queryRes = roomserverAPI.QueryLatestEventsAndStateResponse{RoomExists: true}
	fillMissingRoomVersion(
		ctx,
		"!room:example.com",
		&queryRes,
		gomatrixserverlib.RoomVersionV10,
		func(context.Context, string) (gomatrixserverlib.RoomVersion, error) {
			return "", errors.New("missing room version")
		},
	)
	if queryRes.RoomVersion != gomatrixserverlib.RoomVersionV10 {
		t.Fatalf("expected default room version fallback, got %q", queryRes.RoomVersion)
	}
}
