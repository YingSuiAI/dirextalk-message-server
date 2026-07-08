package dirextalkprojection

import (
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func TestGroupProfileMergesContentWithExistingDefaults(t *testing.T) {
	group := GroupProfile("!group:example.com", dirextalkdomain.GroupRecord{
		Name:         "Existing",
		Topic:        "Old topic",
		AvatarURL:    "mxc://old",
		MemberCount:  3,
		InvitePolicy: "owner",
		Muted:        false,
	}, map[string]any{
		"name":          " New Group ",
		"avatar_url":    " mxc://new ",
		"invite_policy": "",
		"muted":         "true",
	})

	if group.RoomID != "!group:example.com" ||
		group.Name != "New Group" ||
		group.Topic != "Old topic" ||
		group.AvatarURL != "mxc://new" ||
		group.MemberCount != 3 ||
		group.InvitePolicy != "owner" ||
		!group.Muted {
		t.Fatalf("unexpected group profile projection: %#v", group)
	}
}

func TestChannelProfileMergesContentWithExistingDefaults(t *testing.T) {
	channel := ChannelProfile("!channel:example.com", "ch", dirextalkdomain.Channel{
		Name:             "Existing",
		Description:      "Old description",
		AvatarURL:        "mxc://old",
		Visibility:       "public",
		JoinPolicy:       "approval",
		ChannelType:      "chat",
		CommentsEnabled:  true,
		Muted:            false,
		MemberCount:      5,
		PendingJoinCount: 2,
	}, map[string]any{
		"name":             " New Channel ",
		"description":      " New description ",
		"comments_enabled": false,
		"muted":            "1",
	})

	if channel.ChannelID != "ch" ||
		channel.RoomID != "!channel:example.com" ||
		channel.Name != "New Channel" ||
		channel.Description != "New description" ||
		channel.AvatarURL != "mxc://old" ||
		channel.Visibility != "public" ||
		channel.JoinPolicy != "approval" ||
		channel.ChannelType != "chat" ||
		channel.CommentsEnabled ||
		!channel.Muted ||
		channel.MemberCount != 5 ||
		channel.PendingJoinCount != 2 {
		t.Fatalf("unexpected channel profile projection: %#v", channel)
	}
}

func TestMemberPolicyCreatesDefaultMemberAndAppliesFields(t *testing.T) {
	now := time.UnixMilli(123456)
	member := MemberPolicy("!room:example.com", "@alice:example.com", dirextalkdomain.MemberRecord{}, false, map[string]any{
		"role":  " owner ",
		"muted": "true",
	}, now)

	if member.RoomID != "!room:example.com" ||
		member.UserID != "@alice:example.com" ||
		member.Domain != "example.com" ||
		member.Membership != "join" ||
		member.Role != "owner" ||
		!member.Muted ||
		member.JoinedAt != now.UnixMilli() {
		t.Fatalf("unexpected member policy projection: %#v", member)
	}
}

func TestJoinRequestMemberMapsStatusAndPreservesExistingFields(t *testing.T) {
	now := time.UnixMilli(654321)
	member, valid := JoinRequestMember("!room:example.com", "ch", "@alice:example.com", dirextalkdomain.MemberRecord{
		Role:   "owner",
		Domain: "custom.example",
	}, true, map[string]any{"status": " Approved "}, now)
	if !valid {
		t.Fatal("expected join request status to be valid")
	}
	if member.RoomID != "!room:example.com" ||
		member.UserID != "@alice:example.com" ||
		member.Membership != "invite" ||
		member.Role != "owner" ||
		member.Domain != "custom.example" ||
		member.JoinedAt != now.UnixMilli() {
		t.Fatalf("unexpected join request projection: %#v", member)
	}
}
