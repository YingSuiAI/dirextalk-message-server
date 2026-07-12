package conversation

import (
	"context"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func TestViewHydratesDirectConversationAndCapabilities(t *testing.T) {
	roomID := "!direct:example.com"
	hydrator := &moduleHydrator{contacts: map[string]dirextalkdomain.ContactRecord{
		roomID: {
			RoomID:      roomID,
			PeerMXID:    " @alice:example.com ",
			DisplayName: " Alice ",
			AvatarURL:   " mxc://example.com/alice ",
			Status:      "accepted",
		},
	}}
	view, err := New(&moduleStore{}, hydrator).View(context.Background(), activeConversation(roomID, dirextalkdomain.ConversationKindDirect))
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if view.PeerMXID != "@alice:example.com" || view.Title != "Alice" || view.AvatarURL != "mxc://example.com/alice" ||
		view.RelationshipStatus != "accepted" || view.Membership != "join" || view.MemberCount != 2 || view.Role != "member" ||
		view.HydrationState != string(dirextalkdomain.ConversationProjectionReady) || view.HydrationReason != "" {
		t.Fatalf("direct View() = %#v", view)
	}
	want := dirextalkdomain.ConversationCapabilities{
		Open: true, Send: true, SendMedia: true, Call: true, Delete: true,
	}
	if !reflect.DeepEqual(view.Capabilities, want) {
		t.Fatalf("direct capabilities = %#v, want %#v", view.Capabilities, want)
	}
}

func TestViewHydratesGroupAndOnlyPositiveJoinedCountOverrides(t *testing.T) {
	const ownerMXID = "@owner:example.com"
	tests := []struct {
		name        string
		joined      int64
		wantMembers int64
	}{
		{name: "positive count overrides product record", joined: 5, wantMembers: 5},
		{name: "zero count preserves product record", joined: 0, wantMembers: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roomID := "!group:example.com"
			record := activeConversation(roomID, dirextalkdomain.ConversationKindGroup)
			record.Title = "Conversation Group"
			record.AvatarURL = "mxc://example.com/conversation-group"
			hydrator := &moduleHydrator{
				ownerMXID: ownerMXID,
				groups: map[string]dirextalkdomain.GroupRecord{
					roomID: {RoomID: roomID, Name: "Ignored Group", AvatarURL: "mxc://example.com/ignored-group", MemberCount: 3},
				},
				joined: map[string]int64{roomID + "|": tt.joined},
				members: map[string]dirextalkdomain.MemberRecord{
					roomID + "|" + ownerMXID: {RoomID: roomID, UserID: ownerMXID, Membership: "join", Role: "owner"},
				},
			}
			view, err := New(&moduleStore{}, hydrator).View(context.Background(), record)
			if err != nil {
				t.Fatalf("View() error = %v", err)
			}
			if view.Title != "Conversation Group" || view.AvatarURL != "mxc://example.com/conversation-group" || view.MemberCount != tt.wantMembers ||
				view.Membership != "join" || view.Role != "owner" || view.HydrationState != string(dirextalkdomain.ConversationProjectionReady) {
				t.Fatalf("group View() = %#v", view)
			}
			want := dirextalkdomain.ConversationCapabilities{
				Open: true, Send: true, SendMedia: true, Call: true,
				Invite: true, ManageMembers: true, Rename: true, RemoveMembers: true,
				Leave: true, Delete: true,
			}
			if !reflect.DeepEqual(view.Capabilities, want) {
				t.Fatalf("group capabilities = %#v, want %#v", view.Capabilities, want)
			}
		})
	}
}

func TestViewHydratesChannelOwnerCapabilities(t *testing.T) {
	const ownerMXID = "@owner:example.com"
	roomID := "!channel:example.com"
	channelID := "channel"
	hydrator := &moduleHydrator{
		ownerMXID: ownerMXID,
		channels: map[string]dirextalkdomain.Channel{
			roomID: {
				RoomID: roomID, ChannelID: channelID, Name: " Channel ", AvatarURL: " mxc://example.com/channel ",
				ChannelType: " post ", CommentsEnabled: true, MemberCount: 3,
			},
		},
		joined: map[string]int64{roomID + "|" + channelID: 4},
		members: map[string]dirextalkdomain.MemberRecord{
			roomID + "|" + ownerMXID: {RoomID: roomID, ChannelID: channelID, UserID: ownerMXID, Membership: "join", Role: "owner"},
		},
	}
	view, err := New(&moduleStore{}, hydrator).View(context.Background(), activeConversation(roomID, dirextalkdomain.ConversationKindChannel))
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if view.Title != "Channel" || view.AvatarURL != "mxc://example.com/channel" || view.ChannelType != "post" ||
		!view.CommentsEnabled || view.MemberCount != 4 || view.Membership != "join" || view.Role != "owner" ||
		view.HydrationState != string(dirextalkdomain.ConversationProjectionReady) {
		t.Fatalf("channel View() = %#v", view)
	}
	want := dirextalkdomain.ConversationCapabilities{
		Open: true, Send: true, SendMedia: true,
		Invite: true, ManageMembers: true, Rename: true, RemoveMembers: true,
		Leave: true, Delete: true, PostCreate: true, CommentCreate: true,
		ReactionToggle: true, PostRecall: true, CommentRecall: true, CommentsEnabled: true,
	}
	if !reflect.DeepEqual(view.Capabilities, want) {
		t.Fatalf("channel capabilities = %#v, want %#v", view.Capabilities, want)
	}
}

func TestViewHydratesChannelMemberWithCommentsDisabled(t *testing.T) {
	const ownerMXID = "@owner:example.com"
	roomID := "!channel:example.com"
	hydrator := &moduleHydrator{
		ownerMXID: ownerMXID,
		channels: map[string]dirextalkdomain.Channel{
			roomID: {RoomID: roomID, ChannelID: "channel", ChannelType: "", CommentsEnabled: false},
		},
		members: map[string]dirextalkdomain.MemberRecord{
			roomID + "|" + ownerMXID: {RoomID: roomID, UserID: ownerMXID, Membership: "joined", Role: "member"},
		},
	}
	view, err := New(&moduleStore{}, hydrator).View(context.Background(), activeConversation(roomID, dirextalkdomain.ConversationKindChannel))
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if view.ChannelType != "chat" || view.CommentsEnabled || view.Role != "member" {
		t.Fatalf("channel member View() = %#v", view)
	}
	want := dirextalkdomain.ConversationCapabilities{
		Open: true, Send: true, SendMedia: true, Leave: true, ReactionToggle: true,
	}
	if !reflect.DeepEqual(view.Capabilities, want) {
		t.Fatalf("channel member capabilities = %#v, want %#v", view.Capabilities, want)
	}
}

func TestViewSystemAndAgentCapabilities(t *testing.T) {
	tests := []struct {
		name string
		kind dirextalkdomain.ConversationKind
		want dirextalkdomain.ConversationCapabilities
	}{
		{name: "system opens without membership", kind: dirextalkdomain.ConversationKindSystem, want: dirextalkdomain.ConversationCapabilities{Open: true}},
		{name: "agent has no implicit capabilities", kind: dirextalkdomain.ConversationKindAgent, want: dirextalkdomain.ConversationCapabilities{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view, err := New(&moduleStore{}, &moduleHydrator{}).View(context.Background(), activeConversation("!special:example.com", tt.kind))
			if err != nil {
				t.Fatalf("View() error = %v", err)
			}
			if view.HydrationState != string(dirextalkdomain.ConversationProjectionReady) || !reflect.DeepEqual(view.Capabilities, tt.want) {
				t.Fatalf("View(%s) = hydration %q capabilities %#v, want ready %#v", tt.kind, view.HydrationState, view.Capabilities, tt.want)
			}
		})
	}
}

func TestViewPreservesPendingProjectionAndHiddenOwnerMembership(t *testing.T) {
	const ownerMXID = "@owner:example.com"
	roomID := "!group:example.com"
	record := activeConversation(roomID, dirextalkdomain.ConversationKindGroup)
	record.ProjectionState = dirextalkdomain.ConversationProjectionPending
	record.ProjectionReason = "waiting_for_projection"
	hydrator := &moduleHydrator{
		ownerMXID: ownerMXID,
		members: map[string]dirextalkdomain.MemberRecord{
			roomID + "|" + ownerMXID: {RoomID: roomID, UserID: ownerMXID, Membership: "left", Role: "owner"},
		},
	}
	view, err := New(&moduleStore{}, hydrator).View(context.Background(), record)
	if err != nil {
		t.Fatalf("View() error = %v", err)
	}
	if view.Membership != "" || view.Role != "" || view.HydrationState != string(dirextalkdomain.ConversationProjectionPending) ||
		view.HydrationReason != "waiting_for_projection" || !reflect.DeepEqual(view.Capabilities, dirextalkdomain.ConversationCapabilities{}) {
		t.Fatalf("pending hidden-member View() = %#v", view)
	}
}

func activeConversation(roomID string, kind dirextalkdomain.ConversationKind) dirextalkdomain.ConversationRecord {
	return dirextalkdomain.ConversationRecord{
		ConversationID:  "conv_" + string(kind),
		MatrixRoomID:    roomID,
		Kind:            kind,
		Lifecycle:       dirextalkdomain.ConversationLifecycleActive,
		ProjectionState: dirextalkdomain.ConversationProjectionReady,
	}
}
