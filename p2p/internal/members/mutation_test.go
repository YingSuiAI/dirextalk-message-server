package members

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type mutationConversation struct {
	err    error
	calls  []string
	onCall func()
}

func (c *mutationConversation) Operation(_ context.Context, action, status, roomID string) (map[string]any, *dirextalkdomain.ConversationView, error) {
	c.calls = append(c.calls, action+":"+status+":"+roomID)
	if c.onCall != nil {
		c.onCall()
	}
	if c.err != nil {
		return nil, nil, c.err
	}
	return map[string]any{"action": action, "status": status, "room_id": roomID}, &dirextalkdomain.ConversationView{MatrixRoomID: roomID}, nil
}

type mutationHarness struct {
	module       *Module
	existing     dirextalkdomain.MemberRecord
	found        bool
	lookupErr    error
	saveErr      error
	policyErr    *actionbase.Error
	saved        []dirextalkdomain.MemberRecord
	published    []dirextalkdomain.MemberRecord
	conversation *mutationConversation
	ownerMXID    string
	kicks        []string
	leaves       []string
	transportErr *actionbase.Error
	order        []string
	joinStates   []string
	completions  []string
	complete     map[string]any
	completeErr  *actionbase.Error
}

func newMutationHarness() *mutationHarness {
	h := &mutationHarness{ownerMXID: "@owner:example.com", conversation: &mutationConversation{}}
	h.conversation.onCall = func() { h.order = append(h.order, "operation") }
	h.module = New(&testStore{}, Config{
		ResolveTarget: func(raw map[string]any) (string, string) {
			params := actionbase.Params(raw)
			return params.String("room_id"), params.String("channel_id")
		},
		NewMember: func(roomID, channelID, userID string) dirextalkdomain.MemberRecord {
			return dirextalkdomain.MemberRecord{RoomID: roomID, ChannelID: channelID, UserID: userID, Membership: "join", Role: "member"}
		},
		LookupMember: func(context.Context, string, string) (dirextalkdomain.MemberRecord, bool, error) {
			return h.existing, h.found, h.lookupErr
		},
		SaveMember: func(_ context.Context, member dirextalkdomain.MemberRecord) error {
			h.saved = append(h.saved, member)
			h.order = append(h.order, "save")
			return h.saveErr
		},
		PublishPolicy: func(_ context.Context, member dirextalkdomain.MemberRecord) *actionbase.Error {
			h.published = append(h.published, member)
			return h.policyErr
		},
		Conversation: h.conversation,
		OwnerMXID:    func() string { return h.ownerMXID },
		KickMember: func(_ context.Context, roomID, senderMXID, targetMXID, reason string) *actionbase.Error {
			h.kicks = append(h.kicks, roomID+"|"+senderMXID+"|"+targetMXID+"|"+reason)
			h.order = append(h.order, "kick")
			return h.transportErr
		},
		LeaveMember: func(_ context.Context, roomID, userMXID string) *actionbase.Error {
			h.leaves = append(h.leaves, roomID+"|"+userMXID)
			h.order = append(h.order, "leave")
			return h.transportErr
		},
		PublishJoinRequest: func(_ context.Context, roomID, userID, status, reason string) *actionbase.Error {
			h.joinStates = append(h.joinStates, roomID+"|"+userID+"|"+status+"|"+reason)
			h.order = append(h.order, "join-state")
			return nil
		},
		CompleteJoinRequest: func(_ context.Context, approved bool, member dirextalkdomain.MemberRecord, _ map[string]any) (map[string]any, *actionbase.Error) {
			h.completions = append(h.completions, fmt.Sprintf("%t|%s", approved, member.Membership))
			h.order = append(h.order, "complete")
			return h.complete, h.completeErr
		},
	})
	return h
}

func TestMemberMuteAndUnmuteHandlersPreserveWorkflow(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		existing       dirextalkdomain.MemberRecord
		found          bool
		raw            map[string]any
		wantMuted      bool
		wantChannelID  string
		wantMembership string
	}{
		{name: "group mute clears channel", action: actionGroupMute, raw: map[string]any{"room_id": " !group:example.com ", "channel_id": "stale", "user_mxids": []any{" @alice:example.com "}}, wantMuted: true, wantMembership: "join"},
		{name: "channel unmute preserves membership", action: actionChannelUnmute, existing: dirextalkdomain.MemberRecord{RoomID: "!channel:example.com", ChannelID: "old", UserID: "@alice:example.com", Membership: " invite ", Role: "owner", Muted: true}, found: true, raw: map[string]any{"room_id": "!channel:example.com", "channel_id": "current", "user_id": "@alice:example.com"}, wantChannelID: "current", wantMembership: "invite"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newMutationHarness()
			h.existing, h.found = tt.existing, tt.found
			result, actionErr := h.module.Handlers()[tt.action](context.Background(), tt.raw)
			if actionErr != nil {
				t.Fatal(actionErr)
			}
			member := result.(map[string]any)["member"].(dirextalkdomain.MemberRecord)
			if member.Muted != tt.wantMuted || member.ChannelID != tt.wantChannelID || member.Membership != tt.wantMembership {
				t.Fatalf("member = %#v", member)
			}
			if !reflect.DeepEqual(h.saved, []dirextalkdomain.MemberRecord{member}) || !reflect.DeepEqual(h.published, []dirextalkdomain.MemberRecord{member}) {
				t.Fatalf("saved/published = %#v / %#v", h.saved, h.published)
			}
			response := result.(map[string]any)
			if response["status"] != "ok" || response["operation"] == nil || response["conversation"] == nil {
				t.Fatalf("response = %#v", response)
			}
		})
	}
}

func TestMemberMuteStopsBeforePolicyWhenSaveFails(t *testing.T) {
	h := newMutationHarness()
	h.saveErr = errors.New("save failed")
	result, actionErr := h.module.Handlers()[actionGroupMute](context.Background(), map[string]any{"room_id": "!room:example.com", "user_id": "@alice:example.com"})
	if result != nil || actionErr == nil || len(h.saved) != 1 || len(h.published) != 0 || len(h.conversation.calls) != 0 {
		t.Fatalf("failure = (%#v, %#v), saved=%#v policy=%#v ops=%#v", result, actionErr, h.saved, h.published, h.conversation.calls)
	}
}

func TestMemberLifecycleHandlersPreserveMatrixPersistenceOrder(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		raw            map[string]any
		existing       dirextalkdomain.MemberRecord
		found          bool
		wantMembership string
		wantStatus     string
		wantOrder      []string
		wantKick       string
		wantLeave      string
	}{
		{
			name: "group remove", action: actionGroupRemove,
			raw:            map[string]any{"room_id": "!group:example.com", "user_id": "@alice:example.com", "reason": " cleanup "},
			wantMembership: "remove", wantStatus: "ok", wantOrder: []string{"kick", "save", "operation"},
			wantKick: "!group:example.com|@owner:example.com|@alice:example.com|cleanup",
		},
		{
			name: "channel leave uses owner", action: actionChannelLeave,
			raw:            map[string]any{"room_id": "!channel:example.com", "channel_id": "channel_1", "user_id": "@spoofed:example.com"},
			wantMembership: "leave", wantStatus: "ok", wantOrder: []string{"leave", "save", "operation"},
			wantLeave: "!channel:example.com|@owner:example.com",
		},
		{
			name: "group invite reject", action: actionGroupInviteReject,
			raw:      map[string]any{"room_id": "!group:example.com", "channel_id": "stale", "user_id": "@spoofed:example.com"},
			existing: dirextalkdomain.MemberRecord{RoomID: "!group:example.com", ChannelID: "stale", UserID: "@owner:example.com", Membership: " invite ", Role: "member"}, found: true,
			wantMembership: "reject", wantStatus: "rejected", wantOrder: []string{"save", "operation"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newMutationHarness()
			h.existing, h.found = tt.existing, tt.found
			result, actionErr := h.module.Handlers()[tt.action](context.Background(), tt.raw)
			if actionErr != nil {
				t.Fatal(actionErr)
			}
			response := result.(map[string]any)
			member := response["member"].(dirextalkdomain.MemberRecord)
			if member.Membership != tt.wantMembership || response["status"] != tt.wantStatus || (tt.action == actionGroupInviteReject && member.ChannelID != "") {
				t.Fatalf("response = %#v", response)
			}
			if !reflect.DeepEqual(h.order, tt.wantOrder) || !reflect.DeepEqual(h.kicks, compactStrings(tt.wantKick)) || !reflect.DeepEqual(h.leaves, compactStrings(tt.wantLeave)) {
				t.Fatalf("order/kick/leave = %#v / %#v / %#v", h.order, h.kicks, h.leaves)
			}
		})
	}
}

func TestMemberLifecycleGuardsStopBeforePersistence(t *testing.T) {
	tests := []struct {
		name         string
		action       string
		existing     dirextalkdomain.MemberRecord
		found        bool
		transportErr *actionbase.Error
		wantStatus   int
	}{
		{name: "owner remove", action: actionGroupRemove, existing: dirextalkdomain.MemberRecord{RoomID: "!room:example.com", UserID: "@alice:example.com", Role: "owner", Membership: "join"}, found: true, wantStatus: http.StatusConflict},
		{name: "reject requires invite", action: actionGroupInviteReject, existing: dirextalkdomain.MemberRecord{RoomID: "!room:example.com", UserID: "@owner:example.com", Membership: "join"}, found: true, wantStatus: http.StatusNotFound},
		{name: "transport failure", action: actionChannelRemove, transportErr: actionbase.StatusError(http.StatusForbidden, "kick denied"), wantStatus: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newMutationHarness()
			h.existing, h.found, h.transportErr = tt.existing, tt.found, tt.transportErr
			result, actionErr := h.module.Handlers()[tt.action](context.Background(), map[string]any{"room_id": "!room:example.com", "user_id": "@alice:example.com"})
			if result != nil || actionErr == nil || actionErr.Status != tt.wantStatus || len(h.saved) != 0 || len(h.conversation.calls) != 0 {
				t.Fatalf("guard = (%#v, %#v), saved=%#v ops=%#v", result, actionErr, h.saved, h.conversation.calls)
			}
		})
	}
}

func TestChannelJoinRequestResolutionPreservesFinalStatusAndOrder(t *testing.T) {
	tests := []struct {
		name           string
		action         string
		membership     string
		completeStatus string
		wantMember     string
		wantState      string
		wantCompletion string
		wantStatus     string
	}{
		{name: "approve retry", action: actionChannelJoinRequestApprove, membership: " join_failed ", completeStatus: "joined", wantMember: "approved", wantState: "approved", wantCompletion: "true|approved", wantStatus: "joined"},
		{name: "reject pending", action: actionChannelJoinRequestReject, membership: "pending", completeStatus: "reject", wantMember: "reject", wantState: "rejected", wantCompletion: "false|reject", wantStatus: "rejected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newMutationHarness()
			h.existing = dirextalkdomain.MemberRecord{RoomID: "!channel:example.com", ChannelID: "channel_1", UserID: "@alice:example.com", Membership: tt.membership, Role: "member"}
			h.found = true
			h.complete = map[string]any{"status": tt.completeStatus, "member": h.existing}
			result, actionErr := h.module.Handlers()[tt.action](context.Background(), map[string]any{
				"room_id": "!channel:example.com", "channel_id": "channel_1", "user_id": "@alice:example.com", "reason": " reviewed ",
			})
			if actionErr != nil {
				t.Fatal(actionErr)
			}
			response := result.(map[string]any)
			if response["status"] != tt.wantStatus || len(h.saved) != 1 || h.saved[0].Membership != tt.wantMember {
				t.Fatalf("response/saved = %#v / %#v", response, h.saved)
			}
			if !reflect.DeepEqual(h.order, []string{"save", "join-state", "complete", "operation"}) ||
				!reflect.DeepEqual(h.joinStates, []string{"!channel:example.com|@alice:example.com|" + tt.wantState + "|reviewed"}) ||
				!reflect.DeepEqual(h.completions, []string{tt.wantCompletion}) {
				t.Fatalf("order/state/completion = %#v / %#v / %#v", h.order, h.joinStates, h.completions)
			}
			if got := h.conversation.calls; !reflect.DeepEqual(got, []string{tt.action + ":" + tt.wantStatus + ":!channel:example.com"}) {
				t.Fatalf("operation calls = %#v", got)
			}
		})
	}
}

func compactStrings(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}
