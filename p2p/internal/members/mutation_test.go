package members

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type mutationConversation struct {
	err   error
	calls []string
}

func (c *mutationConversation) Operation(_ context.Context, action, status, roomID string) (map[string]any, *dirextalkdomain.ConversationView, error) {
	c.calls = append(c.calls, action+":"+status+":"+roomID)
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
}

func newMutationHarness() *mutationHarness {
	h := &mutationHarness{conversation: &mutationConversation{}}
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
			return h.saveErr
		},
		PublishPolicy: func(_ context.Context, member dirextalkdomain.MemberRecord) *actionbase.Error {
			h.published = append(h.published, member)
			return h.policyErr
		},
		Conversation: h.conversation,
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

func TestMemberMuteValidation(t *testing.T) {
	for _, tt := range []struct {
		name, errorText string
		raw             map[string]any
	}{
		{name: "missing target", raw: map[string]any{"user_id": "@alice:example.com"}, errorText: "room_id or channel_id is required"},
		{name: "missing user", raw: map[string]any{"room_id": "!room:example.com"}, errorText: "user_id is required"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newMutationHarness()
			result, actionErr := h.module.Handlers()[actionChannelMute](context.Background(), tt.raw)
			if result != nil || actionErr == nil || actionErr.Status != http.StatusBadRequest || actionErr.Error != tt.errorText || len(h.saved) != 0 {
				t.Fatalf("validation = (%#v, %#v), saved=%#v", result, actionErr, h.saved)
			}
		})
	}
}

func TestMemberMuteFailureOrder(t *testing.T) {
	tests := []struct {
		name         string
		lookupErr    error
		saveErr      error
		policyErr    *actionbase.Error
		operationErr error
		wantSaved    int
		wantPolicy   int
		wantOps      int
	}{
		{name: "lookup", lookupErr: errors.New("lookup failed")},
		{name: "save", saveErr: errors.New("save failed"), wantSaved: 1},
		{name: "policy", policyErr: actionbase.StatusError(http.StatusForbidden, "policy failed"), wantSaved: 1, wantPolicy: 1},
		{name: "operation", operationErr: errors.New("operation failed"), wantSaved: 1, wantPolicy: 1, wantOps: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newMutationHarness()
			h.lookupErr, h.saveErr, h.policyErr, h.conversation.err = tt.lookupErr, tt.saveErr, tt.policyErr, tt.operationErr
			result, actionErr := h.module.Handlers()[actionGroupMute](context.Background(), map[string]any{"room_id": "!room:example.com", "user_id": "@alice:example.com"})
			if result != nil || actionErr == nil || len(h.saved) != tt.wantSaved || len(h.published) != tt.wantPolicy || len(h.conversation.calls) != tt.wantOps {
				t.Fatalf("failure = (%#v, %#v), saved=%#v policy=%#v ops=%#v", result, actionErr, h.saved, h.published, h.conversation.calls)
			}
		})
	}
}
