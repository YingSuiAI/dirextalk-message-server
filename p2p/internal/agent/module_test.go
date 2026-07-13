package agent

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

type recordingMCPInvoker struct {
	action string
}

func (i *recordingMCPInvoker) InvokeCapability(_ context.Context, action string, _ map[string]any) (any, *dirextalkmcp.Error) {
	i.action = action
	return map[string]any{"action": action}, nil
}

func TestRuntimeActionsUseConfiguredMCPService(t *testing.T) {
	invoker := &recordingMCPInvoker{}
	module := New(Config{MCP: dirextalkmcp.NewService(invoker)})
	handlers := module.Handlers()

	for _, test := range []struct {
		action string
		want   string
	}{
		{"agent.contacts.list", dirextalkmcp.ActionContactsList},
		{"agent.contacts.search", dirextalkmcp.ActionContactsSearch},
		{"agent.rooms.search", dirextalkmcp.ActionRoomsSearch},
		{"agent.messages.list", dirextalkmcp.ActionMessagesList},
		{"agent.messages.send", dirextalkmcp.ActionMessagesSend},
		{"agent.room_members.list", dirextalkmcp.ActionRoomMembersList},
		{"agent.channel_posts.list", dirextalkmcp.ActionChannelPostsList},
		{"agent.channel_comments.list", dirextalkmcp.ActionChannelCommentsList},
		{"agent.channel_comments.create", dirextalkmcp.ActionChannelCommentsCreate},
	} {
		t.Run(test.action, func(t *testing.T) {
			invoker.action = ""
			value, actionErr := handlers[test.action](context.Background(), map[string]any{})
			if actionErr != nil {
				t.Fatalf("invoke %s: %v", test.action, actionErr)
			}
			result := value.(map[string]any)
			if invoker.action != test.want || result["action"] != test.want {
				t.Fatalf("mapped to %q with result %#v, want %q", invoker.action, result, test.want)
			}
		})
	}
}
