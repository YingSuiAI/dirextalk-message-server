package p2p

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

type recordingDirextalkMCPInvoker struct {
	action string
	params map[string]any
}

func (i *recordingDirextalkMCPInvoker) InvokeCapability(ctx context.Context, action string, params map[string]any) (any, *dirextalkmcp.Error) {
	i.action = action
	i.params = params
	return map[string]any{
		"via":    "unified-dirextalkmcp",
		"action": action,
	}, nil
}

func TestDirectMCPServiceInvokeUsesUnifiedDirextalkMCPService(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	invoker := &recordingDirextalkMCPInvoker{}
	service.mcpCapabilities = dirextalkmcp.NewService(invoker)

	result := mustInvokeMCP[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!room:example.com",
	})
	if result["via"] != "unified-dirextalkmcp" || invoker.action != dirextalkmcp.ActionMessagesList {
		t.Fatalf("expected direct MCP service invoke to use unified MCP service, result=%#v invoker=%#v", result, invoker)
	}
	if invoker.params["room_id"] != "!room:example.com" {
		t.Fatalf("expected direct MCP params to reach unified MCP service, got %#v", invoker.params)
	}
}

func TestNativeAgentProductActionsUseUnifiedDirextalkMCPService(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	invoker := &recordingDirextalkMCPInvoker{}
	service.mcpCapabilities = dirextalkmcp.NewService(invoker)

	for _, tc := range []struct {
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
		t.Run(tc.action, func(t *testing.T) {
			invoker.action = ""
			result := mustHandle[map[string]any](t, service, tc.action, map[string]any{})
			if result["via"] != "unified-dirextalkmcp" || invoker.action != tc.want {
				t.Fatalf("action mapped to %q with result %#v, want %q", invoker.action, result, tc.want)
			}
		})
	}

	summary := mustHandle[map[string]any](t, service, "agent.summarize", map[string]any{"text": "hello world"})
	if summary["summary"] != "hello world" {
		t.Fatalf("agent.summarize result = %#v", summary)
	}
}
