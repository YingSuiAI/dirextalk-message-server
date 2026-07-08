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

func TestMCPBodyActionsInvokeUnifiedDirextalkMCPService(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	invoker := &recordingDirextalkMCPInvoker{}
	service.mcpCapabilities = dirextalkmcp.NewService(invoker)
	service.actions = service.actionHandlers()

	result := mustHandle[map[string]any](t, service, dirextalkmcp.ActionMessagesList, map[string]any{
		"room_id": "!room:example.com",
	})
	if result["via"] != "unified-dirextalkmcp" || invoker.action != dirextalkmcp.ActionMessagesList {
		t.Fatalf("expected body action to use unified MCP service, result=%#v invoker=%#v", result, invoker)
	}
	if invoker.params["room_id"] != "!room:example.com" {
		t.Fatalf("expected body action params to reach unified MCP service, got %#v", invoker.params)
	}
}

func TestNativeAgentDirextalkToolsInvokeUnifiedDirextalkMCPService(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	invoker := &recordingDirextalkMCPInvoker{}
	service.mcpCapabilities = dirextalkmcp.NewService(invoker)

	var handler func(context.Context, map[string]any) (any, error)
	for _, tool := range nativeAgentTools(service) {
		if tool.Name == "dirextalk_messages_list" {
			handler = tool.Handler
			break
		}
	}
	if handler == nil {
		t.Fatal("expected dirextalk_messages_list native tool")
	}
	result, err := handler(context.Background(), map[string]any{"room_id": "!room:example.com"})
	if err != nil {
		t.Fatal(err)
	}
	resultMap := result.(map[string]any)
	if resultMap["via"] != "unified-dirextalkmcp" || invoker.action != dirextalkmcp.ActionMessagesList {
		t.Fatalf("expected native tool to use unified MCP service, result=%#v invoker=%#v", result, invoker)
	}
}
