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
