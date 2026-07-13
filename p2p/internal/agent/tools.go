package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/nativeagent"
)

// Tools adapts ProductCore MCP capabilities to Native Agent tools and appends
// the local summarize helper.
func Tools(mcp *dirextalkmcp.Service) []nativeagent.Tool {
	capabilities := dirextalkmcp.Tools()
	if mcp != nil {
		capabilities = mcp.Tools()
	}
	tools := make([]nativeagent.Tool, 0, len(capabilities)+1)
	for _, tool := range capabilities {
		tool := tool
		tools = append(tools, nativeagent.Tool{
			Name:        tool.Name,
			Description: tool.Description,
			Parameters:  cloneMap(tool.InputSchema),
			Write:       tool.Write,
			Handler: func(ctx context.Context, params map[string]any) (any, error) {
				if mcp == nil {
					return nil, fmt.Errorf("Dirextalk MCP capability service is unavailable")
				}
				value, invokeErr := mcp.Invoke(ctx, tool.Action, cloneMap(params))
				if invokeErr != nil {
					return nil, fmt.Errorf("%s", invokeErr.Message)
				}
				return value, nil
			},
		})
	}
	tools = append(tools, nativeagent.Tool{
		Name:        "dirextalk_summarize",
		Description: "Summarize provided text or room messages.",
		Parameters: objectSchema(map[string]any{
			"room_id": stringSchema(),
			"text":    stringSchema(),
			"limit":   numberSchema(),
		}),
		Handler: func(ctx context.Context, params map[string]any) (any, error) {
			return summarize(ctx, mcp, params)
		},
	})
	return tools
}

func summarize(ctx context.Context, mcp *dirextalkmcp.Service, params map[string]any) (map[string]any, error) {
	text := stringValue(params["text"])
	if text == "" && stringValue(params["room_id"]) != "" {
		if mcp == nil {
			return nil, fmt.Errorf("Dirextalk MCP capability service is unavailable")
		}
		value, invokeErr := mcp.Invoke(ctx, dirextalkmcp.ActionMessagesList, cloneMap(params))
		if invokeErr != nil {
			return nil, fmt.Errorf("%s", invokeErr.Message)
		}
		text = jsonValue(value)
	}
	if text == "" {
		return map[string]any{"summary": "", "message": "no content"}, nil
	}
	runes := []rune(strings.Join(strings.Fields(text), " "))
	limit := 500
	if len(runes) < limit {
		limit = len(runes)
	}
	summary := string(runes[:limit])
	if len(runes) > limit {
		summary += "..."
	}
	return map[string]any{"summary": summary, "source_chars": len([]rune(text))}, nil
}

func stringValue(value any) string {
	return actionbase.String(value)
}

func objectSchema(properties map[string]any) map[string]any {
	return map[string]any{"type": "object", "properties": properties}
}

func stringSchema() map[string]any { return map[string]any{"type": "string"} }
func numberSchema() map[string]any { return map[string]any{"type": "number"} }
