package nativeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cloudwego/eino-ext/components/tool/mcp/officialmcp"
	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (r *Runtime) enabledOfficialMCPTools(ctx context.Context, config map[string]any, params map[string]any) ([]einotool.BaseTool, func(), error) {
	var sessions []*mcp.ClientSession
	cleanup := func() {
		for _, session := range sessions {
			_ = session.Close()
		}
	}
	var tools []einotool.BaseTool
	for _, server := range configList(config, "mcp_servers") {
		if !boolParam(server["enabled"]) {
			continue
		}
		session, err := r.openMCPClientSession(ctx, server)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		sessions = append(sessions, session)
		serverID := sanitizeNativeID(trimString(server["id"]))
		baseTools, err := officialmcp.GetTools(ctx, &officialmcp.Config{
			Cli:                   session,
			ToolCallResultHandler: compactMCPToolResult,
		})
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		for _, baseTool := range baseTools {
			info, err := baseTool.Info(ctx)
			if err != nil {
				cleanup()
				return nil, nil, err
			}
			toolName := "mcp__" + serverID + "__" + sanitizeMCPToolName(info.Name)
			tools = append(tools, &prefixedEinoTool{
				BaseTool: baseTool,
				name:     toolName,
				desc:     fallbackString(info.Desc, "MCP tool "+info.Name),
			})
		}
	}
	return tools, cleanup, nil
}

func (r *Runtime) discoverOfficialMCPTools(ctx context.Context, server map[string]any) ([]any, error) {
	session, err := r.openMCPClientSession(ctx, server)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	tools, err := officialmcp.GetTools(ctx, &officialmcp.Config{Cli: session})
	if err != nil {
		return nil, err
	}
	result := make([]any, 0, len(tools))
	for _, tool := range tools {
		info, err := tool.Info(ctx)
		if err != nil {
			return nil, err
		}
		record := map[string]any{
			"name":        info.Name,
			"description": info.Desc,
		}
		if info.ParamsOneOf != nil {
			if js, err := info.ParamsOneOf.ToJSONSchema(); err == nil && js != nil {
				var schemaMap map[string]any
				if data, err := json.Marshal(js); err == nil {
					_ = json.Unmarshal(data, &schemaMap)
				}
				record["inputSchema"] = schemaMap
			}
		}
		result = append(result, record)
	}
	return result, nil
}

func (r *Runtime) openMCPClientSession(ctx context.Context, server map[string]any) (*mcp.ClientSession, error) {
	transport, err := r.mcpTransport(server)
	if err != nil {
		return nil, err
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "dirextalk-native-agent", Version: "0.1.0"}, nil)
	return client.Connect(ctx, transport, nil)
}

func (r *Runtime) mcpTransport(server map[string]any) (mcp.Transport, error) {
	transport := strings.ToLower(fallbackString(trimString(server["transport"]), trimString(server["type"])))
	switch transport {
	case "stdio":
		command := trimString(server["command"])
		if command == "" {
			return nil, fmt.Errorf("mcp stdio command is required")
		}
		cmd := exec.Command(command, stringSliceParam(server["args"])...)
		cmd.Dir = filepath.Join(r.dataDir, "mcp", sanitizeNativeID(trimString(server["id"])))
		_ = os.MkdirAll(cmd.Dir, 0o700)
		cmd.Env = append(runtimeEnv(r.dataDir), envMapToList(server["env"])...)
		return &mcp.CommandTransport{Command: cmd}, nil
	case "sse":
		url := trimString(server["url"])
		if url == "" {
			return nil, fmt.Errorf("mcp sse url is required")
		}
		return &mcp.SSEClientTransport{Endpoint: url, HTTPClient: r.client}, nil
	case "http", "streamable_http", "streamable-http", "":
		url := trimString(server["url"])
		if url == "" {
			return nil, fmt.Errorf("mcp http url is required")
		}
		return &mcp.StreamableClientTransport{Endpoint: url, HTTPClient: r.client, MaxRetries: -1}, nil
	default:
		return nil, fmt.Errorf("unsupported mcp transport %q", transport)
	}
}

func compactMCPToolResult(ctx context.Context, name string, result *mcp.CallToolResult) (*mcp.CallToolResult, error) {
	if result == nil || result.IsError || len(result.Content) == 0 {
		return result, nil
	}
	for _, content := range result.Content {
		textContent, ok := content.(*mcp.TextContent)
		if !ok {
			continue
		}
		runes := []rune(textContent.Text)
		if len(runes) > 4000 {
			textContent.Text = string(runes[:4000]) + "...(truncated)"
		}
	}
	return result, nil
}

type prefixedEinoTool struct {
	einotool.BaseTool
	name string
	desc string
}

func (t *prefixedEinoTool) Info(ctx context.Context) (*schema.ToolInfo, error) {
	info, err := t.BaseTool.Info(ctx)
	if err != nil {
		return nil, err
	}
	clone := *info
	clone.Name = t.name
	clone.Desc = t.desc
	return &clone, nil
}

func (t *prefixedEinoTool) InvokableRun(ctx context.Context, argumentsInJSON string, opts ...einotool.Option) (string, error) {
	invokable, ok := t.BaseTool.(einotool.InvokableTool)
	if !ok {
		return "", fmt.Errorf("mcp tool %q is not invokable", t.name)
	}
	return invokable.InvokableRun(ctx, argumentsInJSON, opts...)
}
