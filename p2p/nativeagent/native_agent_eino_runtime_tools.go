package nativeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

func (r *Runtime) enabledRuntimeEinoTools(config map[string]any) []einotool.BaseTool {
	records := configList(config, "runtime_tools")
	tools := make([]einotool.BaseTool, 0, len(records))
	for _, record := range records {
		if enabled, ok := record["enabled"].(bool); ok && !enabled {
			continue
		}
		id := sanitizeNativeID(fallbackString(trimString(record["id"]), trimString(record["name"])))
		if id == "" {
			continue
		}
		command := fallbackString(trimString(record["command"]), trimString(record["path"]))
		if command == "" {
			continue
		}
		toolName := runtimeEinoToolName(id)
		if toolName == "" {
			continue
		}
		info, err := runtimeEinoToolInfo(toolName, record)
		if err != nil {
			continue
		}
		tools = append(tools, &einoRuntimeTool{
			runtime: r,
			record:  cloneAnyMap(record),
			info:    info,
		})
	}
	return tools
}

func runtimeEinoToolName(id string) string {
	id = strings.ReplaceAll(sanitizeNativeID(id), "-", "_")
	id = strings.Trim(id, "_")
	if id == "" {
		return ""
	}
	return "runtime__" + id
}

func runtimeEinoToolInfo(name string, record map[string]any) (*schema.ToolInfo, error) {
	desc := fallbackString(trimString(record["description"]), "Run an installed server-side runtime CLI tool.")
	params := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"args": map[string]any{
				"type":        "array",
				"description": "Command line arguments to pass to the installed tool.",
				"items":       map[string]any{"type": "string"},
			},
			"timeout_seconds": map[string]any{
				"type":        "number",
				"description": "Optional execution timeout in seconds.",
			},
		},
	}
	data, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	js := &jsonschema.Schema{}
	if err := json.Unmarshal(data, js); err != nil {
		return nil, err
	}
	return &schema.ToolInfo{
		Name:        name,
		Desc:        desc,
		ParamsOneOf: schema.NewParamsOneOfByJSONSchema(js),
	}, nil
}

type einoRuntimeTool struct {
	runtime *Runtime
	record  map[string]any
	info    *schema.ToolInfo
}

func (t *einoRuntimeTool) Info(context.Context) (*schema.ToolInfo, error) {
	return t.info, nil
}

func (t *einoRuntimeTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...einotool.Option) (string, error) {
	var args map[string]any
	if strings.TrimSpace(argumentsInJSON) != "" {
		if err := json.Unmarshal([]byte(argumentsInJSON), &args); err != nil {
			return jsonValue(map[string]any{"error": err.Error()}), nil
		}
	}
	if args == nil {
		args = map[string]any{}
	}
	params := map[string]any{
		"command": fallbackString(trimString(t.record["command"]), trimString(t.record["path"])),
		"path":    trimString(t.record["path"]),
		"args":    stringSliceParam(args["args"]),
	}
	timeout := int64Param(args["timeout_seconds"])
	if timeout <= 0 {
		timeout = int64Param(t.record["timeout_seconds"])
	}
	if timeout > 0 {
		params["timeout_seconds"] = timeout
	}
	result, err := t.runtime.runtimeRun(ctx, params)
	if err != nil {
		return jsonValue(map[string]any{"error": fmt.Sprintf("runtime tool failed: %v", err)}), nil
	}
	return jsonValue(map[string]any{"result": result}), nil
}
