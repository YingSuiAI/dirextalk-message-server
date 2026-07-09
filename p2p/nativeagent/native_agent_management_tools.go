package nativeagent

import "context"

func (r *Runtime) managementTools() []Tool {
	return []Tool{
		{
			Name:        "native_agent_runtime_inspect",
			Description: "Inspect Native Agent runtime configuration, installed skills, MCP servers, and runtime tool metadata without executing commands or changing configuration.",
			Parameters:  objectSchema(nil),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.runtimeInspect(ctx)
			},
		},
		{
			Name:        "native_agent_skills_list",
			Description: "List installed native Agent skills. Use this before installing a skill when the user asks what skills are already available.",
			Parameters:  objectSchema(nil),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.skillsList(ctx)
			},
		},
		{
			Name: "native_agent_skills_install",
			Description: "Install a native Agent skill from SKILL.md content, an HTTPS URL, or a GitHub repository path. " +
				"Use only when the user explicitly asks to install or add a skill. Skill scripts are not executed; installed skill instructions affect the next Agent turn after the prompt is rebuilt.",
			Write: true,
			Parameters: objectSchema(map[string]any{
				"id":       stringSchema(),
				"name":     stringSchema(),
				"content":  stringSchema(),
				"url":      stringSchema(),
				"repo_url": stringSchema(),
				"ref":      stringSchema(),
				"path":     stringSchema(),
				"enabled":  boolSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.skillInstall(ctx, args)
			},
		},
		{
			Name:        "native_agent_skills_enable",
			Description: "Enable an installed native Agent skill by id or name. Enabled skill instructions affect the next Agent turn after the prompt is rebuilt.",
			Write:       true,
			Parameters: objectSchema(map[string]any{
				"id":   stringSchema(),
				"name": stringSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.skillSetEnabled(ctx, args, true)
			},
		},
		{
			Name:        "native_agent_skills_disable",
			Description: "Disable an installed native Agent skill by id or name.",
			Write:       true,
			Parameters: objectSchema(map[string]any{
				"id":   stringSchema(),
				"name": stringSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.skillSetEnabled(ctx, args, false)
			},
		},
		{
			Name:        "native_agent_skills_uninstall",
			Description: "Uninstall a native Agent skill by id or name.",
			Write:       true,
			Parameters: objectSchema(map[string]any{
				"id":   stringSchema(),
				"name": stringSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.skillUninstall(ctx, args)
			},
		},
		{
			Name:        "native_agent_mcp_servers_list",
			Description: "List installed MCP servers for the native Agent, including discovered tools when available.",
			Parameters:  objectSchema(nil),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.mcpServersList(ctx)
			},
		},
		{
			Name: "native_agent_mcp_servers_install",
			Description: "Install a native Agent MCP server from stdio, HTTP, SSE, or streamable HTTP configuration. " +
				"Use only when the user explicitly asks to install or add an MCP server. Discovered MCP tools become callable on the next Agent turn after tools are rebuilt.",
			Write: true,
			Parameters: objectSchema(map[string]any{
				"id":             stringSchema(),
				"name":           stringSchema(),
				"transport":      stringSchema(),
				"url":            stringSchema(),
				"command":        stringSchema(),
				"args":           stringArraySchema(),
				"env":            stringMapSchema(),
				"discover_tools": boolSchema(),
				"enabled":        boolSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.mcpServerInstall(ctx, args)
			},
		},
		{
			Name:        "native_agent_mcp_servers_enable",
			Description: "Enable an installed native Agent MCP server by id or name. Its tools become callable on the next Agent turn after tools are rebuilt.",
			Write:       true,
			Parameters: objectSchema(map[string]any{
				"id":   stringSchema(),
				"name": stringSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.mcpServerSetEnabled(ctx, args, true)
			},
		},
		{
			Name:        "native_agent_mcp_servers_disable",
			Description: "Disable an installed native Agent MCP server by id or name.",
			Write:       true,
			Parameters: objectSchema(map[string]any{
				"id":   stringSchema(),
				"name": stringSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.mcpServerSetEnabled(ctx, args, false)
			},
		},
		{
			Name:        "native_agent_mcp_servers_uninstall",
			Description: "Uninstall a native Agent MCP server by id or name.",
			Write:       true,
			Parameters: objectSchema(map[string]any{
				"id":   stringSchema(),
				"name": stringSchema(),
			}),
			Handler: func(ctx context.Context, args map[string]any) (any, error) {
				return r.mcpServerUninstall(ctx, args)
			},
		},
	}
}

func stringArraySchema() map[string]any {
	return map[string]any{"type": "array", "items": stringSchema()}
}

func stringMapSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": stringSchema()}
}
