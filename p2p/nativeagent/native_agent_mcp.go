package nativeagent

import (
	"context"
	"strings"
	"time"
)

func (r *Runtime) mcpServersList(ctx context.Context) (map[string]any, error) {
	config, _, err := r.agentConfig(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"servers": configList(config, "mcp_servers")}, nil
}

func (r *Runtime) mcpServerInstall(ctx context.Context, params map[string]any) (map[string]any, error) {
	if err := r.ensureDataDirs(); err != nil {
		return nil, err
	}
	server := nestedOrSelf(params, "server")
	id := sanitizeNativeID(fallbackString(trimString(server["id"]), trimString(server["name"])))
	if id == "" {
		id = randomToken("mcp")
	}
	record := cloneAnyMap(server)
	record["id"] = id
	if _, ok := record["enabled"]; !ok {
		record["enabled"] = true
	}
	if boolParam(params["discover_tools"]) || len(configList(record, "tools")) == 0 {
		tools, err := r.discoverMCPTools(ctx, record)
		if err != nil {
			return nil, err
		}
		record["tools"] = tools
	}
	record["installed_at"] = time.Now().UTC().UnixMilli()
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		config["mcp_servers"] = upsertConfigRecord(configList(config, "mcp_servers"), record)
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true, "server": record}, nil
}

func (r *Runtime) mcpServerSetEnabled(ctx context.Context, params map[string]any, enabled bool) (map[string]any, error) {
	return r.setManagedConfigRecordEnabled(ctx, params, enabled, mcpServerConfigRecord)
}

func (r *Runtime) mcpServerUninstall(ctx context.Context, params map[string]any) (map[string]any, error) {
	return r.uninstallManagedConfigRecord(ctx, params, mcpServerConfigRecord)
}

func (r *Runtime) discoverMCPTools(ctx context.Context, server map[string]any) ([]any, error) {
	return r.discoverOfficialMCPTools(ctx, server)
}

func sanitizeMCPToolName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return strings.Trim(b.String(), "_")
}
