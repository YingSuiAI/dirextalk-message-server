package nativeagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	id := sanitizeNativeID(fallbackString(trimString(params["id"]), trimString(params["name"])))
	if id == "" {
		return nil, fmt.Errorf("mcp server id is required")
	}
	var updated map[string]any
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		records := configList(config, "mcp_servers")
		for _, record := range records {
			if sanitizeNativeID(trimString(record["id"])) == id {
				record["enabled"] = enabled
				updated = record
				break
			}
		}
		config["mcp_servers"] = records
	}); err != nil {
		return nil, err
	}
	if updated == nil {
		return nil, fmt.Errorf("mcp server %q is not installed", id)
	}
	return map[string]any{"ok": true, "server": updated}, nil
}

func (r *Runtime) mcpServerUninstall(ctx context.Context, params map[string]any) (map[string]any, error) {
	id := sanitizeNativeID(fallbackString(trimString(params["id"]), trimString(params["name"])))
	if id == "" {
		return nil, fmt.Errorf("mcp server id is required")
	}
	removed := false
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		records := configList(config, "mcp_servers")
		filtered := records[:0]
		for _, record := range records {
			if sanitizeNativeID(trimString(record["id"])) == id {
				removed = true
				continue
			}
			filtered = append(filtered, record)
		}
		config["mcp_servers"] = filtered
	}); err != nil {
		return nil, err
	}
	_ = os.RemoveAll(filepath.Join(r.dataDir, "mcp", id))
	return map[string]any{"ok": removed, "id": id}, nil
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
