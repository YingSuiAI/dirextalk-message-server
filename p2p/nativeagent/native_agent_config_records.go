package nativeagent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type managedConfigRecordSpec struct {
	configKey   string
	entityName  string
	responseKey string
	dataDir     string
}

var (
	skillConfigRecord = managedConfigRecordSpec{
		configKey:   "skills",
		entityName:  "skill",
		responseKey: "skill",
		dataDir:     "skills",
	}
	mcpServerConfigRecord = managedConfigRecordSpec{
		configKey:   "mcp_servers",
		entityName:  "mcp server",
		responseKey: "server",
		dataDir:     "mcp",
	}
)

func (r *Runtime) setManagedConfigRecordEnabled(
	ctx context.Context,
	params map[string]any,
	enabled bool,
	spec managedConfigRecordSpec,
) (map[string]any, error) {
	id := managedConfigRecordID(params)
	if id == "" {
		return nil, fmt.Errorf("%s id is required", spec.entityName)
	}

	var updated map[string]any
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		records := configList(config, spec.configKey)
		for _, record := range records {
			if sanitizeNativeID(trimString(record["id"])) == id {
				record["enabled"] = enabled
				updated = record
				break
			}
		}
		config[spec.configKey] = records
	}); err != nil {
		return nil, err
	}
	if updated == nil {
		return nil, fmt.Errorf("%s %q is not installed", spec.entityName, id)
	}
	return map[string]any{"ok": true, spec.responseKey: updated}, nil
}

func (r *Runtime) uninstallManagedConfigRecord(
	ctx context.Context,
	params map[string]any,
	spec managedConfigRecordSpec,
) (map[string]any, error) {
	id := managedConfigRecordID(params)
	if id == "" {
		return nil, fmt.Errorf("%s id is required", spec.entityName)
	}

	removed := false
	if err := r.updateAgentConfig(ctx, func(config map[string]any) {
		records := configList(config, spec.configKey)
		filtered := records[:0]
		for _, record := range records {
			if sanitizeNativeID(trimString(record["id"])) == id {
				removed = true
				continue
			}
			filtered = append(filtered, record)
		}
		config[spec.configKey] = filtered
	}); err != nil {
		return nil, err
	}
	_ = os.RemoveAll(filepath.Join(r.dataDir, spec.dataDir, id))
	return map[string]any{"ok": removed, "id": id}, nil
}

func managedConfigRecordID(params map[string]any) string {
	return sanitizeNativeID(fallbackString(trimString(params["id"]), trimString(params["name"])))
}
