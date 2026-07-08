package p2p

import "context"

func agentConfigToNativeMap(cfg agentConfig) map[string]any {
	out := cloneAnyMap(cfg.Native)
	out["display_name"] = cfg.DisplayName
	out["avatar_url"] = cfg.AvatarURL
	out["context_window"] = cfg.ContextWindow
	out["enabled"] = cfg.Enabled
	out["model"] = cfg.Model
	out["system_prompt"] = cfg.SystemPrompt
	out["mcp_blocked_room_ids"] = append([]string(nil), cfg.MCPBlockedRoomIDs...)
	return sanitizePluginConfig(agentPluginID, out, nil)
}

func agentConfigFromNativeMap(current agentConfig, config map[string]any) agentConfig {
	merged := agentConfigToNativeMap(current)
	for key, value := range config {
		merged[key] = value
	}
	merged = sanitizePluginConfig(agentPluginID, merged, nil)

	next := current
	if _, ok := merged["display_name"]; ok {
		next.DisplayName = trimString(merged["display_name"])
	}
	if _, ok := merged["avatar_url"]; ok {
		next.AvatarURL = trimString(merged["avatar_url"])
	}
	if value := int64Param(merged["context_window"]); value > 0 {
		next.ContextWindow = value
	}
	if _, ok := merged["enabled"]; ok {
		next.Enabled = boolParam(merged["enabled"])
	}
	if _, ok := merged["model"]; ok {
		next.Model = trimString(merged["model"])
	}
	if _, ok := merged["system_prompt"]; ok {
		next.SystemPrompt = trimString(merged["system_prompt"])
	}
	if _, ok := merged["mcp_blocked_room_ids"]; ok {
		next.MCPBlockedRoomIDs = stringSliceParam(merged["mcp_blocked_room_ids"])
	}

	native := make(map[string]any)
	for key, value := range merged {
		if nativeAgentSharedConfigKey(key) {
			continue
		}
		native[key] = value
	}
	if len(native) > 0 {
		next.Native = native
	} else {
		next.Native = nil
	}
	return normalizeAgentConfig(next)
}

func migrateLegacyAgentPluginConfig(ctx context.Context, store Store, state *portalState) (bool, error) {
	if store == nil || state == nil {
		return false, nil
	}
	plugin, ok, err := store.GetPlugin(ctx, agentPluginID)
	if err != nil || !ok {
		return false, err
	}
	legacy := sanitizePluginConfig(agentPluginID, plugin.Config, nil)
	if len(legacy) == 0 {
		return false, nil
	}
	before := agentConfigToNativeMap(state.AgentConfig)
	state.AgentConfig = mergeLegacyAgentConfig(state.AgentConfig, legacy)
	after := agentConfigToNativeMap(state.AgentConfig)
	return jsonValue(before) != jsonValue(after), nil
}

func mergeLegacyAgentConfig(current agentConfig, legacy map[string]any) agentConfig {
	next := current
	if trimString(next.DisplayName) == "" && trimString(legacy["display_name"]) != "" {
		next.DisplayName = trimString(legacy["display_name"])
	}
	if trimString(next.AvatarURL) == "" {
		if _, ok := legacy["avatar_url"]; ok {
			next.AvatarURL = trimString(legacy["avatar_url"])
		}
	}
	if next.ContextWindow <= 0 {
		if value := int64Param(legacy["context_window"]); value > 0 {
			next.ContextWindow = value
		}
	}
	if agentConfigEmpty(current) {
		if _, ok := legacy["enabled"]; ok {
			next.Enabled = boolParam(legacy["enabled"])
		}
	}
	if trimString(next.Model) == "" && trimString(legacy["model"]) != "" {
		next.Model = trimString(legacy["model"])
	}
	if trimString(next.SystemPrompt) == "" && trimString(legacy["system_prompt"]) != "" {
		next.SystemPrompt = trimString(legacy["system_prompt"])
	}
	if len(next.MCPBlockedRoomIDs) == 0 {
		next.MCPBlockedRoomIDs = stringSliceParam(legacy["mcp_blocked_room_ids"])
	}

	native := cloneAnyMap(next.Native)
	for key, value := range legacy {
		if nativeAgentSharedConfigKey(key) {
			continue
		}
		if _, exists := native[key]; !exists {
			native[key] = value
		}
	}
	if len(native) > 0 {
		next.Native = native
	}
	return normalizeAgentConfig(next)
}

func agentConfigEmpty(cfg agentConfig) bool {
	return trimString(cfg.DisplayName) == "" &&
		trimString(cfg.AvatarURL) == "" &&
		cfg.ContextWindow == 0 &&
		!cfg.Enabled &&
		trimString(cfg.Model) == "" &&
		trimString(cfg.SystemPrompt) == "" &&
		len(cfg.MCPBlockedRoomIDs) == 0 &&
		len(cfg.Native) == 0
}

func nativeAgentSharedConfigKey(key string) bool {
	switch key {
	case "display_name", "avatar_url", "context_window", "enabled", "model", "system_prompt", "mcp_blocked_room_ids":
		return true
	default:
		return false
	}
}
