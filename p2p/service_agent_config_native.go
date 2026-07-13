package p2p

import (
	"context"

	agentmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agent"
)

// These wrappers keep the root Service and its focused compatibility tests
// stable while Native Agent configuration ownership lives in internal/agent.
func agentConfigToNativeMap(cfg agentConfig) map[string]any {
	return agentmodule.ToNativeMap(cfg)
}

func agentConfigFromNativeMap(current agentConfig, config map[string]any) agentConfig {
	return agentmodule.FromNativeMap(current, config)
}

func migrateLegacyAgentPluginConfig(ctx context.Context, store Store, state *portalState) (bool, error) {
	return agentmodule.MigrateLegacyPluginConfig(ctx, store, state, agentmodule.LegacyPluginID)
}
