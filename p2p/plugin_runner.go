package p2p

import pluginsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/plugins"

// Public aliases preserve Config.PluginRunner and custom runner integrations.
type PluginRunner = pluginsmodule.Runner
type PluginRunnerOperation = pluginsmodule.RunnerOperation
type PluginInvokeRequest = pluginsmodule.InvokeRequest
type PluginStreamEvent = pluginsmodule.StreamEvent
