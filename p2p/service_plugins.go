package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	pluginStatusInstalled = "installed"
	pluginStatusEnabled   = "enabled"
	pluginStatusDisabled  = "disabled"
	pluginStatusRemoved   = "uninstalled"

	pluginJobStatusSucceeded = "succeeded"
	pluginJobStatusFailed    = "failed"
)

func officialPluginCatalog() []pluginCatalogEntry {
	return []pluginCatalogEntry{
		{
			ID:             "io.dirextalk.agent",
			Name:           "Dirextalk Agent",
			Version:        "0.1.0",
			Description:    "Official Pydantic AI Agent plugin with Dirextalk tools, skills, and MCP server support.",
			Image:          "docker.io/dirextalk/agent-plugin",
			Digest:         "sha256:4acd5a6e76fb8ba07b89adff210d21725a2c0801e087108b57a55d65d73a8e5a",
			MinBaseVersion: "0.1.0",
			Permissions: []string{
				"matrix.messages.read",
				"matrix.messages.send",
				"rooms.members.read",
				"mcp.call",
				"skills.install",
			},
			Events: []string{
				"message.created",
				"agent.mentioned",
			},
			Actions: []string{
				"agent.chat",
				"agent.summarize",
			},
			ConfigSchema: map[string]any{
				"provider":    []string{"openai", "anthropic", "deepseek", "gemini", "vertex", "openai_compatible", "openrouter", "litellm"},
				"model":       "string",
				"base_url":    "string",
				"api_key_ref": "secret",
			},
		},
	}
}

func findOfficialPlugin(pluginID string) (pluginCatalogEntry, bool) {
	pluginID = strings.TrimSpace(pluginID)
	for _, entry := range officialPluginCatalog() {
		if entry.ID == pluginID {
			return entry, true
		}
	}
	return pluginCatalogEntry{}, false
}

func (s *Service) loadPlugins(ctx context.Context) error {
	if s.store == nil {
		return nil
	}
	plugins, err := s.store.ListPlugins(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, plugin := range plugins {
		s.plugins[plugin.ID] = normalizePluginInstance(plugin)
	}
	return nil
}

func (s *Service) pluginCatalogListAction(context.Context, map[string]any) (any, *apiError) {
	return map[string]any{"plugins": officialPluginCatalog()}, nil
}

func (s *Service) pluginInstalledListAction(ctx context.Context, _ map[string]any) (any, *apiError) {
	plugins, err := s.listPluginInstances(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"plugins": plugins}, nil
}

func (s *Service) pluginInstallAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.applyPluginAction(ctx, "install", params)
}

func (s *Service) pluginEnableAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.applyPluginAction(ctx, "enable", params)
}

func (s *Service) pluginDisableAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.applyPluginAction(ctx, "disable", params)
}

func (s *Service) pluginUninstallAction(ctx context.Context, params map[string]any) (any, *apiError) {
	return s.applyPluginAction(ctx, "uninstall", params)
}

func (s *Service) pluginConfigGetAction(ctx context.Context, params map[string]any) (any, *apiError) {
	plugin, apiErr := s.requirePlugin(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{"plugin_id": plugin.ID, "config": plugin.Config}, nil
}

func (s *Service) pluginConfigUpdateAction(ctx context.Context, params map[string]any) (any, *apiError) {
	plugin, apiErr := s.requirePlugin(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	config, ok := params["config"].(map[string]any)
	if !ok {
		return nil, badRequest("config object is required")
	}
	plugin.Config = cloneAnyMap(config)
	plugin.UpdatedAt = time.Now().UTC().UnixMilli()
	if err := s.savePlugin(ctx, plugin); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{"plugin_id": plugin.ID, "config": plugin.Config}, nil
}

func (s *Service) pluginJobGetAction(ctx context.Context, params map[string]any) (any, *apiError) {
	jobID := trimString(params["job_id"])
	if jobID == "" {
		return nil, badRequest("job_id is required")
	}
	job, ok, err := s.getPluginJob(ctx, jobID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "plugin job not found")
	}
	return job, nil
}

func (s *Service) pluginHealthAction(ctx context.Context, params map[string]any) (any, *apiError) {
	plugin, apiErr := s.requirePlugin(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"plugin_id": plugin.ID,
		"status":    plugin.Status,
		"enabled":   plugin.Enabled,
		"ok":        plugin.Enabled && plugin.Status == pluginStatusEnabled,
	}, nil
}

func (s *Service) pluginLogsTailAction(ctx context.Context, params map[string]any) (any, *apiError) {
	plugin, apiErr := s.requirePlugin(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"plugin_id": plugin.ID,
		"logs":      []string{},
		"message":   "plugin log tail is not available from the first-version runner",
	}, nil
}

func (s *Service) applyPluginAction(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	pluginID := trimString(params["plugin_id"])
	if pluginID == "" {
		return nil, badRequest("plugin_id is required")
	}
	entry, ok := findOfficialPlugin(pluginID)
	if !ok {
		return nil, badRequest("unknown official plugin")
	}
	now := time.Now().UTC().UnixMilli()
	plugin, exists, err := s.getPlugin(ctx, pluginID)
	if err != nil {
		return nil, internalError(err)
	}
	if action != "install" && !exists {
		return nil, statusError(http.StatusNotFound, "plugin is not installed")
	}
	if action == "install" {
		plugin = pluginFromCatalogEntry(entry, now)
		if config, ok := params["config"].(map[string]any); ok {
			plugin.Config = cloneAnyMap(config)
		}
	} else {
		plugin.Name = entry.Name
		plugin.Version = entry.Version
		plugin.Image = entry.Image
		plugin.Digest = entry.Digest
	}

	job := pluginJob{
		JobID:     randomToken("plugin_job"),
		PluginID:  pluginID,
		Action:    action,
		Status:    pluginJobStatusSucceeded,
		CreatedAt: now,
		UpdatedAt: now,
	}
	op := PluginRunnerOperation{
		Action:        action,
		PluginID:      pluginID,
		Name:          entry.Name,
		Version:       entry.Version,
		Image:         entry.Image,
		Digest:        entry.Digest,
		ContainerName: pluginContainerName(pluginID),
		Config:        cloneAnyMap(plugin.Config),
		Env:           s.pluginRuntimeEnv(plugin),
	}
	if err := s.pluginRunner.ApplyPlugin(ctx, op); err != nil {
		job.Status = pluginJobStatusFailed
		job.Message = err.Error()
		_ = s.savePluginJob(ctx, job)
		return nil, statusError(http.StatusBadGateway, err.Error())
	}

	switch action {
	case "install":
		plugin.Status = pluginStatusInstalled
		plugin.Enabled = false
	case "enable":
		plugin.Status = pluginStatusEnabled
		plugin.Enabled = true
	case "disable":
		plugin.Status = pluginStatusDisabled
		plugin.Enabled = false
	case "uninstall":
		plugin.Status = pluginStatusRemoved
		plugin.Enabled = false
	default:
		return nil, internalError(errors.New("unhandled plugin action"))
	}
	plugin.LastJobID = job.JobID
	plugin.UpdatedAt = now
	if plugin.CreatedAt == 0 {
		plugin.CreatedAt = now
	}
	if err := s.savePlugin(ctx, plugin); err != nil {
		return nil, internalError(err)
	}
	if err := s.savePluginJob(ctx, job); err != nil {
		return nil, internalError(err)
	}
	return map[string]any{
		"plugin_id": plugin.ID,
		"status":    plugin.Status,
		"enabled":   plugin.Enabled,
		"job_id":    job.JobID,
		"plugin":    plugin,
	}, nil
}

func pluginFromCatalogEntry(entry pluginCatalogEntry, now int64) pluginInstance {
	return pluginInstance{
		ID:        entry.ID,
		Name:      entry.Name,
		Version:   entry.Version,
		Image:     entry.Image,
		Digest:    entry.Digest,
		Status:    pluginStatusInstalled,
		Enabled:   false,
		Config:    map[string]any{},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (s *Service) requirePlugin(ctx context.Context, params map[string]any) (pluginInstance, *apiError) {
	pluginID := trimString(params["plugin_id"])
	if pluginID == "" {
		return pluginInstance{}, badRequest("plugin_id is required")
	}
	plugin, ok, err := s.getPlugin(ctx, pluginID)
	if err != nil {
		return pluginInstance{}, internalError(err)
	}
	if !ok {
		return pluginInstance{}, statusError(http.StatusNotFound, "plugin is not installed")
	}
	return plugin, nil
}

func (s *Service) listPluginInstances(ctx context.Context) ([]pluginInstance, error) {
	if s.store != nil {
		plugins, err := s.store.ListPlugins(ctx)
		if err != nil {
			return nil, err
		}
		for i := range plugins {
			plugins[i] = normalizePluginInstance(plugins[i])
		}
		return plugins, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	plugins := make([]pluginInstance, 0, len(s.plugins))
	for _, plugin := range s.plugins {
		plugins = append(plugins, normalizePluginInstance(plugin))
	}
	return plugins, nil
}

func (s *Service) getPlugin(ctx context.Context, pluginID string) (pluginInstance, bool, error) {
	if s.store != nil {
		plugin, ok, err := s.store.GetPlugin(ctx, pluginID)
		if err != nil || !ok {
			return plugin, ok, err
		}
		return normalizePluginInstance(plugin), true, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	plugin, ok := s.plugins[pluginID]
	if !ok {
		return pluginInstance{}, false, nil
	}
	return normalizePluginInstance(plugin), true, nil
}

func (s *Service) savePlugin(ctx context.Context, plugin pluginInstance) error {
	plugin = normalizePluginInstance(plugin)
	if s.store != nil {
		return s.store.UpsertPlugin(ctx, plugin)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plugins[plugin.ID] = plugin
	return nil
}

func (s *Service) savePluginJob(ctx context.Context, job pluginJob) error {
	if s.store != nil {
		return s.store.UpsertPluginJob(ctx, job)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pluginJobs[job.JobID] = job
	return nil
}

func (s *Service) getPluginJob(ctx context.Context, jobID string) (pluginJob, bool, error) {
	if s.store != nil {
		return s.store.GetPluginJob(ctx, jobID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.pluginJobs[jobID]
	return job, ok, nil
}

func normalizePluginInstance(plugin pluginInstance) pluginInstance {
	plugin.ID = strings.TrimSpace(plugin.ID)
	plugin.Name = strings.TrimSpace(plugin.Name)
	plugin.Version = strings.TrimSpace(plugin.Version)
	plugin.Image = strings.TrimSpace(plugin.Image)
	plugin.Digest = strings.TrimSpace(plugin.Digest)
	plugin.Status = strings.TrimSpace(plugin.Status)
	if plugin.Config == nil {
		plugin.Config = map[string]any{}
	}
	return plugin
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func (s *Service) pluginRuntimeEnv(plugin pluginInstance) map[string]string {
	s.mu.Lock()
	homeserver := strings.TrimSpace(s.homeserver)
	agentToken := s.agentToken
	s.mu.Unlock()
	env := map[string]string{
		"DIREXTALK_BASE_URL":        fallbackString(homeserver, "http://message-server:8008"),
		"DIREXTALK_AGENT_TOKEN":     agentToken,
		"DIREXTALK_AGENT_TOKEN_REF": "env:DIREXTALK_AGENT_TOKEN",
	}
	if plugin.ID == "io.dirextalk.agent" {
		mergeAgentPluginEnv(env, plugin.Config)
	}
	return env
}

func mergeAgentPluginEnv(env map[string]string, config map[string]any) {
	if value := pluginConfigString(config, "provider"); value != "" {
		env["AGENT_MODEL_PROVIDER"] = value
	}
	if value := pluginConfigString(config, "model"); value != "" {
		env["AGENT_MODEL"] = value
	}
	if value := pluginConfigString(config, "api_key_ref"); value != "" {
		env["AGENT_API_KEY_REF"] = value
		if name, ok := envRefName(value); ok {
			if secret := os.Getenv(name); secret != "" {
				env[name] = secret
			}
		}
	}
	if value := pluginConfigString(config, "base_url"); value != "" {
		env["AGENT_BASE_URL"] = value
	}
	if value := pluginConfigString(config, "display_name"); value != "" {
		env["AGENT_DISPLAY_NAME"] = value
	}
	if value := pluginConfigString(config, "system_prompt"); value != "" {
		env["AGENT_SYSTEM_PROMPT"] = value
	}
	if value := pluginConfigString(config, "temperature"); value != "" {
		env["AGENT_TEMPERATURE"] = value
	}
	if value := pluginConfigString(config, "max_output_tokens"); value != "" {
		env["AGENT_MAX_OUTPUT_TOKENS"] = value
	}
	if value := pluginConfigString(config, "context_window"); value != "" {
		env["AGENT_CONTEXT_WINDOW"] = value
	}
	if value := pluginConfigListString(config, "enabled_tools"); value != "" {
		env["AGENT_ENABLED_TOOLS"] = value
	}
	if value := pluginConfigJSON(config, "skills"); value != "" {
		env["AGENT_SKILLS_JSON"] = value
	}
	if value := pluginConfigJSON(config, "mcp_servers"); value != "" {
		env["AGENT_MCP_SERVERS_JSON"] = value
	}
}

func pluginConfigString(config map[string]any, key string) string {
	if config == nil {
		return ""
	}
	switch value := config[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case float64, bool, int, int64:
		return strings.TrimSpace(jsonValue(value))
	default:
		return ""
	}
}

func pluginConfigListString(config map[string]any, key string) string {
	if config == nil {
		return ""
	}
	switch value := config[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case []string:
		return strings.Join(value, ",")
	case []any:
		items := make([]string, 0, len(value))
		for _, item := range value {
			if text := trimString(item); text != "" {
				items = append(items, text)
			}
		}
		return strings.Join(items, ",")
	default:
		return ""
	}
}

func pluginConfigJSON(config map[string]any, key string) string {
	if config == nil {
		return ""
	}
	value, ok := config[key]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func envRefName(ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	const prefix = "env:"
	if !strings.HasPrefix(ref, prefix) {
		return "", false
	}
	name := strings.TrimSpace(strings.TrimPrefix(ref, prefix))
	if name == "" || !pluginEnvNamePattern.MatchString(name) {
		return "", false
	}
	return name, true
}

func jsonValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}
