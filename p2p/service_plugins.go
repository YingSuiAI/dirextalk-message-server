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
			Image:          "docker.io/dirextalk/agent-plugin:latest",
			Digest:         "",
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
				"agent.chat.stream",
				"agent.context.compress",
				"agent.models.list",
				"agent.skills.list",
				"agent.skills.registry.search",
				"agent.mcp.servers.list",
				"agent.mcp.registry.search",
				"agent.config.propose_patch",
				"agent.contacts.list",
				"agent.contacts.search",
				"agent.rooms.search",
				"agent.messages.list",
				"agent.messages.send",
				"agent.summarize",
			},
			ConfigSchema: map[string]any{
				"providers":           []string{"openai", "anthropic", "deepseek", "gemini", "vertex", "openai_compatible", "openrouter", "litellm"},
				"provider":            []string{"openai", "anthropic", "deepseek", "gemini", "vertex", "openai_compatible", "openrouter", "litellm"},
				"model":               "string",
				"base_url":            "string",
				"system_prompt":       "string",
				"enabled_tools":       []string{"search_contacts", "search_rooms", "list_messages", "send_message", "summarize_conversation"},
				"skills_registry_url": "string",
				"mcp_registry_url":    "string",
				"skills":              []map[string]any{},
				"mcp_servers":         []map[string]any{},
				"temperature":         "number",
				"max_output_tokens":   "number",
				"context_window":      "number",
				"top_p":               "number",
				"top_k":               "number",
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
	secretStatus, apiErr := s.pluginSecretStatus(ctx, plugin)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"plugin_id":     plugin.ID,
		"config":        plugin.Config,
		"secret_status": secretStatus,
	}, nil
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
	secrets := pluginSecretsFromParams(params)
	plugin.Config = sanitizePluginConfig(plugin.ID, config, secrets)
	plugin.UpdatedAt = time.Now().UTC().UnixMilli()
	if err := s.savePlugin(ctx, plugin); err != nil {
		return nil, internalError(err)
	}
	if err := s.savePluginSecrets(ctx, plugin.ID, secrets); err != nil {
		return nil, internalError(err)
	}
	secretStatus, apiErr := s.pluginSecretStatus(ctx, plugin)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"plugin_id":     plugin.ID,
		"config":        plugin.Config,
		"secret_status": secretStatus,
	}, nil
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

func (s *Service) pluginInvokeAction(ctx context.Context, params map[string]any) (any, *apiError) {
	req, clientAction, apiErr := s.pluginInvokeRequest(ctx, params, false)
	if apiErr != nil {
		return nil, apiErr
	}
	result, err := s.pluginRunner.InvokePlugin(ctx, req)
	if err != nil {
		return nil, statusError(http.StatusBadGateway, err.Error())
	}
	return map[string]any{
		"plugin_id": req.PluginID,
		"action":    clientAction,
		"result":    result,
	}, nil
}

func (s *Service) pluginInvokeStreamAction(context.Context, map[string]any) (any, *apiError) {
	return nil, badRequest("action requires websocket")
}

func (s *Service) pluginInvokeRequest(ctx context.Context, params map[string]any, stream bool) (PluginInvokeRequest, string, *apiError) {
	pluginID := trimString(params["plugin_id"])
	if pluginID == "" {
		return PluginInvokeRequest{}, "", badRequest("plugin_id is required")
	}
	entry, ok := findOfficialPlugin(pluginID)
	if !ok {
		return PluginInvokeRequest{}, "", badRequest("unknown official plugin")
	}
	plugin, exists, err := s.getPlugin(ctx, pluginID)
	if err != nil {
		return PluginInvokeRequest{}, "", internalError(err)
	}
	if !exists {
		return PluginInvokeRequest{}, "", statusError(http.StatusNotFound, "plugin is not installed")
	}
	if !plugin.Enabled || plugin.Status != pluginStatusEnabled {
		return PluginInvokeRequest{}, "", statusError(http.StatusConflict, "plugin is not enabled")
	}
	clientAction := trimString(params["action"])
	if clientAction == "" {
		return PluginInvokeRequest{}, "", badRequest("action is required")
	}
	runnerAction := clientAction
	if stream {
		if strings.HasSuffix(clientAction, ".stream") {
			runnerAction = clientAction
		} else {
			runnerAction = clientAction + ".stream"
		}
	}
	if !pluginActionAllowed(entry, runnerAction) {
		return PluginInvokeRequest{}, "", badRequest("plugin action is not allowed")
	}
	if !stream && strings.HasSuffix(runnerAction, ".stream") {
		return PluginInvokeRequest{}, "", badRequest("stream action requires websocket")
	}
	invokeParams := map[string]any{}
	if rawParams, ok := params["params"].(map[string]any); ok {
		invokeParams = cloneAnyMap(rawParams)
	} else if params["params"] != nil {
		return PluginInvokeRequest{}, "", badRequest("params must be an object")
	}
	return PluginInvokeRequest{
		PluginID:      plugin.ID,
		ContainerName: pluginContainerName(plugin.ID),
		Action:        runnerAction,
		Params:        invokeParams,
	}, strings.TrimSuffix(clientAction, ".stream"), nil
}

func pluginActionAllowed(entry pluginCatalogEntry, action string) bool {
	action = strings.TrimSpace(action)
	for _, allowed := range entry.Actions {
		if strings.TrimSpace(allowed) == action {
			return true
		}
	}
	return false
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
	secrets := pluginSecretsFromParams(params)
	if action == "install" {
		plugin = pluginFromCatalogEntry(entry, now)
		if config, ok := params["config"].(map[string]any); ok {
			plugin.Config = sanitizePluginConfig(plugin.ID, config, secrets)
		}
	} else {
		plugin.Name = entry.Name
		plugin.Version = entry.Version
		plugin.Image = entry.Image
		plugin.Digest = entry.Digest
	}
	runtimeEnv := map[string]string{}
	if action == "enable" {
		var apiErr *apiError
		runtimeEnv, apiErr = s.pluginRuntimeEnv(ctx, plugin)
		if apiErr != nil {
			return nil, apiErr
		}
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
		Env:           runtimeEnv,
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
	if err := s.savePluginSecrets(ctx, plugin.ID, secrets); err != nil {
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

func (s *Service) savePluginSecrets(ctx context.Context, pluginID string, secrets map[string]string) error {
	if len(secrets) == 0 {
		return nil
	}
	now := time.Now().UTC().UnixMilli()
	if s.store != nil {
		for name, value := range secrets {
			if value == "" {
				continue
			}
			if err := s.store.UpsertPluginSecret(ctx, pluginSecret{
				PluginID:  pluginID,
				Name:      name,
				Value:     value,
				UpdatedAt: now,
			}); err != nil {
				return err
			}
		}
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pluginSecrets[pluginID] == nil {
		s.pluginSecrets[pluginID] = map[string]pluginSecret{}
	}
	for name, value := range secrets {
		if value == "" {
			continue
		}
		s.pluginSecrets[pluginID][name] = pluginSecret{
			PluginID:  pluginID,
			Name:      name,
			Value:     value,
			UpdatedAt: now,
		}
	}
	return nil
}

func (s *Service) getPluginSecret(ctx context.Context, pluginID, name string) (pluginSecret, bool, error) {
	if s.store != nil {
		return s.store.GetPluginSecret(ctx, pluginID, name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	secrets := s.pluginSecrets[pluginID]
	if secrets == nil {
		return pluginSecret{}, false, nil
	}
	secret, ok := secrets[name]
	return secret, ok, nil
}

func (s *Service) pluginSecretStatus(ctx context.Context, plugin pluginInstance) (map[string]any, *apiError) {
	names := pluginSecretNamesFromConfig(plugin.Config)
	status := make(map[string]any, len(names))
	for _, name := range names {
		secret, ok, err := s.getPluginSecret(ctx, plugin.ID, name)
		if err != nil {
			return nil, internalError(err)
		}
		status[name] = map[string]any{
			"configured": ok && secret.Value != "",
			"updated_at": secret.UpdatedAt,
		}
	}
	return status, nil
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

func pluginSecretsFromParams(params map[string]any) map[string]string {
	secrets := map[string]string{}
	if params == nil {
		return secrets
	}
	if rawSecrets, ok := params["secrets"].(map[string]any); ok {
		for key, value := range rawSecrets {
			name := strings.TrimSpace(key)
			secret := trimString(value)
			if name != "" && secret != "" {
				secrets[name] = secret
			}
		}
	}
	if config, ok := params["config"].(map[string]any); ok {
		collectPluginConfigSecrets(config, secrets)
	}
	return secrets
}

func collectPluginConfigSecrets(config map[string]any, secrets map[string]string) {
	if secrets == nil || config == nil {
		return
	}
	if value := trimString(config["api_key"]); value != "" {
		secrets["api_key"] = value
	}
	profiles, ok := config["model_profiles"].([]any)
	if !ok {
		return
	}
	for index, rawProfile := range profiles {
		profile, ok := rawProfile.(map[string]any)
		if !ok {
			continue
		}
		if value := trimString(profile["api_key"]); value != "" {
			secrets[pluginProfileSecretName(profile, index)] = value
		}
	}
}

func sanitizePluginConfig(pluginID string, config map[string]any, secrets map[string]string) map[string]any {
	sanitized := cloneAnyMap(config)
	if sanitized == nil {
		sanitized = map[string]any{}
	}
	if secrets == nil {
		secrets = map[string]string{}
	}
	if value := trimString(sanitized["api_key"]); value != "" {
		secrets["api_key"] = value
	}
	delete(sanitized, "api_key")
	if pluginID == "io.dirextalk.agent" {
		if secrets["api_key"] != "" {
			sanitized["api_key_ref"] = "secret:api_key"
		}
		if profiles, ok := sanitized["model_profiles"].([]any); ok {
			sanitized["model_profiles"] = sanitizeAgentModelProfiles(profiles, secrets)
		}
	}
	return sanitized
}

func sanitizeAgentModelProfiles(profiles []any, secrets map[string]string) []any {
	sanitized := make([]any, 0, len(profiles))
	for index, rawProfile := range profiles {
		profile, ok := rawProfile.(map[string]any)
		if !ok {
			sanitized = append(sanitized, rawProfile)
			continue
		}
		cloned := cloneAnyMap(profile)
		if value := trimString(cloned["api_key"]); value != "" {
			name := pluginProfileSecretName(cloned, index)
			secrets[name] = value
			cloned["api_key_ref"] = "secret:" + name
		}
		delete(cloned, "api_key")
		sanitized = append(sanitized, cloned)
	}
	return sanitized
}

func pluginSecretNamesFromConfig(config map[string]any) []string {
	seen := map[string]bool{}
	add := func(ref string) {
		name, ok := secretRefName(ref)
		if ok {
			seen[name] = true
		}
	}
	add(pluginConfigString(config, "api_key_ref"))
	if profiles, ok := config["model_profiles"].([]any); ok {
		for _, rawProfile := range profiles {
			profile, ok := rawProfile.(map[string]any)
			if !ok {
				continue
			}
			add(pluginConfigString(profile, "api_key_ref"))
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	return names
}

func secretRefName(ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	const prefix = "secret:"
	if !strings.HasPrefix(ref, prefix) {
		return "", false
	}
	name := strings.TrimSpace(strings.TrimPrefix(ref, prefix))
	return name, name != ""
}

func pluginProfileSecretName(profile map[string]any, index int) string {
	id := strings.TrimSpace(pluginConfigString(profile, "id"))
	if id == "" {
		id = pluginConfigString(profile, "name")
	}
	if id == "" {
		id = jsonValue(index)
	}
	return "model_profile_" + strings.ToLower(pluginEnvSuffix(id)) + "_api_key"
}

func (s *Service) pluginRuntimeEnv(ctx context.Context, plugin pluginInstance) (map[string]string, *apiError) {
	s.mu.Lock()
	homeserver := strings.TrimSpace(s.homeserver)
	agentToken := s.agentToken
	s.mu.Unlock()
	env := map[string]string{
		"DIREXTALK_BASE_URL":        pluginBackendBaseURL(homeserver),
		"DIREXTALK_AGENT_TOKEN":     agentToken,
		"DIREXTALK_AGENT_TOKEN_REF": "env:DIREXTALK_AGENT_TOKEN",
	}
	if plugin.ID == "io.dirextalk.agent" {
		if apiErr := s.mergeAgentPluginEnv(ctx, plugin.ID, env, plugin.Config); apiErr != nil {
			return nil, apiErr
		}
	}
	return env, nil
}

func pluginBackendBaseURL(homeserver string) string {
	if configured := strings.TrimSpace(os.Getenv("P2P_PLUGIN_BACKEND_BASE_URL")); configured != "" {
		return configured
	}
	homeserver = strings.TrimSpace(homeserver)
	if homeserver == "" || isAutoHomeserver(homeserver) {
		return "http://message-server:8008"
	}
	return homeserver
}

func (s *Service) mergeAgentPluginEnv(ctx context.Context, pluginID string, env map[string]string, config map[string]any) *apiError {
	if value := pluginConfigString(config, "provider"); value != "" {
		env["AGENT_MODEL_PROVIDER"] = value
	}
	if value := pluginConfigString(config, "model"); value != "" {
		env["AGENT_MODEL"] = value
	}
	if value := pluginConfigString(config, "base_url"); value != "" {
		env["AGENT_BASE_URL"] = value
	}
	if value := pluginConfigString(config, "default_model_profile_id"); value != "" {
		env["AGENT_DEFAULT_MODEL_PROFILE_ID"] = value
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
	if value := pluginConfigString(config, "top_p"); value != "" {
		env["AGENT_TOP_P"] = value
	}
	if value := pluginConfigString(config, "top_k"); value != "" {
		env["AGENT_TOP_K"] = value
	}
	if value := pluginConfigString(config, "reasoning_mode"); value != "" {
		env["AGENT_REASONING_MODE"] = value
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
	if profiles, ok := config["model_profiles"]; ok {
		data, err := json.Marshal(profiles)
		if err != nil {
			return internalError(err)
		}
		env["AGENT_MODEL_PROFILES_JSON"] = string(data)
	}
	return nil
}

func (s *Service) resolvePluginSecretRef(ctx context.Context, pluginID string, env map[string]string, ref string, envName string) (string, *apiError) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", nil
	}
	if name, ok := secretRefName(ref); ok {
		secret, exists, err := s.getPluginSecret(ctx, pluginID, name)
		if err != nil {
			return "", internalError(err)
		}
		if !exists || secret.Value == "" {
			return "", badRequest("plugin secret is not configured: " + name)
		}
		env[envName] = secret.Value
		return "env:" + envName, nil
	}
	if name, ok := envRefName(ref); ok {
		if secret := os.Getenv(name); secret != "" {
			env[name] = secret
		}
		return ref, nil
	}
	return ref, nil
}

func (s *Service) resolveAgentModelProfiles(ctx context.Context, pluginID string, env map[string]string, profiles any) ([]any, *apiError) {
	rawProfiles, ok := profiles.([]any)
	if !ok {
		return nil, badRequest("model_profiles must be an array")
	}
	resolved := make([]any, 0, len(rawProfiles))
	for index, rawProfile := range rawProfiles {
		profile, ok := rawProfile.(map[string]any)
		if !ok {
			resolved = append(resolved, rawProfile)
			continue
		}
		cloned := cloneAnyMap(profile)
		if ref := pluginConfigString(cloned, "api_key_ref"); ref != "" {
			envName := "AGENT_PROFILE_API_KEY_" + pluginEnvSuffix(pluginProfileEnvID(cloned, index))
			resolvedRef, apiErr := s.resolvePluginSecretRef(ctx, pluginID, env, ref, envName)
			if apiErr != nil {
				return nil, apiErr
			}
			cloned["api_key_ref"] = resolvedRef
		}
		resolved = append(resolved, cloned)
	}
	return resolved, nil
}

func pluginProfileEnvID(profile map[string]any, index int) string {
	if value := pluginConfigString(profile, "id"); value != "" {
		return value
	}
	if value := pluginConfigString(profile, "name"); value != "" {
		return value
	}
	return jsonValue(index)
}

func pluginEnvSuffix(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	var builder strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'A' && ch <= 'Z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		default:
			builder.WriteByte('_')
		}
	}
	suffix := strings.Trim(builder.String(), "_")
	if suffix == "" {
		return "DEFAULT"
	}
	return suffix
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
