package plugins

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkplugin"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) install(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	return m.apply(ctx, "install", params)
}

func (m *Module) enable(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	return m.apply(ctx, "enable", params)
}

func (m *Module) disable(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	return m.apply(ctx, "disable", params)
}

func (m *Module) uninstall(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	return m.apply(ctx, "uninstall", params)
}

func (m *Module) configGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	plugin, apiErr := m.requirePlugin(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	secretStatus, apiErr := m.secretStatus(ctx, plugin)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"plugin_id":     plugin.ID,
		"config":        plugin.Config,
		"secret_status": secretStatus,
	}, nil
}

func (m *Module) configUpdate(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	plugin, apiErr := m.requirePlugin(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	config, ok := params["config"].(map[string]any)
	if !ok {
		return nil, actionbase.BadRequest("config object is required")
	}
	secrets := secretsFromParams(params)
	plugin.Config = sanitizeConfig(config, secrets)
	plugin.UpdatedAt = m.now().UTC().UnixMilli()
	if err := m.save(ctx, plugin); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if err := m.saveSecrets(ctx, plugin.ID, secrets); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if plugin.Enabled && plugin.Status == StatusEnabled {
		op := RunnerOperation{
			Action:        "enable",
			PluginID:      plugin.ID,
			Name:          plugin.Name,
			Version:       plugin.Version,
			Image:         plugin.Image,
			Digest:        plugin.Digest,
			ContainerName: ContainerName(plugin.ID),
			Config:        cloneMap(plugin.Config),
			Env:           m.runtimeEnv(plugin),
			Volumes:       runtimeVolumes(plugin),
		}
		if err := m.runner.ApplyPlugin(ctx, op); err != nil {
			return nil, actionbase.StatusError(http.StatusBadGateway, err.Error())
		}
	}
	secretStatus, apiErr := m.secretStatus(ctx, plugin)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"plugin_id":     plugin.ID,
		"config":        plugin.Config,
		"secret_status": secretStatus,
	}, nil
}

func (m *Module) apply(ctx context.Context, action string, params map[string]any) (any, *actionbase.Error) {
	pluginID := actionbase.String(params["plugin_id"])
	if pluginID == "" {
		return nil, actionbase.BadRequest("plugin_id is required")
	}
	entry, ok := FindOfficialPlugin(pluginID)
	if !ok {
		return nil, actionbase.BadRequest("unknown official plugin")
	}
	now := m.now().UTC().UnixMilli()
	plugin, exists, err := m.get(ctx, pluginID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if action != "install" && !exists {
		return nil, actionbase.StatusError(http.StatusNotFound, "plugin is not installed")
	}
	secrets := secretsFromParams(params)
	if action == "install" {
		plugin = instanceFromCatalog(entry, now)
		if config, ok := params["config"].(map[string]any); ok {
			plugin.Config = sanitizeConfig(config, secrets)
		}
	} else {
		plugin.Name = entry.Name
		plugin.Version = entry.Version
		plugin.Image = entry.Image
		plugin.Digest = entry.Digest
	}
	runtimeEnv := map[string]string{}
	if action == "enable" {
		runtimeEnv = m.runtimeEnv(plugin)
	}

	job := dirextalkplugin.Job{
		JobID:     m.newJobID(),
		PluginID:  pluginID,
		Action:    action,
		Status:    jobStatusSucceeded,
		CreatedAt: now,
		UpdatedAt: now,
	}
	op := RunnerOperation{
		Action:        action,
		PluginID:      pluginID,
		Name:          entry.Name,
		Version:       entry.Version,
		Image:         entry.Image,
		Digest:        entry.Digest,
		ContainerName: ContainerName(pluginID),
		Config:        cloneMap(plugin.Config),
		Env:           runtimeEnv,
		Volumes:       runtimeVolumes(plugin),
	}
	if err := m.runner.ApplyPlugin(ctx, op); err != nil {
		job.Status = jobStatusFailed
		job.Message = err.Error()
		_ = m.saveJob(ctx, job)
		return nil, actionbase.StatusError(http.StatusBadGateway, err.Error())
	}

	switch action {
	case "install":
		plugin.Status = StatusInstalled
		plugin.Enabled = false
	case "enable":
		plugin.Status = StatusEnabled
		plugin.Enabled = true
	case "disable":
		plugin.Status = StatusDisabled
		plugin.Enabled = false
	case "uninstall":
		plugin.Status = StatusRemoved
		plugin.Enabled = false
	default:
		return nil, actionbase.InternalError(errors.New("unhandled plugin action"))
	}
	plugin.LastJobID = job.JobID
	plugin.UpdatedAt = now
	if plugin.CreatedAt == 0 {
		plugin.CreatedAt = now
	}
	if err := m.save(ctx, plugin); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if err := m.saveSecrets(ctx, plugin.ID, secrets); err != nil {
		return nil, actionbase.InternalError(err)
	}
	if err := m.saveJob(ctx, job); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{
		"plugin_id": plugin.ID,
		"status":    plugin.Status,
		"enabled":   plugin.Enabled,
		"job_id":    job.JobID,
		"plugin":    plugin,
	}, nil
}

func instanceFromCatalog(entry dirextalkplugin.CatalogEntry, now int64) dirextalkplugin.Instance {
	return dirextalkplugin.Instance{
		ID:        entry.ID,
		Name:      entry.Name,
		Version:   entry.Version,
		Image:     entry.Image,
		Digest:    entry.Digest,
		Status:    StatusInstalled,
		Enabled:   false,
		Config:    map[string]any{},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (m *Module) requirePlugin(ctx context.Context, params map[string]any) (dirextalkplugin.Instance, *actionbase.Error) {
	pluginID := actionbase.String(params["plugin_id"])
	if pluginID == "" {
		return dirextalkplugin.Instance{}, actionbase.BadRequest("plugin_id is required")
	}
	if pluginID == LegacyAgentPluginID {
		return dirextalkplugin.Instance{}, actionbase.StatusError(http.StatusNotFound, "plugin is not installed")
	}
	plugin, ok, err := m.get(ctx, pluginID)
	if err != nil {
		return dirextalkplugin.Instance{}, actionbase.InternalError(err)
	}
	if !ok {
		return dirextalkplugin.Instance{}, actionbase.StatusError(http.StatusNotFound, "plugin is not installed")
	}
	return plugin, nil
}

func (m *Module) now() time.Time {
	if m != nil && m.config.Now != nil {
		return m.config.Now()
	}
	return time.Now()
}

func (m *Module) newJobID() string {
	if m != nil && m.config.NewJobID != nil {
		return m.config.NewJobID()
	}
	return "plugin_job_" + m.now().UTC().Format("20060102150405.000000000")
}
