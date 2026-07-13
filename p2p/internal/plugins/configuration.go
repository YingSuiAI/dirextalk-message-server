package plugins

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkplugin"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func NormalizeInstance(plugin dirextalkplugin.Instance) dirextalkplugin.Instance {
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

func cloneMap(values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func secretsFromParams(params map[string]any) map[string]string {
	secrets := map[string]string{}
	if params == nil {
		return secrets
	}
	if rawSecrets, ok := params["secrets"].(map[string]any); ok {
		for key, value := range rawSecrets {
			name := strings.TrimSpace(key)
			secret := actionbase.String(value)
			if name != "" && secret != "" {
				secrets[name] = secret
			}
		}
	}
	if config, ok := params["config"].(map[string]any); ok {
		collectConfigSecrets(config, secrets)
	}
	return secrets
}

func collectConfigSecrets(config map[string]any, secrets map[string]string) {
	if secrets == nil || config == nil {
		return
	}
	if value := actionbase.String(config["api_key"]); value != "" {
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
		if value := actionbase.String(profile["api_key"]); value != "" {
			secrets[profileSecretName(profile, index)] = value
		}
	}
}

func sanitizeConfig(config map[string]any, secrets map[string]string) map[string]any {
	sanitized := cloneMap(config)
	if secrets == nil {
		secrets = map[string]string{}
	}
	if value := actionbase.String(sanitized["api_key"]); value != "" {
		secrets["api_key"] = value
	}
	delete(sanitized, "api_key")
	if profiles, ok := config["model_profiles"].([]any); ok {
		clonedProfiles := make([]any, len(profiles))
		for index, rawProfile := range profiles {
			profile, ok := rawProfile.(map[string]any)
			if !ok {
				clonedProfiles[index] = rawProfile
				continue
			}
			cloned := cloneMap(profile)
			if value := actionbase.String(cloned["api_key"]); value != "" {
				secrets[profileSecretName(cloned, index)] = value
			}
			delete(cloned, "api_key")
			clonedProfiles[index] = cloned
		}
		sanitized["model_profiles"] = clonedProfiles
	}
	return sanitized
}

func (m *Module) saveSecrets(ctx context.Context, pluginID string, secrets map[string]string) error {
	if len(secrets) == 0 || m == nil || m.store == nil {
		return nil
	}
	now := m.now().UTC().UnixMilli()
	for name, value := range secrets {
		if value == "" {
			continue
		}
		if err := m.store.UpsertPluginSecret(ctx, dirextalkplugin.Secret{
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

func (m *Module) secretStatus(ctx context.Context, plugin dirextalkplugin.Instance) (map[string]any, *actionbase.Error) {
	names := secretNamesFromConfig(plugin.Config)
	status := make(map[string]any, len(names))
	for _, name := range names {
		secret, ok, err := m.store.GetPluginSecret(ctx, plugin.ID, name)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		status[name] = map[string]any{
			"configured": ok && secret.Value != "",
			"updated_at": secret.UpdatedAt,
		}
	}
	return status, nil
}

func secretNamesFromConfig(config map[string]any) []string {
	seen := map[string]bool{}
	add := func(ref string) {
		name, ok := secretRefName(ref)
		if ok {
			seen[name] = true
		}
	}
	add(configString(config, "api_key_ref"))
	if profiles, ok := config["model_profiles"].([]any); ok {
		for _, rawProfile := range profiles {
			profile, ok := rawProfile.(map[string]any)
			if !ok {
				continue
			}
			add(configString(profile, "api_key_ref"))
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

func profileSecretName(profile map[string]any, index int) string {
	id := strings.TrimSpace(configString(profile, "id"))
	if id == "" {
		id = configString(profile, "name")
	}
	if id == "" {
		id = jsonValue(index)
	}
	return "model_profile_" + strings.ToLower(envSuffix(id)) + "_api_key"
}

func (m *Module) runtimeEnv(plugin dirextalkplugin.Instance) map[string]string {
	env := map[string]string{
		"DIREXTALK_BASE_URL": backendBaseURL(m.config.Homeserver),
	}
	if plugin.ID == OpsPluginID {
		mergeOpsEnv(env)
	}
	return env
}

func runtimeVolumes(plugin dirextalkplugin.Instance) []string {
	if plugin.ID != OpsPluginID {
		return nil
	}
	socket := fallback(strings.TrimSpace(os.Getenv("P2P_OPS_DOCKER_SOCKET")), "/var/run/docker.sock")
	backupVolume := fallback(strings.TrimSpace(os.Getenv("P2P_OPS_BACKUP_VOLUME")), "p2p_ops_backups")
	return []string{
		socket + ":/var/run/docker.sock",
		backupVolume + ":/var/lib/dirextalk-ops",
	}
}

func mergeOpsEnv(env map[string]string) {
	env["OPS_BACKUP_ROOT"] = "/var/lib/dirextalk-ops/backups"
	env["OPS_MAX_BACKUPS"] = fallback(strings.TrimSpace(os.Getenv("P2P_OPS_MAX_BACKUPS")), "10")
	env["OPS_MESSAGE_SERVER_CONTAINER"] = fallback(strings.TrimSpace(os.Getenv("P2P_OPS_MESSAGE_SERVER_CONTAINER")), "dirextalk-p2p-message-server-1")
	env["OPS_POSTGRES_CONTAINER"] = fallback(strings.TrimSpace(os.Getenv("P2P_OPS_POSTGRES_CONTAINER")), "dirextalk-p2p-postgres-1")
	env["OPS_POSTGRES_USER"] = fallback(strings.TrimSpace(os.Getenv("P2P_OPS_POSTGRES_USER")), "dirextalk_message_server")
	env["OPS_POSTGRES_PASSWORD"] = fallback(strings.TrimSpace(os.Getenv("P2P_OPS_POSTGRES_PASSWORD")), "dirextalk_message_server")
}

func backendBaseURL(homeserver string) string {
	if configured := strings.TrimSpace(os.Getenv("P2P_PLUGIN_BACKEND_BASE_URL")); configured != "" {
		return configured
	}
	homeserver = strings.TrimSpace(homeserver)
	if homeserver == "" || autoHomeserver(homeserver) {
		return "http://message-server:8008"
	}
	return homeserver
}

func autoHomeserver(value string) bool {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "auto") {
		return true
	}
	parsed, err := url.Parse(value)
	return err == nil && strings.EqualFold(parsed.Hostname(), "auto")
}

func envSuffix(value string) string {
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

func configString(config map[string]any, key string) string {
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

func jsonValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func fallback(value, fallbackValue string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallbackValue
	}
	return value
}
