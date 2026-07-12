package storage

import "context"

func (s *MemoryStore) UpsertPlugin(ctx context.Context, plugin pluginInstance) error {
	plugin = clonePlugin(plugin)
	s.mu.Lock()
	s.plugins[plugin.ID] = plugin
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) ListPlugins(ctx context.Context) ([]pluginInstance, error) {
	s.mu.RLock()
	plugins := make([]pluginInstance, 0, len(s.plugins))
	for _, plugin := range s.plugins {
		plugins = append(plugins, clonePlugin(plugin))
	}
	s.mu.RUnlock()
	return plugins, nil
}

func (s *MemoryStore) GetPlugin(ctx context.Context, id string) (pluginInstance, bool, error) {
	s.mu.RLock()
	plugin, ok := s.plugins[id]
	s.mu.RUnlock()
	if !ok {
		return pluginInstance{}, false, nil
	}
	return clonePlugin(plugin), true, nil
}

func (s *MemoryStore) UpsertPluginJob(ctx context.Context, job pluginJob) error {
	s.mu.Lock()
	s.pluginJobs[job.JobID] = job
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) GetPluginJob(ctx context.Context, jobID string) (pluginJob, bool, error) {
	s.mu.RLock()
	job, ok := s.pluginJobs[jobID]
	s.mu.RUnlock()
	return job, ok, nil
}

func (s *MemoryStore) UpsertPluginSecret(ctx context.Context, secret pluginSecret) error {
	s.mu.Lock()
	if s.pluginSecrets[secret.PluginID] == nil {
		s.pluginSecrets[secret.PluginID] = make(map[string]pluginSecret)
	}
	s.pluginSecrets[secret.PluginID][secret.Name] = secret
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) GetPluginSecret(ctx context.Context, pluginID, name string) (pluginSecret, bool, error) {
	s.mu.RLock()
	secret, ok := s.pluginSecrets[pluginID][name]
	s.mu.RUnlock()
	return secret, ok, nil
}
