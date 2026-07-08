package p2p

import "context"

type pluginOnlyStore struct{}

func (pluginOnlyStore) UpsertPlugin(context.Context, pluginInstance) error {
	return nil
}

func (pluginOnlyStore) ListPlugins(context.Context) ([]pluginInstance, error) {
	return nil, nil
}

func (pluginOnlyStore) GetPlugin(context.Context, string) (pluginInstance, bool, error) {
	return pluginInstance{}, false, nil
}

func (pluginOnlyStore) UpsertPluginJob(context.Context, pluginJob) error {
	return nil
}

func (pluginOnlyStore) GetPluginJob(context.Context, string) (pluginJob, bool, error) {
	return pluginJob{}, false, nil
}

func (pluginOnlyStore) UpsertPluginSecret(context.Context, pluginSecret) error {
	return nil
}

func (pluginOnlyStore) GetPluginSecret(context.Context, string, string) (pluginSecret, bool, error) {
	return pluginSecret{}, false, nil
}

var _ pluginStore = pluginOnlyStore{}
