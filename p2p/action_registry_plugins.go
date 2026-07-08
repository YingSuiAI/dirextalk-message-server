package p2p

func (s *Service) registerPluginActions(actions map[string]actionHandler) {
	actions["plugins.catalog.list"] = s.pluginCatalogListAction
	actions["plugins.installed.list"] = s.pluginInstalledListAction
	actions["plugins.install"] = s.pluginInstallAction
	actions["plugins.enable"] = s.pluginEnableAction
	actions["plugins.disable"] = s.pluginDisableAction
	actions["plugins.uninstall"] = s.pluginUninstallAction
	actions["plugins.config.get"] = s.pluginConfigGetAction
	actions["plugins.config.update"] = s.pluginConfigUpdateAction
	actions["plugins.job.get"] = s.pluginJobGetAction
	actions["plugins.health"] = s.pluginHealthAction
	actions["plugins.logs.tail"] = s.pluginLogsTailAction
	actions["plugins.invoke"] = s.pluginInvokeAction
	actions["plugins.invoke.stream"] = s.pluginInvokeStreamAction
}
