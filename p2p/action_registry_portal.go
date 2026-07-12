package p2p

func (s *Service) registerPortalActions(actions map[string]actionHandler) {
	actions["portal.bootstrap"] = s.bootstrap
	actions["portal.auth"] = s.auth
	actions["portal.status"] = s.portalStatusAction
	actions["portal.password"] = s.changePortalPassword
	actions["portal.account.delete"] = s.deleteAccount
}

func (s *Service) registerProfileAndSyncActions(actions map[string]actionHandler) {
	actions["profile.get"] = s.getProfileAction
	actions["profile.update"] = s.updateProfile
	actions["sync.bootstrap"] = s.syncBootstrapAction
	actions["sync.read_marker"] = s.updateReadMarker
}
