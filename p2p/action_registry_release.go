package p2p

func (s *Service) registerReleaseActions(actions map[string]actionHandler) {
	actions["client.version.report"] = s.reportClientVersion
	actions["release.v1.status"] = s.releaseStatus
	actions["release.v1.apply"] = s.applyRelease
}
