package p2p

func (s *Service) registerChannelActions(actions map[string]actionHandler) {
	actions["channels.read_marker"] = s.updateReadMarker
}
