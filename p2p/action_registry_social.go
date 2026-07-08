package p2p

func (s *Service) registerSocialActions(actions map[string]actionHandler) {
	actions["follows.list"] = s.followListAction
	actions["follows.add"] = s.followAdd
	actions["follows.remove"] = s.followRemove
	actions["favorites.list"] = s.favoriteListAction
	actions["favorites.add"] = s.favoriteMessage
	actions["favorites.delete"] = s.favoriteDelete
	actions["favorites.delete_batch"] = s.favoriteDeleteBatch
	actions["reports.submit"] = s.reportSubmit
}

func (s *Service) registerCallActions(actions map[string]actionHandler) {
	actions["calls.create"] = s.callSession
	actions["calls.incoming"] = s.callSession
	actions["calls.get"] = s.callGet
	actions["calls.event"] = s.callEvent
	actions["calls.active"] = s.callListAction(true)
	actions["calls.list"] = s.callListAction(false)
}
