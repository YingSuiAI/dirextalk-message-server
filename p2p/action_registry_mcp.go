package p2p

func (s *Service) registerMCPActions(actions map[string]actionHandler) {
	for _, action := range s.dirextalkMCPService().Actions() {
		actions[action] = s.invokeDirextalkMCPAction(action)
	}
}
