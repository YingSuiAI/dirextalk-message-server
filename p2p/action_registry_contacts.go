package p2p

func (s *Service) registerContactActions(actions map[string]actionHandler) {
	actions["contacts.request"] = s.contactRequest
	actions["rooms.reactivate"] = s.roomReactivate
}
