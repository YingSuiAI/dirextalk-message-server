package p2p

func (s *Service) registerContactActions(actions map[string]actionHandler) {
	actions["contacts.request"] = s.contactRequest
	actions["contacts.reactivate"] = s.contactReactivate
	actions["rooms.reactivate"] = s.roomReactivate
}
