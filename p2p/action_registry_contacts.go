package p2p

func (s *Service) registerContactActions(actions map[string]actionHandler) {
	actions["contacts.request"] = s.contactRequest
	actions["contacts.reactivate"] = s.contactReactivate
	actions["rooms.reactivate"] = s.roomReactivate
	actions["contacts.requests.accept"] = s.contactMutationAction("contacts.requests.accept")
	actions["contacts.delete"] = s.contactMutationAction("contacts.delete")
}
