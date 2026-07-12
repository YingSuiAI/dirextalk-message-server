package p2p

func (s *Service) registerContactActions(actions map[string]actionHandler) {
	actions["contacts.list"] = s.contactListAction
	actions["contacts.request"] = s.contactRequest
	actions["contacts.reactivate"] = s.contactReactivate
	actions["rooms.reactivate"] = s.roomReactivate
	actions["contacts.requests.accept"] = s.contactMutationAction("contacts.requests.accept")
	actions["contacts.requests.reject"] = s.contactMutationAction("contacts.requests.reject")
	actions["contacts.requests.delete"] = s.contactMutationAction("contacts.requests.delete")
	actions["contacts.delete"] = s.contactMutationAction("contacts.delete")
	actions["contacts.update"] = s.contactUpdate
}
