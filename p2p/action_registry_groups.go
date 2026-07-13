package p2p

func (s *Service) registerGroupActions(actions map[string]actionHandler) {
	actions["groups.create"] = s.groupResult
	actions["groups.update"] = s.groupUpdate
	actions["groups.invite"] = s.inviteMembersAction("group")
	actions["groups.join"] = s.joinMemberAction("group")
	actions["groups.list"] = s.groupListAction
	actions["groups.dissolve"] = s.dissolveGroup
	actions["groups.mute"] = s.groupPolicyMutationAction("groups.mute")
	actions["groups.unmute"] = s.groupPolicyMutationAction("groups.unmute")
	actions["groups.invite_policy.update"] = s.groupPolicyMutationAction("groups.invite_policy.update")
}
