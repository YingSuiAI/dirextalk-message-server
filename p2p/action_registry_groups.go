package p2p

func (s *Service) registerGroupActions(actions map[string]actionHandler) {
	actions["groups.create"] = s.groupResult
	actions["groups.update"] = s.groupUpdate
	actions["groups.invite"] = s.inviteMembersAction("group")
	actions["groups.join"] = s.joinMemberAction("group")
	actions["groups.list"] = s.groupListAction
	actions["groups.dissolve"] = s.dissolveGroup
	actions["groups.leave"] = s.memberMutationAction("group", "groups.leave")
	actions["groups.invite.reject"] = s.memberMutationAction("group", "groups.invite.reject")
	actions["groups.member.remove"] = s.memberMutationAction("group", "groups.member.remove")
	actions["groups.member.mute"] = s.memberMutationAction("group", "groups.member.mute")
	actions["groups.member.unmute"] = s.memberMutationAction("group", "groups.member.unmute")
	actions["groups.mute"] = s.groupPolicyMutationAction("groups.mute")
	actions["groups.unmute"] = s.groupPolicyMutationAction("groups.unmute")
	actions["groups.invite_policy.update"] = s.groupPolicyMutationAction("groups.invite_policy.update")
}
