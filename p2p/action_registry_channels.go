package p2p

func (s *Service) registerChannelActions(actions map[string]actionHandler) {
	actions["channels.create"] = s.channelResult
	actions["channels.update"] = s.channelUpdate
	actions["channels.join"] = s.joinMemberAction("channel")
	actions["channels.invite_grant.create"] = s.channelInviteGrantCreate
	actions["channels.invite"] = s.inviteMembersAction("channel")
	actions["channels.dissolve"] = s.dissolveChannel
	actions["channels.mute"] = s.channelPolicyMutationAction("channels.mute")
	actions["channels.unmute"] = s.channelPolicyMutationAction("channels.unmute")
	actions["channels.read_marker"] = s.updateReadMarker
	actions["channels.list"] = s.channelListAction
	actions["channels.public.search"] = s.channelPublicSearch
	actions["channels.public.get"] = s.channelPublicGet
	actions["channels.public.join_request"] = s.channelJoinRequest
	actions["channels.public.join_result"] = s.channelJoinResult
	actions["users.public_channels"] = s.userPublicChannels
	actions["channels.posts.list"] = s.channelPostsAction
	actions["channels.posts.create"] = s.channelPost
	actions["channels.posts.recall"] = s.recallChannelContentAction("channels.posts.recall")
	actions["channels.comments.recall"] = s.recallChannelContentAction("channels.comments.recall")
	actions["channels.comments.list"] = s.channelCommentsAction
	actions["channels.comments.create"] = s.channelComment
	actions["channels.post_reaction.toggle"] = s.channelReactionAction("channels.post_reaction.toggle")
	actions["channels.comment_reaction.toggle"] = s.channelReactionAction("channels.comment_reaction.toggle")
	actions["channels.my_comments"] = s.myChannelCommentsAction
	actions["channels.my_reactions"] = s.myReactionsAction
}
