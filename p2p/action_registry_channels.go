package p2p

func (s *Service) registerChannelActions(actions map[string]actionHandler) {
	actions["channels.read_marker"] = s.updateReadMarker
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
