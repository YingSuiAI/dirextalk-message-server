package p2p

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"

type channelCommentStorageRecord = dirextalkdomain.ChannelCommentRecord

func channelCommentStorageRecordFromComment(comment channelCommentRecord) channelCommentStorageRecord {
	return channelCommentStorageRecord{
		CommentID:         comment.CommentID,
		PostID:            comment.PostID,
		ChannelID:         comment.ChannelID,
		EventID:           comment.EventID,
		AuthorMXID:        comment.AuthorMXID,
		AuthorName:        comment.AuthorName,
		Body:              comment.Body,
		MessageType:       comment.MessageType,
		MediaJSON:         comment.MediaJSON,
		ReplyToCommentID:  comment.ReplyToCommentID,
		ReplyToAuthorMXID: comment.ReplyToAuthorMXID,
		MentionsJSON:      comment.MentionsJSON,
		OriginServerTS:    comment.OriginServerTS,
	}
}

func channelCommentRecordFromStorage(comment channelCommentStorageRecord) channelCommentRecord {
	return channelCommentRecord{
		CommentID:         comment.CommentID,
		PostID:            comment.PostID,
		ChannelID:         comment.ChannelID,
		EventID:           comment.EventID,
		AuthorMXID:        comment.AuthorMXID,
		AuthorName:        comment.AuthorName,
		Body:              comment.Body,
		MessageType:       comment.MessageType,
		MediaJSON:         comment.MediaJSON,
		ReplyToCommentID:  comment.ReplyToCommentID,
		ReplyToAuthorMXID: comment.ReplyToAuthorMXID,
		MentionsJSON:      comment.MentionsJSON,
		OriginServerTS:    comment.OriginServerTS,
	}
}

func channelCommentRecordsFromStorage(comments []channelCommentStorageRecord) []channelCommentRecord {
	if len(comments) == 0 {
		return []channelCommentRecord{}
	}
	result := make([]channelCommentRecord, 0, len(comments))
	for _, comment := range comments {
		result = append(result, channelCommentRecordFromStorage(comment))
	}
	return result
}
