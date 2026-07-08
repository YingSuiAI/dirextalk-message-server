package p2p

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"

type channelPostStorageRecord = dirextalkdomain.ChannelPostRecord

func channelPostStorageRecordFromPost(post channelPostRecord) channelPostStorageRecord {
	return channelPostStorageRecord{
		PostID:         post.PostID,
		ChannelID:      post.ChannelID,
		RoomID:         post.RoomID,
		EventID:        post.EventID,
		AuthorMXID:     post.AuthorMXID,
		AuthorName:     post.AuthorName,
		Body:           post.Body,
		MessageType:    post.MessageType,
		MediaJSON:      post.MediaJSON,
		OriginServerTS: post.OriginServerTS,
		CommentCount:   post.CommentCount,
	}
}

func channelPostRecordFromStorage(post channelPostStorageRecord) channelPostRecord {
	return channelPostRecord{
		PostID:         post.PostID,
		ChannelID:      post.ChannelID,
		RoomID:         post.RoomID,
		EventID:        post.EventID,
		AuthorMXID:     post.AuthorMXID,
		AuthorName:     post.AuthorName,
		Body:           post.Body,
		MessageType:    post.MessageType,
		MediaJSON:      post.MediaJSON,
		OriginServerTS: post.OriginServerTS,
		CommentCount:   post.CommentCount,
	}
}

func channelPostRecordsFromStorage(posts []channelPostStorageRecord) []channelPostRecord {
	if len(posts) == 0 {
		return []channelPostRecord{}
	}
	result := make([]channelPostRecord, 0, len(posts))
	for _, post := range posts {
		result = append(result, channelPostRecordFromStorage(post))
	}
	return result
}
