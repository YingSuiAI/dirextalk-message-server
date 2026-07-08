package p2p

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type channelContentStore interface {
	InsertChannelPost(ctx context.Context, post channelPostRecord) error
	GetChannelPostByID(ctx context.Context, postID, channelID string) (channelPostRecord, bool, error)
	GetChannelPostByEventID(ctx context.Context, eventID, channelID string) (channelPostRecord, bool, error)
	ListChannelPosts(ctx context.Context, channelID string) ([]channelPostRecord, error)
	ListChannelPostsPage(ctx context.Context, channelID string, fromTS, snapshotTS, cursorTS int64, cursorID string, limit int) ([]channelPostRecord, bool, error)
	InsertChannelComment(ctx context.Context, comment channelCommentRecord) error
	GetChannelCommentByID(ctx context.Context, commentID, postID string) (channelCommentRecord, bool, error)
	GetChannelCommentByEventID(ctx context.Context, eventID, channelID string) (channelCommentRecord, bool, error)
	ListChannelComments(ctx context.Context, postID string) ([]channelCommentRecord, error)
	ListChannelCommentsPage(ctx context.Context, postID string, fromTS, snapshotTS, cursorTS int64, cursorID string, limit int) ([]channelCommentRecord, bool, error)
	DeleteChannelPost(ctx context.Context, postID string) (bool, error)
	DeleteChannelComment(ctx context.Context, commentID string) (bool, error)
}

func (s *Service) channelContentStore() channelContentStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) channelPost(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	channelID := fallbackString(trimString(params["channel_id"]), "channel")
	postID := "post_" + randomToken("post")
	s.mu.Lock()
	authorMXID := s.ownerMXID
	authorName := s.profile.DisplayName
	s.mu.Unlock()
	roomID, apiErr := s.roomIDForChannel(ctx, channelID, trimString(params["room_id"]))
	if apiErr != nil {
		return nil, apiErr
	}
	body := fallbackString(trimString(params["body"]), trimString(params["content"]))
	msgType := fallbackString(trimString(params["message_type"]), "text")
	mediaJSON, media, mediaErr := mediaPayloadParam(params["media_json"])
	if mediaErr != nil {
		return nil, badRequest("media_json is invalid")
	}
	eventID := "$" + postID + ":" + s.serverName
	originServerTS := now.UnixMilli()
	if s.transport != nil && roomID != "" {
		content := channelMessageContent("channel_post", body, msgType, mediaJSON, media)
		content["channel_id"] = channelID
		content["post_id"] = postID
		res, err := s.transport.SendMessage(ctx, SendMessageRequest{
			SenderMXID:  authorMXID,
			RoomID:      roomID,
			MessageType: msgType,
			Timestamp:   now,
			Content:     content,
		})
		if err != nil {
			return nil, transportWriteError(err)
		}
		eventID = res.EventID
		originServerTS = res.OriginServerTS
	}
	post := channelPostRecord{
		PostID:         postID,
		ChannelID:      channelID,
		RoomID:         roomID,
		EventID:        eventID,
		AuthorMXID:     authorMXID,
		AuthorName:     authorName,
		Body:           body,
		MessageType:    msgType,
		MediaJSON:      mediaJSON,
		OriginServerTS: originServerTS,
		CommentCount:   0,
	}
	s.mu.Lock()
	s.posts = append(s.posts, post)
	s.mu.Unlock()
	if store := s.channelContentStore(); store != nil {
		if err := store.InsertChannelPost(ctx, post); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.attachChannelPostOperation(ctx, &post, "channels.posts.create", "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return post, nil
}

func (s *Service) channelPosts(ctx context.Context, params map[string]any) any {
	channelID := trimString(params["channel_id"])
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if store := s.channelContentStore(); store != nil {
		posts, err := store.ListChannelPosts(ctx, channelID)
		if err != nil {
			return map[string]any{"posts": []channelPostRecord{}}
		}
		s.enrichChannelPosts(ctx, posts, ownerMXID)
		return map[string]any{"posts": posts}
	}
	s.mu.Lock()
	posts := make([]channelPostRecord, 0, len(s.posts))
	for _, post := range s.posts {
		if channelID == "" || post.ChannelID == channelID {
			posts = append(posts, post)
		}
	}
	s.mu.Unlock()
	s.enrichChannelPosts(ctx, posts, ownerMXID)
	return map[string]any{"posts": posts}
}

func (s *Service) channelComment(ctx context.Context, params map[string]any) (any, *apiError) {
	now := time.Now().UTC()
	commentID := "comment_" + randomToken("comment")
	s.mu.Lock()
	authorMXID := s.ownerMXID
	authorName := s.profile.DisplayName
	s.mu.Unlock()
	channelID := trimString(params["channel_id"])
	postID := trimString(params["post_id"])
	if postID == "" {
		return nil, badRequest("post_id is required")
	}
	if _, ok, err := s.channelPostByID(ctx, postID, channelID); err != nil {
		return nil, internalError(err)
	} else if !ok {
		return nil, statusError(http.StatusNotFound, "post not found")
	}
	body := fallbackString(trimString(params["body"]), trimString(params["content"]))
	msgType := fallbackString(trimString(params["message_type"]), "text")
	mediaJSON, media, mediaErr := mediaPayloadParam(params["media_json"])
	if mediaErr != nil {
		return nil, badRequest("media_json is invalid")
	}
	replyToCommentID := trimString(params["reply_to_comment_id"])
	replyToAuthorMXID := trimString(params["reply_to_author_mxid"])
	mentionsJSON, err := jsonArrayStringParam(params["mentions"])
	if err != nil {
		return nil, badRequest("mentions is invalid")
	}
	if _, ok := params["mentions"]; !ok {
		mentionsJSON, err = jsonArrayStringParam(params["mentions_json"])
		if err != nil {
			return nil, badRequest("mentions_json is invalid")
		}
	}
	eventID := "$" + commentID + ":" + s.serverName
	originServerTS := now.UnixMilli()
	roomID, apiErr := s.roomIDForChannel(ctx, channelID, trimString(params["room_id"]))
	if apiErr != nil {
		return nil, apiErr
	}
	if s.transport != nil && roomID != "" {
		content := channelMessageContent("channel_comment", body, msgType, mediaJSON, media)
		content["channel_id"] = channelID
		content["post_id"] = postID
		content["comment_id"] = commentID
		content["reply_to_comment_id"] = replyToCommentID
		content["reply_to_author_mxid"] = replyToAuthorMXID
		content["mentions_json"] = mentionsJSON
		res, err := s.transport.SendMessage(ctx, SendMessageRequest{
			SenderMXID:  authorMXID,
			RoomID:      roomID,
			MessageType: msgType,
			Timestamp:   now,
			Content:     content,
		})
		if err != nil {
			return nil, transportWriteError(err)
		}
		eventID = res.EventID
		originServerTS = res.OriginServerTS
	}
	comment := channelCommentRecord{
		CommentID:         commentID,
		PostID:            postID,
		ChannelID:         channelID,
		EventID:           eventID,
		AuthorMXID:        authorMXID,
		AuthorName:        authorName,
		Body:              body,
		MessageType:       msgType,
		MediaJSON:         mediaJSON,
		ReplyToCommentID:  replyToCommentID,
		ReplyToAuthorMXID: replyToAuthorMXID,
		MentionsJSON:      mentionsJSON,
		OriginServerTS:    originServerTS,
		ReactionCount:     0,
		ReactedByMe:       false,
	}
	s.mu.Lock()
	s.comments = append(s.comments, comment)
	s.mu.Unlock()
	if store := s.channelContentStore(); store != nil {
		if err := store.InsertChannelComment(ctx, comment); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.attachChannelCommentOperation(ctx, &comment, "channels.comments.create", "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return comment, nil
}

func (s *Service) channelComments(ctx context.Context, params map[string]any) any {
	postID := trimString(params["post_id"])
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if store := s.channelContentStore(); store != nil {
		comments, err := store.ListChannelComments(ctx, postID)
		if err != nil {
			return map[string]any{"comments": []channelCommentRecord{}}
		}
		s.enrichChannelComments(ctx, comments, ownerMXID)
		return map[string]any{"comments": comments}
	}
	s.mu.Lock()
	comments := make([]channelCommentRecord, 0, len(s.comments))
	for _, comment := range s.comments {
		if postID == "" || comment.PostID == postID {
			comments = append(comments, comment)
		}
	}
	s.mu.Unlock()
	s.enrichChannelComments(ctx, comments, ownerMXID)
	return map[string]any{"comments": comments}
}

func (s *Service) roomIDForChannel(ctx context.Context, channelID, fallbackRoomID string) (string, *apiError) {
	if strings.TrimSpace(fallbackRoomID) != "" {
		return strings.TrimSpace(fallbackRoomID), nil
	}
	if strings.TrimSpace(channelID) == "" {
		return "", nil
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, "")
	if err != nil {
		return "", internalError(err)
	}
	if !ok {
		return "", nil
	}
	return ch.RoomID, nil
}

func mediaPayloadParam(value any) (string, map[string]any, error) {
	switch v := value.(type) {
	case nil:
		return "", nil, nil
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return "", nil, nil
		}
		var media map[string]any
		if err := json.Unmarshal([]byte(text), &media); err != nil {
			return "", nil, err
		}
		raw, err := json.Marshal(media)
		if err != nil {
			return "", nil, err
		}
		return string(raw), media, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return "", nil, err
		}
		var media map[string]any
		if err = json.Unmarshal(raw, &media); err != nil {
			return "", nil, err
		}
		normalized, err := json.Marshal(media)
		if err != nil {
			return "", nil, err
		}
		return string(normalized), media, nil
	}
}

func channelMessageContent(kind, body, msgType, mediaJSON string, media map[string]any) map[string]any {
	content := map[string]any{
		"msgtype":     matrixMessageType(msgType, mediaMessageType(msgType) || len(media) > 0),
		"body":        body,
		"p2p_kind":    kind,
		"client_type": msgType,
	}
	if mediaJSON != "" {
		content["media_json"] = mediaJSON
	}
	for key, value := range media {
		if key == "body" || key == "msgtype" || key == "p2p_kind" || key == "client_type" || key == "media_json" {
			continue
		}
		if key == "mxc" && content["url"] == nil {
			content["url"] = value
			continue
		}
		content[key] = value
	}
	return content
}

func mediaMessageType(messageType string) bool {
	switch strings.TrimSpace(messageType) {
	case "image", "m.image", "video", "m.video", "audio", "m.audio", "file", "m.file":
		return true
	default:
		return false
	}
}

func (s *Service) enrichChannelPosts(ctx context.Context, posts []channelPostRecord, ownerMXID string) {
	for i := range posts {
		comments, err := s.listChannelCommentsForPost(ctx, posts[i].PostID)
		if err == nil {
			posts[i].CommentCount = int64(len(comments))
		}
		count, err := s.countActiveReactions(ctx, "post", posts[i].PostID, "like")
		if err == nil {
			posts[i].ReactionCount = count
		}
		if ownerMXID != "" {
			if reaction, ok, err := s.getReaction(ctx, "post", posts[i].PostID, "like", ownerMXID); err == nil && ok {
				posts[i].ReactedByMe = reaction.Active
			}
		}
	}
}

func (s *Service) enrichChannelComments(ctx context.Context, comments []channelCommentRecord, ownerMXID string) {
	for i := range comments {
		count, err := s.countActiveReactions(ctx, "comment", comments[i].CommentID, "like")
		if err == nil {
			comments[i].ReactionCount = count
		}
		if ownerMXID != "" {
			if reaction, ok, err := s.getReaction(ctx, "comment", comments[i].CommentID, "like", ownerMXID); err == nil && ok {
				comments[i].ReactedByMe = reaction.Active
			}
		}
	}
}

func (s *Service) listChannelCommentsForPost(ctx context.Context, postID string) ([]channelCommentRecord, error) {
	if store := s.channelContentStore(); store != nil {
		return store.ListChannelComments(ctx, postID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	comments := make([]channelCommentRecord, 0, len(s.comments))
	for _, comment := range s.comments {
		if postID == "" || comment.PostID == postID {
			comments = append(comments, comment)
		}
	}
	return comments, nil
}

func (s *Service) myChannelComments(ctx context.Context, params map[string]any) any {
	channelID := trimString(params["channel_id"])
	postID := trimString(params["post_id"])
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if store := s.channelContentStore(); store != nil {
		comments, err := store.ListChannelComments(ctx, postID)
		if err != nil {
			return map[string]any{"comments": []channelCommentRecord{}}
		}
		filtered := make([]channelCommentRecord, 0, len(comments))
		for _, comment := range comments {
			if comment.AuthorMXID != ownerMXID {
				continue
			}
			if channelID != "" && comment.ChannelID != channelID {
				continue
			}
			filtered = append(filtered, comment)
		}
		return map[string]any{"comments": filtered}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	comments := make([]channelCommentRecord, 0, len(s.comments))
	for _, comment := range s.comments {
		if comment.AuthorMXID != ownerMXID {
			continue
		}
		if channelID != "" && comment.ChannelID != channelID {
			continue
		}
		if postID != "" && comment.PostID != postID {
			continue
		}
		comments = append(comments, comment)
	}
	return map[string]any{"comments": comments}
}

func (s *Service) recallChannelContent(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	if action == "channels.posts.recall" {
		postID := trimString(params["post_id"])
		if postID == "" {
			return nil, badRequest("post_id is required")
		}
		post, ok, err := s.channelPostByID(ctx, postID, trimString(params["channel_id"]))
		if err != nil {
			return nil, transportWriteError(err)
		}
		if !ok {
			return nil, statusError(http.StatusNotFound, "post not found")
		}
		if s.transport == nil {
			if apiErr := s.authorizeChannelContentRecall(ctx, post.RoomID, post.AuthorMXID); apiErr != nil {
				return nil, apiErr
			}
		}
		eventID, roomID := post.EventID, post.RoomID
		if err := s.redactEvent(ctx, roomID, eventID, trimString(params["reason"])); err != nil {
			return nil, transportWriteError(err)
		}
		s.mu.Lock()
		filtered := s.posts[:0]
		for _, post := range s.posts {
			if post.PostID != postID {
				filtered = append(filtered, post)
			}
		}
		s.posts = filtered
		s.mu.Unlock()
		if store := s.channelContentStore(); store != nil {
			if _, err := store.DeleteChannelPost(ctx, postID); err != nil {
				return nil, internalError(err)
			}
		}
		result := map[string]any{"status": "ok"}
		if err := s.attachConversationOperation(ctx, result, action, "ok", roomID); err != nil {
			return nil, internalError(err)
		}
		return result, nil
	}
	commentID := trimString(params["comment_id"])
	if commentID == "" {
		return nil, badRequest("comment_id is required")
	}
	postID := trimString(params["post_id"])
	comment, ok, err := s.channelCommentByID(ctx, commentID, postID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		return nil, statusError(http.StatusNotFound, "comment not found")
	}
	roomID, err := s.roomIDForChannelComment(ctx, comment, trimString(params["room_id"]))
	if err != nil {
		return nil, internalError(err)
	}
	if s.transport == nil {
		if apiErr := s.authorizeChannelContentRecall(ctx, roomID, comment.AuthorMXID); apiErr != nil {
			return nil, apiErr
		}
	}
	eventID := comment.EventID
	if err := s.redactEvent(ctx, roomID, eventID, trimString(params["reason"])); err != nil {
		return nil, transportWriteError(err)
	}
	s.mu.Lock()
	filtered := s.comments[:0]
	for _, comment := range s.comments {
		if comment.CommentID != commentID {
			filtered = append(filtered, comment)
		}
	}
	s.comments = filtered
	s.mu.Unlock()
	if store := s.channelContentStore(); store != nil {
		if _, err := store.DeleteChannelComment(ctx, commentID); err != nil {
			return nil, internalError(err)
		}
	}
	result := map[string]any{"status": "ok"}
	if err := s.attachConversationOperation(ctx, result, action, "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) redactEvent(ctx context.Context, roomID, eventID, reason string) error {
	if s.transport == nil || roomID == "" || eventID == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	_, err := s.transport.RedactEvent(ctx, RedactEventRequest{
		RoomID:     roomID,
		EventID:    eventID,
		SenderMXID: senderMXID,
		Reason:     reason,
		Timestamp:  time.Now().UTC(),
	})
	return err
}

func (s *Service) channelPostByID(ctx context.Context, postID, channelID string) (channelPostRecord, bool, error) {
	if store := s.channelContentStore(); store != nil {
		return store.GetChannelPostByID(ctx, postID, channelID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, post := range s.posts {
		if post.PostID == postID && (channelID == "" || post.ChannelID == channelID) {
			return post, true, nil
		}
	}
	return channelPostRecord{}, false, nil
}

func (s *Service) channelPostByEventID(ctx context.Context, eventID, channelID string) (channelPostRecord, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return channelPostRecord{}, false, nil
	}
	if store := s.channelContentStore(); store != nil {
		return store.GetChannelPostByEventID(ctx, eventID, channelID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, post := range s.posts {
		if post.EventID == eventID && (channelID == "" || post.ChannelID == channelID) {
			return post, true, nil
		}
	}
	return channelPostRecord{}, false, nil
}

func (s *Service) channelCommentByID(ctx context.Context, commentID, postID string) (channelCommentRecord, bool, error) {
	if store := s.channelContentStore(); store != nil {
		return store.GetChannelCommentByID(ctx, commentID, postID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, comment := range s.comments {
		if comment.CommentID == commentID && (postID == "" || comment.PostID == postID) {
			return comment, true, nil
		}
	}
	return channelCommentRecord{}, false, nil
}

func (s *Service) channelCommentByEventID(ctx context.Context, eventID, channelID string) (channelCommentRecord, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return channelCommentRecord{}, false, nil
	}
	if store := s.channelContentStore(); store != nil {
		return store.GetChannelCommentByEventID(ctx, eventID, channelID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, comment := range s.comments {
		if comment.EventID == eventID && (channelID == "" || comment.ChannelID == channelID) {
			return comment, true, nil
		}
	}
	return channelCommentRecord{}, false, nil
}

func (s *Service) channelReactionTargetByEventID(ctx context.Context, eventID, channelID string) (targetType, targetID, postID, commentID, resolvedChannelID string, err error) {
	if comment, ok, lookupErr := s.channelCommentByEventID(ctx, eventID, channelID); lookupErr != nil {
		return "", "", "", "", "", lookupErr
	} else if ok {
		return "comment", comment.CommentID, comment.PostID, comment.CommentID, comment.ChannelID, nil
	}
	if post, ok, lookupErr := s.channelPostByEventID(ctx, eventID, channelID); lookupErr != nil {
		return "", "", "", "", "", lookupErr
	} else if ok {
		return "post", post.PostID, post.PostID, "", post.ChannelID, nil
	}
	return "", "", "", "", "", nil
}

func (s *Service) roomIDForChannelComment(ctx context.Context, comment channelCommentRecord, fallbackRoomID string) (string, error) {
	if fallbackRoomID != "" {
		return fallbackRoomID, nil
	}
	if comment.PostID != "" {
		post, ok, err := s.channelPostByID(ctx, comment.PostID, comment.ChannelID)
		if err != nil {
			return "", err
		}
		if ok && post.RoomID != "" {
			return post.RoomID, nil
		}
	}
	if comment.ChannelID != "" {
		ch, ok, err := s.channelByIDOrRoom(ctx, comment.ChannelID, "")
		if err != nil {
			return "", err
		}
		if ok {
			return ch.RoomID, nil
		}
	}
	return "", nil
}

func (s *Service) authorizeChannelContentRecall(ctx context.Context, roomID, authorMXID string) *apiError {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	if ownerMXID != "" && ownerMXID == authorMXID {
		return nil
	}
	if apiErr := s.requireOwnerMember(ctx, roomID); apiErr != nil {
		if apiErr.Status != http.StatusForbidden {
			return apiErr
		}
		return statusError(http.StatusForbidden, "content author or channel owner role is required")
	}
	return nil
}

func (s *Service) channelReaction(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	reactionName := fallbackString(trimString(params["reaction"]), "like")
	targetType := "post"
	targetID := trimString(params["post_id"])
	roomID := trimString(params["room_id"])
	eventID := ""
	channelID := trimString(params["channel_id"])
	postID := trimString(params["post_id"])
	commentID := trimString(params["comment_id"])
	if action == "channels.comment_reaction.toggle" {
		targetType = "comment"
		targetID = commentID
	}
	if targetID == "" {
		return nil, badRequest(targetType + "_id is required")
	}
	if targetType == "post" {
		post, ok, err := s.channelPostByID(ctx, targetID, channelID)
		if err != nil {
			return nil, internalError(err)
		}
		if ok {
			eventID = post.EventID
			roomID = fallbackString(roomID, post.RoomID)
			channelID = fallbackString(channelID, post.ChannelID)
			postID = post.PostID
		}
	} else {
		comment, ok, err := s.channelCommentByID(ctx, targetID, postID)
		if err != nil {
			return nil, internalError(err)
		}
		if ok {
			eventID = comment.EventID
			channelID = fallbackString(channelID, comment.ChannelID)
			postID = fallbackString(postID, comment.PostID)
			commentID = comment.CommentID
			resolvedRoomID, err := s.roomIDForChannelComment(ctx, comment, roomID)
			if err != nil {
				return nil, internalError(err)
			}
			roomID = resolvedRoomID
		}
	}
	s.mu.Lock()
	userID := s.ownerMXID
	s.mu.Unlock()
	existing, ok, err := s.getReaction(ctx, targetType, targetID, reactionName, userID)
	if err != nil {
		return nil, internalError(err)
	}
	record := reactionRecord{
		TargetType: targetType,
		TargetID:   targetID,
		ChannelID:  channelID,
		PostID:     postID,
		CommentID:  commentID,
		Reaction:   reactionName,
		UserID:     userID,
		Active:     true,
		CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	if ok {
		record = existing
		record.Active = !existing.Active
	}
	if s.transport != nil && roomID != "" && eventID != "" {
		_, sendErr := s.transport.SendMessage(ctx, SendMessageRequest{
			SenderMXID:  userID,
			RoomID:      roomID,
			EventType:   "m.reaction",
			MessageType: "m.reaction",
			Timestamp:   time.Now().UTC(),
			Content: map[string]any{
				"m.relates_to": map[string]any{
					"rel_type": "m.annotation",
					"event_id": eventID,
					"key":      reactionName,
				},
				"channel_id": channelID,
				"post_id":    postID,
				"comment_id": commentID,
				"reaction":   reactionName,
				"active":     record.Active,
			},
		})
		if sendErr != nil {
			return nil, transportWriteError(sendErr)
		}
	}
	if saveErr := s.saveReaction(ctx, record); saveErr != nil {
		return nil, internalError(saveErr)
	}
	count, countErr := s.countActiveReactions(ctx, targetType, targetID, reactionName)
	if countErr != nil {
		return nil, internalError(countErr)
	}
	result := map[string]any{
		"post_id":        record.PostID,
		"comment_id":     record.CommentID,
		"channel_id":     record.ChannelID,
		"reaction":       record.Reaction,
		"active":         record.Active,
		"reaction_count": count,
	}
	if err := s.attachConversationOperation(ctx, result, action, "ok", roomID); err != nil {
		return nil, internalError(err)
	}
	return result, nil
}

func (s *Service) attachChannelPostOperation(ctx context.Context, post *channelPostRecord, action, status, roomID string) error {
	operation, conversation, err := s.conversationOperation(ctx, action, status, roomID)
	if err != nil {
		return err
	}
	post.Operation = operation
	post.Conversation = conversation
	return nil
}

func (s *Service) attachChannelCommentOperation(ctx context.Context, comment *channelCommentRecord, action, status, roomID string) error {
	operation, conversation, err := s.conversationOperation(ctx, action, status, roomID)
	if err != nil {
		return err
	}
	comment.Operation = operation
	comment.Conversation = conversation
	return nil
}

type reactionStore interface {
	UpsertReaction(ctx context.Context, reaction reactionRecord) error
	GetReaction(ctx context.Context, targetType, targetID, reaction, userID string) (reactionRecord, bool, error)
	CountActiveReactions(ctx context.Context, targetType, targetID, reaction string) (int64, error)
	ListReactions(ctx context.Context, userID string) ([]reactionRecord, error)
}

func (s *Service) reactionStore() reactionStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) getReaction(ctx context.Context, targetType, targetID, reaction, userID string) (reactionRecord, bool, error) {
	if store := s.reactionStore(); store != nil {
		return store.GetReaction(ctx, targetType, targetID, reaction, userID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.reactions[reactionKey(targetType, targetID, reaction, userID)]
	return record, ok, nil
}

func (s *Service) saveReaction(ctx context.Context, record reactionRecord) error {
	s.mu.Lock()
	s.reactions[reactionKey(record.TargetType, record.TargetID, record.Reaction, record.UserID)] = record
	s.mu.Unlock()
	if store := s.reactionStore(); store != nil {
		return store.UpsertReaction(ctx, record)
	}
	return nil
}

func (s *Service) countActiveReactions(ctx context.Context, targetType, targetID, reaction string) (int64, error) {
	if store := s.reactionStore(); store != nil {
		return store.CountActiveReactions(ctx, targetType, targetID, reaction)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	for _, record := range s.reactions {
		if record.TargetType == targetType && record.TargetID == targetID && record.Reaction == reaction && record.Active {
			count++
		}
	}
	return count, nil
}

func (s *Service) myReactions(ctx context.Context) any {
	s.mu.Lock()
	userID := s.ownerMXID
	s.mu.Unlock()
	if store := s.reactionStore(); store != nil {
		reactions, err := store.ListReactions(ctx, userID)
		if err == nil {
			return map[string]any{"reactions": s.reactionHistory(ctx, reactions, userID)}
		}
	}
	s.mu.Lock()
	reactions := make([]reactionRecord, 0, len(s.reactions))
	for _, record := range s.reactions {
		if record.UserID == userID && record.Active {
			reactions = append(reactions, record)
		}
	}
	s.mu.Unlock()
	return map[string]any{"reactions": s.reactionHistory(ctx, reactions, userID)}
}

func (s *Service) reactionHistory(ctx context.Context, reactions []reactionRecord, ownerMXID string) []channelReactionHistory {
	history := make([]channelReactionHistory, 0, len(reactions))
	for _, reaction := range reactions {
		item := channelReactionHistory{Reaction: reaction}
		channelID := strings.TrimSpace(reaction.ChannelID)
		postID := strings.TrimSpace(reaction.PostID)
		if strings.EqualFold(reaction.TargetType, "post") && postID == "" {
			postID = strings.TrimSpace(reaction.TargetID)
		}
		commentID := strings.TrimSpace(reaction.CommentID)
		if strings.EqualFold(reaction.TargetType, "comment") && commentID == "" {
			commentID = strings.TrimSpace(reaction.TargetID)
		}
		if channelID != "" {
			if ch, ok, err := s.channelByIDOrRoom(ctx, channelID, ""); err == nil && ok {
				if enriched, err := s.channelWithCurrentCounts(ctx, ch); err == nil {
					ch = enriched
				}
				item.Channel = &ch
			}
		}
		if postID != "" {
			if post, ok, err := s.channelPostByID(ctx, postID, channelID); err == nil && ok {
				posts := []channelPostRecord{post}
				s.enrichChannelPosts(ctx, posts, ownerMXID)
				item.Post = &posts[0]
				if channelID == "" {
					channelID = strings.TrimSpace(posts[0].ChannelID)
				}
			}
		}
		if commentID != "" {
			if comment, ok, err := s.channelCommentByID(ctx, commentID, postID); err == nil && ok {
				comments := []channelCommentRecord{comment}
				s.enrichChannelComments(ctx, comments, ownerMXID)
				item.Comment = &comments[0]
				if postID == "" {
					postID = strings.TrimSpace(comments[0].PostID)
				}
				if channelID == "" {
					channelID = strings.TrimSpace(comments[0].ChannelID)
				}
			}
		}
		if item.Post == nil && postID != "" {
			if post, ok, err := s.channelPostByID(ctx, postID, channelID); err == nil && ok {
				posts := []channelPostRecord{post}
				s.enrichChannelPosts(ctx, posts, ownerMXID)
				item.Post = &posts[0]
				if channelID == "" {
					channelID = strings.TrimSpace(posts[0].ChannelID)
				}
			}
		}
		if item.Channel == nil && channelID != "" {
			if ch, ok, err := s.channelByIDOrRoom(ctx, channelID, ""); err == nil && ok {
				if enriched, err := s.channelWithCurrentCounts(ctx, ch); err == nil {
					ch = enriched
				}
				item.Channel = &ch
			}
		}
		history = append(history, item)
	}
	return history
}
