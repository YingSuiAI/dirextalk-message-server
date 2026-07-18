package nativeagent

import (
	"encoding/json"
	"strings"

	"github.com/cloudwego/eino/schema"
)

const nativeAgentReferencePreviewRunes = 240

func nativeAgentReferences(produced []*schema.Message) []map[string]any {
	references := make([]map[string]any, 0)
	seenRooms := map[string]struct{}{}
	seenPosts := map[string]struct{}{}

	addRoom := func(roomID, roomType, title, preview string) {
		roomID = strings.TrimSpace(roomID)
		roomType = normalizedReferenceRoomType(roomType)
		if roomID == "" {
			return
		}
		if _, exists := seenRooms[roomID]; exists {
			return
		}
		seenRooms[roomID] = struct{}{}
		reference := map[string]any{
			"kind":    "room",
			"room_id": roomID,
		}
		if roomType != "" {
			reference["room_type"] = roomType
		}
		if title = strings.TrimSpace(title); title != "" {
			reference["title"] = title
		}
		if preview = referencePreview(preview); preview != "" {
			reference["preview"] = preview
		}
		references = append(references, reference)
	}

	addPost := func(roomID, channelID, postID, title, preview string) {
		roomID = strings.TrimSpace(roomID)
		channelID = strings.TrimSpace(channelID)
		postID = strings.TrimSpace(postID)
		if roomID == "" || channelID == "" || postID == "" {
			return
		}
		key := channelID + "\x00" + postID
		if _, exists := seenPosts[key]; exists {
			return
		}
		seenPosts[key] = struct{}{}
		reference := map[string]any{
			"kind":       "channel_post",
			"room_id":    roomID,
			"channel_id": channelID,
			"post_id":    postID,
		}
		if title = strings.TrimSpace(title); title != "" {
			reference["title"] = title
		}
		if preview = referencePreview(preview); preview != "" {
			reference["preview"] = preview
		}
		references = append(references, reference)
	}

	for _, message := range produced {
		if message == nil || message.Role != schema.Tool {
			continue
		}
		var envelope struct {
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal([]byte(message.Content), &envelope); err != nil || len(envelope.Result) == 0 || string(envelope.Result) == "null" {
			continue
		}

		switch strings.TrimSpace(message.ToolName) {
		case "dirextalk_contacts_list", "dirextalk_contacts_search":
			var result struct {
				Contacts []struct {
					RoomID      string `json:"room_id"`
					DisplayName string `json:"display_name"`
				} `json:"contacts"`
			}
			if json.Unmarshal(envelope.Result, &result) != nil {
				continue
			}
			for _, contact := range result.Contacts {
				addRoom(contact.RoomID, "direct", contact.DisplayName, "")
			}
		case "dirextalk_rooms_search":
			var result struct {
				Rooms []struct {
					Type   string `json:"type"`
					Name   string `json:"name"`
					RoomID string `json:"room_id"`
				} `json:"rooms"`
			}
			if json.Unmarshal(envelope.Result, &result) != nil {
				continue
			}
			for _, room := range result.Rooms {
				if normalizedReferenceRoomType(room.Type) == "" {
					continue
				}
				addRoom(room.RoomID, room.Type, room.Name, "")
			}
		case "dirextalk_messages_list":
			var result struct {
				RoomID   string `json:"room_id"`
				Name     string `json:"name"`
				Messages []struct {
					Msg string `json:"msg"`
				} `json:"messages"`
			}
			if json.Unmarshal(envelope.Result, &result) != nil {
				continue
			}
			preview := ""
			if len(result.Messages) > 0 {
				preview = result.Messages[0].Msg
			}
			addRoom(result.RoomID, "", result.Name, preview)
		case "dirextalk_channel_posts_list":
			var result struct {
				RoomID    string `json:"room_id"`
				ChannelID string `json:"channel_id"`
				Name      string `json:"name"`
				Posts     []struct {
					PostID string `json:"post_id"`
					Msg    string `json:"msg"`
				} `json:"posts"`
			}
			if json.Unmarshal(envelope.Result, &result) != nil {
				continue
			}
			for _, post := range result.Posts {
				addPost(result.RoomID, result.ChannelID, post.PostID, result.Name, post.Msg)
			}
		}
	}
	return references
}

func normalizedReferenceRoomType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "contact", "direct":
		return "direct"
	case "group":
		return "group"
	case "channel":
		return "channel"
	default:
		return ""
	}
}

func referencePreview(value string) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= nativeAgentReferencePreviewRunes {
		return value
	}
	return string(runes[:nativeAgentReferencePreviewRunes]) + "..."
}
