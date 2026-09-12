package dendrite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalktransport"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func (t *DendriteTransport) ReadGroupAgentRoom(ctx context.Context, roomID string) (dirextalktransport.GroupAgentRoom, error) {
	out := dirextalktransport.GroupAgentRoom{RoomID: roomID, Joined: map[string]bool{}, JoinedAt: map[string]int64{}}
	rid, err := spec.NewRoomID(roomID)
	if err != nil {
		return out, err
	}
	var state roomserverAPI.QueryCurrentStateResponse
	if err = t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{RoomID: roomID, AllowWildcards: true, StateTuples: []gomatrixserverlib.StateKeyTuple{
		{EventType: spec.MRoomCreate, StateKey: ""}, {EventType: spec.MRoomPowerLevels, StateKey: ""},
		{EventType: DirextalkRoomProfileEventType, StateKey: ""}, {EventType: spec.MRoomMember, StateKey: "*"},
		{EventType: dirextalkdomain.GroupAgentStateEventType, StateKey: ""},
	}}, &state); err != nil {
		return out, err
	}
	resolve := func(sender spec.SenderID) (string, error) {
		u, e := t.rsAPI.QueryUserIDForSender(ctx, *rid, sender)
		if e != nil {
			return "", e
		}
		if u != nil {
			return u.String(), nil
		}
		return validRoomCreatorMXID(string(sender)), nil
	}
	creator := ""
	users := map[string]int64{}
	displayNames := map[string]string{}
	hasLevels := false
	var defaultLevel int64
	for tuple, event := range state.StateEvents {
		if event == nil {
			continue
		}
		var content map[string]any
		if err = json.Unmarshal(event.Content(), &content); err != nil {
			return out, err
		}
		switch tuple.EventType {
		case spec.MRoomCreate:
			out.IsGroup = trimString(content["type"]) == DirextalkRoomTypeGroup
			creator, err = resolve(event.SenderID())
			if err != nil {
				return out, err
			}
		case DirextalkRoomProfileEventType:
			if kind := trimString(content["room_type"]); kind != "" {
				out.IsGroup = kind == DirextalkRoomTypeGroup
			}
			out.Dissolved = boolParam(content["dissolved"])
		case spec.MRoomMember:
			userID, e := resolve(spec.SenderID(tuple.StateKey))
			if e != nil {
				return out, e
			}
			if userID != "" {
				out.Joined[userID] = trimString(content["membership"]) == "join"
				out.JoinedAt[userID] = int64(event.OriginServerTS())
				if name := trimString(content["displayname"]); name != "" {
					displayNames[userID] = name
				}
			}
		case spec.MRoomPowerLevels:
			hasLevels = true
			var levels struct {
				Users        map[string]int64 `json:"users"`
				UsersDefault int64            `json:"users_default"`
			}
			if err = json.Unmarshal(event.Content(), &levels); err != nil {
				return out, err
			}
			defaultLevel = levels.UsersDefault
			for sender, level := range levels.Users {
				userID, e := resolve(spec.SenderID(sender))
				if e != nil {
					return out, e
				}
				if userID != "" {
					users[userID] = level
				}
			}
		case dirextalkdomain.GroupAgentStateEventType:
			var b dirextalkdomain.GroupAgentBinding
			if err = json.Unmarshal(event.Content(), &b); err != nil {
				return out, err
			}
			out.Binding = &b
		}
	}
	if !hasLevels {
		users[creator] = 100
	}
	for user, joined := range out.Joined {
		if joined {
			if _, exists := users[user]; !exists {
				users[user] = defaultLevel
			}
		}
	}
	var highest int64 = 99
	ambiguous := false
	for user, level := range users {
		if !out.Joined[user] {
			continue
		}
		if level > highest {
			out.OwnerMXID = user
			highest = level
			ambiguous = false
		} else if level == highest {
			ambiguous = true
		}
	}
	if ambiguous {
		out.OwnerMXID = ""
	}
	out.OwnerDisplayName = displayNames[out.OwnerMXID]
	return out, nil
}

func (t *DendriteTransport) ReadGroupAgentMessage(ctx context.Context, roomID, eventID string) (dirextalktransport.GroupAgentMessage, error) {
	out := dirextalktransport.GroupAgentMessage{}
	var res roomserverAPI.QueryEventsByIDResponse
	if err := t.rsAPI.QueryEventsByID(ctx, &roomserverAPI.QueryEventsByIDRequest{RoomID: roomID, EventIDs: []string{eventID}}, &res); err != nil {
		return out, err
	}
	for _, event := range res.Events {
		if event == nil || event.EventID() != eventID || event.RoomID().String() != roomID || event.Type() != "m.room.message" || event.StateKey() != nil {
			continue
		}
		var c struct {
			Body        string `json:"body"`
			MsgType     string `json:"msgtype"`
			ProductType string `json:"msg_type"`
			Mentions    struct {
				UserIDs []string `json:"user_ids"`
			} `json:"m.mentions"`
			LegacyMentions     []legacyGroupAgentMention `json:"mentions"`
			LegacyMentionsJSON string                    `json:"mentions_json"`
			GroupAgent         json.RawMessage           `json:"io.dirextalk.group_agent"`
		}
		if err := json.Unmarshal(event.Content(), &c); err != nil {
			return out, err
		}
		if c.MsgType != "m.text" || strings.TrimSpace(c.Body) == "" {
			return out, nil
		}
		u, err := t.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
		if err != nil {
			return out, err
		}
		sender := validRoomCreatorMXID(string(event.SenderID()))
		if u != nil {
			sender = u.String()
		}
		if sender == "" {
			return out, fmt.Errorf("group Agent source sender is unresolved")
		}
		displayName := t.groupAgentSenderDisplayName(ctx, roomID, sender)
		mentions := groupAgentMentionUserIDs(c.Mentions.UserIDs, c.LegacyMentions, c.LegacyMentionsJSON)
		return dirextalktransport.GroupAgentMessage{RoomID: roomID, EventID: eventID, SenderMXID: sender, SenderDisplayName: displayName, Body: c.Body, OriginServerTS: int64(event.OriginServerTS()), Mentions: mentions, Generated: len(c.GroupAgent) > 0 || strings.HasPrefix(c.ProductType, "group_agent_")}, nil
	}
	return out, fmt.Errorf("group Agent source event unavailable")
}

// groupAgentSenderDisplayName reads the sender's current in-room profile name so
// the shared group conversation can attribute every message. A missing or
// unusable name returns "" and the caller falls back to the authenticated MXID.
func (t *DendriteTransport) groupAgentSenderDisplayName(ctx context.Context, roomID, senderMXID string) string {
	if t == nil || t.rsAPI == nil || strings.TrimSpace(senderMXID) == "" {
		return ""
	}
	var state roomserverAPI.QueryCurrentStateResponse
	if err := t.rsAPI.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{RoomID: roomID, StateTuples: []gomatrixserverlib.StateKeyTuple{{EventType: spec.MRoomMember, StateKey: senderMXID}}}, &state); err != nil {
		return ""
	}
	member := state.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: senderMXID}]
	if member == nil {
		return ""
	}
	var content struct {
		DisplayName string `json:"displayname"`
		Membership  string `json:"membership"`
	}
	if json.Unmarshal(member.Content(), &content) != nil || content.Membership != "join" {
		return ""
	}
	return dirextalktransport.SanitizeGroupAgentDisplayName(content.DisplayName)
}

type legacyGroupAgentMention struct {
	UserID string `json:"user_id"`
}

const (
	legacyGroupAgentMentionLimit = 64
	legacyGroupAgentMentionBytes = 8 << 10
)

// groupAgentMentionUserIDs returns the mention identities that address the
// group Agent. Matrix `m.mentions` is the standard field and stays
// authoritative. Clients older than that field publish only the product
// `mentions` array (or its `mentions_json` string) for a mention the user
// explicitly picked, so those are accepted as a fallback. Every identity is
// validated against the authoritative binding by the caller, and a mention is
// only a trigger: it never grants a member any permission.
func groupAgentMentionUserIDs(mentions []string, legacy []legacyGroupAgentMention, legacyJSON string) []string {
	if len(mentions) > 0 {
		return mentions
	}
	if len(legacy) > 0 {
		return legacyMentionUserIDs(legacy)
	}
	if raw := strings.TrimSpace(legacyJSON); raw != "" && len(raw) <= legacyGroupAgentMentionBytes {
		var parsed []legacyGroupAgentMention
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			return legacyMentionUserIDs(parsed)
		}
	}
	return nil
}

func legacyMentionUserIDs(legacy []legacyGroupAgentMention) []string {
	out := make([]string, 0, len(legacy))
	for i, mention := range legacy {
		if i >= legacyGroupAgentMentionLimit {
			break
		}
		if userID := strings.TrimSpace(mention.UserID); userID != "" {
			out = append(out, userID)
		}
	}
	return out
}
