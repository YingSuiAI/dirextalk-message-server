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
			GroupAgent json.RawMessage `json:"io.dirextalk.group_agent"`
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
		return dirextalktransport.GroupAgentMessage{RoomID: roomID, EventID: eventID, SenderMXID: sender, Body: c.Body, OriginServerTS: int64(event.OriginServerTS()), Mentions: c.Mentions.UserIDs, Generated: len(c.GroupAgent) > 0 || strings.HasPrefix(c.ProductType, "group_agent_")}, nil
	}
	return out, fmt.Errorf("group Agent source event unavailable")
}
