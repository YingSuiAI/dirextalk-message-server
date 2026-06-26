package p2p

import (
	"context"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/productpolicy"
	"github.com/YingSuiAI/direxio-message-server/p2p/domain"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const (
	DirexioRoomTypeDirect  = productpolicy.DirexioRoomTypeDirect
	DirexioRoomTypeGroup   = productpolicy.DirexioRoomTypeGroup
	DirexioRoomTypeChannel = productpolicy.DirexioRoomTypeChannel

	DirexioRoomProfileEventType  = productpolicy.DirexioRoomProfileEventType
	DirexioMemberPolicyEventType = productpolicy.DirexioMemberPolicyEventType
	DirexioJoinRequestEventType  = productpolicy.DirexioJoinRequestEventType

	AgentRoomMessageEventType    = "agent_room.message"
	AgentGatewayContentKey       = "io.direxio.agent_gateway"
	AgentGatewaySourceContentKey = "io.direxio.gateway_source"
)

type p2pEvent = domain.Event

func direxioRoomType(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "direct", "dm", "contact":
		return DirexioRoomTypeDirect
	case "group":
		return DirexioRoomTypeGroup
	case "channel":
		return DirexioRoomTypeChannel
	default:
		return ""
	}
}

func historyVisibilityStateEvent(visibility gomatrixserverlib.HistoryVisibility) RoomStateEvent {
	return RoomStateEvent{
		Type:     spec.MRoomHistoryVisibility,
		StateKey: "",
		Content: map[string]any{
			"history_visibility": string(visibility),
		},
	}
}

func joinedHistoryVisibilityStateEvent() RoomStateEvent {
	return historyVisibilityStateEvent(gomatrixserverlib.HistoryVisibilityJoined)
}

func sharedHistoryVisibilityStateEvent() RoomStateEvent {
	return historyVisibilityStateEvent(gomatrixserverlib.HistoryVisibilityShared)
}

func channelHistoryVisibilityStateEvent(channelType string) (RoomStateEvent, bool) {
	switch strings.ToLower(strings.TrimSpace(channelType)) {
	case "", "chat", "text":
		return joinedHistoryVisibilityStateEvent(), true
	case "post":
		return sharedHistoryVisibilityStateEvent(), true
	default:
		return RoomStateEvent{}, false
	}
}

func roomProfileForDirect(name, requesterMXID, targetMXID, requesterDisplayName, requesterAvatarURL, remark string, dissolved bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirexioRoomProfileEventType,
		StateKey: "",
		Content: map[string]any{
			"room_type":      DirexioRoomTypeDirect,
			"name":           strings.TrimSpace(name),
			"visibility":     "private",
			"join_policy":    "invite",
			"invite_policy":  "owner",
			"requester_mxid": strings.TrimSpace(requesterMXID),
			"target_mxid":    strings.TrimSpace(targetMXID),
			"display_name":   strings.TrimSpace(requesterDisplayName),
			"avatar_url":     strings.TrimSpace(requesterAvatarURL),
			"domain":         domainFromMXID(requesterMXID),
			"remark":         strings.TrimSpace(remark),
			"dissolved":      dissolved,
		},
	}
}

func roomProfileForGroup(group groupRecord, dissolved bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirexioRoomProfileEventType,
		StateKey: "",
		Content: map[string]any{
			"room_type":     DirexioRoomTypeGroup,
			"room_id":       group.RoomID,
			"name":          group.Name,
			"topic":         group.Topic,
			"avatar_url":    group.AvatarURL,
			"invite_policy": fallbackString(group.InvitePolicy, "member"),
			"muted":         group.Muted,
			"dissolved":     dissolved,
		},
	}
}

func roomProfileForChannel(ch channel, dissolved bool) RoomStateEvent {
	return RoomStateEvent{
		Type:     DirexioRoomProfileEventType,
		StateKey: "",
		Content: map[string]any{
			"room_type":        DirexioRoomTypeChannel,
			"channel_id":       ch.ChannelID,
			"room_id":          ch.RoomID,
			"name":             ch.Name,
			"description":      ch.Description,
			"avatar_url":       ch.AvatarURL,
			"visibility":       fallbackString(ch.Visibility, "private"),
			"join_policy":      fallbackString(ch.JoinPolicy, "invite"),
			"channel_type":     fallbackString(ch.ChannelType, "chat"),
			"comments_enabled": ch.CommentsEnabled,
			"muted":            ch.Muted,
			"dissolved":        dissolved,
		},
	}
}

func (s *Service) appendP2PEvent(ctx context.Context, event p2pEvent) error {
	now := time.Now().UTC()
	if event.CreatedAt == "" {
		event.CreatedAt = now.Format(time.RFC3339Nano)
	}
	s.mu.Lock()
	if event.Seq <= 0 || event.Seq <= s.nextEventSeq {
		event.Seq = now.UnixNano()
		if event.Seq <= s.nextEventSeq {
			event.Seq = s.nextEventSeq + 1
		}
	}
	s.nextEventSeq = event.Seq
	s.events = append(s.events, event)
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.InsertEvent(ctx, event); err != nil {
			return err
		}
	}
	s.notifyP2PEventWaiters()
	return nil
}

func (s *Service) listP2PEvents(ctx context.Context, since int64, limit int) ([]p2pEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if s.store != nil {
		return s.store.ListEvents(ctx, since, limit)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]p2pEvent, 0, limit)
	for _, event := range s.events {
		if event.Seq <= since {
			continue
		}
		events = append(events, event)
		if len(events) >= limit {
			break
		}
	}
	return events, nil
}

func (s *Service) p2pEventWaiter() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventNotify == nil {
		s.eventNotify = make(chan struct{})
	}
	return s.eventNotify
}

func (s *Service) notifyP2PEventWaiters() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventNotify == nil {
		s.eventNotify = make(chan struct{})
	}
	close(s.eventNotify)
	s.eventNotify = make(chan struct{})
}
