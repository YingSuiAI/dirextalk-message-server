package p2p

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
)

func (s *Service) channelStore() channelStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) createChannelRoom(ctx context.Context, ch channel) (string, *apiError) {
	initialState := []RoomStateEvent{channelStateEvent(ch, false)}
	if historyVisibilityState, ok := channelHistoryVisibilityStateEvent(ch.ChannelType); ok {
		initialState = append([]RoomStateEvent{historyVisibilityState}, initialState...)
	}
	return s.ensureProductRoom(ctx, "channel", CreateRoomRequest{
		Name:         ch.Name,
		Topic:        ch.Description,
		Visibility:   ch.Visibility,
		RoomType:     DirextalkRoomTypeChannel,
		IsDirect:     false,
		InitialState: initialState,
	})
}

func (s *Service) channelPublicGet(ctx context.Context, params map[string]any) (any, *apiError) {
	channelID := trimString(params["channel_id"])
	roomID := trimString(params["room_id"])
	if channelID == "" && roomID == "" {
		return nil, badRequest("channel_id or room_id is required")
	}
	ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !ok {
		if remote, fetched, apiErr := s.remotePublicChannelGet(ctx, channelID, roomID, params); apiErr != nil {
			return nil, apiErr
		} else if fetched {
			ch = remote
			ok = true
		}
	}
	if !ok {
		if roomID != "" {
			s.mu.Lock()
			transport := s.transport
			s.mu.Unlock()
			if transport != nil {
				fetched, found, fetchErr := transport.GetRoomChannel(ctx, roomID)
				if fetchErr != nil {
					if roomServer := domainFromMatrixID(roomID, "!"); roomServer != "" && roomServer != s.serverName {
						return nil, statusError(404, "channel not found")
					}
					return nil, internalError(fetchErr)
				}
				if found {
					ch = fetched
					ok = true
					if err := s.saveChannel(ctx, ch); err != nil {
						return nil, internalError(err)
					}
				}
			}
		}
		if !ok {
			return nil, statusError(404, "channel not found")
		}
	}
	if !strings.EqualFold(ch.Visibility, "public") {
		return nil, statusError(404, "channel not found")
	}
	ch, err = s.channelWithCurrentCounts(ctx, ch)
	if err != nil {
		return nil, internalError(err)
	}
	return ch, nil
}

func (s *Service) channelPublicSearch(ctx context.Context, params map[string]any) (any, *apiError) {
	rawQuery := trimString(params["q"])
	query := strings.ToLower(rawQuery)
	limit := int(int64Param(params["limit"]))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if matrixRoomIDQuery(rawQuery) {
		ch, apiErr := s.channelPublicGet(ctx, map[string]any{
			"room_id":              rawQuery,
			"remote_node_base_url": remoteNodeBaseURLParam(params),
		})
		if apiErr != nil {
			if apiErr.Status == 404 {
				return map[string]any{"channels": []channel{}, "results": []channel{}}, nil
			}
			return nil, apiErr
		}
		channelResult, ok := ch.(channel)
		if !ok {
			return nil, internalError(fmt.Errorf("public get returned %T", ch))
		}
		return map[string]any{"channels": []channel{channelResult}, "results": []channel{channelResult}}, nil
	}
	results, err := s.channelsModule.SearchPublic(ctx, query, limit)
	if err != nil {
		return nil, internalError(err)
	}
	for i := range results {
		ch, err := s.channelWithCurrentCounts(ctx, results[i])
		if err != nil {
			return nil, internalError(err)
		}
		results[i] = ch
	}
	return map[string]any{"channels": results, "results": results}, nil
}

func (s *Service) userPublicChannels(ctx context.Context, params map[string]any) (any, *apiError) {
	userID := fallbackString(trimString(params["user_id"]), trimString(params["user_mxid"]))
	userID = fallbackString(userID, trimString(params["mxid"]))
	if userID == "" {
		return nil, badRequest("user_id is required")
	}
	if remoteNodeBaseURLParam(params) != "" {
		ownerNode := domainFromMXID(userID)
		if ownerNode == "" {
			return nil, badRequest("valid user_id is required")
		}
		var remote struct {
			UserID   string    `json:"user_id"`
			Channels []channel `json:"channels"`
			Results  []channel `json:"results"`
		}
		status, err := s.remotePublicAction(ctx, ownerNode, "users.public_channels", params, &remote)
		if err != nil {
			if status != 0 && status != http.StatusBadGateway {
				return nil, statusError(status, err.Error())
			}
			return nil, statusError(http.StatusBadGateway, err.Error())
		}
		if status != http.StatusOK {
			return nil, statusError(status, "target node public channels lookup failed")
		}
		channels := remote.Channels
		if channels == nil {
			channels = remote.Results
		}
		return map[string]any{"user_id": fallbackString(remote.UserID, userID), "channels": channels, "results": channels}, nil
	}
	publicChannels, err := s.channelsModule.ListPublic(ctx, userID)
	if err != nil {
		return nil, internalError(err)
	}
	for i := range publicChannels {
		ch, err := s.channelWithCurrentCounts(ctx, publicChannels[i])
		if err != nil {
			return nil, internalError(err)
		}
		publicChannels[i] = ch
	}
	return map[string]any{"user_id": userID, "channels": publicChannels}, nil
}

func (s *Service) saveChannel(ctx context.Context, ch channel) error {
	return s.channelsModule.Save(ctx, ch)
}

func (s *Service) setChannelMemberMute(ctx context.Context, roomID, channelID string, muted bool) *apiError {
	if err := s.setProductMemberMute(ctx, roomID, channelID, muted); err != nil {
		return internalError(err)
	}
	return nil
}

func channelStateEvent(ch channel, dissolved bool) RoomStateEvent {
	return roomProfileForChannel(ch, dissolved)
}

func (s *Service) publishChannelState(ctx context.Context, ch channel, dissolved bool) error {
	if s.transport == nil || strings.TrimSpace(ch.RoomID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     ch.RoomID,
		SenderMXID: senderMXID,
		Event:      channelStateEvent(ch, dissolved),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) publishChannelHistoryVisibilityState(ctx context.Context, ch channel) error {
	if s.transport == nil || strings.TrimSpace(ch.RoomID) == "" {
		return nil
	}
	if historyVisibilityState, ok := channelHistoryVisibilityStateEvent(ch.ChannelType); ok {
		s.mu.Lock()
		senderMXID := s.ownerMXID
		s.mu.Unlock()
		return s.transport.SendStateEvent(ctx, SendStateEventRequest{
			RoomID:     ch.RoomID,
			SenderMXID: senderMXID,
			Event:      historyVisibilityState,
		})
	}
	return nil
}

func (s *Service) publishJoinRequestState(ctx context.Context, roomID, userID, status, reason string) *apiError {
	if s.transport == nil || strings.TrimSpace(roomID) == "" || strings.TrimSpace(userID) == "" {
		return nil
	}
	status = strings.ToLower(strings.TrimSpace(status))
	switch status {
	case "pending", "approved", "rejected":
	default:
		return badRequest("invalid join request status")
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(senderMXID) == "" {
		return nil
	}
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     roomID,
		SenderMXID: senderMXID,
		Event:      roomStateEvent(dirextalkstate.JoinRequestState(roomID, userID, status, reason, time.Now().UTC())),
	}); err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Service) publishMemberPolicyState(ctx context.Context, member memberRecord) *apiError {
	if s.transport == nil || strings.TrimSpace(member.RoomID) == "" || strings.TrimSpace(member.UserID) == "" {
		return nil
	}
	s.mu.Lock()
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(senderMXID) == "" {
		return nil
	}
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     member.RoomID,
		SenderMXID: senderMXID,
		Event:      roomStateEvent(dirextalkstate.MemberPolicyState(member)),
	}); err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Service) deleteChannel(ctx context.Context, channelID string) error {
	return s.channelsModule.Delete(ctx, channelID)
}

func (s *Service) channelByIDOrRoom(ctx context.Context, channelID, roomID string) (channel, bool, error) {
	return s.channelsModule.ByIDOrRoom(ctx, channelID, roomID)
}

func (s *Service) channelSnapshot(ctx context.Context, channelID string) channel {
	return s.channelsModule.Snapshot(ctx, channelID)
}

func (s *Service) channelWithCurrentCounts(ctx context.Context, ch channel) (channel, error) {
	return s.channelsModule.WithCurrentCounts(ctx, ch)
}

func (s *Service) refreshStoredChannelCounts(ctx context.Context, channelID string) error {
	return s.channelsModule.RefreshCounts(ctx, channelID)
}
