package channels

import (
	"context"
	"errors"
	"net/http"
	"strings"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

// RemoteUserChannelsResult is the protocol-neutral result returned by the
// root remote-public adapter.
type RemoteUserChannelsResult struct {
	UserID   string
	Channels []Channel
}

func (m *Module) PublicGet(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	channel, actionErr := m.publicGet(ctx, raw)
	if actionErr != nil {
		return nil, actionErr
	}
	return channel, nil
}

func (m *Module) publicGet(ctx context.Context, raw map[string]any) (Channel, *actionbase.Error) {
	params := actionbase.Params(raw)
	channelID := params.String("channel_id")
	roomID := params.String("room_id")
	if channelID == "" && roomID == "" {
		return Channel{}, actionbase.BadRequest("channel_id or room_id is required")
	}
	channel, found, err := m.ByIDOrRoom(ctx, channelID, roomID)
	if err != nil {
		return Channel{}, actionbase.InternalError(err)
	}
	if !found && m.config.RemotePublicGet != nil {
		channel, found, actionErr := m.config.RemotePublicGet(ctx, channelID, roomID, raw)
		if actionErr != nil {
			return Channel{}, actionErr
		}
		if found {
			return m.visiblePublicChannel(ctx, channel)
		}
	}
	if !found && roomID != "" && m.config.FetchRoomChannel != nil {
		var actionErr *actionbase.Error
		channel, found, actionErr = m.config.FetchRoomChannel(ctx, roomID)
		if actionErr != nil {
			return Channel{}, actionErr
		}
		if found {
			if err := m.Save(ctx, channel); err != nil {
				return Channel{}, actionbase.InternalError(err)
			}
		}
	}
	if !found {
		return Channel{}, actionbase.StatusError(http.StatusNotFound, "channel not found")
	}
	return m.visiblePublicChannel(ctx, channel)
}

func (m *Module) visiblePublicChannel(ctx context.Context, channel Channel) (Channel, *actionbase.Error) {
	if !strings.EqualFold(channel.Visibility, "public") {
		return Channel{}, actionbase.StatusError(http.StatusNotFound, "channel not found")
	}
	channel, err := m.WithCurrentCounts(ctx, channel)
	if err != nil {
		return Channel{}, actionbase.InternalError(err)
	}
	return channel, nil
}

func (m *Module) PublicSearch(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	rawQuery := params.String("q")
	query := strings.ToLower(rawQuery)
	limit := int(params.Int64("limit"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if m.config.IsMatrixRoomID != nil && m.config.IsMatrixRoomID(rawQuery) {
		channel, actionErr := m.publicGet(ctx, map[string]any{
			"room_id":              rawQuery,
			"remote_node_base_url": params.String("remote_node_base_url"),
		})
		if actionErr != nil {
			if actionErr.Status == http.StatusNotFound {
				return map[string]any{"channels": []Channel{}, "results": []Channel{}}, nil
			}
			return nil, actionErr
		}
		channels := []Channel{channel}
		return map[string]any{"channels": channels, "results": channels}, nil
	}
	results, err := m.SearchPublic(ctx, query, limit)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	for i := range results {
		channel, err := m.WithCurrentCounts(ctx, results[i])
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		results[i] = channel
	}
	return map[string]any{"channels": results, "results": results}, nil
}

func (m *Module) UserPublicChannels(ctx context.Context, raw map[string]any) (any, *actionbase.Error) {
	params := actionbase.Params(raw)
	userID := params.FirstString("user_id", "user_mxid", "mxid")
	if userID == "" {
		return nil, actionbase.BadRequest("user_id is required")
	}
	if params.String("remote_node_base_url") != "" {
		if m.config.RemoteUserChannels == nil {
			return nil, actionbase.InternalError(errors.New("remote user channels adapter is not configured"))
		}
		remote, actionErr := m.config.RemoteUserChannels(ctx, userID, raw)
		if actionErr != nil {
			return nil, actionErr
		}
		return map[string]any{
			"user_id":  fallback(remote.UserID, userID),
			"channels": remote.Channels,
			"results":  remote.Channels,
		}, nil
	}
	channels, err := m.ListPublic(ctx, userID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	for i := range channels {
		channel, err := m.WithCurrentCounts(ctx, channels[i])
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		channels[i] = channel
	}
	return map[string]any{"user_id": userID, "channels": channels}, nil
}
