package social

import (
	"context"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (m *Module) handleFavoritesAdd(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	values := actionbase.Params(params)
	now := m.now().UTC()
	favorite := dirextalkdomain.FavoriteRecord{
		EventID:        values.String("event_id"),
		RoomID:         values.String("room_id"),
		SenderID:       values.String("sender_id"),
		SenderName:     values.String("sender_name"),
		Content:        values.String("content"),
		MessageType:    values.String("message_type"),
		OriginServerTS: values.Int64("origin_server_ts"),
		CreatedAt:      now.Format(time.RFC3339Nano),
	}

	m.favoriteMu.Lock()
	defer m.favoriteMu.Unlock()

	if favorite.EventID != "" {
		existing, ok, err := m.store.FindFavoriteByEvent(ctx, favorite.EventID, favorite.RoomID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if ok {
			favorite.ID = existing.ID
			if existing.CreatedAt != "" {
				favorite.CreatedAt = existing.CreatedAt
			}
			if existing.ID > m.lastFavoriteID {
				m.lastFavoriteID = existing.ID
			}
		} else {
			id, err := m.nextFavoriteIDLocked(ctx, now)
			if err != nil {
				return nil, actionbase.InternalError(err)
			}
			favorite.ID = id
		}
	} else {
		id, err := m.nextFavoriteIDLocked(ctx, now)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		favorite.ID = id
	}

	if err := m.store.UpsertFavorite(ctx, favorite); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return favorite, nil
}

func (m *Module) nextFavoriteIDLocked(ctx context.Context, now time.Time) (int64, error) {
	if !m.favoriteIDsSet {
		favorites, err := m.store.ListFavorites(ctx, "")
		if err != nil {
			return 0, err
		}
		for _, favorite := range favorites {
			if favorite.ID > m.lastFavoriteID {
				m.lastFavoriteID = favorite.ID
			}
		}
		m.favoriteIDsSet = true
	}
	id := now.UnixMilli()
	if id <= m.lastFavoriteID {
		id = m.lastFavoriteID + 1
	}
	m.lastFavoriteID = id
	return id, nil
}

// ListFavorites returns favorites from the module's sole state source.
func (m *Module) ListFavorites(ctx context.Context, messageType string) ([]dirextalkdomain.FavoriteRecord, error) {
	return m.store.ListFavorites(ctx, strings.TrimSpace(messageType))
}

func (m *Module) handleFavoritesList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	favorites, err := m.ListFavorites(ctx, actionbase.Params(params).String("message_type"))
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"favorites": favorites}, nil
}

func (m *Module) handleFavoritesDelete(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	id := actionbase.Params(params).Int64("id")
	if err := m.store.DeleteFavorite(ctx, id); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"status": "ok"}, nil
}

func (m *Module) handleFavoritesDeleteBatch(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	ids := actionbase.Params(params).Int64s("ids")
	if len(ids) == 0 {
		return nil, actionbase.BadRequest("ids is required")
	}
	if len(ids) > 500 {
		return nil, actionbase.BadRequest("ids is too large")
	}
	for _, id := range ids {
		if err := m.store.DeleteFavorite(ctx, id); err != nil {
			return nil, actionbase.InternalError(err)
		}
	}
	return map[string]any{"status": "ok", "deleted": ids}, nil
}
