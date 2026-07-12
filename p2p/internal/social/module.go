// Package social owns ProductCore favorite and follow actions.
package social

import (
	"context"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionFollowsList          = "follows.list"
	actionFollowsAdd           = "follows.add"
	actionFollowsRemove        = "follows.remove"
	actionFavoritesList        = "favorites.list"
	actionFavoritesAdd         = "favorites.add"
	actionFavoritesDelete      = "favorites.delete"
	actionFavoritesDeleteBatch = "favorites.delete_batch"
)

// Store is the durable owner-local repository used by Module.
type Store interface {
	UpsertFavorite(ctx context.Context, favorite dirextalkdomain.FavoriteRecord) error
	FindFavoriteByEvent(ctx context.Context, eventID, roomID string) (dirextalkdomain.FavoriteRecord, bool, error)
	ListFavorites(ctx context.Context, messageType string) ([]dirextalkdomain.FavoriteRecord, error)
	DeleteFavorite(ctx context.Context, id int64) error
	UpsertFollow(ctx context.Context, follow dirextalkdomain.FollowRecord) error
	ListFollows(ctx context.Context) ([]dirextalkdomain.FollowRecord, error)
	DeleteFollow(ctx context.Context, domain string) error
}

// Config provides the clock used for durable timestamps and favorite IDs.
type Config struct {
	Now func() time.Time
}

// Module implements favorites and follows over one Store path.
type Module struct {
	store Store
	now   func() time.Time

	favoriteMu     sync.Mutex
	lastFavoriteID int64
	favoriteIDsSet bool
}

func New(store Store, cfg Config) *Module {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Module{store: store, now: now}
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionFollowsList:          m.handleFollowsList,
		actionFollowsAdd:           m.handleFollowsAdd,
		actionFollowsRemove:        m.handleFollowsRemove,
		actionFavoritesList:        m.handleFavoritesList,
		actionFavoritesAdd:         m.handleFavoritesAdd,
		actionFavoritesDelete:      m.handleFavoritesDelete,
		actionFavoritesDeleteBatch: m.handleFavoritesDeleteBatch,
	}
}

func (m *Module) handleFollowsList(ctx context.Context, _ map[string]any) (any, *actionbase.Error) {
	follows, err := m.store.ListFollows(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"follows": follows}, nil
}

func (m *Module) handleFollowsAdd(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	domain := actionbase.Params(params).String("domain")
	if domain == "" {
		return nil, actionbase.BadRequest("domain is required")
	}
	follow := dirextalkdomain.FollowRecord{
		Domain:    domain,
		CreatedAt: m.now().UTC().Format(time.RFC3339Nano),
	}
	if err := m.store.UpsertFollow(ctx, follow); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return follow, nil
}

func (m *Module) handleFollowsRemove(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	domain := actionbase.Params(params).String("domain")
	if err := m.store.DeleteFollow(ctx, domain); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"status": "ok"}, nil
}
