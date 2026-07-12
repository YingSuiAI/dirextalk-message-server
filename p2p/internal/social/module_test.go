package social

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type testStore struct {
	mu sync.Mutex

	favorites map[int64]dirextalkdomain.FavoriteRecord
	follows   map[string]dirextalkdomain.FollowRecord

	findFavoriteErr    error
	listFavoritesErr   error
	upsertFavoriteErr  error
	deleteFavoriteErrs map[int64]error
	listFollowsErr     error
	upsertFollowErr    error
	deleteFollowErr    error

	deletedFavoriteIDs   []int64
	deletedFollowDomains []string
}

func newTestStore() *testStore {
	return &testStore{
		favorites:          make(map[int64]dirextalkdomain.FavoriteRecord),
		follows:            make(map[string]dirextalkdomain.FollowRecord),
		deleteFavoriteErrs: make(map[int64]error),
	}
}

func (s *testStore) UpsertFavorite(_ context.Context, favorite dirextalkdomain.FavoriteRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upsertFavoriteErr != nil {
		return s.upsertFavoriteErr
	}
	s.favorites[favorite.ID] = favorite
	return nil
}

func (s *testStore) FindFavoriteByEvent(_ context.Context, eventID, roomID string) (dirextalkdomain.FavoriteRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.findFavoriteErr != nil {
		return dirextalkdomain.FavoriteRecord{}, false, s.findFavoriteErr
	}
	for _, favorite := range s.favorites {
		if favorite.EventID != eventID {
			continue
		}
		if roomID != "" && favorite.RoomID != "" && favorite.RoomID != roomID {
			continue
		}
		return favorite, true, nil
	}
	return dirextalkdomain.FavoriteRecord{}, false, nil
}

func (s *testStore) ListFavorites(_ context.Context, messageType string) ([]dirextalkdomain.FavoriteRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listFavoritesErr != nil {
		return nil, s.listFavoritesErr
	}
	favorites := make([]dirextalkdomain.FavoriteRecord, 0, len(s.favorites))
	for _, favorite := range s.favorites {
		if messageType == "" || favorite.MessageType == messageType {
			favorites = append(favorites, favorite)
		}
	}
	sort.Slice(favorites, func(i, j int) bool { return favorites[i].ID < favorites[j].ID })
	return favorites, nil
}

func (s *testStore) DeleteFavorite(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedFavoriteIDs = append(s.deletedFavoriteIDs, id)
	if err := s.deleteFavoriteErrs[id]; err != nil {
		return err
	}
	delete(s.favorites, id)
	return nil
}

func (s *testStore) UpsertFollow(_ context.Context, follow dirextalkdomain.FollowRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.upsertFollowErr != nil {
		return s.upsertFollowErr
	}
	s.follows[follow.Domain] = follow
	return nil
}

func (s *testStore) ListFollows(context.Context) ([]dirextalkdomain.FollowRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listFollowsErr != nil {
		return nil, s.listFollowsErr
	}
	follows := make([]dirextalkdomain.FollowRecord, 0, len(s.follows))
	for _, follow := range s.follows {
		follows = append(follows, follow)
	}
	sort.Slice(follows, func(i, j int) bool { return follows[i].Domain < follows[j].Domain })
	return follows, nil
}

func (s *testStore) DeleteFollow(_ context.Context, domain string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedFollowDomains = append(s.deletedFollowDomains, domain)
	if s.deleteFollowErr != nil {
		return s.deleteFollowErr
	}
	delete(s.follows, domain)
	return nil
}

func TestHandlersOwnExactSocialActionsAndFollowContract(t *testing.T) {
	fixed := time.Date(2026, 7, 12, 3, 4, 5, 600, time.FixedZone("UTC+8", 8*60*60))
	store := newTestStore()
	module := New(store, Config{Now: func() time.Time { return fixed }})
	handlers := module.Handlers()
	wantNames := []string{
		"favorites.add", "favorites.delete", "favorites.delete_batch", "favorites.list",
		"follows.add", "follows.list", "follows.remove",
	}
	gotNames := make([]string, 0, len(handlers))
	for name, handler := range handlers {
		if handler == nil {
			t.Fatalf("handler %q is nil", name)
		}
		gotNames = append(gotNames, name)
	}
	sort.Strings(gotNames)
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("Handlers() names = %v, want %v", gotNames, wantNames)
	}

	if result, apiErr := handlers["follows.add"](context.Background(), map[string]any{"domain": "  "}); result != nil || apiErr == nil || apiErr.Status != 400 || apiErr.Error != "domain is required" {
		t.Fatalf("blank follows.add = (%#v, %#v)", result, apiErr)
	}
	result, apiErr := handlers["follows.add"](context.Background(), map[string]any{"domain": " remote.example "})
	if apiErr != nil {
		t.Fatalf("follows.add error = %#v", apiErr)
	}
	follow := result.(dirextalkdomain.FollowRecord)
	if follow.Domain != "remote.example" || follow.CreatedAt != fixed.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("follows.add result = %#v", follow)
	}

	result, apiErr = handlers["follows.list"](context.Background(), nil)
	if apiErr != nil {
		t.Fatalf("follows.list error = %#v", apiErr)
	}
	listed := result.(map[string]any)["follows"].([]dirextalkdomain.FollowRecord)
	if len(listed) != 1 || listed[0] != follow {
		t.Fatalf("follows.list = %#v", result)
	}

	result, apiErr = handlers["follows.remove"](context.Background(), nil)
	if apiErr != nil || !reflect.DeepEqual(result, map[string]any{"status": "ok"}) {
		t.Fatalf("empty follows.remove = (%#v, %#v)", result, apiErr)
	}
	if !reflect.DeepEqual(store.deletedFollowDomains, []string{""}) {
		t.Fatalf("DeleteFollow calls = %v, want empty domain", store.deletedFollowDomains)
	}
}

func TestFollowStoreFailuresReturnStableInternalErrors(t *testing.T) {
	tests := []struct {
		name   string
		action string
		setup  func(*testStore)
		params map[string]any
	}{
		{name: "add", action: "follows.add", params: map[string]any{"domain": "remote.example"}, setup: func(s *testStore) { s.upsertFollowErr = errors.New("write failed") }},
		{name: "list", action: "follows.list", setup: func(s *testStore) { s.listFollowsErr = errors.New("read failed") }},
		{name: "remove", action: "follows.remove", setup: func(s *testStore) { s.deleteFollowErr = errors.New("delete failed") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore()
			tt.setup(store)
			result, apiErr := New(store, Config{}).Handlers()[tt.action](context.Background(), tt.params)
			if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error == "" {
				t.Fatalf("%s failure = (%#v, %#v)", tt.action, result, apiErr)
			}
		})
	}
}

func assertInternalError(t *testing.T, result any, apiErr *actionbase.Error, message string) {
	t.Helper()
	if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: "+message {
		t.Fatalf("result/error = (%#v, %#v), want 500 %q", result, apiErr, "internal error: "+message)
	}
}
