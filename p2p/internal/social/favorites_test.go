package social

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

func TestFavoriteAddPreservesIdempotencyCreatedAtAndEmptyEventBehavior(t *testing.T) {
	current := time.Date(2026, 7, 12, 1, 2, 3, 4000000, time.UTC)
	store := newTestStore()
	handlers := New(store, Config{Now: func() time.Time { return current }}).Handlers()

	firstResult, apiErr := handlers["favorites.add"](context.Background(), map[string]any{
		"event_id":         " $event ",
		"room_id":          " !room:example.com ",
		"sender_id":        " @alice:example.com ",
		"sender_name":      " Alice ",
		"content":          " first ",
		"message_type":     " text ",
		"origin_server_ts": float64(123),
	})
	if apiErr != nil {
		t.Fatalf("first favorites.add error = %#v", apiErr)
	}
	first := firstResult.(dirextalkdomain.FavoriteRecord)
	if first.ID != current.UnixMilli() || first.EventID != "$event" || first.RoomID != "!room:example.com" ||
		first.SenderID != "@alice:example.com" || first.SenderName != "Alice" || first.Content != "first" ||
		first.MessageType != "text" || first.OriginServerTS != 123 || first.CreatedAt != current.Format(time.RFC3339Nano) {
		t.Fatalf("first favorite = %#v", first)
	}

	current = current.Add(time.Minute)
	secondResult, apiErr := handlers["favorites.add"](context.Background(), map[string]any{
		"event_id": "$event", "room_id": "!room:example.com", "content": "updated", "message_type": "text",
	})
	if apiErr != nil {
		t.Fatalf("second favorites.add error = %#v", apiErr)
	}
	second := secondResult.(dirextalkdomain.FavoriteRecord)
	if second.ID != first.ID || second.CreatedAt != first.CreatedAt || second.Content != "updated" {
		t.Fatalf("idempotent favorite = %#v, first %#v", second, first)
	}

	otherRoomResult, apiErr := handlers["favorites.add"](context.Background(), map[string]any{
		"event_id": "$event", "room_id": "!other:example.com", "content": "other",
	})
	if apiErr != nil {
		t.Fatalf("other-room favorites.add error = %#v", apiErr)
	}
	otherRoom := otherRoomResult.(dirextalkdomain.FavoriteRecord)
	if otherRoom.ID == first.ID {
		t.Fatalf("different room reused favorite id: %#v", otherRoom)
	}

	emptyOne, apiErr := handlers["favorites.add"](context.Background(), map[string]any{"room_id": "!room:example.com", "content": "one"})
	if apiErr != nil {
		t.Fatalf("empty-event first add error = %#v", apiErr)
	}
	emptyTwo, apiErr := handlers["favorites.add"](context.Background(), map[string]any{"room_id": "!room:example.com", "content": "two"})
	if apiErr != nil {
		t.Fatalf("empty-event second add error = %#v", apiErr)
	}
	if emptyOne.(dirextalkdomain.FavoriteRecord).ID == emptyTwo.(dirextalkdomain.FavoriteRecord).ID {
		t.Fatalf("empty event unexpectedly idempotent: %#v %#v", emptyOne, emptyTwo)
	}
}

func TestFavoriteIDsAreMonotonicForConcurrentSameMillisecondAdds(t *testing.T) {
	fixed := time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC)
	module := New(newTestStore(), Config{Now: func() time.Time { return fixed }})
	handler := module.Handlers()["favorites.add"]

	const count = 64
	ids := make(chan int64, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, apiErr := handler(context.Background(), map[string]any{"content": "same millisecond"})
			if apiErr != nil {
				errs <- errors.New(apiErr.Error)
				return
			}
			ids <- result.(dirextalkdomain.FavoriteRecord).ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent favorites.add error = %v", err)
	}
	seen := make(map[int64]struct{}, count)
	for id := range ids {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate favorite id %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("unique IDs = %d, want %d", len(seen), count)
	}
	for offset := int64(0); offset < count; offset++ {
		if _, ok := seen[fixed.UnixMilli()+offset]; !ok {
			t.Fatalf("missing monotonic favorite id %d in %v", fixed.UnixMilli()+offset, seen)
		}
	}
}

func TestFavoriteIDInitializationPreservesStoredRowsAcrossRestartAndClockRollback(t *testing.T) {
	now := time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC)
	existingID := now.Add(time.Hour).UnixMilli()
	store := newTestStore()
	store.favorites[existingID] = dirextalkdomain.FavoriteRecord{
		ID:      existingID,
		EventID: "$existing",
		Content: "preserve me",
	}

	result, apiErr := New(store, Config{Now: func() time.Time { return now }}).
		Handlers()["favorites.add"](context.Background(), map[string]any{"event_id": "$new"})
	if apiErr != nil {
		t.Fatalf("favorites.add after restart error = %#v", apiErr)
	}
	created := result.(dirextalkdomain.FavoriteRecord)
	if created.ID != existingID+1 {
		t.Fatalf("new favorite ID = %d, want %d", created.ID, existingID+1)
	}
	if preserved := store.favorites[existingID]; preserved.EventID != "$existing" || preserved.Content != "preserve me" {
		t.Fatalf("stored favorite was overwritten: %#v", preserved)
	}
	if stored := store.favorites[created.ID]; stored.EventID != "$new" {
		t.Fatalf("new favorite was not stored separately: %#v", stored)
	}
}

func TestFavoriteListUsesStoreAndReturnsReadFailure(t *testing.T) {
	store := newTestStore()
	store.favorites[1] = dirextalkdomain.FavoriteRecord{ID: 1, MessageType: "text"}
	store.favorites[2] = dirextalkdomain.FavoriteRecord{ID: 2, MessageType: "image"}
	module := New(store, Config{})

	favorites, err := module.ListFavorites(context.Background(), "text")
	if err != nil || len(favorites) != 1 || favorites[0].ID != 1 {
		t.Fatalf("ListFavorites(text) = (%#v, %v)", favorites, err)
	}
	result, apiErr := module.Handlers()["favorites.list"](context.Background(), map[string]any{"message_type": " text "})
	if apiErr != nil {
		t.Fatalf("favorites.list error = %#v", apiErr)
	}
	listed := result.(map[string]any)["favorites"].([]dirextalkdomain.FavoriteRecord)
	if len(listed) != 1 || listed[0].ID != 1 {
		t.Fatalf("favorites.list = %#v", result)
	}

	store.listFavoritesErr = errors.New("read failed")
	if _, err := module.ListFavorites(context.Background(), ""); err == nil || err.Error() != "read failed" {
		t.Fatalf("ListFavorites failure = %v", err)
	}
	result, apiErr = module.Handlers()["favorites.list"](context.Background(), nil)
	assertInternalError(t, result, apiErr, "read failed")
}

func TestFavoriteDeleteAndBatchContracts(t *testing.T) {
	t.Run("single delete permits zero", func(t *testing.T) {
		store := newTestStore()
		result, apiErr := New(store, Config{}).Handlers()["favorites.delete"](context.Background(), map[string]any{"id": float64(0)})
		if apiErr != nil || !reflect.DeepEqual(result, map[string]any{"status": "ok"}) || !reflect.DeepEqual(store.deletedFavoriteIDs, []int64{0}) {
			t.Fatalf("favorites.delete(0) = (%#v, %#v), calls=%v", result, apiErr, store.deletedFavoriteIDs)
		}
	})

	t.Run("batch rejects empty and more than 500", func(t *testing.T) {
		store := newTestStore()
		handler := New(store, Config{}).Handlers()["favorites.delete_batch"]
		result, apiErr := handler(context.Background(), nil)
		if result != nil || apiErr == nil || apiErr.Status != 400 || apiErr.Error != "ids is required" {
			t.Fatalf("empty batch = (%#v, %#v)", result, apiErr)
		}
		ids := make([]int64, 501)
		for i := range ids {
			ids[i] = int64(i + 1)
		}
		result, apiErr = handler(context.Background(), map[string]any{"ids": ids})
		if result != nil || apiErr == nil || apiErr.Status != 400 || apiErr.Error != "ids is too large" {
			t.Fatalf("501 batch = (%#v, %#v)", result, apiErr)
		}
		if len(store.deletedFavoriteIDs) != 0 {
			t.Fatalf("invalid batch deleted IDs %v", store.deletedFavoriteIDs)
		}
	})

	t.Run("batch stops after partial delete failure", func(t *testing.T) {
		store := newTestStore()
		for _, id := range []int64{1, 2, 3} {
			store.favorites[id] = dirextalkdomain.FavoriteRecord{ID: id}
		}
		store.deleteFavoriteErrs[2] = errors.New("delete failed")
		result, apiErr := New(store, Config{}).Handlers()["favorites.delete_batch"](context.Background(), map[string]any{"ids": []int64{1, 2, 3}})
		assertInternalError(t, result, apiErr, "delete failed")
		if !reflect.DeepEqual(store.deletedFavoriteIDs, []int64{1, 2}) {
			t.Fatalf("DeleteFavorite calls = %v, want [1 2]", store.deletedFavoriteIDs)
		}
		if _, exists := store.favorites[1]; exists {
			t.Fatal("first favorite was not deleted before failure")
		}
		if _, exists := store.favorites[3]; !exists {
			t.Fatal("third favorite was deleted after failure")
		}
	})
}

func TestFavoriteWriteFailuresReturnStableInternalErrors(t *testing.T) {
	t.Run("find", func(t *testing.T) {
		store := newTestStore()
		store.findFavoriteErr = errors.New("find failed")
		result, apiErr := New(store, Config{}).Handlers()["favorites.add"](context.Background(), map[string]any{"event_id": "$event"})
		assertInternalError(t, result, apiErr, "find failed")
	})
	t.Run("upsert", func(t *testing.T) {
		store := newTestStore()
		store.upsertFavoriteErr = errors.New("write failed")
		result, apiErr := New(store, Config{}).Handlers()["favorites.add"](context.Background(), nil)
		assertInternalError(t, result, apiErr, "write failed")
	})
	t.Run("initialize IDs", func(t *testing.T) {
		store := newTestStore()
		store.listFavoritesErr = errors.New("read failed")
		result, apiErr := New(store, Config{}).Handlers()["favorites.add"](context.Background(), nil)
		assertInternalError(t, result, apiErr, "read failed")
	})
	t.Run("delete", func(t *testing.T) {
		store := newTestStore()
		store.deleteFavoriteErrs[7] = errors.New("delete failed")
		result, apiErr := New(store, Config{}).Handlers()["favorites.delete"](context.Background(), map[string]any{"id": int64(7)})
		assertInternalError(t, result, apiErr, "delete failed")
	})
}
