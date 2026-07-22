package blocks

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

type testStore struct {
	mu sync.Mutex

	blocks  map[string]dirextalkdomain.BlockRecord
	upserts []dirextalkdomain.BlockRecord
	deletes [][2]string
	lists   int

	upsertErr error
	deleteErr error
	listErr   error
}

func newTestStore() *testStore {
	return &testStore{blocks: make(map[string]dirextalkdomain.BlockRecord)}
}

func (s *testStore) UpsertBlock(_ context.Context, block dirextalkdomain.BlockRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, block)
	if s.upsertErr != nil {
		return s.upsertErr
	}
	s.blocks[dirextalkdomain.BlockKey(block.TargetType, block.TargetID)] = block
	return nil
}

func (s *testStore) DeleteBlock(_ context.Context, targetType, targetID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes = append(s.deletes, [2]string{targetType, targetID})
	if s.deleteErr != nil {
		return false, s.deleteErr
	}
	key := dirextalkdomain.BlockKey(targetType, targetID)
	_, removed := s.blocks[key]
	delete(s.blocks, key)
	return removed, nil
}

func (s *testStore) ListBlocks(context.Context) ([]dirextalkdomain.BlockRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	if s.listErr != nil {
		return nil, s.listErr
	}
	blocks := make([]dirextalkdomain.BlockRecord, 0, len(s.blocks))
	for _, block := range s.blocks {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].TargetID < blocks[j].TargetID })
	return blocks, nil
}

func TestHandlersOwnExactBlockActionsAndContactSnapshots(t *testing.T) {
	fixed := time.Date(2026, 7, 12, 12, 34, 56, 0, time.FixedZone("UTC+8", 8*60*60))
	store := newTestStore()
	lookupCalls := []string{}
	module := New(store, Config{
		Now: func() time.Time { return fixed },
		LookupContact: func(_ context.Context, peerMXID string) (dirextalkdomain.ContactRecord, bool, error) {
			lookupCalls = append(lookupCalls, peerMXID)
			return dirextalkdomain.ContactRecord{
				PeerMXID:    peerMXID,
				RoomID:      "!direct:example.com",
				DisplayName: " Alice ",
				AvatarURL:   "mxc://example.com/alice",
			}, true, nil
		},
	})
	handlers := module.Handlers()
	names := make([]string, 0, len(handlers))
	for name, handler := range handlers {
		if handler == nil {
			t.Fatalf("handler %q is nil", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if want := []string{"blocks.add", "blocks.list", "blocks.remove"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("handler names = %v, want %v", names, want)
	}

	result, apiErr := handlers["blocks.add"](context.Background(), map[string]any{
		"type":     " friend ",
		"user_ids": []any{" @alice:remote.example "},
	})
	if apiErr != nil {
		t.Fatalf("blocks.add error = %#v", apiErr)
	}
	block := result.(map[string]any)["block"].(dirextalkdomain.BlockRecord)
	if result.(map[string]any)["status"] != "blocked" || block.TargetType != "contact" ||
		block.TargetID != "@alice:remote.example" || block.PeerMXID != "@alice:remote.example" ||
		block.RoomID != "!direct:example.com" || block.DisplayName != "Alice" ||
		block.AvatarURL != "mxc://example.com/alice" || block.CreatedAt != fixed.UTC().UnixMilli() {
		t.Fatalf("blocks.add result = %#v", result)
	}
	if !reflect.DeepEqual(lookupCalls, []string{"@alice:remote.example"}) {
		t.Fatalf("contact lookup calls = %v", lookupCalls)
	}

	store.blocks["group|ignored"] = dirextalkdomain.BlockRecord{TargetType: "group", TargetID: "ignored"}
	list, apiErr := handlers["blocks.list"](context.Background(), nil)
	if apiErr != nil {
		t.Fatalf("blocks.list error = %#v", apiErr)
	}
	listed := list.(map[string]any)
	contacts, ok := listed["contacts"].([]dirextalkdomain.BlockRecord)
	if !ok || len(contacts) != 1 || contacts[0] != block || len(listed) != 1 {
		t.Fatalf("blocks.list result = %#v", listed)
	}

	removed, apiErr := handlers["blocks.remove"](context.Background(), map[string]any{
		"target_type": "member",
		"target_id":   "@alice:remote.example",
	})
	if apiErr != nil {
		t.Fatalf("blocks.remove error = %#v", apiErr)
	}
	wantRemoved := map[string]any{
		"status": "ok", "removed": true, "target_type": "contact", "target_id": "@alice:remote.example",
	}
	if !reflect.DeepEqual(removed, wantRemoved) {
		t.Fatalf("blocks.remove result = %#v, want %#v", removed, wantRemoved)
	}
}

func TestBlockValidationFallbackAndLookupFailures(t *testing.T) {
	module := New(newTestStore(), Config{})
	for _, tt := range []struct {
		name    string
		params  map[string]any
		message string
	}{
		{name: "missing type", params: nil, message: "target_type is required"},
		{name: "invalid type", params: map[string]any{"target_type": "group"}, message: "target_type must be contact"},
		{name: "missing peer", params: map[string]any{"target_type": "contact"}, message: "peer_mxid is required"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, apiErr := module.Handlers()["blocks.add"](context.Background(), tt.params)
			if result != nil || apiErr == nil || apiErr.Status != 400 || apiErr.Error != tt.message {
				t.Fatalf("blocks.add = (%#v, %#v), want 400 %q", result, apiErr, tt.message)
			}
		})
	}

	result, apiErr := module.Handlers()["blocks.add"](context.Background(), map[string]any{
		"target_type": "contact", "target_id": "@bob:remote.example",
	})
	if apiErr != nil {
		t.Fatalf("target_id fallback error = %#v", apiErr)
	}
	block := result.(map[string]any)["block"].(dirextalkdomain.BlockRecord)
	if block.PeerMXID != "@bob:remote.example" || block.DisplayName != "bob" {
		t.Fatalf("target_id fallback block = %#v", block)
	}

	lookupFailure := New(newTestStore(), Config{LookupContact: func(context.Context, string) (dirextalkdomain.ContactRecord, bool, error) {
		return dirextalkdomain.ContactRecord{}, false, errors.New("lookup failed")
	}})
	result, apiErr = lookupFailure.Handlers()["blocks.add"](context.Background(), map[string]any{
		"target_type": "contact", "peer_mxid": "@alice:remote.example",
	})
	if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: lookup failed" {
		t.Fatalf("lookup failure = (%#v, %#v)", result, apiErr)
	}
}

func TestBlockStoreFailuresAndExistsMatching(t *testing.T) {
	store := newTestStore()
	module := New(store, Config{})
	handlers := module.Handlers()

	store.upsertErr = errors.New("write failed")
	result, apiErr := handlers["blocks.add"](context.Background(), map[string]any{
		"target_type": "contact", "peer_mxid": "@alice:remote.example",
	})
	if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: write failed" {
		t.Fatalf("upsert failure = (%#v, %#v)", result, apiErr)
	}
	store.upsertErr = nil
	store.blocks["contact|alice"] = dirextalkdomain.BlockRecord{
		TargetType: "contact", TargetID: "alice", RoomID: "!room:example.com",
		ChannelID: "channel", PeerMXID: "@alice:remote.example",
	}
	for _, identifier := range []string{"alice", "!room:example.com", "channel", "@alice:remote.example"} {
		exists, err := module.Exists(context.Background(), " user ", " ", identifier)
		if err != nil || !exists {
			t.Fatalf("Exists(%q) = (%t, %v)", identifier, exists, err)
		}
	}
	beforeLists := store.lists
	if exists, err := module.Exists(context.Background(), "group", "alice"); err != nil || exists || store.lists != beforeLists {
		t.Fatalf("invalid target Exists = (%t, %v), lists=%d/%d", exists, err, store.lists, beforeLists)
	}
	if exists, err := module.Exists(context.Background(), "contact", " "); err != nil || exists || store.lists != beforeLists {
		t.Fatalf("empty identifiers Exists = (%t, %v), lists=%d/%d", exists, err, store.lists, beforeLists)
	}

	store.listErr = errors.New("read failed")
	result, apiErr = handlers["blocks.list"](context.Background(), nil)
	if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: read failed" {
		t.Fatalf("list failure = (%#v, %#v)", result, apiErr)
	}
	if exists, err := module.Exists(context.Background(), "contact", "alice"); err == nil || exists || err.Error() != "read failed" {
		t.Fatalf("Exists list failure = (%t, %v)", exists, err)
	}
	store.listErr = nil
	store.deleteErr = errors.New("delete failed")
	result, apiErr = handlers["blocks.remove"](context.Background(), map[string]any{
		"target_type": "contact", "peer_mxid": "@alice:remote.example",
	})
	if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: delete failed" {
		t.Fatalf("delete failure = (%#v, %#v)", result, apiErr)
	}
}

func TestExistsInRoomRequiresExactPeerAndRoomPair(t *testing.T) {
	store := newTestStore()
	module := New(store, Config{})
	store.blocks["contact|alice"] = dirextalkdomain.BlockRecord{
		TargetType: "contact", TargetID: "@alice:remote.example",
		PeerMXID: "@alice:remote.example", RoomID: "!direct-a:example.com",
	}
	if blocked, err := module.ExistsInRoom(context.Background(), "contact", "@alice:remote.example", "!direct-a:example.com"); err != nil || !blocked {
		t.Fatalf("exact block lookup = (%t, %v), want true", blocked, err)
	}
	if blocked, err := module.ExistsInRoom(context.Background(), "contact", "@alice:remote.example", "!direct-b:example.com"); err != nil || blocked {
		t.Fatalf("different room block lookup = (%t, %v), want false", blocked, err)
	}
}
