package calls

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

type storeQuery struct {
	roomID     string
	activeOnly bool
}

type testStore struct {
	mu sync.Mutex

	calls       map[string]dirextalkdomain.CallRecord
	listQueries []storeQuery
	upserts     []dirextalkdomain.CallRecord
	listErr     error
	upsertErr   error
}

func newTestStore() *testStore {
	return &testStore{calls: make(map[string]dirextalkdomain.CallRecord)}
}

func (s *testStore) UpsertCall(_ context.Context, call dirextalkdomain.CallRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, call)
	if s.upsertErr != nil {
		return s.upsertErr
	}
	s.calls[call.CallID] = call
	return nil
}

func (s *testStore) ListCalls(_ context.Context, roomID string, activeOnly bool) ([]dirextalkdomain.CallRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listQueries = append(s.listQueries, storeQuery{roomID: roomID, activeOnly: activeOnly})
	if s.listErr != nil {
		return nil, s.listErr
	}
	calls := make([]dirextalkdomain.CallRecord, 0, len(s.calls))
	for _, call := range s.calls {
		if roomID != "" && call.RoomID != roomID {
			continue
		}
		if activeOnly && dirextalkdomain.TerminalCallState(call.State) {
			continue
		}
		calls = append(calls, call)
	}
	sort.Slice(calls, func(i, j int) bool { return calls[i].CallID < calls[j].CallID })
	return calls, nil
}

type eventRecorder struct {
	mu     sync.Mutex
	events []dirextalkdomain.Event
	err    error
}

func (r *eventRecorder) publish(_ context.Context, event dirextalkdomain.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return r.err
}

func TestHandlersOwnExactCallActionsAndCreateFields(t *testing.T) {
	fixed := time.Date(2026, 7, 12, 10, 11, 12, 13000000, time.FixedZone("UTC+8", 8*60*60))
	store := newTestStore()
	events := &eventRecorder{}
	module := New(store, Config{
		ServerName:   "example.com",
		OwnerMXID:    "@owner:example.com",
		Now:          func() time.Time { return fixed },
		NewCallID:    func() string { return "call_generated" },
		PublishEvent: events.publish,
	})
	handlers := module.Handlers()

	gotNames := make([]string, 0, len(handlers))
	for name, handler := range handlers {
		if handler == nil {
			t.Fatalf("handler %q is nil", name)
		}
		gotNames = append(gotNames, name)
	}
	sort.Strings(gotNames)
	wantNames := []string{"calls.active", "calls.create", "calls.event", "calls.get", "calls.incoming", "calls.list"}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("Handlers() names = %v, want %v", gotNames, wantNames)
	}

	result, apiErr := handlers["calls.create"](context.Background(), nil)
	if apiErr != nil {
		t.Fatalf("calls.create error = %#v", apiErr)
	}
	created := result.(dirextalkdomain.CallRecord)
	want := dirextalkdomain.CallRecord{
		CallID:        "call_generated",
		RoomID:        "!call:example.com",
		RoomType:      "direct",
		MediaType:     "voice",
		CreatedByMXID: "@owner:example.com",
		State:         "ringing",
		CreatedAt:     fixed.UTC().Format(time.RFC3339Nano),
	}
	if created != want {
		t.Fatalf("calls.create = %#v, want %#v", created, want)
	}
	if len(store.listQueries) != 1 || store.listQueries[0] != (storeQuery{}) {
		t.Fatalf("create ListCalls queries = %#v, want empty/all lookup", store.listQueries)
	}
	if len(store.upserts) != 1 || store.upserts[0] != want {
		t.Fatalf("create UpsertCall calls = %#v", store.upserts)
	}
	if len(events.events) != 1 {
		t.Fatalf("published events = %#v", events.events)
	}
	wantEvent := dirextalkdomain.Event{
		Type:    "call.changed",
		RoomID:  want.RoomID,
		Payload: map[string]any{"call": want},
	}
	if !reflect.DeepEqual(events.events[0], wantEvent) {
		t.Fatalf("published event = %#v, want %#v", events.events[0], wantEvent)
	}

	explicitResult, apiErr := handlers["calls.incoming"](context.Background(), map[string]any{
		"call_id":         " explicit ",
		"room_id":         " !room:example.com ",
		"event":           " dialing ",
		"media_type":      " video ",
		"created_by_mxid": " @alice:example.com ",
		"created_at":      "2026-07-11T20:00:00-04:00",
	})
	if apiErr != nil {
		t.Fatalf("calls.incoming error = %#v", apiErr)
	}
	explicit := explicitResult.(dirextalkdomain.CallRecord)
	if explicit.CallID != "explicit" || explicit.RoomID != "!room:example.com" || explicit.State != "dialing" ||
		explicit.MediaType != "video" || explicit.CreatedByMXID != "@alice:example.com" ||
		explicit.CreatedAt != "2026-07-12T00:00:00Z" {
		t.Fatalf("explicit calls.incoming = %#v", explicit)
	}
}

func TestGetListAndActiveUseStoreWithStableErrors(t *testing.T) {
	store := newTestStore()
	store.calls["active"] = dirextalkdomain.CallRecord{CallID: "active", RoomID: "!one:example.com", State: "connected"}
	store.calls["ended"] = dirextalkdomain.CallRecord{CallID: "ended", RoomID: "!two:example.com", State: "ended"}
	handlers := New(store, Config{}).Handlers()

	result, apiErr := handlers["calls.get"](context.Background(), nil)
	if result != nil || apiErr == nil || apiErr.Status != 400 || apiErr.Error != "call_id is required" {
		t.Fatalf("blank calls.get = (%#v, %#v)", result, apiErr)
	}
	result, apiErr = handlers["calls.get"](context.Background(), map[string]any{"call_id": " missing "})
	if result != nil || apiErr == nil || apiErr.Status != 404 || apiErr.Error != "call not found" {
		t.Fatalf("missing calls.get = (%#v, %#v)", result, apiErr)
	}
	result, apiErr = handlers["calls.get"](context.Background(), map[string]any{"call_id": " active "})
	if apiErr != nil || result != store.calls["active"] {
		t.Fatalf("existing calls.get = (%#v, %#v)", result, apiErr)
	}

	result, apiErr = handlers["calls.list"](context.Background(), map[string]any{"room_id": " !two:example.com "})
	if apiErr != nil {
		t.Fatalf("calls.list error = %#v", apiErr)
	}
	listed := result.(map[string]any)["calls"].([]dirextalkdomain.CallRecord)
	if len(listed) != 1 || listed[0].CallID != "ended" {
		t.Fatalf("calls.list = %#v", result)
	}
	result, apiErr = handlers["calls.active"](context.Background(), nil)
	if apiErr != nil {
		t.Fatalf("calls.active error = %#v", apiErr)
	}
	active := result.(map[string]any)["calls"].([]dirextalkdomain.CallRecord)
	if len(active) != 1 || active[0].CallID != "active" {
		t.Fatalf("calls.active = %#v", result)
	}
	gotQueries := store.listQueries[len(store.listQueries)-2:]
	wantQueries := []storeQuery{{roomID: "!two:example.com"}, {activeOnly: true}}
	if !reflect.DeepEqual(gotQueries, wantQueries) {
		t.Fatalf("list queries = %#v, want %#v", gotQueries, wantQueries)
	}

	store.listErr = errors.New("read failed")
	for _, name := range []string{"calls.list", "calls.active"} {
		result, apiErr = handlers[name](context.Background(), nil)
		if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: read failed" {
			t.Fatalf("%s store failure = (%#v, %#v)", name, result, apiErr)
		}
	}
}

func TestStoreAndPublisherFailuresPreserveCommitOrdering(t *testing.T) {
	t.Run("all lookup actions return ListCalls failure", func(t *testing.T) {
		for _, tt := range []struct {
			action string
			params map[string]any
		}{
			{action: "calls.create", params: map[string]any{"call_id": "new"}},
			{action: "calls.incoming", params: map[string]any{"call_id": "new"}},
			{action: "calls.get", params: map[string]any{"call_id": "existing"}},
			{action: "calls.event", params: map[string]any{"call_id": "existing", "event": "ended"}},
		} {
			store := newTestStore()
			store.listErr = errors.New("read failed")
			result, apiErr := New(store, Config{}).Handlers()[tt.action](context.Background(), tt.params)
			if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: read failed" {
				t.Fatalf("%s ListCalls failure = (%#v, %#v)", tt.action, result, apiErr)
			}
		}
	})

	t.Run("upsert failure does not publish", func(t *testing.T) {
		for _, tt := range []struct {
			action string
			params map[string]any
			seed   *dirextalkdomain.CallRecord
		}{
			{action: "calls.create", params: map[string]any{"call_id": "new"}},
			{
				action: "calls.event",
				params: map[string]any{"call_id": "existing", "event": "ended"},
				seed:   &dirextalkdomain.CallRecord{CallID: "existing", State: "ringing"},
			},
		} {
			store := newTestStore()
			if tt.seed != nil {
				store.calls[tt.seed.CallID] = *tt.seed
			}
			store.upsertErr = errors.New("write failed")
			events := &eventRecorder{}
			result, apiErr := New(store, Config{PublishEvent: events.publish}).Handlers()[tt.action](context.Background(), tt.params)
			if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: write failed" {
				t.Fatalf("%s UpsertCall failure = (%#v, %#v)", tt.action, result, apiErr)
			}
			if len(events.events) != 0 {
				t.Fatalf("%s published after failed UpsertCall: %#v", tt.action, events.events)
			}
		}
	})

	t.Run("publish failure leaves durable call committed", func(t *testing.T) {
		store := newTestStore()
		events := &eventRecorder{err: errors.New("publish failed")}
		result, apiErr := New(store, Config{
			NewCallID:    func() string { return "committed" },
			PublishEvent: events.publish,
		}).Handlers()["calls.create"](context.Background(), nil)
		if result != nil || apiErr == nil || apiErr.Status != 500 || apiErr.Error != "internal error: publish failed" {
			t.Fatalf("publish failure = (%#v, %#v)", result, apiErr)
		}
		stored, ok := store.calls["committed"]
		if !ok || stored.CallID != "committed" {
			t.Fatalf("call was not committed before publish failure: %#v", store.calls)
		}
		if len(events.events) != 1 || events.events[0].Payload["call"] != stored {
			t.Fatalf("publish attempt = %#v, stored %#v", events.events, stored)
		}
	})
}
