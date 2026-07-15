package legacygateway_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	legacygateway "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/legacygateway"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestModuleSerializesConcurrentExactReplayBeforeIngress(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	event := room.CreateAndInsert(t, owner, legacygateway.InvocationEventType, map[string]any{
		"request_id": "01890f00-0000-7000-8000-000000000110", "installation_id": "01890f00-0000-7000-8000-000000000111",
		"dispatch_mode": "single", "grant_version": 1, "input_event_id": "$input", "required_capabilities": []string{"chat.streaming"}, "idempotency_key": "once",
	})
	output := roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeNewRoomEvent, NewRoomEvent: &roomserverAPI.OutputNewRoomEvent{Event: event}}
	ingress := &blockingIngress{started: make(chan struct{}), release: make(chan struct{}), secondCall: make(chan struct{})}
	module, err := legacygateway.New(storage.NewMemoryStore(), ingress, legacygateway.Config{
		TenantID: "01890f00-0000-7000-8000-000000000101", ConversationID: "01890f00-0000-7000-8000-000000000102",
		Identity: func() legacygateway.Identity {
			return legacygateway.Identity{AgentRoomID: room.ID, OwnerMXID: owner.ID}
		},
		ResolveSender: func(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error) {
			return spec.NewUserID(owner.ID, true)
		},
		NewRequestEventID: func() (string, error) { return "01890f00-0000-7000-8000-000000000112", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	errorsOut := make(chan error, 2)
	go func() { errorsOut <- module.ProcessOutputEvent(context.Background(), output) }()
	<-ingress.started
	secondAttempted := make(chan struct{})
	go func() {
		close(secondAttempted)
		errorsOut <- module.ProcessOutputEvent(context.Background(), output)
	}()
	<-secondAttempted
	select {
	case <-ingress.secondCall:
		t.Fatal("concurrent replay reached ingress before first admission completed")
	case <-time.After(20 * time.Millisecond):
	}
	if got := ingress.callCount(); got != 1 {
		t.Fatalf("concurrent replay reached ingress %d times", got)
	}
	close(ingress.release)
	for range 2 {
		if err := <-errorsOut; err != nil {
			t.Fatal(err)
		}
	}
	if got := ingress.callCount(); got != 1 {
		t.Fatalf("accepted replay reached ingress %d times", got)
	}
}

type blockingIngress struct {
	mu               sync.Mutex
	calls            int
	started, release chan struct{}
	secondCall       chan struct{}
}

func (i *blockingIngress) CreateRun(_ context.Context, request legacygateway.CreateRunRequest) (legacygateway.CreateRunReceipt, error) {
	i.mu.Lock()
	i.calls++
	first := i.calls == 1
	if i.calls == 2 && i.secondCall != nil {
		close(i.secondCall)
	}
	i.mu.Unlock()
	if first {
		close(i.started)
		<-i.release
	}
	return legacygateway.CreateRunReceipt{RequestID: request.RequestID, RunID: "01890f00-0000-7000-8000-000000000120", Inserted: true, RoutingState: legacygateway.RoutingQueued}, nil
}
func (i *blockingIngress) callCount() int { i.mu.Lock(); defer i.mu.Unlock(); return i.calls }

func TestModuleRetriesWithFirstReservedRequestAndStopsAfterAcceptance(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	event := room.CreateAndInsert(t, owner, legacygateway.InvocationEventType, map[string]any{
		"request_id":             "01890f00-0000-7000-8000-000000000010",
		"installation_id":        "01890f00-0000-7000-8000-000000000011",
		"preferred_connector_id": nil,
		"dispatch_mode":          "single",
		"grant_version":          4,
		"input_event_id":         "$input-event",
		"required_capabilities":  []string{"chat.streaming"},
		"idempotency_key":        "opaque-once-key",
	})
	output := roomserverAPI.OutputEvent{
		Type:         roomserverAPI.OutputTypeNewRoomEvent,
		NewRoomEvent: &roomserverAPI.OutputNewRoomEvent{Event: event},
	}

	ingress := &retryingIngress{failuresRemaining: 1}
	requestEventIDs := []string{
		"01890f00-0000-7000-8000-000000000012",
		"01890f00-0000-7000-8000-000000000013",
		"01890f00-0000-7000-8000-000000000014",
	}
	nextRequestEventID := 0
	resolvedOwner, err := spec.NewUserID("@resolved-owner:example.test", true)
	if err != nil {
		t.Fatal(err)
	}
	module, err := legacygateway.New(storage.NewMemoryStore(), ingress, legacygateway.Config{
		TenantID:       "01890f00-0000-7000-8000-000000000001",
		ConversationID: "01890f00-0000-7000-8000-000000000002",
		Identity: func() legacygateway.Identity {
			return legacygateway.Identity{AgentRoomID: room.ID, OwnerMXID: resolvedOwner.String()}
		},
		ResolveSender: func(_ context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			if roomID.String() != room.ID {
				t.Fatalf("resolved sender in room %q, want %q", roomID.String(), room.ID)
			}
			if string(senderID) != owner.ID {
				t.Fatalf("resolved sender %q, want event sender %q", senderID, owner.ID)
			}
			return resolvedOwner, nil
		},
		Now: func() time.Time { return time.Date(2026, 7, 14, 4, 0, 0, 0, time.UTC) },
		NewRequestEventID: func() (string, error) {
			id := requestEventIDs[nextRequestEventID]
			nextRequestEventID++
			return id, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := module.ProcessOutputEvent(context.Background(), output); err == nil {
		t.Fatal("expected transient ingress failure to request a JetStream retry")
	}
	if err := module.ProcessOutputEvent(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	if err := module.ProcessOutputEvent(context.Background(), output); err != nil {
		t.Fatal(err)
	}

	if len(ingress.requests) != 2 {
		t.Fatalf("expected one retry and no call after acceptance, got %d calls", len(ingress.requests))
	}
	for _, request := range ingress.requests {
		if request.RequestEventID != requestEventIDs[0] {
			t.Fatalf("retry did not use first durable request_event_id: %#v", ingress.requests)
		}
	}
}

func TestModuleRecoversWhenCreateRunResponseIsLost(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	event := room.CreateAndInsert(t, owner, legacygateway.InvocationEventType, map[string]any{
		"request_id": "01890f00-0000-7000-8000-000000000210", "installation_id": "01890f00-0000-7000-8000-000000000211",
		"dispatch_mode": "single", "grant_version": 1, "input_event_id": "$input", "required_capabilities": []string{"chat.streaming"}, "idempotency_key": "response-lost",
	})
	output := roomserverAPI.OutputEvent{Type: roomserverAPI.OutputTypeNewRoomEvent, NewRoomEvent: &roomserverAPI.OutputNewRoomEvent{Event: event}}
	ingress := &responseLossIngress{}
	module, err := legacygateway.New(storage.NewMemoryStore(), ingress, legacygateway.Config{
		TenantID: "01890f00-0000-7000-8000-000000000201", ConversationID: "01890f00-0000-7000-8000-000000000202",
		Identity: func() legacygateway.Identity {
			return legacygateway.Identity{AgentRoomID: room.ID, OwnerMXID: owner.ID}
		},
		ResolveSender: func(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error) {
			return spec.NewUserID(owner.ID, true)
		},
		NewRequestEventID: func() (string, error) { return "01890f00-0000-7000-8000-000000000212", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := module.ProcessOutputEvent(context.Background(), output); err == nil {
		t.Fatal("lost CreateRun response must request redelivery")
	}
	if err := module.ProcessOutputEvent(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	if err := module.ProcessOutputEvent(context.Background(), output); err != nil {
		t.Fatal(err)
	}
	if len(ingress.requests) != 2 {
		t.Fatalf("response-loss recovery reached ingress %d times", len(ingress.requests))
	}
	if !reflect.DeepEqual(ingress.requests[0], ingress.requests[1]) {
		t.Fatalf("response-loss retry changed CreateRun request:\nfirst:  %#v\nsecond: %#v", ingress.requests[0], ingress.requests[1])
	}
}

type responseLossIngress struct {
	requests []legacygateway.CreateRunRequest
}

func (ingress *responseLossIngress) CreateRun(_ context.Context, request legacygateway.CreateRunRequest) (legacygateway.CreateRunReceipt, error) {
	ingress.requests = append(ingress.requests, request)
	if len(ingress.requests) == 1 {
		return legacygateway.CreateRunReceipt{}, errors.New("response lost after durable CreateRun")
	}
	return legacygateway.CreateRunReceipt{
		RequestID: request.RequestID, RunID: "01890f00-0000-7000-8000-000000000220", Inserted: false, RoutingState: legacygateway.RoutingQueued,
	}, nil
}

func TestModuleRetriesWhenSenderResolutionFails(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	event := room.CreateAndInsert(t, owner, legacygateway.InvocationEventType, map[string]any{})
	output := roomserverAPI.OutputEvent{
		Type:         roomserverAPI.OutputTypeNewRoomEvent,
		NewRoomEvent: &roomserverAPI.OutputNewRoomEvent{Event: event},
	}
	ingress := &retryingIngress{}
	module, err := legacygateway.New(storage.NewMemoryStore(), ingress, legacygateway.Config{
		TenantID:       "01890f00-0000-7000-8000-000000000001",
		ConversationID: "01890f00-0000-7000-8000-000000000002",
		Identity: func() legacygateway.Identity {
			return legacygateway.Identity{AgentRoomID: room.ID, OwnerMXID: owner.ID}
		},
		ResolveSender: func(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error) {
			return nil, errors.New("roomserver unavailable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := module.ProcessOutputEvent(context.Background(), output); err == nil {
		t.Fatal("expected sender resolution failure to request a JetStream retry")
	}
	if len(ingress.requests) != 0 {
		t.Fatalf("sender resolution failure reached Agent Control: %#v", ingress.requests)
	}
}

func TestModuleAcknowledgesResolvedNonOwnerSenderWithoutIngress(t *testing.T) {
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	event := room.CreateAndInsert(t, owner, legacygateway.InvocationEventType, map[string]any{})
	output := roomserverAPI.OutputEvent{
		Type:         roomserverAPI.OutputTypeNewRoomEvent,
		NewRoomEvent: &roomserverAPI.OutputNewRoomEvent{Event: event},
	}
	nonOwner, err := spec.NewUserID("@not-owner:example.test", true)
	if err != nil {
		t.Fatal(err)
	}
	ingress := &retryingIngress{}
	module, err := legacygateway.New(storage.NewMemoryStore(), ingress, legacygateway.Config{
		TenantID:       "01890f00-0000-7000-8000-000000000001",
		ConversationID: "01890f00-0000-7000-8000-000000000002",
		Identity: func() legacygateway.Identity {
			return legacygateway.Identity{AgentRoomID: room.ID, OwnerMXID: owner.ID}
		},
		ResolveSender: func(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error) {
			return nonOwner, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := module.ProcessOutputEvent(context.Background(), output); err != nil {
		t.Fatalf("non-owner event should be acknowledged and ignored: %v", err)
	}
	if len(ingress.requests) != 0 {
		t.Fatalf("non-owner sender reached Agent Control: %#v", ingress.requests)
	}
}

type retryingIngress struct {
	failuresRemaining int
	requests          []legacygateway.CreateRunRequest
}

func (ingress *retryingIngress) CreateRun(
	_ context.Context,
	request legacygateway.CreateRunRequest,
) (legacygateway.CreateRunReceipt, error) {
	ingress.requests = append(ingress.requests, request)
	if ingress.failuresRemaining > 0 {
		ingress.failuresRemaining--
		return legacygateway.CreateRunReceipt{}, errors.New("temporary ingress failure")
	}
	if request.RequestID == "" {
		return legacygateway.CreateRunReceipt{}, errors.New("missing request id")
	}
	return legacygateway.CreateRunReceipt{
		RequestID:    request.RequestID,
		RunID:        "01890f00-0000-7000-8000-000000000020",
		Inserted:     true,
		RoutingState: legacygateway.RoutingQueued,
	}, nil
}
