package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	operationsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/operations"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestGroupJoinReplayReturnsEquivalentSuccess(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com",
		"name":    "Group",
	})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@alice:remote.example",
	})
	params := map[string]any{
		"room_id":         group.RoomID,
		"user_id":         "@alice:remote.example",
		"invite_event_id": "$invite",
		"direct_room_id":  "!direct:remote.example",
	}

	first := mustHandle[map[string]any](t, service, "groups.join", cloneParams(params))
	second, apiErr := service.Handle(context.Background(), "groups.join", cloneParams(params))
	if apiErr != nil {
		t.Fatalf("groups.join replay failed: %#v", apiErr)
	}
	replayed := second.(map[string]any)
	if first["status"] != "ok" || replayed["status"] != "ok" || first["room_id"] != replayed["room_id"] {
		t.Fatalf("group join responses are not equivalent: first=%#v replay=%#v", first, replayed)
	}
}

func TestPublicOperationIDCannotUseInternalWorkflowNamespace(t *testing.T) {
	for _, operationID := range []any{"_workflow_reserved", 123, map[string]any{"id": "op"}, ""} {
		service := NewService(Config{ServerName: "example.com"})
		result, apiErr := service.Handle(context.Background(), "contacts.request", map[string]any{
			"mxid": "@alice:remote.example", "operation_id": operationID,
		})
		if result != nil || apiErr == nil || apiErr.Status != http.StatusBadRequest || apiErr.Code != actionbase.OperationIDInvalidCode {
			t.Fatalf("invalid public operation ID %#v was accepted: result=%#v err=%#v", operationID, result, apiErr)
		}
	}
}

func TestGroupCardEventIDDoesNotOverrideMatrixInviteGeneration(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com", "name": "Group",
	})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example",
	})
	member, found, err := service.lookupMember(context.Background(), group.RoomID, "@alice:remote.example")
	if err != nil || !found {
		t.Fatalf("group invite projection = (%#v, %v, %v)", member, found, err)
	}
	member.RequestID = "$matrix-member-invite"
	if err := service.saveMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"room_id": group.RoomID, "user_id": member.UserID,
		"invite_event_id": "$direct-room-card-event", "direct_room_id": "!direct:remote.example",
	}
	first := mustHandle[map[string]any](t, service, "groups.join", cloneParams(params))
	second := mustHandle[map[string]any](t, service, "groups.join", cloneParams(params))
	if first["status"] != "ok" || second["status"] != "ok" ||
		trimString(first["operation_id"]) == "" || first["operation_id"] != second["operation_id"] {
		t.Fatalf("group card replay did not use persisted Matrix generation: first=%#v second=%#v", first, second)
	}
}

func TestGeneratedChannelRequestIDKeepsOperationStableAcrossReplay(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	params := map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:remote.example",
	}

	first := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(params))
	second := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(params))
	firstOperationID, secondOperationID := trimString(first["operation_id"]), trimString(second["operation_id"])
	if firstOperationID == "" || firstOperationID != secondOperationID {
		t.Fatalf("generated request generation changed across replay: first=%#v second=%#v", first, second)
	}
	member, ok, err := service.lookupMember(context.Background(), ch.RoomID, "@alice:remote.example")
	if err != nil || !ok || member.RequestID == "" {
		t.Fatalf("generated request generation was not persisted: ok=%v member=%#v err=%v", ok, member, err)
	}
}

func TestPublicOperationErrorUsesLocalCanonicalOperationID(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	params := map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:remote.example",
		"operation_id": "caller-operation", "request_id": "caller-request",
	}
	record, recordErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(params))
	if recordErr != nil {
		t.Fatal(recordErr)
	}
	result, apiErr := service.handleRecoverableOperation(
		context.Background(),
		"channels.public.join_request",
		cloneParams(params),
		func(context.Context, map[string]any) (any, *apiError) {
			return nil, &apiError{
				Status: http.StatusBadGateway, Error: "remote failure", Code: actionbase.OperationRecoveryCode,
				OperationID: "remote-selected-operation",
			}
		},
	)
	if result != nil || apiErr == nil || apiErr.OperationID != record.OperationID {
		t.Fatalf("remote operation ID overrode local durable identity: result=%#v err=%#v canonical=%#v", result, apiErr, record)
	}
}

func TestExplicitRetainedRoomRebuildBusyDoesNotReportJoinedBeforeGenerationPersists(t *testing.T) {
	const (
		roomID            = "!group:example.com"
		userID            = "@alice:remote.example"
		rebuildGeneration = "rebuild_group_busy"
	)
	transport := &recordingTransport{
		roomID: roomID,
		roomMembers: []memberRecord{{
			RoomID: roomID, UserID: userID, Membership: "join", Role: "member",
		}},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: roomID, UserID: userID, Membership: "join", Role: "member", RequestID: "old-generation",
	}); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"room_id": roomID, "user_id": userID, "rebuild_generation": rebuildGeneration,
	}
	record, recordErr := service.operationRecordFor(context.Background(), "groups.invite", cloneParams(params))
	if recordErr != nil {
		t.Fatal(recordErr)
	}
	assertReconciling := func(name string, value any, apiErr *apiError) {
		t.Helper()
		if apiErr != nil {
			t.Fatalf("%s returned error: %#v", name, apiErr)
		}
		result, ok := value.(map[string]any)
		if !ok || result["status"] != "joining" || result["error_code"] != actionbase.OperationRecoveryCode ||
			result["operation_id"] != record.OperationID || result["current_room_id"] != roomID {
			t.Fatalf("%s reported false rebuild success: %#v", name, value)
		}
	}
	busy, busyErr := service.operationWorkflowBusyResult(context.Background(), record)
	assertReconciling("workflow busy", busy, busyErr)
	inFlight, inFlightErr := service.operationInFlightResult(context.Background(), record)
	assertReconciling("operation in flight", inFlight, inFlightErr)
	if _, claimed, err := service.store.ClaimOperation(
		context.Background(), record, "in-flight-worker", operationLeaseDurationMillis,
	); err != nil || !claimed {
		t.Fatalf("claim in-flight rebuild: claimed=%v err=%v", claimed, err)
	}
	handleInFlight, handleInFlightErr := service.Handle(context.Background(), "groups.invite", cloneParams(params))
	assertReconciling("handle in-flight", handleInFlight, handleInFlightErr)
	workflow, ok := durableBusinessWorkflowRecord(record)
	if !ok {
		t.Fatal("explicit rebuild did not define durable workflow")
	}
	if _, claimed, err := service.store.ClaimOperation(
		context.Background(), workflow, "workflow-worker", operationLeaseDurationMillis,
	); err != nil || !claimed {
		t.Fatalf("claim rebuild workflow: claimed=%v err=%v", claimed, err)
	}
	busyCtx, cancelBusy := context.WithTimeout(context.Background(), 25*time.Millisecond)
	handleBusy, handleBusyErr := service.Handle(busyCtx, "groups.invite", cloneParams(params))
	cancelBusy()
	assertReconciling("handle workflow busy", handleBusy, handleBusyErr)
	if len(transport.inviteRequests) != 0 || len(transport.kicks) != 0 {
		t.Fatalf("busy rebuild performed side effects: invites=%#v kicks=%#v", transport.inviteRequests, transport.kicks)
	}

	current, found, err := service.lookupMember(context.Background(), roomID, userID)
	if err != nil || !found {
		t.Fatalf("lookup retained member: found=%v member=%#v err=%v", found, current, err)
	}
	current.RequestID = rebuildGeneration
	if err := service.saveMember(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	record.Status = operationStatusCompleted
	record.ResultJSON = `{"status":"ok","current_room_id":"!group:example.com"}`
	completed, completedErr := service.operationInFlightResult(context.Background(), record)
	if completedErr != nil || completed.(map[string]any)["status"] != "ok" {
		t.Fatalf("completed current rebuild cache was not reusable: result=%#v err=%#v", completed, completedErr)
	}
}

func TestBusyRecoveryResponsesPreserveLegacyHydration(t *testing.T) {
	ctx := context.Background()
	store := p2pstorage.NewMemoryStore()
	first := newService(Config{ServerName: "example.com"}, store, nil, portalState{}, false)
	replay := newService(Config{ServerName: "example.com"}, store, nil, portalState{}, false)

	t.Run("contact workflow busy", func(t *testing.T) {
		const (
			peerMXID = "@alice:remote.example"
			roomID   = "!direct:example.com"
		)
		contact := dirextalkdomain.ContactRecord{
			PeerMXID: peerMXID, RoomID: roomID, Status: "accepted", RequestID: "contact-generation",
		}
		if err := store.UpsertContact(ctx, contact); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertConversation(ctx, dirextalkdomain.ConversationRecord{
			MatrixRoomID: roomID, Kind: dirextalkdomain.ConversationKindDirect,
			Lifecycle: dirextalkdomain.ConversationLifecycleActive, PeerMXID: peerMXID,
		}); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{"peer_mxid": peerMXID, "room_id": roomID, "request_id": contact.RequestID}
		record, apiErr := first.operationRecordFor(ctx, "contacts.requests.accept", cloneParams(params))
		if apiErr != nil {
			t.Fatal(apiErr)
		}
		workflow, ok := durableBusinessWorkflowRecord(record)
		if !ok {
			t.Fatal("contact accept did not define a durable workflow")
		}
		if _, claimed, err := store.ClaimOperation(ctx, workflow, "contact-worker", operationLeaseDurationMillis); err != nil || !claimed {
			t.Fatalf("claim contact workflow: claimed=%v err=%v", claimed, err)
		}

		busyCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		value, handleErr := replay.Handle(busyCtx, "contacts.requests.accept", cloneParams(params))
		cancel()
		if handleErr != nil {
			t.Fatalf("busy contact replay returned error: %#v", handleErr)
		}
		view, ok := value.(contactRecord)
		if !ok || view.Operation == nil || view.Operation["action"] != "contacts.requests.accept" ||
			view.Operation["status"] != "accepted" || view.Conversation == nil ||
			view.Conversation.MatrixRoomID != roomID || view.OperationID != record.OperationID {
			t.Fatalf("busy contact replay lost legacy hydration: %#v", value)
		}
	})

	t.Run("group workflow busy", func(t *testing.T) {
		group := mustHandle[groupRecord](t, first, "groups.create", map[string]any{
			"room_id": "!group:example.com", "name": "Group",
		})
		member := memberRecord{
			RoomID: group.RoomID, UserID: "@bob:remote.example", Membership: "invite", Role: "member",
			RequestID: "group-generation",
		}
		if err := store.UpsertMember(ctx, member); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{
			"room_id": group.RoomID, "user_id": member.UserID, "request_id": member.RequestID,
		}
		record, apiErr := first.operationRecordFor(ctx, "groups.join", cloneParams(params))
		if apiErr != nil {
			t.Fatal(apiErr)
		}
		workflow, ok := durableBusinessWorkflowRecord(record)
		if !ok {
			t.Fatal("group join did not define a durable workflow")
		}
		if _, claimed, err := store.ClaimOperation(ctx, workflow, "group-worker", operationLeaseDurationMillis); err != nil || !claimed {
			t.Fatalf("claim group workflow: claimed=%v err=%v", claimed, err)
		}

		busyCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
		value, handleErr := replay.Handle(busyCtx, "groups.join", cloneParams(params))
		cancel()
		if handleErr != nil {
			t.Fatalf("busy group replay returned error: %#v", handleErr)
		}
		result, ok := value.(map[string]any)
		operation, operationOK := result["operation"].(map[string]any)
		conversation, conversationOK := result["conversation"].(dirextalkdomain.ConversationView)
		if !ok || !operationOK || operation["action"] != "groups.join" || operation["status"] != "joining" ||
			!conversationOK || conversation.MatrixRoomID != group.RoomID || result["operation_id"] != record.OperationID {
			t.Fatalf("busy group replay lost legacy hydration: %#v", value)
		}
	})

	t.Run("channel operation in flight", func(t *testing.T) {
		ch := mustHandle[channel](t, first, "channels.create", map[string]any{
			"channel_id": "busy-channel", "name": "Busy", "visibility": "public", "join_policy": "approval",
		})
		member := memberRecord{
			RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@carol:remote.example",
			Membership: "pending", Role: "member", RequestID: "channel-generation",
		}
		if err := store.UpsertMember(ctx, member); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{
			"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": member.UserID,
			"request_id": member.RequestID,
		}
		record, apiErr := first.operationRecordFor(ctx, "channels.join_request.approve", cloneParams(params))
		if apiErr != nil {
			t.Fatal(apiErr)
		}
		if _, claimed, err := store.ClaimOperation(ctx, record, "channel-worker", operationLeaseDurationMillis); err != nil || !claimed {
			t.Fatalf("claim channel operation: claimed=%v err=%v", claimed, err)
		}

		value, handleErr := replay.Handle(ctx, "channels.join_request.approve", cloneParams(params))
		if handleErr != nil {
			t.Fatalf("in-flight channel replay returned error: %#v", handleErr)
		}
		result, ok := value.(map[string]any)
		operation, operationOK := result["operation"].(map[string]any)
		conversation, conversationOK := result["conversation"].(dirextalkdomain.ConversationView)
		hydratedChannel, channelOK := result["channel"].(channel)
		if !ok || !operationOK || operation["action"] != "channels.join_request.approve" ||
			operation["status"] != "pending" || !conversationOK || conversation.MatrixRoomID != ch.RoomID ||
			!channelOK || hydratedChannel.ChannelID != ch.ChannelID || result["operation_id"] != record.OperationID {
			t.Fatalf("in-flight channel replay lost legacy hydration: %#v", value)
		}
	})

	t.Run("synthetic operation in flight", func(t *testing.T) {
		group := mustHandle[groupRecord](t, first, "groups.create", map[string]any{
			"room_id": "!synthetic-group:example.com", "name": "Synthetic",
		})
		params := map[string]any{
			"room_id": group.RoomID, "user_id": "@dave:remote.example", "request_id": "missing-generation",
		}
		record, apiErr := first.operationRecordFor(ctx, "groups.join", cloneParams(params))
		if apiErr != nil {
			t.Fatal(apiErr)
		}
		if _, claimed, err := store.ClaimOperation(ctx, record, "synthetic-worker", operationLeaseDurationMillis); err != nil || !claimed {
			t.Fatalf("claim synthetic operation: claimed=%v err=%v", claimed, err)
		}

		value, handleErr := replay.Handle(ctx, "groups.join", cloneParams(params))
		if handleErr != nil {
			t.Fatalf("synthetic in-flight replay returned error: %#v", handleErr)
		}
		result, ok := value.(map[string]any)
		operation, operationOK := result["operation"].(map[string]any)
		conversation, conversationOK := result["conversation"].(dirextalkdomain.ConversationView)
		if !ok || result["status"] != "joining" || !operationOK || operation["action"] != "groups.join" ||
			operation["status"] != "joining" || !conversationOK || conversation.MatrixRoomID != group.RoomID ||
			result["operation_id"] != record.OperationID {
			t.Fatalf("synthetic in-flight replay lost legacy hydration: %#v", value)
		}
	})
}

type blockingFirstMemberSaveStore struct {
	*p2pstorage.MemoryStore
	mu          sync.Mutex
	memberSaves int
	entered     chan struct{}
	release     chan struct{}
	secondSave  chan struct{}
}

func (s *blockingFirstMemberSaveStore) UpsertMember(ctx context.Context, member memberRecord) error {
	s.mu.Lock()
	s.memberSaves++
	call := s.memberSaves
	s.mu.Unlock()
	if err := s.MemoryStore.UpsertMember(ctx, member); err != nil {
		return err
	}
	switch call {
	case 1:
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	case 2:
		close(s.secondSave)
	}
	return nil
}

func (s *blockingFirstMemberSaveStore) memberSaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memberSaves
}

func TestInitialPublicJoinGenerationSerializesAcrossServiceInstances(t *testing.T) {
	store := &blockingFirstMemberSaveStore{
		MemoryStore: p2pstorage.NewMemoryStore(),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
		secondSave:  make(chan struct{}),
	}
	transport := &recordingTransport{roomID: "!channel:example.com"}
	firstService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	secondService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	roomChannel := channel{
		ChannelID: "channel_1", RoomID: "!channel:example.com", Name: "Channel",
		Visibility: "public", JoinPolicy: "approval", ChannelType: "chat",
	}
	if err := firstService.saveChannel(context.Background(), roomChannel); err != nil {
		t.Fatal(err)
	}
	baseParams := map[string]any{
		"room_id": roomChannel.RoomID, "channel_id": roomChannel.ChannelID,
		"user_id": "@alice:remote.example",
	}
	firstParams := cloneParams(baseParams)
	firstParams["operation_id"] = "op-public-join-a"
	firstParams["request_id"] = "caller-request-a"
	secondParams := cloneParams(baseParams)
	secondParams["operation_id"] = "op-public-join-b"
	secondParams["request_id"] = "caller-request-b"
	firstOperation, firstOperationErr := firstService.operationRecordFor(
		context.Background(), "channels.public.join_request", cloneParams(firstParams),
	)
	if firstOperationErr != nil {
		t.Fatal(firstOperationErr)
	}
	secondOperation, secondOperationErr := secondService.operationRecordFor(
		context.Background(), "channels.public.join_request", cloneParams(secondParams),
	)
	if secondOperationErr != nil {
		t.Fatal(secondOperationErr)
	}
	if firstOperation.RequestID == "" || firstOperation.RequestID != secondOperation.RequestID ||
		firstOperation.OperationID != secondOperation.OperationID {
		t.Fatalf("unauthenticated caller IDs selected different initial generations: first=%#v second=%#v", firstOperation, secondOperation)
	}

	type joinResult struct {
		value any
		err   *apiError
	}
	firstDone := make(chan joinResult, 1)
	go func() {
		value, apiErr := firstService.Handle(context.Background(), "channels.public.join_request", cloneParams(firstParams))
		firstDone <- joinResult{value: value, err: apiErr}
	}()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first public join request did not enter member persistence")
	}
	current, found, err := store.LookupMember(context.Background(), roomChannel.RoomID, "@alice:remote.example")
	if err != nil || !found || current.RequestID != firstOperation.RequestID {
		close(store.release)
		<-firstDone
		t.Fatalf("first request generation was not visible while persistence was blocked: found=%v member=%#v err=%v", found, current, err)
	}

	secondDone := make(chan joinResult, 1)
	go func() {
		value, apiErr := secondService.Handle(context.Background(), "channels.public.join_request", cloneParams(secondParams))
		secondDone <- joinResult{value: value, err: apiErr}
	}()
	crossedPersistence := false
	var earlySecond *joinResult
	select {
	case <-store.secondSave:
		crossedPersistence = true
	case result := <-secondDone:
		earlySecond = &result
	case <-time.After(100 * time.Millisecond):
	}
	close(store.release)
	first := <-firstDone
	var second joinResult
	if earlySecond != nil {
		second = *earlySecond
	} else {
		select {
		case second = <-secondDone:
		case <-time.After(5 * time.Second):
			t.Fatal("second public join request did not finish after the first workflow released")
		}
	}
	if crossedPersistence || earlySecond != nil {
		t.Fatalf("second request crossed the in-flight first-generation workflow: crossed_save=%v early_result=%#v", crossedPersistence, earlySecond)
	}
	if first.err != nil || second.err != nil {
		t.Fatalf("public join requests failed: first=(%#v, %#v) second=(%#v, %#v)", first.value, first.err, second.value, second.err)
	}
	firstResponse := first.value.(map[string]any)
	secondResponse := second.value.(map[string]any)
	firstMember, firstMemberOK := firstResponse["member"].(memberRecord)
	secondMember, secondMemberOK := secondResponse["member"].(memberRecord)
	if !firstMemberOK || !secondMemberOK || firstMember.RequestID != firstOperation.RequestID ||
		secondMember.RequestID != firstOperation.RequestID {
		t.Fatalf("responses did not converge on the first request generation: first=%#v second=%#v", firstResponse, secondResponse)
	}
	if trimString(firstResponse["operation_id"]) == "" || firstResponse["operation_id"] != secondResponse["operation_id"] {
		t.Fatalf("public caller IDs did not collapse onto the winning generation: first=%#v second=%#v", firstResponse, secondResponse)
	}
	for _, callerID := range []string{"op-public-join-a", "op-public-join-b"} {
		if operation, found, err := store.LookupOperation(context.Background(), callerID); err != nil || found {
			t.Fatalf("caller-selected public operation key was persisted: id=%s found=%v operation=%#v err=%v", callerID, found, operation, err)
		}
	}
	if operation, found, err := store.LookupOperation(context.Background(), firstOperation.OperationID); err != nil || !found ||
		operation.RequestID != firstOperation.RequestID {
		t.Fatalf("canonical public operation was not persisted once: found=%v operation=%#v err=%v", found, operation, err)
	}
	stored, found, err := store.LookupMember(context.Background(), roomChannel.RoomID, firstMember.UserID)
	if err != nil || !found || stored.RequestID != firstOperation.RequestID || stored.Membership != "pending" {
		t.Fatalf("store did not retain only the first generation: found=%v member=%#v err=%v", found, stored, err)
	}
	if store.memberSaveCount() != 1 || len(transport.stateEvents) != 1 {
		t.Fatalf("duplicate persistence or publication escaped serialization: saves=%d state_events=%#v", store.memberSaveCount(), transport.stateEvents)
	}
}

func TestPublicJoinWorkflowTimeoutKeepsCanonicalOperationID(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	base := map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:remote.example",
	}
	firstParams := cloneParams(base)
	firstParams["operation_id"] = "caller-operation-a"
	firstParams["request_id"] = "caller-request-a"
	secondParams := cloneParams(base)
	secondParams["operation_id"] = "caller-operation-b"
	secondParams["request_id"] = "caller-request-b"
	record, recordErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(firstParams))
	secondRecord, secondRecordErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(secondParams))
	if recordErr != nil || secondRecordErr != nil || record.OperationID != secondRecord.OperationID || record.RequestID != secondRecord.RequestID {
		t.Fatalf("caller IDs selected different workflow identity: first=%#v/%#v second=%#v/%#v", record, recordErr, secondRecord, secondRecordErr)
	}
	workflow, ok := durableBusinessWorkflowRecord(record)
	if !ok {
		t.Fatal("public join request did not define a durable workflow")
	}
	if _, claimed, err := service.store.ClaimOperation(context.Background(), workflow, "blocking-worker", operationLeaseDurationMillis); err != nil || !claimed {
		t.Fatalf("claim blocking workflow: claimed=%v err=%v", claimed, err)
	}
	call := func(params map[string]any) map[string]any {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		value, apiErr := service.Handle(ctx, "channels.public.join_request", cloneParams(params))
		if apiErr != nil {
			t.Fatalf("workflow-busy public join returned error: %#v", apiErr)
		}
		result, ok := value.(map[string]any)
		if !ok || result["status"] != "joining" {
			t.Fatalf("workflow-busy public join result = %#v", value)
		}
		return result
	}
	first := call(firstParams)
	second := call(secondParams)
	if first["operation_id"] != record.OperationID || second["operation_id"] != record.OperationID {
		t.Fatalf("workflow timeout exposed caller-specific operation IDs: first=%#v second=%#v canonical=%#v", first, second, record)
	}
	if operation, found, err := service.store.LookupOperation(context.Background(), record.OperationID); err != nil || found {
		t.Fatalf("workflow-busy response claimed an unexecuted business operation: found=%v operation=%#v err=%v", found, operation, err)
	}
}

func TestRoomOnlyPublicJoinIdentitySurvivesChannelProjectionBackfill(t *testing.T) {
	service := NewService(Config{ServerName: "local.example"})
	bootstrapService(t, service)
	roomID := "!public:remote.example"
	userID := service.OwnerMXID()
	params := map[string]any{"room_id": roomID, "user_id": userID}

	before, apiErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(params))
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if err := service.saveChannel(context.Background(), channel{
		ChannelID: "remote-channel", RoomID: roomID, Name: "Remote", Visibility: "public", JoinPolicy: "approval",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: roomID, ChannelID: "remote-channel", UserID: userID,
		Membership: "pending", Role: "member", RequestID: before.RequestID,
	}); err != nil {
		t.Fatal(err)
	}
	after, apiErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(params))
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if before.OperationID != after.OperationID || before.RequestID != after.RequestID {
		t.Fatalf("room-only identity changed after channel backfill: before=%#v after=%#v", before, after)
	}
}

func TestPublicJoinCallerIDsCannotChooseInitialOrTerminalGeneration(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	userID := "@alice:remote.example"
	operationA := map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID,
		"operation_id": "operation-a", "request_id": "caller-request-a",
	}
	operationB := cloneParams(operationA)
	operationB["operation_id"] = "operation-b"
	operationB["request_id"] = "caller-request-b"
	initialA, initialAErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(operationA))
	initialB, initialBErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(operationB))
	legacyParams := map[string]any{"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID}
	initialLegacy, initialLegacyErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(legacyParams))
	if initialAErr != nil || initialBErr != nil || initialA.RequestID == "" ||
		initialLegacyErr != nil || initialA.RequestID != initialB.RequestID || initialA.OperationID != initialB.OperationID ||
		initialA.RequestID != initialLegacy.RequestID || initialA.OperationID != initialLegacy.OperationID {
		t.Fatalf("caller IDs selected different initial generation: a=%#v/%#v b=%#v/%#v legacy=%#v/%#v", initialA, initialAErr, initialB, initialBErr, initialLegacy, initialLegacyErr)
	}
	firstA := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(operationA))
	firstB := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(operationB))
	memberA := firstA["member"].(memberRecord)
	memberB := firstB["member"].(memberRecord)
	if memberA.RequestID != initialA.RequestID || memberB.RequestID != initialA.RequestID ||
		firstA["operation_id"] != firstB["operation_id"] {
		t.Fatalf("active generation did not converge: first=%#v second=%#v", firstA, firstB)
	}
	mustHandle[map[string]any](t, service, "channels.join_request.reject", map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID, "request_id": memberA.RequestID,
	})
	nextA, nextAErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(operationA))
	nextB, nextBErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(operationB))
	nextLegacy, nextLegacyErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(legacyParams))
	if nextAErr != nil || nextBErr != nil || nextA.RequestID == memberA.RequestID ||
		nextLegacyErr != nil || nextA.RequestID != nextB.RequestID || nextA.OperationID != nextB.OperationID ||
		nextA.RequestID != nextLegacy.RequestID || nextA.OperationID != nextLegacy.OperationID {
		t.Fatalf("caller IDs selected different terminal-next generation: a=%#v/%#v b=%#v/%#v legacy=%#v/%#v", nextA, nextAErr, nextB, nextBErr, nextLegacy, nextLegacyErr)
	}
	nextFirst := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(operationA))
	nextReplay := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(operationB))
	nextMember := nextFirst["member"].(memberRecord)
	current, ok, err := service.lookupMember(context.Background(), ch.RoomID, userID)
	if err != nil || !ok || current.RequestID != nextA.RequestID || current.Membership != "pending" ||
		nextMember.RequestID != nextA.RequestID || nextReplay["operation_id"] != nextFirst["operation_id"] {
		t.Fatalf("terminal-next generation did not converge: first=%#v replay=%#v ok=%v current=%#v err=%v", nextFirst, nextReplay, ok, current, err)
	}
}

func TestLegacyPublicJoinResultBindsServerCanonicalGeneration(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	member := memberRecord{
		RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: service.OwnerMXID(),
		Membership: "pending", Role: "member",
	}
	if err := service.saveMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": member.UserID,
		"request_id": "caller-selected-generation", "status": "rejected",
	}
	canonical, apiErr := service.operationRecordFor(context.Background(), "channels.public.join_result", cloneParams(params))
	if apiErr != nil || canonical.RequestID == "" || canonical.RequestID == params["request_id"] {
		t.Fatalf("derive canonical legacy callback generation: record=%#v err=%#v", canonical, apiErr)
	}

	result := mustHandle[map[string]any](t, service, "channels.public.join_result", cloneParams(params))
	stored, found, err := service.lookupMember(context.Background(), ch.RoomID, member.UserID)
	if err != nil || !found || result["status"] != "rejected" || stored.Membership != "reject" ||
		stored.RequestID != canonical.RequestID {
		t.Fatalf("legacy callback bound caller generation: result=%#v found=%v member=%#v err=%v canonical=%#v", result, found, stored, err, canonical)
	}
}

func TestPublicJoinFailedGenerationCASUsesDurableBaseFence(t *testing.T) {
	store := &failMemberGenerationOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	service := newService(Config{ServerName: "example.com"}, store, nil, portalState{}, false)
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	userID := "@alice:remote.example"
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: userID,
		Membership: "rejected", Role: "member", RequestID: "request-base",
	}); err != nil {
		t.Fatal(err)
	}
	paramsA := map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": userID,
		"operation_id": "operation-a", "request_id": "caller-request-a",
	}
	paramsB := cloneParams(paramsA)
	paramsB["operation_id"] = "operation-b"
	paramsB["request_id"] = "caller-request-b"
	recordA, recordAErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(paramsA))
	recordB, recordBErr := service.operationRecordFor(context.Background(), "channels.public.join_request", cloneParams(paramsB))
	if recordAErr != nil || recordBErr != nil || recordA.OperationID != recordB.OperationID || recordA.RequestID != recordB.RequestID {
		t.Fatalf("failed-generation callers did not converge before execution: a=%#v/%#v b=%#v/%#v", recordA, recordAErr, recordB, recordBErr)
	}
	store.failNext = true
	result, failedErr := service.Handle(context.Background(), "channels.public.join_request", cloneParams(paramsA))
	if result != nil || failedErr == nil || failedErr.OperationID != recordA.OperationID {
		t.Fatalf("injected generation CAS failure = (%#v, %#v), canonical=%#v", result, failedErr, recordA)
	}
	failed, ok, err := store.LookupOperation(context.Background(), recordA.OperationID)
	if err != nil || !ok || failed.Status != operationStatusFailed || failed.BaseRequestID != "request-base" {
		t.Fatalf("failed operation did not retain base generation: ok=%v operation=%#v err=%v", ok, failed, err)
	}
	retried := mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(paramsB))
	member := retried["member"].(memberRecord)
	if retried["status"] != "pending" || member.RequestID != failed.RequestID || retried["operation_id"] != recordA.OperationID {
		t.Fatalf("same-base retry did not resume canonical generation: operation=%#v result=%#v", failed, retried)
	}
}

func TestContactInviteGenerationChangesOperationIdentity(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	contact := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound", RequestID: "$invite-a",
	}
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID}
	first := mustHandle[contactRecord](t, service, "contacts.requests.accept", cloneParams(params))

	contact.Status = "pending_inbound"
	contact.RequestID = "$invite-b"
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	second := mustHandle[contactRecord](t, service, "contacts.requests.accept", cloneParams(params))
	if first.OperationID == "" || second.OperationID == "" || first.OperationID == second.OperationID {
		t.Fatalf("distinct Matrix invite generations reused one operation: first=%#v second=%#v", first, second)
	}
}

func TestCompletedDecisionReplayCannotMutateNewGeneration(t *testing.T) {
	t.Run("channel", func(t *testing.T) {
		service := NewService(Config{ServerName: "example.com"})
		ch := mustHandle[channel](t, service, "channels.create", map[string]any{
			"channel_id": "channel_1", "name": "Channel", "visibility": "public", "join_policy": "approval",
		})
		member := memberRecord{
			RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:remote.example",
			Membership: "pending", Role: "member", RequestID: "request-a",
		}
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{
			"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": member.UserID, "request_id": "request-a",
		}
		mustHandle[map[string]any](t, service, "channels.join_request.reject", cloneParams(params))
		member.Membership, member.RequestID = "pending", "request-b"
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
		mustHandle[map[string]any](t, service, "channels.join_request.reject", cloneParams(params))
		current, ok, err := service.lookupMember(context.Background(), ch.RoomID, member.UserID)
		if err != nil || !ok || current.Membership != "pending" || current.RequestID != "request-b" {
			t.Fatalf("old channel decision mutated the new generation: ok=%v current=%#v err=%v", ok, current, err)
		}
	})

	t.Run("contact", func(t *testing.T) {
		service := NewService(Config{ServerName: "example.com"})
		contact := contactRecord{
			RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", Status: "pending_inbound", RequestID: "$invite-a",
		}
		if err := service.saveContact(context.Background(), contact); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID, "request_id": "$invite-a"}
		mustHandle[contactRecord](t, service, "contacts.requests.reject", cloneParams(params))
		contact.Status, contact.RequestID = "pending_inbound", "$invite-b"
		if err := service.saveContact(context.Background(), contact); err != nil {
			t.Fatal(err)
		}
		mustHandle[contactRecord](t, service, "contacts.requests.reject", cloneParams(params))
		current, ok, err := service.lookupContactByPeer(context.Background(), contact.PeerMXID)
		if err != nil || !ok || current.Status != "pending_inbound" || current.RequestID != "$invite-b" {
			t.Fatalf("old contact decision mutated the new invite: ok=%v current=%#v err=%v", ok, current, err)
		}
	})
}

type lostCommittedJoinResponseTransport struct {
	statefulJoinTransport
	attempts int
}

func (t *lostCommittedJoinResponseTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	t.attempts++
	result, err := t.statefulJoinTransport.JoinRoom(ctx, req)
	if err == nil && t.attempts == 1 {
		return JoinRoomResult{}, io.EOF
	}
	return result, err
}

type deadlineProbeContactJoinTransport struct {
	recordingTransport
	err error
}

func (t *deadlineProbeContactJoinTransport) ListRoomMembers(context.Context, string) ([]memberRecord, error) {
	return nil, t.err
}

func TestContactAcceptLostJoinResponseStaysRetryableUntilMatrixFact(t *testing.T) {
	transport := &lostCommittedJoinResponseTransport{statefulJoinTransport: statefulJoinTransport{
		recordingTransport: recordingTransport{roomID: "!direct:remote.example"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	existing := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"room_id": existing.RoomID, "peer_mxid": existing.PeerMXID}

	first := mustHandle[contactRecord](t, service, "contacts.requests.accept", cloneParams(params))
	if first.Status != "joining" || first.ErrorCode != "matrix_join_unconfirmed" || first.RoomID != existing.RoomID {
		t.Fatalf("lost contact join response was not recoverable: %#v", first)
	}
	bootstrap := mustHandle[map[string]any](t, service, "sync.bootstrap", nil)
	contacts := bootstrap["contacts"].([]contactRecord)
	pending := bootstrap["pending"].(map[string]any)["friend_requests"].([]map[string]any)
	if contact := findContact(contacts, existing.PeerMXID); contact.Status != "joining" || contact.RoomID != existing.RoomID || len(pending) != 1 {
		t.Fatalf("pending contact was not retained for retry: contacts=%#v pending=%#v", contacts, pending)
	}

	second := mustHandle[contactRecord](t, service, "contacts.requests.accept", cloneParams(params))
	if second.Status != "accepted" || second.RoomID != existing.RoomID || second.CurrentRoomID != existing.RoomID {
		t.Fatalf("contact retry did not converge from Matrix fact: %#v", second)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("contact retry repeated a committed Matrix join: %#v", transport.joinRequests)
	}
}

func TestContactAcceptTextDeadlineExceededStaysRecoverable(t *testing.T) {
	transport := &failOnceJoinTransport{
		err: errors.New("federation MakeJoin failed: context deadline exceeded"), failures: 1,
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	existing := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"room_id": existing.RoomID, "peer_mxid": existing.PeerMXID}

	result, apiErr := service.Handle(context.Background(), "contacts.requests.accept", cloneParams(params))
	if apiErr != nil {
		t.Fatalf("text deadline join should return a recoverable result: %#v", apiErr)
	}
	view, ok := result.(contactRecord)
	if !ok || view.Status != "joining" || view.ErrorCode != actionbase.MatrixJoinUnconfirmedCode ||
		view.CurrentRoomID != existing.RoomID {
		t.Fatalf("text deadline result = %T %#v", result, result)
	}
	record, recordErr := service.operationRecordFor(context.Background(), "contacts.requests.accept", cloneParams(params))
	if recordErr != nil {
		t.Fatal(recordErr)
	}
	stored, found, err := service.store.LookupOperation(context.Background(), record.OperationID)
	if err != nil || !found || stored.Status != operationStatusReconciling || stored.Phase != operationPhaseMatrixUnconfirmed {
		t.Fatalf("deadline operation = %#v, found=%v, err=%v", stored, found, err)
	}
	if transport.attempts != 1 {
		t.Fatalf("deadline join attempts = %d, want 1", transport.attempts)
	}
}

func TestContactAcceptTextDeadlineProbeStaysRecoverable(t *testing.T) {
	transport := &deadlineProbeContactJoinTransport{
		err: errors.New("federation MakeJoin probe failed: context deadline exceeded"),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	existing := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"room_id": existing.RoomID, "peer_mxid": existing.PeerMXID}

	result, apiErr := service.Handle(context.Background(), "contacts.requests.accept", cloneParams(params))
	if apiErr != nil {
		t.Fatalf("text deadline probe should return a recoverable result: %#v", apiErr)
	}
	view, ok := result.(contactRecord)
	if !ok || view.Status != "joining" || view.ErrorCode != actionbase.MatrixJoinUnconfirmedCode ||
		view.CurrentRoomID != existing.RoomID {
		t.Fatalf("text deadline probe result = %T %#v", result, result)
	}
	record, recordErr := service.operationRecordFor(context.Background(), "contacts.requests.accept", cloneParams(params))
	if recordErr != nil {
		t.Fatal(recordErr)
	}
	stored, found, err := service.store.LookupOperation(context.Background(), record.OperationID)
	if err != nil || !found || stored.Status != operationStatusReconciling || stored.Phase != operationPhaseMatrixUnconfirmed {
		t.Fatalf("deadline probe operation = %#v, found=%v, err=%v", stored, found, err)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("deadline probe must not dispatch JoinRoom: %#v", transport.joinRequests)
	}
}

func TestGroupJoinLostSendJoinResponseReconcilesMatrixFactWithoutRedispatch(t *testing.T) {
	transport := &lostCommittedJoinResponseTransport{statefulJoinTransport: statefulJoinTransport{
		recordingTransport: recordingTransport{roomID: "!group:remote.example"},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": transport.roomID,
		"name":    "Remote Group",
	})
	userID := "@alice:remote.example"
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID,
		"user_id": userID,
	})
	params := map[string]any{
		"room_id":         group.RoomID,
		"user_id":         userID,
		"invite_event_id": "$direct-room-card-event",
		"operation_id":    "op-group-lost-send-join-response",
	}

	first := mustHandle[map[string]any](t, service, "groups.join", cloneParams(params))
	if first["status"] != "joining" || first["error_code"] != "matrix_join_unconfirmed" ||
		first["room_id"] != group.RoomID {
		t.Fatalf("lost /send_join response was not exposed as recoverable: %#v", first)
	}
	if len(transport.joinRequests) != 1 || transport.attempts != 1 {
		t.Fatalf("first join dispatch count = %d, attempts = %d", len(transport.joinRequests), transport.attempts)
	}

	replayed := mustHandle[map[string]any](t, service, "groups.join", cloneParams(params))
	if replayed["status"] != "ok" || replayed["room_id"] != group.RoomID {
		t.Fatalf("group retry did not converge from the committed Matrix fact: %#v", replayed)
	}
	member, ok, err := service.lookupMember(context.Background(), group.RoomID, userID)
	if err != nil || !ok || member.Membership != "join" {
		t.Fatalf("group retry did not repair the joined projection: member=%#v ok=%v err=%v", member, ok, err)
	}
	if len(transport.joinRequests) != 1 || transport.attempts != 1 {
		t.Fatalf("group retry repeated a committed Matrix join: requests=%#v attempts=%d", transport.joinRequests, transport.attempts)
	}
}

func TestContactRejectRepairsMatrixCommittedAcceptAfterContactWriteFailure(t *testing.T) {
	store := &failContactOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &statefulJoinTransport{recordingTransport: recordingTransport{roomID: "!direct:remote.example"}}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	existing := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound", RequestID: "$invite-a",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	params := map[string]any{
		"room_id": existing.RoomID, "peer_mxid": existing.PeerMXID, "request_id": existing.RequestID,
	}
	result, apiErr := service.Handle(context.Background(), "contacts.requests.accept", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Code != actionbase.OperationRecoveryCode {
		t.Fatalf("accept contact-write crash window = (%#v, %#v)", result, apiErr)
	}
	pending, ok, err := service.lookupContactByPeer(context.Background(), existing.PeerMXID)
	if err != nil || !ok || pending.Status != "pending_inbound" {
		t.Fatalf("contact failure did not preserve pre-commit projection: ok=%v contact=%#v err=%v", ok, pending, err)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	rejected := mustHandle[contactRecord](t, restarted, "contacts.requests.reject", cloneParams(params))
	if rejected.Status != "accepted" || rejected.RoomID != existing.RoomID {
		t.Fatalf("reject ignored authoritative Matrix join: %#v", rejected)
	}
	current, ok, err := restarted.lookupContactByPeer(context.Background(), existing.PeerMXID)
	if err != nil || !ok || current.Status != "accepted" || current.Remark != "" {
		t.Fatalf("Matrix join did not repair accepted contact: ok=%v contact=%#v err=%v", ok, current, err)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("reject recovery repeated Matrix join: %#v", transport.joinRequests)
	}
}

func TestContactRejectReturnsCurrentAcceptedOrJoiningState(t *testing.T) {
	tests := []struct {
		status    string
		errorCode string
	}{
		{status: "accepted"},
		{status: "joining", errorCode: "matrix_join_unconfirmed"},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			service := NewService(Config{ServerName: "example.com"})
			contact := contactRecord{
				RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
				Domain: "remote.example", Status: tt.status,
			}
			if err := service.saveContact(context.Background(), contact); err != nil {
				t.Fatal(err)
			}
			result := mustHandle[contactRecord](t, service, "contacts.requests.reject", map[string]any{
				"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
			})
			if result.Status != tt.status || result.ErrorCode != tt.errorCode {
				t.Fatalf("reject changed current state: %#v", result)
			}
		})
	}
}

func TestContactRejectReplayRechecksMatrixJoinBeforeUsingRejectedCache(t *testing.T) {
	transport := &recordingTransport{roomID: "!direct:remote.example"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	contact := contactRecord{
		RoomID: transport.roomID, PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound", RequestID: "$invite-a",
	}
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
		"request_id": contact.RequestID, "operation_id": "operation-reject",
	}
	first := mustHandle[contactRecord](t, service, "contacts.requests.reject", cloneParams(params))
	if first.Status != "rejected" {
		t.Fatalf("first reject did not create terminal cache: %#v", first)
	}
	transport.roomMembers = []memberRecord{{
		RoomID: contact.RoomID, UserID: service.OwnerMXID(), Membership: "join", Role: "member",
	}}
	replayed := mustHandle[contactRecord](t, service, "contacts.requests.reject", cloneParams(params))
	if replayed.Status != "accepted" {
		t.Fatalf("rejected cache hid authoritative Matrix join: %#v", replayed)
	}
	current, ok, err := service.lookupContactByPeer(context.Background(), contact.PeerMXID)
	if err != nil || !ok || current.Status != "accepted" {
		t.Fatalf("Matrix join did not repair cached reject: ok=%v contact=%#v err=%v", ok, current, err)
	}
}

func TestContactRejectReplayRechecksMatrixBeforeUsingAcceptedCache(t *testing.T) {
	transport := &recordingTransport{
		roomID: "!direct:remote.example",
		roomMembers: []memberRecord{{
			RoomID: "!direct:remote.example", UserID: "@owner:example.com", Membership: "join", Role: "member",
		}},
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	contact := contactRecord{
		RoomID: transport.roomID, PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "accepted", RequestID: "$invite-a",
	}
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
		"request_id": contact.RequestID, "operation_id": "operation-reject-accepted",
	}
	first := mustHandle[contactRecord](t, service, "contacts.requests.reject", cloneParams(params))
	if first.Status != "accepted" {
		t.Fatalf("authoritative Matrix join was not returned: %#v", first)
	}

	transport.roomMembers = nil
	replayed := mustHandle[contactRecord](t, service, "contacts.requests.reject", cloneParams(params))
	if replayed.Status != "rejected" {
		t.Fatalf("accepted cache hid missing Matrix join: %#v", replayed)
	}
	current, ok, err := service.lookupContactByPeer(context.Background(), contact.PeerMXID)
	if err != nil || !ok || current.Status != "rejected" {
		t.Fatalf("stale accepted projection was not repaired: ok=%v contact=%#v err=%v", ok, current, err)
	}
}

func TestDeletedContactSupersedesOldDecisionReplay(t *testing.T) {
	for _, action := range []string{"contacts.requests.accept", "contacts.requests.reject"} {
		t.Run(action, func(t *testing.T) {
			service := NewService(Config{ServerName: "example.com"})
			contact := contactRecord{
				RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
				Domain: "remote.example", Status: "pending_inbound", RequestID: "$invite-a",
			}
			if err := service.saveContact(context.Background(), contact); err != nil {
				t.Fatal(err)
			}
			params := map[string]any{
				"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
				"request_id": contact.RequestID, "operation_id": "old-contact-decision-" + strings.ReplaceAll(action, ".", "-"),
			}
			mustHandle[contactRecord](t, service, action, cloneParams(params))
			mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
				"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
			})

			result, apiErr := service.Handle(context.Background(), action, cloneParams(params))
			if result != nil || apiErr == nil || apiErr.Status != http.StatusGone || apiErr.Code != actionbase.RequestExpiredCode {
				t.Fatalf("deleted contact replay = (%#v, %#v), want request_expired", result, apiErr)
			}
			current, found, err := service.lookupContactByPeer(context.Background(), contact.PeerMXID)
			if err != nil || !found || current.Status != "deleted" {
				t.Fatalf("old decision restored deleted contact: found=%v contact=%#v err=%v", found, current, err)
			}
		})
	}
}

type failContactOnceStore struct {
	*p2pstorage.MemoryStore
	failNext bool
}

type failMemberGenerationOnceStore struct {
	*p2pstorage.MemoryStore
	failNext bool
}

func (s *failMemberGenerationOnceStore) CompareAndSwapMemberGeneration(
	ctx context.Context,
	member dirextalkdomain.MemberRecord,
	expectedRequestID,
	expectedMembership string,
) (bool, error) {
	if s.failNext {
		s.failNext = false
		return false, errors.New("injected member generation write failure")
	}
	return s.MemoryStore.CompareAndSwapMemberGeneration(ctx, member, expectedRequestID, expectedMembership)
}

func (s *failContactOnceStore) UpsertContact(ctx context.Context, contact dirextalkdomain.ContactRecord) error {
	if s.failNext {
		s.failNext = false
		return errors.New("injected contact write failure")
	}
	return s.MemoryStore.UpsertContact(ctx, contact)
}

func (s *failContactOnceStore) CompareAndSwapContact(
	ctx context.Context,
	contact,
	expected dirextalkdomain.ContactRecord,
) (bool, error) {
	if s.failNext {
		s.failNext = false
		return false, errors.New("injected contact write failure")
	}
	return s.MemoryStore.CompareAndSwapContact(ctx, contact, expected)
}

func (s *failContactOnceStore) CompareAndSwapContactProjection(
	ctx context.Context,
	contact,
	expected dirextalkdomain.ContactRecord,
) (bool, error) {
	if s.failNext {
		s.failNext = false
		return false, errors.New("injected contact projection write failure")
	}
	return s.MemoryStore.CompareAndSwapContactProjection(ctx, contact, expected)
}

type failConversationOnceStore struct {
	*p2pstorage.MemoryStore
	failNext bool
}

type failOperationCommitOnceStore struct {
	*p2pstorage.MemoryStore
	failNext bool
}

type workflowLeaseTakeoverStore struct {
	*p2pstorage.MemoryStore
	mu                 sync.Mutex
	workflow           operationsmodule.Record
	tookOver           bool
	heartbeatWasFenced bool
}

func (s *workflowLeaseTakeoverStore) ClaimOperation(
	ctx context.Context,
	record operationsmodule.Record,
	owner string,
	leaseDurationMillis int64,
) (operationsmodule.Record, bool, error) {
	claimed, ok, err := s.MemoryStore.ClaimOperation(ctx, record, owner, leaseDurationMillis)
	if err != nil || !ok {
		return claimed, ok, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.HasPrefix(record.OperationID, "_workflow_") {
		s.workflow = claimed
		return claimed, ok, nil
	}
	if s.workflow.OperationID != "" && !s.tookOver {
		takeover := s.workflow
		takeover.Revision++
		takeover.LeaseOwner = "workflow-takeover"
		takeover.LeaseUntil = time.Now().Add(time.Minute).UnixMilli()
		takeover.UpdatedAt = time.Now().UTC().UnixMilli()
		if err := s.MemoryStore.UpsertOperation(ctx, takeover); err != nil {
			return operationsmodule.Record{}, false, err
		}
		s.tookOver = true
	}
	return claimed, ok, nil
}

func (s *workflowLeaseTakeoverStore) CompareAndSwapOperation(
	ctx context.Context,
	record operationsmodule.Record,
	expectedRevision int64,
	owner string,
	leaseDurationMillis int64,
) (operationsmodule.Record, bool, error) {
	updated, ok, err := s.MemoryStore.CompareAndSwapOperation(
		ctx, record, expectedRevision, owner, leaseDurationMillis,
	)
	if strings.HasPrefix(record.OperationID, "_workflow_") && leaseDurationMillis > 0 && err == nil && !ok {
		s.mu.Lock()
		s.heartbeatWasFenced = true
		s.mu.Unlock()
	}
	return updated, ok, err
}

func (s *workflowLeaseTakeoverStore) takeoverState() (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tookOver, s.heartbeatWasFenced
}

func (s *failOperationCommitOnceStore) UpsertOperation(ctx context.Context, record operationsmodule.Record) error {
	if s.failNext && record.Phase == operationPhaseMatrixCommitted {
		s.failNext = false
		return errors.New("injected operation commit failure")
	}
	return s.MemoryStore.UpsertOperation(ctx, record)
}

func (s *failOperationCommitOnceStore) CompareAndSwapOperation(
	ctx context.Context,
	record operationsmodule.Record,
	expectedRevision int64,
	owner string,
	leaseDurationMillis int64,
) (operationsmodule.Record, bool, error) {
	if s.failNext && record.Phase == operationPhaseMatrixCommitted {
		s.failNext = false
		return operationsmodule.Record{}, false, errors.New("injected operation commit failure")
	}
	return s.MemoryStore.CompareAndSwapOperation(ctx, record, expectedRevision, owner, leaseDurationMillis)
}

func TestLostWorkflowLeaseFencesHandlerBeforeMatrixDispatch(t *testing.T) {
	store := &workflowLeaseTakeoverStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &recordingTransport{roomID: "!channel:example.com"}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	handlerCalled := false
	service.actions["channels.join"] = func(ctx context.Context, _ map[string]any) (any, *apiError) {
		handlerCalled = true
		_, _ = transport.JoinRoom(ctx, JoinRoomRequest{
			RoomIDOrAlias: "!channel:example.com",
			UserMXID:      "@alice:example.com",
		})
		return map[string]any{"status": "ok"}, nil
	}

	result, apiErr := service.Handle(context.Background(), "channels.join", map[string]any{
		"room_id": "!channel:example.com", "channel_id": "channel_1",
		"user_id": "@alice:example.com", "operation_id": "op-lost-workflow-lease",
	})
	if result != nil || apiErr == nil || apiErr.Code != actionbase.OperationRecoveryCode {
		t.Fatalf("lost workflow lease result = (%#v, %#v)", result, apiErr)
	}
	tookOver, heartbeatWasFenced := store.takeoverState()
	if !tookOver || !heartbeatWasFenced {
		t.Fatalf("workflow takeover did not fence the old owner heartbeat: took_over=%v heartbeat_fenced=%v", tookOver, heartbeatWasFenced)
	}
	if handlerCalled || len(transport.joinRequests) != 0 {
		t.Fatalf("lost workflow owner dispatched handler/Matrix side effect: handler_called=%v joins=%#v", handlerCalled, transport.joinRequests)
	}
}

type idempotentCreateTransport struct {
	recordingTransport
	rooms map[string]CreateRoomResult
}

type keyedGenerationCreateTransport struct {
	recordingTransport
	rooms map[string]CreateRoomResult
}

type cancelAfterCreateTransport struct {
	recordingTransport
	cancel context.CancelFunc
}

type blockingCreateTransport struct {
	recordingTransport
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	calls   int
}

func (t *blockingCreateTransport) CreateRoom(ctx context.Context, req CreateRoomRequest) (CreateRoomResult, error) {
	t.calls++
	t.once.Do(func() {
		close(t.entered)
		<-t.release
	})
	return t.recordingTransport.CreateRoom(ctx, req)
}

func (t *cancelAfterCreateTransport) CreateRoom(ctx context.Context, req CreateRoomRequest) (CreateRoomResult, error) {
	result, err := t.recordingTransport.CreateRoom(ctx, req)
	t.cancel()
	return result, err
}

func (t *idempotentCreateTransport) CreateRoom(ctx context.Context, req CreateRoomRequest) (CreateRoomResult, error) {
	if req.IdempotencyKey != "" {
		if result, ok := t.rooms[req.IdempotencyKey]; ok {
			return result, nil
		}
	}
	result, err := t.recordingTransport.CreateRoom(ctx, req)
	if err == nil && req.IdempotencyKey != "" {
		if t.rooms == nil {
			t.rooms = make(map[string]CreateRoomResult)
		}
		t.rooms[req.IdempotencyKey] = result
	}
	return result, err
}

func (t *keyedGenerationCreateTransport) CreateRoom(_ context.Context, req CreateRoomRequest) (CreateRoomResult, error) {
	t.createRooms = append(t.createRooms, req)
	if result, ok := t.rooms[req.IdempotencyKey]; ok {
		return result, nil
	}
	roomID := "!direct-first:example.com"
	if len(t.rooms) > 0 {
		roomID = "!direct-next:example.com"
	}
	result := CreateRoomResult{RoomID: roomID}
	if t.rooms == nil {
		t.rooms = make(map[string]CreateRoomResult)
	}
	t.rooms[req.IdempotencyKey] = result
	return result, nil
}

func TestOperationClaimPreventsDuplicateMutationAcrossServiceInstances(t *testing.T) {
	store := p2pstorage.NewMemoryStore()
	transport := &blockingCreateTransport{
		recordingTransport: recordingTransport{roomID: "!direct:example.com"},
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
	}
	firstService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	secondService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	params := map[string]any{"mxid": "@alice:remote.example", "display_name": "Alice"}

	type outcome struct {
		result any
		err    *apiError
	}
	firstDone := make(chan outcome, 1)
	go func() {
		result, apiErr := firstService.Handle(context.Background(), "contacts.request", cloneParams(params))
		firstDone <- outcome{result: result, err: apiErr}
	}()
	select {
	case <-transport.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first service did not enter CreateRoom")
	}

	second, apiErr := secondService.Handle(context.Background(), "contacts.request", cloneParams(params))
	if apiErr != nil {
		t.Fatalf("concurrent duplicate returned an error: %#v", apiErr)
	}
	secondContact := second.(contactRecord)
	if secondContact.Status != "joining" || secondContact.ErrorCode != actionbase.OperationRecoveryCode {
		t.Fatalf("concurrent duplicate did not report in-flight recovery: %#v", secondContact)
	}
	if transport.calls != 1 {
		t.Fatalf("concurrent service repeated CreateRoom: calls=%d", transport.calls)
	}
	close(transport.release)
	first := <-firstDone
	if first.err != nil || first.result.(contactRecord).Status != "pending_outbound" {
		t.Fatalf("claimed operation did not finish: result=%#v err=%#v", first.result, first.err)
	}
}

func TestContactRequestResponseLossReplayKeepsOperationIdentity(t *testing.T) {
	for _, tt := range []struct {
		name        string
		operationID string
	}{
		{name: "derived operation"},
		{name: "explicit operation", operationID: "contact-request-replay"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := p2pstorage.NewMemoryStore()
			transport := &idempotentCreateTransport{
				recordingTransport: recordingTransport{roomID: "!direct:example.com"},
			}
			service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
			params := map[string]any{"mxid": "@alice:remote.example", "display_name": "Alice"}
			if tt.operationID != "" {
				params["operation_id"] = tt.operationID
			}
			first := mustHandle[contactRecord](t, service, "contacts.request", cloneParams(params))
			second := mustHandle[contactRecord](t, service, "contacts.request", cloneParams(params))
			if first.OperationID == "" || first.OperationID != second.OperationID ||
				first.RoomID != second.RoomID || first.Status != second.Status {
				t.Fatalf("response-loss replay changed contact operation: first=%#v second=%#v", first, second)
			}
			if len(transport.createRooms) != 1 {
				t.Fatalf("response-loss replay created duplicate rooms: %#v", transport.createRooms)
			}
		})
	}
}

func TestInvalidPublicRecoveryRequestsDoNotPersistOperations(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	validChannel := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	tests := []struct {
		name        string
		action      string
		operationID string
		params      map[string]any
	}{
		{
			name: "join request missing local channel", action: "channels.public.join_request",
			operationID: "invalid-public-join-request",
			params:      map[string]any{"room_id": "!missing:example.com", "user_id": "@alice:remote.example"},
		},
		{
			name: "join result missing local channel", action: "channels.public.join_result",
			operationID: "invalid-public-join-result",
			params: map[string]any{
				"room_id": "!missing:remote.example", "user_id": service.OwnerMXID(),
				"request_id": "missing-request", "status": "approved",
			},
		},
		{
			name: "join request invalid Matrix user", action: "channels.public.join_request",
			operationID: "invalid-public-join-user",
			params:      map[string]any{"room_id": validChannel.RoomID, "user_id": "not-a-matrix-user"},
		},
		{
			name: "join request oversized request id", action: "channels.public.join_request",
			operationID: "invalid-public-request-id",
			params: map[string]any{
				"room_id": validChannel.RoomID, "user_id": "@alice:remote.example",
				"request_id": strings.Repeat("r", 513),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := cloneParams(tt.params)
			params["operation_id"] = tt.operationID
			candidate, candidateErr := service.operationRecordFor(context.Background(), tt.action, cloneParams(params))
			if candidateErr != nil {
				t.Fatalf("derive canonical invalid-request identity: %#v", candidateErr)
			}
			if _, apiErr := service.Handle(context.Background(), tt.action, params); apiErr == nil {
				t.Fatal("expected public target preflight to reject invalid request")
			}
			if operation, found, err := service.store.LookupOperation(context.Background(), candidate.OperationID); err != nil || found {
				t.Fatalf("invalid public request persisted canonical operation: id=%s found=%v operation=%#v err=%v", candidate.OperationID, found, operation, err)
			}
			if workflow, ok := durableBusinessWorkflowRecord(candidate); ok {
				if operation, found, err := service.store.LookupOperation(context.Background(), workflow.OperationID); err != nil || found {
					t.Fatalf("invalid public request persisted workflow: id=%s found=%v operation=%#v err=%v", workflow.OperationID, found, operation, err)
				}
			}
		})
	}
}

func TestExistingPublicOperationStillValidatesReplayShape(t *testing.T) {
	t.Run("join request bounded request ID", func(t *testing.T) {
		service := NewService(Config{ServerName: "example.com"})
		ch := mustHandle[channel](t, service, "channels.create", map[string]any{
			"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
		})
		params := map[string]any{
			"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:remote.example",
		}
		mustHandle[map[string]any](t, service, "channels.public.join_request", cloneParams(params))
		params["request_id"] = strings.Repeat("r", 513)
		if result, apiErr := service.Handle(context.Background(), "channels.public.join_request", params); result != nil ||
			apiErr == nil || apiErr.Status != http.StatusBadRequest {
			t.Fatalf("existing join request operation bypassed request validation: result=%#v err=%#v", result, apiErr)
		}
	})

	t.Run("join result status and request ID", func(t *testing.T) {
		service := NewService(Config{ServerName: "example.com"})
		ch := mustHandle[channel](t, service, "channels.create", map[string]any{
			"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
		})
		ownerMXID := service.OwnerMXID()
		if err := service.saveMember(context.Background(), memberRecord{
			RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: ownerMXID,
			Membership: "pending", Role: "member", RequestID: "request-a",
		}); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{
			"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": ownerMXID,
			"request_id": "request-a", "status": "rejected",
		}
		mustHandle[map[string]any](t, service, "channels.public.join_result", cloneParams(params))
		for name, mutate := range map[string]func(map[string]any){
			"invalid status": func(replay map[string]any) { replay["status"] = "unknown" },
			"oversized request ID": func(replay map[string]any) {
				replay["request_id"] = strings.Repeat("r", 513)
			},
		} {
			t.Run(name, func(t *testing.T) {
				replay := cloneParams(params)
				mutate(replay)
				if result, apiErr := service.Handle(context.Background(), "channels.public.join_result", replay); result != nil ||
					apiErr == nil || apiErr.Status != http.StatusBadRequest {
					t.Fatalf("existing join result operation bypassed shape validation: result=%#v err=%#v", result, apiErr)
				}
			})
		}
	})
}

func TestPublicRecoveryPreflightDoesNotInspectOtherActionParams(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if result, apiErr := service.Handle(context.Background(), "profile.get", map[string]any{
		"operation_id": []any{"legacy-action-owned-value"},
	}); apiErr != nil || result == nil {
		t.Fatalf("public recovery preflight changed unrelated action params: result=%#v err=%#v", result, apiErr)
	}
}

func TestWorkflowBusyResultPreservesApprovedChannelState(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
	})
	member := memberRecord{
		RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:remote.example",
		Membership: "approved", Role: "member", RequestID: "request-a",
	}
	if err := service.saveMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	result, apiErr := service.operationWorkflowBusyResult(context.Background(), operationsmodule.Record{
		OperationID: "operation-reject-approved", Action: "channels.join_request.reject",
		RoomID: member.RoomID, UserID: member.UserID, RequestID: member.RequestID,
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	current := result.(map[string]any)
	if current["status"] != "approved" || trimString(current["error_code"]) != "" {
		t.Fatalf("busy reject rewrote current approved state: %#v", current)
	}
}

func TestChannelRejectCallbackCannotOverwriteNewRequestGeneration(t *testing.T) {
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request envelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Action != "channels.public.join_result" || trimString(request.Params["request_id"]) != "request-a" {
			t.Errorf("unexpected callback: %#v", request)
		}
		close(callbackEntered)
		<-callbackRelease
		writeJSON(w, http.StatusOK, map[string]any{"status": "rejected"})
	}))
	defer remote.Close()

	service := NewService(Config{ServerName: "example.com", RemoteNodeAllowPrivateBaseURLs: true})
	ch := mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "channel_1", "name": "Channel", "visibility": "public", "join_policy": "approval",
	})
	member := memberRecord{
		RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:remote.example",
		Membership: "pending", Role: "member", RequestID: "request-a", RequesterNodeBaseURL: remote.URL + "/_p2p",
	}
	if err := service.saveMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		result any
		err    *apiError
	}
	rejectDone := make(chan outcome, 1)
	go func() {
		result, apiErr := service.Handle(context.Background(), "channels.join_request.reject", map[string]any{
			"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": member.UserID,
			"request_id": "request-a", "requester_node_base_url": remote.URL + "/_p2p",
		})
		rejectDone <- outcome{result: result, err: apiErr}
	}()
	select {
	case <-callbackEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("reject callback was not dispatched")
	}

	newRequest := mustHandle[map[string]any](t, service, "channels.public.join_request", map[string]any{
		"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": member.UserID, "request_id": "request-b",
	})
	if newRequest["status"] != "pending" {
		close(callbackRelease)
		<-rejectDone
		t.Fatalf("new request generation was not accepted: %#v", newRequest)
	}
	newMember, ok := newRequest["member"].(memberRecord)
	if !ok || newMember.RequestID == "" || newMember.RequestID == member.RequestID {
		close(callbackRelease)
		<-rejectDone
		t.Fatalf("new request did not receive a server-owned generation: %#v", newRequest)
	}
	close(callbackRelease)
	rejected := <-rejectDone
	if rejected.err != nil || rejected.result.(map[string]any)["status"] != "pending" {
		t.Fatalf("old reject did not return current generation: result=%#v err=%#v", rejected.result, rejected.err)
	}
	current, ok, err := service.lookupMember(context.Background(), ch.RoomID, member.UserID)
	if err != nil || !ok || current.Membership != "pending" || current.RequestID != newMember.RequestID {
		t.Fatalf("old callback overwrote request B: ok=%v current=%#v err=%v", ok, current, err)
	}
}

func (s *failConversationOnceStore) UpsertConversation(ctx context.Context, record dirextalkdomain.ConversationRecord) error {
	if s.failNext {
		s.failNext = false
		return errors.New("injected conversation write failure")
	}
	return s.MemoryStore.UpsertConversation(ctx, record)
}

func (s *failConversationOnceStore) CompareAndSwapContactProjection(
	ctx context.Context,
	contact,
	expected dirextalkdomain.ContactRecord,
) (bool, error) {
	if s.failNext {
		s.failNext = false
		return false, errors.New("injected conversation write failure")
	}
	return s.MemoryStore.CompareAndSwapContactProjection(ctx, contact, expected)
}

type statefulJoinTransport struct {
	recordingTransport
}

func (t *statefulJoinTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	result, err := t.recordingTransport.JoinRoom(ctx, req)
	if err == nil {
		t.roomMembers = append(t.roomMembers, memberRecord{
			RoomID: result.RoomID, UserID: req.UserMXID, DisplayName: req.DisplayName,
			AvatarURL: req.AvatarURL, Membership: "join", Role: "member",
		})
	}
	return result, err
}

type failOnceChannelRefreshTransport struct {
	*statefulJoinTransport
	failNext bool
}

type blockingJoinDecisionTransport struct {
	*statefulJoinTransport
	blockOnce sync.Once
	entered   chan struct{}
	release   chan struct{}
}

type blockingContactAcceptTransport struct {
	*statefulJoinTransport
	blockOnce sync.Once
	entered   chan struct{}
	release   chan struct{}
	calls     int
}

func (t *blockingContactAcceptTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	t.calls++
	t.blockOnce.Do(func() {
		close(t.entered)
		<-t.release
	})
	return t.statefulJoinTransport.JoinRoom(ctx, req)
}

func (t *blockingJoinDecisionTransport) SendStateEvent(ctx context.Context, req SendStateEventRequest) error {
	t.blockOnce.Do(func() {
		close(t.entered)
		<-t.release
	})
	return t.statefulJoinTransport.SendStateEvent(ctx, req)
}

func (t *failOnceChannelRefreshTransport) GetRoomChannel(ctx context.Context, roomID string) (channel, bool, error) {
	if t.failNext {
		t.failNext = false
		return channel{}, false, errors.New("injected channel projection failure")
	}
	return t.statefulJoinTransport.GetRoomChannel(ctx, roomID)
}

func TestContactAcceptAndRejectSerializeAcrossServiceInstances(t *testing.T) {
	store := p2pstorage.NewMemoryStore()
	transport := &blockingContactAcceptTransport{
		statefulJoinTransport: &statefulJoinTransport{recordingTransport: recordingTransport{
			roomID: "!direct:remote.example",
		}},
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	acceptService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	rejectService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	contact := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound", RequestID: "$invite-a",
	}
	if err := acceptService.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}

	type decisionResult struct {
		value any
		err   *apiError
	}
	acceptDone := make(chan decisionResult, 1)
	rejectDone := make(chan decisionResult, 1)
	go func() {
		value, apiErr := acceptService.Handle(context.Background(), "contacts.requests.accept", map[string]any{
			"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
			"request_id": contact.RequestID, "operation_id": "op-contact-accept",
		})
		acceptDone <- decisionResult{value: value, err: apiErr}
	}()
	select {
	case <-transport.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("accept did not enter Matrix join")
	}
	go func() {
		value, apiErr := rejectService.Handle(context.Background(), "contacts.requests.reject", map[string]any{
			"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID,
			"request_id": contact.RequestID, "operation_id": "op-contact-reject",
		})
		rejectDone <- decisionResult{value: value, err: apiErr}
	}()
	select {
	case result := <-rejectDone:
		close(transport.release)
		<-acceptDone
		t.Fatalf("reject crossed the in-flight accept workflow: %#v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(transport.release)

	accepted := <-acceptDone
	rejected := <-rejectDone
	if accepted.err != nil || accepted.value.(contactRecord).Status != "accepted" {
		t.Fatalf("accept result = (%#v, %#v)", accepted.value, accepted.err)
	}
	if rejected.err != nil || rejected.value.(contactRecord).Status != "accepted" {
		t.Fatalf("reject did not preserve the committed accept: (%#v, %#v)", rejected.value, rejected.err)
	}
	current, ok, err := acceptService.lookupContactByPeer(context.Background(), contact.PeerMXID)
	if err != nil || !ok || current.Status != "accepted" || current.RoomID != contact.RoomID {
		t.Fatalf("contact decision did not converge: current=%#v ok=%v err=%v", current, ok, err)
	}
	if transport.calls != 1 || len(transport.joinRequests) != 1 {
		t.Fatalf("contact decision repeated Matrix join: calls=%d requests=%#v", transport.calls, transport.joinRequests)
	}
}

func TestContactAcceptReplacementSurvivesStoreFailureAndRestart(t *testing.T) {
	store := &failContactOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!replacement-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	existing := contactRecord{
		RoomID: "!old-dm:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	params := map[string]any{"room_id": existing.RoomID, "peer_mxid": existing.PeerMXID}

	result, apiErr := service.Handle(context.Background(), "contacts.requests.accept", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Code != "operation_recovery_failed" || apiErr.CurrentRoomID != "!replacement-dm:example.com" {
		t.Fatalf("first accept failure = (%#v, %#v)", result, apiErr)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("first attempt replacement rooms = %#v", transport.createRooms)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	replayed := mustHandle[contactRecord](t, restarted, "contacts.requests.accept", cloneParams(params))
	if replayed.Status != "accepted" || replayed.RoomID != "!replacement-dm:example.com" ||
		replayed.CurrentRoomID != replayed.RoomID || replayed.OperationID == "" {
		t.Fatalf("restart replay did not recover replacement: %#v", replayed)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("restart replay created a duplicate replacement: %#v", transport.createRooms)
	}
}

func TestContactRequestReusesCreatedRoomAfterOperationCommitWriteFailure(t *testing.T) {
	store := &failOperationCommitOnceStore{MemoryStore: p2pstorage.NewMemoryStore(), failNext: true}
	transport := &idempotentCreateTransport{recordingTransport: recordingTransport{roomID: "!direct:example.com"}}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	params := map[string]any{"mxid": "@alice:remote.example", "display_name": "Alice"}

	result, apiErr := service.Handle(context.Background(), "contacts.request", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Code != "operation_recovery_failed" {
		t.Fatalf("operation commit write failure = (%#v, %#v)", result, apiErr)
	}
	if len(transport.createRooms) != 1 || transport.createRooms[0].IdempotencyKey == "" {
		t.Fatalf("first create was not idempotent: %#v", transport.createRooms)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	replayed := mustHandle[contactRecord](t, restarted, "contacts.request", cloneParams(params))
	if replayed.Status != "pending_outbound" || replayed.RoomID != "!direct:example.com" {
		t.Fatalf("restart did not recover created room: %#v", replayed)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("restart created a duplicate direct room: %#v", transport.createRooms)
	}
}

func TestDeletedContactLegacyRequestUsesNewStableCreateGeneration(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Action != "contacts.reactivate" {
			t.Fatalf("unexpected remote action %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	store := &failContactOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &keyedGenerationCreateTransport{}
	service := newService(Config{
		ServerName: "example.com", RemoteNodeAllowPrivateBaseURLs: true,
	}, store, transport, portalState{}, false)
	peerMXID := "@alice:remote.example"
	first := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid": peerMXID, "display_name": "Alice", "domain": "remote.example",
	})
	if first.Status != "pending_outbound" || first.RoomID != "!direct-first:example.com" || first.RequestID == "" {
		t.Fatalf("initial contact request = %#v", first)
	}
	mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": first.RoomID, "peer_mxid": peerMXID,
	})

	legacyParams := map[string]any{
		"mxid": peerMXID, "display_name": "Alice Again", "domain": "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	}
	store.failNext = true
	result, apiErr := service.Handle(context.Background(), "contacts.request", cloneParams(legacyParams))
	if result != nil || apiErr == nil || apiErr.Code != actionbase.OperationRecoveryCode {
		t.Fatalf("replacement contact save failure = (%#v, %#v)", result, apiErr)
	}
	deleted, ok, err := service.lookupContactByPeer(context.Background(), peerMXID)
	if err != nil || !ok || !dirextalkdomain.ContactDeleted(deleted.Status) || deleted.RequestID != first.RequestID {
		t.Fatalf("failed replacement changed deleted generation: ok=%v contact=%#v err=%v", ok, deleted, err)
	}

	replayed := mustHandle[contactRecord](t, service, "contacts.request", cloneParams(legacyParams))
	if replayed.Status != "pending_outbound" || replayed.RoomID != "!direct-next:example.com" ||
		replayed.RequestID == "" || replayed.RequestID == first.RequestID {
		t.Fatalf("legacy replacement did not use a new stable generation: %#v", replayed)
	}
	if len(transport.createRooms) != 2 ||
		transport.createRooms[0].IdempotencyKey == transport.createRooms[1].IdempotencyKey {
		t.Fatalf("replacement reused old create idempotency key or created twice: %#v", transport.createRooms)
	}
}

func TestContactRequestFinishesAfterRequestContextCancellation(t *testing.T) {
	requestCtx, cancel := context.WithCancel(context.Background())
	transport := &cancelAfterCreateTransport{
		recordingTransport: recordingTransport{roomID: "!direct:example.com"}, cancel: cancel,
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	result, apiErr := service.Handle(requestCtx, "contacts.request", map[string]any{
		"mxid": "@alice:remote.example", "display_name": "Alice",
	})
	if apiErr != nil {
		t.Fatalf("contacts.request after CreateRoom commit failed: %#v", apiErr)
	}
	contact := result.(contactRecord)
	if contact.Status != "pending_outbound" || contact.RoomID != "!direct:example.com" {
		t.Fatalf("contact settlement did not finish: %#v", contact)
	}
}

func TestContactAcceptRepairsConversationAfterPartialSaveAndRestart(t *testing.T) {
	store := &failConversationOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &statefulJoinTransport{recordingTransport: recordingTransport{roomID: "!direct:remote.example"}}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	contact := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	params := map[string]any{"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID}

	result, apiErr := service.Handle(context.Background(), "contacts.requests.accept", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Code != "operation_recovery_failed" {
		t.Fatalf("partial contact save = (%#v, %#v)", result, apiErr)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	replayed := mustHandle[contactRecord](t, restarted, "contacts.requests.accept", cloneParams(params))
	if replayed.Status != "accepted" || replayed.RoomID != contact.RoomID || replayed.Conversation == nil {
		t.Fatalf("restart did not repair contact conversation: %#v", replayed)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("conversation repair repeated Matrix join: %#v", transport.joinRequests)
	}
}

func TestContactRequestRepairsConversationAfterPartialSaveAndRestart(t *testing.T) {
	store := &failConversationOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &statefulJoinTransport{recordingTransport: recordingTransport{roomID: "!direct:remote.example"}}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	contact := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	params := map[string]any{"mxid": contact.PeerMXID, "display_name": contact.DisplayName}

	result, apiErr := service.Handle(context.Background(), "contacts.request", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Code != "operation_recovery_failed" || apiErr.CurrentRoomID != contact.RoomID {
		t.Fatalf("partial contact request save = (%#v, %#v)", result, apiErr)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("first contact request join calls = %#v", transport.joinRequests)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	replayed := mustHandle[contactRecord](t, restarted, "contacts.request", cloneParams(params))
	if replayed.Status != "accepted" || replayed.RoomID != contact.RoomID || replayed.Conversation == nil {
		t.Fatalf("restart did not repair requested contact conversation: %#v", replayed)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("contact request replay repeated Matrix join: %#v", transport.joinRequests)
	}
}

func TestContactRequestReplaysCommittedJoinAfterContactWriteFailure(t *testing.T) {
	store := &failContactOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &statefulJoinTransport{recordingTransport: recordingTransport{roomID: "!direct:remote.example"}}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	contact := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	params := map[string]any{"mxid": contact.PeerMXID, "display_name": contact.DisplayName}

	result, apiErr := service.Handle(context.Background(), "contacts.request", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Code != "operation_recovery_failed" || apiErr.CurrentRoomID != contact.RoomID {
		t.Fatalf("contact request write failure = (%#v, %#v)", result, apiErr)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("first contact request join calls = %#v", transport.joinRequests)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	replayed := mustHandle[contactRecord](t, restarted, "contacts.request", cloneParams(params))
	if replayed.Status != "accepted" || replayed.RoomID != contact.RoomID || replayed.Conversation == nil {
		t.Fatalf("restart did not settle committed contact join: %#v", replayed)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("contact write replay repeated Matrix join: %#v", transport.joinRequests)
	}
}

func TestContactRejectProjectionFailureReplaysAtomicallyAfterRestart(t *testing.T) {
	store := &failConversationOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	service := newService(Config{ServerName: "example.com"}, store, nil, portalState{}, false)
	contact := contactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example",
		DisplayName: "Alice", Domain: "remote.example", Status: "pending_inbound", RequestID: "request-a",
	}
	if err := service.saveContact(context.Background(), contact); err != nil {
		t.Fatal(err)
	}
	store.failNext = true
	params := map[string]any{
		"room_id": contact.RoomID, "peer_mxid": contact.PeerMXID, "avatar_url": "mxc://remote.example/rejected",
	}

	result, apiErr := service.Handle(context.Background(), "contacts.requests.reject", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Status != http.StatusInternalServerError {
		t.Fatalf("atomic reject projection failure = (%#v, %#v)", result, apiErr)
	}
	current, found, err := service.lookupContactByPeer(context.Background(), contact.PeerMXID)
	if err != nil || !found || current.Status != "pending_inbound" || current.AvatarURL != "" {
		t.Fatalf("failed reject partially committed contact: found=%v contact=%#v err=%v", found, current, err)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, nil, portalState{}, false)
	replayed := mustHandle[contactRecord](t, restarted, "contacts.requests.reject", cloneParams(params))
	if replayed.Status != "rejected" || replayed.RoomID != contact.RoomID ||
		replayed.AvatarURL != "mxc://remote.example/rejected" || replayed.Conversation == nil {
		t.Fatalf("restart did not atomically repair rejected contact projection: %#v", replayed)
	}
	conversation, found, err := restarted.conversationModule.GetRecord(context.Background(), "", contact.RoomID)
	if err != nil || !found || conversation.AvatarURL != replayed.AvatarURL || conversation.Lifecycle != "pending" {
		t.Fatalf("rejected conversation projection is inconsistent: found=%v conversation=%#v err=%v", found, conversation, err)
	}
}

func TestGroupJoinRepairsConversationAfterCommittedJoinAndRestart(t *testing.T) {
	store := &failConversationOnceStore{MemoryStore: p2pstorage.NewMemoryStore()}
	transport := &statefulJoinTransport{recordingTransport: recordingTransport{roomID: "!group:remote.example"}}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Remote Group"})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:example.com",
	})
	store.failNext = true
	params := map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:example.com", "invite_event_id": "$invite",
	}

	result, apiErr := service.Handle(context.Background(), "groups.join", cloneParams(params))
	if result != nil || apiErr == nil || apiErr.Code != "operation_recovery_failed" || apiErr.CurrentRoomID != group.RoomID {
		t.Fatalf("first group join failure = (%#v, %#v)", result, apiErr)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("first group join calls = %#v", transport.joinRequests)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	replayed := mustHandle[map[string]any](t, restarted, "groups.join", cloneParams(params))
	conversation, ok := replayed["conversation"].(conversationView)
	if replayed["status"] != "ok" || !ok || conversation.MatrixRoomID != group.RoomID || replayed["operation_id"] == "" {
		t.Fatalf("restart replay did not repair group conversation: %#v", replayed)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("restart replay repeated Matrix join: %#v", transport.joinRequests)
	}
}

func TestChannelMatrixJoinSurvivesProjectionFailureAndRestart(t *testing.T) {
	store := p2pstorage.NewMemoryStore()
	roomChannel := channel{
		ChannelID: "channel_1", RoomID: "!channel:example.com", Name: "Channel",
		Visibility: "public", JoinPolicy: "approval", ChannelType: "chat",
	}
	transport := &failOnceChannelRefreshTransport{statefulJoinTransport: &statefulJoinTransport{
		recordingTransport: recordingTransport{roomID: roomChannel.RoomID, roomChannel: roomChannel},
	}, failNext: true}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	if err := service.saveChannel(context.Background(), roomChannel); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: roomChannel.RoomID, ChannelID: roomChannel.ChannelID,
		UserID: "@alice:example.com", Membership: "pending", Role: "member",
	}); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"room_id": roomChannel.RoomID, "channel_id": roomChannel.ChannelID, "user_id": "@alice:example.com",
	}

	first := mustHandle[map[string]any](t, service, "channels.join_request.approve", cloneParams(params))
	if first["status"] != "joined" || first["error_code"] != "operation_recovery_failed" {
		t.Fatalf("projection failure denied Matrix join fact: %#v", first)
	}
	member, ok, err := service.lookupMember(context.Background(), roomChannel.RoomID, "@alice:example.com")
	if err != nil || !ok || member.Membership != "join" {
		t.Fatalf("projection failure rewrote joined member: member=%#v ok=%v err=%v", member, ok, err)
	}

	restarted := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	second := mustHandle[map[string]any](t, restarted, "channels.join_request.approve", cloneParams(params))
	if second["status"] != "joined" || trimString(second["error_code"]) != "" {
		t.Fatalf("restart did not repair channel projection: %#v", second)
	}
	if len(transport.joinRequests) != 1 {
		t.Fatalf("projection repair repeated Matrix join: %#v", transport.joinRequests)
	}
}

func TestChannelApproveAndRejectSerializeOneRequestGeneration(t *testing.T) {
	roomChannel := channel{
		ChannelID: "channel_1", RoomID: "!channel:example.com", Name: "Channel",
		Visibility: "public", JoinPolicy: "approval", ChannelType: "chat",
	}
	transport := &blockingJoinDecisionTransport{
		statefulJoinTransport: &statefulJoinTransport{recordingTransport: recordingTransport{
			roomID: roomChannel.RoomID, roomChannel: roomChannel,
		}},
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	if err := service.saveChannel(context.Background(), roomChannel); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: roomChannel.RoomID, ChannelID: roomChannel.ChannelID,
		UserID: "@alice:example.com", Membership: "pending", Role: "member",
	}); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{
		"room_id": roomChannel.RoomID, "channel_id": roomChannel.ChannelID, "user_id": "@alice:example.com",
	}
	type decisionResult struct {
		value any
		err   *apiError
	}
	approved := make(chan decisionResult, 1)
	rejected := make(chan decisionResult, 1)
	go func() {
		value, apiErr := service.Handle(context.Background(), "channels.join_request.approve", cloneParams(params))
		approved <- decisionResult{value: value, err: apiErr}
	}()
	<-transport.entered
	go func() {
		value, apiErr := service.Handle(context.Background(), "channels.join_request.reject", cloneParams(params))
		rejected <- decisionResult{value: value, err: apiErr}
	}()
	select {
	case result := <-rejected:
		close(transport.release)
		t.Fatalf("reject crossed in-flight approve boundary: %#v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(transport.release)
	approveResult := <-approved
	rejectResult := <-rejected
	if approveResult.err != nil || approveResult.value.(map[string]any)["status"] != "joined" {
		t.Fatalf("approve result = (%#v, %#v)", approveResult.value, approveResult.err)
	}
	if rejectResult.err != nil || rejectResult.value.(map[string]any)["status"] != "joined" {
		t.Fatalf("reject did not preserve Matrix join: (%#v, %#v)", rejectResult.value, rejectResult.err)
	}
}

func TestChannelApproveAndRejectSerializeAcrossServiceInstances(t *testing.T) {
	store := p2pstorage.NewMemoryStore()
	roomChannel := channel{
		ChannelID: "channel_1", RoomID: "!channel:example.com", Name: "Channel",
		Visibility: "public", JoinPolicy: "approval", ChannelType: "chat",
	}
	transport := &blockingJoinDecisionTransport{
		statefulJoinTransport: &statefulJoinTransport{recordingTransport: recordingTransport{
			roomID: roomChannel.RoomID, roomChannel: roomChannel,
		}},
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	approveService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	rejectService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	if err := approveService.saveChannel(context.Background(), roomChannel); err != nil {
		t.Fatal(err)
	}
	member := memberRecord{
		RoomID: roomChannel.RoomID, ChannelID: roomChannel.ChannelID,
		UserID: "@alice:example.com", Membership: "pending", Role: "member", RequestID: "request-a",
	}
	if err := approveService.saveMember(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	baseParams := map[string]any{
		"room_id": roomChannel.RoomID, "channel_id": roomChannel.ChannelID,
		"user_id": member.UserID, "request_id": member.RequestID,
	}

	type decisionResult struct {
		value any
		err   *apiError
	}
	approved := make(chan decisionResult, 1)
	rejected := make(chan decisionResult, 1)
	go func() {
		params := cloneParams(baseParams)
		params["operation_id"] = "op-channel-approve"
		value, apiErr := approveService.Handle(context.Background(), "channels.join_request.approve", params)
		approved <- decisionResult{value: value, err: apiErr}
	}()
	select {
	case <-transport.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("approve did not enter Matrix state publication")
	}
	go func() {
		params := cloneParams(baseParams)
		params["operation_id"] = "op-channel-reject"
		value, apiErr := rejectService.Handle(context.Background(), "channels.join_request.reject", params)
		rejected <- decisionResult{value: value, err: apiErr}
	}()
	select {
	case result := <-rejected:
		close(transport.release)
		<-approved
		t.Fatalf("reject crossed the in-flight approve workflow: %#v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(transport.release)

	approveResult := <-approved
	rejectResult := <-rejected
	if approveResult.err != nil || approveResult.value.(map[string]any)["status"] != "joined" {
		t.Fatalf("approve result = (%#v, %#v)", approveResult.value, approveResult.err)
	}
	if rejectResult.err != nil || rejectResult.value.(map[string]any)["status"] != "joined" {
		t.Fatalf("reject did not preserve the committed join: (%#v, %#v)", rejectResult.value, rejectResult.err)
	}
	current, ok, err := approveService.lookupMember(context.Background(), member.RoomID, member.UserID)
	if err != nil || !ok || current.Membership != "join" || current.RequestID != member.RequestID {
		t.Fatalf("channel decision did not converge: current=%#v ok=%v err=%v", current, ok, err)
	}
	if len(transport.joinRequests) != 1 || len(transport.kicks) != 0 {
		t.Fatalf("channel decision repeated or reversed Matrix side effects: joins=%#v kicks=%#v", transport.joinRequests, transport.kicks)
	}
}

func TestChannelGrantOnlyJoinSerializesAcrossServiceInstances(t *testing.T) {
	store := p2pstorage.NewMemoryStore()
	roomChannel := channel{
		ChannelID: "channel_1", RoomID: "!channel:example.com", Name: "Channel",
		Visibility: "private", JoinPolicy: "invite", ChannelType: "chat",
	}
	transport := &blockingContactAcceptTransport{
		statefulJoinTransport: &statefulJoinTransport{recordingTransport: recordingTransport{
			roomID: roomChannel.RoomID, roomChannel: roomChannel,
		}},
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	firstService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	secondService := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	if err := firstService.saveChannel(context.Background(), roomChannel); err != nil {
		t.Fatal(err)
	}
	shareRoomID := "!share:example.com"
	if err := firstService.saveGroup(context.Background(), groupRecord{RoomID: shareRoomID, Name: "Share Room"}); err != nil {
		t.Fatal(err)
	}
	userID := "@alice:example.com"
	if err := firstService.saveMember(context.Background(), memberRecord{
		RoomID: shareRoomID, UserID: userID, Membership: "join", Role: "member",
	}); err != nil {
		t.Fatal(err)
	}
	grant := channelInviteGrant{
		GrantID: "grant-a", ChannelID: roomChannel.ChannelID, RoomID: roomChannel.RoomID,
		ShareRoomID: shareRoomID, CreatedBy: firstService.OwnerMXID(), CreatedAt: time.Now().UTC().UnixMilli(),
	}
	if err := store.UpsertChannelInviteGrant(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	baseParams := map[string]any{
		"grant_id": grant.GrantID, "share_room_id": shareRoomID, "user_id": userID,
	}

	type joinResult struct {
		value any
		err   *apiError
	}
	firstDone := make(chan joinResult, 1)
	secondDone := make(chan joinResult, 1)
	go func() {
		params := cloneParams(baseParams)
		params["operation_id"] = "op-grant-join-a"
		value, apiErr := firstService.Handle(context.Background(), "channels.join", params)
		firstDone <- joinResult{value: value, err: apiErr}
	}()
	select {
	case <-transport.entered:
	case <-time.After(5 * time.Second):
		close(transport.release)
		t.Fatal("first grant join did not dispatch Matrix join")
	}
	go func() {
		params := cloneParams(baseParams)
		params["operation_id"] = "op-grant-join-b"
		value, apiErr := secondService.Handle(context.Background(), "channels.join", params)
		secondDone <- joinResult{value: value, err: apiErr}
	}()
	select {
	case result := <-secondDone:
		close(transport.release)
		<-firstDone
		t.Fatalf("grant-only replay crossed the in-flight durable workflow: %#v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(transport.release)

	first := <-firstDone
	second := <-secondDone
	if first.err != nil || first.value.(map[string]any)["status"] != "ok" {
		t.Fatalf("first grant join = (%#v, %#v)", first.value, first.err)
	}
	if second.err != nil || second.value.(map[string]any)["status"] != "ok" {
		t.Fatalf("second grant join = (%#v, %#v)", second.value, second.err)
	}
	if first.value.(map[string]any)["room_id"] != roomChannel.RoomID ||
		second.value.(map[string]any)["room_id"] != roomChannel.RoomID {
		t.Fatalf("grant-only joins did not resolve the retained room: first=%#v second=%#v", first.value, second.value)
	}
	if transport.calls != 1 || len(transport.joinRequests) != 1 {
		t.Fatalf("grant-only replay dispatched duplicate Matrix joins: calls=%d requests=%#v", transport.calls, transport.joinRequests)
	}
}

func TestChannelCallbackSettlementCannotDowngradeSameGenerationMatrixJoin(t *testing.T) {
	tests := []struct {
		name             string
		action           string
		callbackStatus   string
		callbackHTTPCode int
		waitingStatus    string
	}{
		{
			name:             "approve callback error",
			action:           "channels.join_request.approve",
			callbackHTTPCode: http.StatusInternalServerError,
			waitingStatus:    "joining",
		},
		{
			name:             "reject callback acknowledged",
			action:           "channels.join_request.reject",
			callbackStatus:   "rejected",
			callbackHTTPCode: http.StatusOK,
			waitingStatus:    "reject",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackEntered := make(chan struct{})
			callbackRelease := make(chan struct{})
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request envelope
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if request.Action != "channels.public.join_result" || trimString(request.Params["request_id"]) != "request-a" {
					t.Errorf("unexpected callback: %#v", request)
				}
				close(callbackEntered)
				<-callbackRelease
				if tt.callbackHTTPCode != http.StatusOK {
					writeJSON(w, tt.callbackHTTPCode, map[string]any{"error": "injected callback failure"})
					return
				}
				writeJSON(w, http.StatusOK, map[string]any{"status": tt.callbackStatus})
			}))
			defer remote.Close()

			transport := &recordingTransport{roomID: "!channel:owner.example"}
			owner := newChannelOwnerForRecovery(t, transport, remote.URL+"/_p2p")
			member, found, err := owner.lookupMember(context.Background(), "!channel:owner.example", "@owner:requester.example")
			if err != nil || !found {
				t.Fatalf("pending member = (%#v, %v, %v)", member, found, err)
			}
			member.RequestID = "request-a"
			if err := owner.saveMember(context.Background(), member); err != nil {
				t.Fatal(err)
			}

			type decisionResult struct {
				value any
				err   *apiError
			}
			decisionDone := make(chan decisionResult, 1)
			go func() {
				value, apiErr := owner.Handle(context.Background(), tt.action, map[string]any{
					"room_id": "!channel:owner.example", "channel_id": "channel_1",
					"user_id": member.UserID, "request_id": member.RequestID,
					"requester_node_base_url": remote.URL + "/_p2p",
				})
				decisionDone <- decisionResult{value: value, err: apiErr}
			}()
			select {
			case <-callbackEntered:
			case <-time.After(5 * time.Second):
				close(callbackRelease)
				t.Fatal("channel decision did not reach requester callback")
			}

			inFlight, found, err := owner.lookupMember(context.Background(), member.RoomID, member.UserID)
			if err != nil || !found || inFlight.RequestID != member.RequestID || inFlight.Membership != tt.waitingStatus {
				close(callbackRelease)
				<-decisionDone
				t.Fatalf("callback did not retain its expected generation/state: member=%#v found=%v err=%v", inFlight, found, err)
			}
			inFlight.Membership = "join"
			if err := owner.saveMember(context.Background(), inFlight); err != nil {
				close(callbackRelease)
				<-decisionDone
				t.Fatal(err)
			}
			transport.roomMembers = append(transport.roomMembers, memberRecord{
				RoomID: inFlight.RoomID, ChannelID: inFlight.ChannelID, UserID: inFlight.UserID,
				Membership: "join", Role: "member", RequestID: inFlight.RequestID,
			})
			close(callbackRelease)

			result := <-decisionDone
			if result.err != nil || result.value.(map[string]any)["status"] != "joined" {
				t.Fatalf("callback settlement did not preserve Matrix join: result=%#v err=%#v", result.value, result.err)
			}
			current, found, err := owner.lookupMember(context.Background(), inFlight.RoomID, inFlight.UserID)
			if err != nil || !found || current.Membership != "join" || current.RequestID != inFlight.RequestID {
				t.Fatalf("callback settlement downgraded joined projection: current=%#v found=%v err=%v", current, found, err)
			}
			if len(transport.kicks) != 0 {
				t.Fatalf("callback settlement kicked an authoritative member: %#v", transport.kicks)
			}
		})
	}
}

type timeoutFirstInviteTransport struct {
	recordingTransport
	attempts int
}

func (t *timeoutFirstInviteTransport) InviteUser(ctx context.Context, req InviteUserRequest) error {
	t.invites = append(t.invites, req.InviterMXID+" -> "+req.InviteeMXID+" in "+req.RoomID)
	t.inviteRequests = append(t.inviteRequests, req)
	t.attempts++
	if t.attempts == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

func TestChannelInviteTimeoutRemainsRecoverableWithoutKick(t *testing.T) {
	requester, requesterTransport, remote := newRequesterJoinCallbackServer(t, false)
	defer remote.Close()
	_ = requester
	ownerTransport := &timeoutFirstInviteTransport{recordingTransport: recordingTransport{roomID: "!channel:owner.example"}}
	owner := newChannelOwnerForRecovery(t, ownerTransport, remote.URL+"/_p2p")
	params := map[string]any{
		"room_id": "!channel:owner.example", "channel_id": "channel_1", "user_id": "@owner:requester.example",
	}

	first := mustHandle[map[string]any](t, owner, "channels.join_request.approve", cloneParams(params))
	if first["status"] != "joining" || first["error_code"] != "join_result_unconfirmed" {
		t.Fatalf("invite timeout was not recoverable: %#v", first)
	}
	second := mustHandle[map[string]any](t, owner, "channels.join_request.approve", cloneParams(params))
	if second["status"] != "joined" {
		t.Fatalf("invite timeout retry did not join: %#v", second)
	}
	if len(ownerTransport.inviteRequests) != 2 || len(ownerTransport.kicks) != 0 || len(requesterTransport.joinRequests) != 1 {
		t.Fatalf("unsafe timeout retry: invites=%#v kicks=%#v requester_joins=%#v", ownerTransport.inviteRequests, ownerTransport.kicks, requesterTransport.joinRequests)
	}
	for _, invite := range ownerTransport.inviteRequests {
		if invite.PublicJoinRequestID == "" || invite.PublicJoinRequestID != ownerTransport.inviteRequests[0].PublicJoinRequestID {
			t.Fatalf("technical Matrix invite did not retain one public-join generation: %#v", ownerTransport.inviteRequests)
		}
	}
}

func TestChannelCallbackAckLossReplaysWithoutKickOrDuplicateJoin(t *testing.T) {
	_, requesterTransport, remote := newRequesterJoinCallbackServer(t, true)
	defer remote.Close()
	ownerTransport := &recordingTransport{roomID: "!channel:owner.example"}
	owner := newChannelOwnerForRecovery(t, ownerTransport, remote.URL+"/_p2p")
	params := map[string]any{
		"room_id": "!channel:owner.example", "channel_id": "channel_1", "user_id": "@owner:requester.example",
	}

	first := mustHandle[map[string]any](t, owner, "channels.join_request.approve", cloneParams(params))
	if first["status"] != "joining" || first["error_code"] != "join_result_unconfirmed" {
		t.Fatalf("lost callback ACK was not reported as joining: %#v", first)
	}
	restarted := newService(Config{
		ServerName: "owner.example", RemoteNodeAllowPrivateBaseURLs: true,
	}, owner.store, ownerTransport, portalState{}, false)
	second := mustHandle[map[string]any](t, restarted, "channels.join_request.approve", cloneParams(params))
	if second["status"] != "joined" {
		t.Fatalf("callback replay did not converge: %#v", second)
	}
	if len(ownerTransport.inviteRequests) != 1 || len(ownerTransport.kicks) != 0 || len(requesterTransport.joinRequests) != 1 {
		t.Fatalf("callback replay repeated destructive/Matrix effects: invites=%#v kicks=%#v requester_joins=%#v", ownerTransport.inviteRequests, ownerTransport.kicks, requesterTransport.joinRequests)
	}
}

func TestChannelCallbackTerminalGonePreservesStableError(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusGone, map[string]any{
			"error": "join request expired", "code": "request_expired", "error_code": "request_expired",
		})
	}))
	defer remote.Close()
	transport := &recordingTransport{roomID: "!channel:owner.example"}
	owner := newChannelOwnerForRecovery(t, transport, remote.URL+"/_p2p")

	result, apiErr := owner.Handle(context.Background(), "channels.join_request.approve", map[string]any{
		"room_id": "!channel:owner.example", "channel_id": "channel_1", "user_id": "@owner:requester.example",
	})
	if result != nil || apiErr == nil || apiErr.Status != http.StatusGone || apiErr.Code != "request_expired" || apiErr.OperationID == "" {
		t.Fatalf("terminal callback response = (%#v, %#v)", result, apiErr)
	}
	if len(transport.kicks) != 0 {
		t.Fatalf("terminal callback failure kicked member: %#v", transport.kicks)
	}
}

func newChannelOwnerForRecovery(t *testing.T, transport Transport, requesterBaseURL string) *Service {
	t.Helper()
	service := NewServiceWithTransport(Config{
		ServerName: "owner.example", RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	if err := service.saveChannel(context.Background(), channel{
		ChannelID: "channel_1", RoomID: "!channel:owner.example", Name: "Channel",
		Visibility: "public", JoinPolicy: "approval", ChannelType: "chat",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: "!channel:owner.example", ChannelID: "channel_1", UserID: "@owner:requester.example",
		Membership: "pending", Role: "member", RequesterNodeBaseURL: requesterBaseURL,
	}); err != nil {
		t.Fatal(err)
	}
	return service
}

func newRequesterJoinCallbackServer(t *testing.T, abortFirst bool) (*Service, *statefulJoinTransport, *httptest.Server) {
	t.Helper()
	transport := &statefulJoinTransport{recordingTransport: recordingTransport{
		roomID: "!channel:owner.example",
		roomChannel: channel{
			ChannelID: "channel_1", RoomID: "!channel:owner.example", Name: "Channel",
			Visibility: "public", JoinPolicy: "approval", ChannelType: "chat",
		},
	}}
	service := NewServiceWithTransport(Config{ServerName: "requester.example"}, transport)
	if err := service.saveChannel(context.Background(), transport.roomChannel); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: "!channel:owner.example", ChannelID: "channel_1", UserID: "@owner:requester.example",
		Membership: "pending", Role: "member",
	}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request envelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		result, apiErr := service.Handle(r.Context(), request.Action, request.Params)
		if apiErr != nil {
			writeJSON(w, apiErr.Status, apiErr)
			return
		}
		calls++
		if abortFirst && calls == 1 {
			panic(http.ErrAbortHandler)
		}
		writeJSON(w, http.StatusOK, result)
	}))
	return service, transport, server
}

type alreadyJoinedJoinErrorTransport struct {
	recordingTransport
}

func (t *alreadyJoinedJoinErrorTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	t.joins = append(t.joins, req.UserMXID+" in "+req.RoomIDOrAlias)
	t.joinRequests = append(t.joinRequests, req)
	return JoinRoomResult{}, errors.New("user is already joined to room")
}

func TestGroupJoinDoesNotTrustAlreadyJoinedErrorWithoutLocalMatrixFact(t *testing.T) {
	transport := &alreadyJoinedJoinErrorTransport{recordingTransport: recordingTransport{roomID: "!group:example.com"}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Group"})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example",
	})

	result, apiErr := service.Handle(context.Background(), "groups.join", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example", "invite_event_id": "$invite",
	})
	if apiErr != nil {
		t.Fatalf("unconfirmed join should be a recoverable 200 result: %#v", apiErr)
	}
	response := result.(map[string]any)
	if response["status"] != "joining" || response["error_code"] != "matrix_join_unconfirmed" {
		t.Fatalf("unexpected unconfirmed response: %#v", response)
	}
	member, ok, err := service.lookupMember(context.Background(), group.RoomID, "@alice:remote.example")
	if err != nil || !ok || member.Membership != "joining" {
		t.Fatalf("unconfirmed Matrix join was not retained for reconciliation: member=%#v ok=%v err=%v", member, ok, err)
	}
}

func TestGroupJoinUsesConfirmedLocalMatrixFactWithoutRepeatingJoin(t *testing.T) {
	transport := &alreadyJoinedJoinErrorTransport{recordingTransport: recordingTransport{
		roomID: "!group:example.com",
		roomMembers: []memberRecord{{
			RoomID: "!group:example.com", UserID: "@alice:remote.example", Membership: "join", Role: "member",
		}},
	}}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{"name": "Group"})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example",
	})

	result := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example", "invite_event_id": "$invite",
	})
	if result["status"] != "ok" || len(transport.joinRequests) != 0 {
		t.Fatalf("confirmed join was not reconciled idempotently: result=%#v joins=%#v", result, transport.joinRequests)
	}
}

func TestGroupJoinReplayRepairsMissingConversation(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:example.com", "name": "Group",
	})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example",
	})
	if err := service.store.DeleteConversationByRoomID(context.Background(), group.RoomID); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "groups.join", map[string]any{
		"room_id": group.RoomID, "user_id": "@alice:remote.example", "invite_event_id": "$invite",
	})
	conversation, ok := result["conversation"].(conversationView)
	if !ok || conversation.MatrixRoomID != group.RoomID {
		t.Fatalf("group replay did not repair conversation: %#v", result)
	}
}

func TestGroupInviteRejectReplayReturnsRejected(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveGroup(context.Background(), groupRecord{
		RoomID: "!group:remote.example", Name: "Group", InvitePolicy: "member",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: "!group:remote.example", UserID: "@owner:example.com", Membership: "invite", Role: "member",
	}); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"room_id": "!group:remote.example"}
	first := mustHandle[map[string]any](t, service, "groups.invite.reject", cloneParams(params))
	second := mustHandle[map[string]any](t, service, "groups.invite.reject", cloneParams(params))
	if first["status"] != "rejected" || second["status"] != "rejected" {
		t.Fatalf("group reject replay changed result: first=%#v second=%#v", first, second)
	}
}

type cancelAwareMemberStore struct {
	*p2pstorage.MemoryStore
}

func (s *cancelAwareMemberStore) UpsertMember(ctx context.Context, member memberRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.MemoryStore.UpsertMember(ctx, member)
}

type cancelAfterJoinTransport struct {
	recordingTransport
	cancel context.CancelFunc
}

func (t *cancelAfterJoinTransport) JoinRoom(ctx context.Context, req JoinRoomRequest) (JoinRoomResult, error) {
	result, err := t.recordingTransport.JoinRoom(ctx, req)
	t.cancel()
	return result, err
}

func TestGroupJoinFinishesProjectionAfterRequestContextCancellation(t *testing.T) {
	store := &cancelAwareMemberStore{MemoryStore: p2pstorage.NewMemoryStore()}
	requestCtx, cancel := context.WithCancel(context.Background())
	transport := &cancelAfterJoinTransport{
		recordingTransport: recordingTransport{roomID: "!group:remote.example"},
		cancel:             cancel,
	}
	service := newService(Config{ServerName: "example.com"}, store, transport, portalState{}, false)
	group := mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!group:remote.example",
		"name":    "Remote Group",
	})
	mustHandle[map[string]any](t, service, "groups.invite", map[string]any{
		"room_id": group.RoomID,
		"user_id": "@owner:example.com",
	})

	result, apiErr := service.Handle(requestCtx, "groups.join", map[string]any{
		"room_id":         group.RoomID,
		"user_id":         "@owner:example.com",
		"invite_event_id": "$invite",
	})
	if apiErr != nil {
		t.Fatalf("groups.join after transport commit failed: %#v", apiErr)
	}
	if result.(map[string]any)["status"] != "ok" {
		t.Fatalf("unexpected group join result: %#v", result)
	}
	member, ok, err := service.lookupMember(context.Background(), group.RoomID, "@owner:example.com")
	if err != nil || !ok || member.Membership != "join" {
		t.Fatalf("joined member projection was not finished: member=%#v ok=%v err=%v", member, ok, err)
	}
}

func TestChannelJoinRequestTerminalAndInflightReplaysAreNotNotFound(t *testing.T) {
	tests := []struct {
		name       string
		action     string
		membership string
		matrixJoin bool
		status     string
		errorCode  string
	}{
		{name: "approve joining", action: "channels.join_request.approve", membership: "joining", status: "approved"},
		{name: "approve rejected", action: "channels.join_request.approve", membership: "reject", status: "rejected"},
		{name: "approve joined", action: "channels.join_request.approve", membership: "join", matrixJoin: true, status: "joined"},
		{name: "reject rejected", action: "channels.join_request.reject", membership: "reject", status: "rejected"},
		{name: "reject joining", action: "channels.join_request.reject", membership: "joining", status: "joining", errorCode: "join_result_unconfirmed"},
		{name: "reject join failed", action: "channels.join_request.reject", membership: "join_failed", status: "join_failed", errorCode: "matrix_join_failed"},
		{name: "reject projected join unconfirmed", action: "channels.join_request.reject", membership: "join", status: "joining", errorCode: "join_result_unconfirmed"},
		{name: "reject joined", action: "channels.join_request.reject", membership: "join", matrixJoin: true, status: "joined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &recordingTransport{roomID: "!channel:example.com"}
			if tt.matrixJoin {
				transport.roomMembers = []memberRecord{{
					RoomID: "!channel:example.com", UserID: "@alice:remote.example", Membership: "join", Role: "member",
				}}
			}
			service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
			ch := mustHandle[channel](t, service, "channels.create", map[string]any{
				"channel_id": "channel_1",
				"room_id":    "!channel:example.com",
				"name":       "Channel",
			})
			if err := service.saveMember(context.Background(), memberRecord{
				RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:remote.example",
				Membership: tt.membership, Role: "member",
			}); err != nil {
				t.Fatal(err)
			}

			result, apiErr := service.Handle(context.Background(), tt.action, map[string]any{
				"room_id": ch.RoomID, "channel_id": ch.ChannelID, "user_id": "@alice:remote.example",
			})
			if apiErr != nil {
				t.Fatalf("%s replay failed: %#v", tt.action, apiErr)
			}
			response := result.(map[string]any)
			if got := response["status"]; got != tt.status || trimString(response["error_code"]) != tt.errorCode ||
				trimString(response["operation_id"]) == "" || trimString(response["current_room_id"]) != ch.RoomID {
				t.Fatalf("replay response does not match contract: %#v", response)
			}
		})
	}
}

func TestRejectedMemberDecisionReplayRepairsMatrixJoin(t *testing.T) {
	t.Run("channel", func(t *testing.T) {
		transport := &recordingTransport{roomID: "!channel:example.com"}
		service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
		ch := mustHandle[channel](t, service, "channels.create", map[string]any{
			"channel_id": "public", "name": "Public", "visibility": "public", "join_policy": "approval",
		})
		member := memberRecord{
			RoomID: ch.RoomID, ChannelID: ch.ChannelID, UserID: "@alice:remote.example",
			Membership: "pending", Role: "member", RequestID: "request-a",
		}
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{
			"room_id": member.RoomID, "channel_id": member.ChannelID, "user_id": member.UserID,
			"request_id": member.RequestID, "operation_id": "reject-channel-before-matrix-join",
		}
		first := mustHandle[map[string]any](t, service, "channels.join_request.reject", cloneParams(params))
		if first["status"] != "rejected" {
			t.Fatalf("first channel reject = %#v", first)
		}
		transport.roomMembers = []memberRecord{{
			RoomID: member.RoomID, UserID: member.UserID, Membership: "join", Role: "member",
		}}
		replayed := mustHandle[map[string]any](t, service, "channels.join_request.reject", cloneParams(params))
		if replayed["status"] != "joined" {
			t.Fatalf("rejected channel cache hid Matrix join: %#v", replayed)
		}
		current, found, err := service.lookupMember(context.Background(), member.RoomID, member.UserID)
		if err != nil || !found || current.Membership != "join" {
			t.Fatalf("channel Matrix join did not repair projection: found=%v member=%#v err=%v", found, current, err)
		}
	})

	t.Run("group invite reject", func(t *testing.T) {
		transport := &recordingTransport{roomID: "!group:remote.example"}
		service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
		if err := service.saveGroup(context.Background(), groupRecord{RoomID: transport.roomID, Name: "Remote Group"}); err != nil {
			t.Fatal(err)
		}
		member := memberRecord{
			RoomID: transport.roomID, UserID: service.OwnerMXID(), Membership: "invite", Role: "member", RequestID: "$invite-a",
		}
		if err := service.saveMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{
			"room_id": member.RoomID, "invite_event_id": member.RequestID,
			"operation_id": "reject-group-before-matrix-join",
		}
		first := mustHandle[map[string]any](t, service, "groups.invite.reject", cloneParams(params))
		if first["status"] != "rejected" {
			t.Fatalf("first group reject = %#v", first)
		}
		transport.roomMembers = []memberRecord{{
			RoomID: member.RoomID, UserID: member.UserID, Membership: "join", Role: "member",
		}}
		replayed := mustHandle[map[string]any](t, service, "groups.invite.reject", cloneParams(params))
		if replayed["status"] != "joined" {
			t.Fatalf("rejected group cache hid Matrix join: %#v", replayed)
		}
		current, found, err := service.lookupMember(context.Background(), member.RoomID, member.UserID)
		if err != nil || !found || current.Membership != "join" {
			t.Fatalf("group Matrix join did not repair projection: found=%v member=%#v err=%v", found, current, err)
		}
	})
}

func TestContactAcceptStaleRoomReportsAuthoritativeCurrentRoom(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID: "!current:example.com", PeerMXID: "@alice:remote.example", DisplayName: "Alice", Status: "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id": "!stale:remote.example", "peer_mxid": "@alice:remote.example",
	})
	if result.RoomID != "!current:example.com" || result.CurrentRoomID != result.RoomID || result.OperationID == "" {
		t.Fatalf("stale room replay did not report authoritative room: %#v", result)
	}
}

func TestContactRejectStaleRoomKeepsAuthoritativeReplacementRoom(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID: "!current:example.com", PeerMXID: "@alice:remote.example", DisplayName: "Alice", Status: "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[contactRecord](t, service, "contacts.requests.reject", map[string]any{
		"room_id": "!stale:remote.example", "peer_mxid": "@alice:remote.example",
	})
	if result.Status != "rejected" || result.RoomID != "!current:example.com" || result.CurrentRoomID != result.RoomID {
		t.Fatalf("stale reject changed authoritative room: %#v", result)
	}
	if _, found, err := service.lookupContactByRoom(context.Background(), "!stale:remote.example"); err != nil || found {
		t.Fatalf("stale room was recreated: found=%v err=%v", found, err)
	}
}

func TestChannelPublicJoinResultReplayAfterJoinedReturnsJoined(t *testing.T) {
	transport := &recordingTransport{
		roomID: "!channel:owner.example",
		roomMembers: []memberRecord{{
			RoomID: "!channel:owner.example", UserID: "@owner:requester.example", Membership: "join", Role: "member",
		}},
	}
	service := NewServiceWithTransport(Config{ServerName: "requester.example"}, transport)
	if err := service.saveChannel(context.Background(), channel{
		ChannelID: "channel_1", RoomID: "!channel:owner.example", Name: "Channel",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(context.Background(), memberRecord{
		RoomID: "!channel:owner.example", ChannelID: "channel_1", UserID: "@owner:requester.example",
		Membership: "join", Role: "member",
	}); err != nil {
		t.Fatal(err)
	}

	result, apiErr := service.Handle(context.Background(), "channels.public.join_result", map[string]any{
		"room_id": "!channel:owner.example", "channel_id": "channel_1",
		"user_id": "@owner:requester.example", "status": "joined", "request_id": "request-1",
	})
	if apiErr != nil {
		t.Fatalf("channels.public.join_result replay failed: %#v", apiErr)
	}
	if got := result.(map[string]any)["status"]; got != "joined" {
		t.Fatalf("status = %#v, want joined; result=%#v", got, result)
	}
	if len(transport.joinRequests) != 0 || len(transport.kicks) != 0 {
		t.Fatalf("joined callback replay repeated Matrix side effects: joins=%#v kicks=%#v", transport.joinRequests, transport.kicks)
	}
}

func TestContactAcceptReplayWithOldRoomAndPeerReturnsReplacement(t *testing.T) {
	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!replacement-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	existing := contactRecord{
		RoomID: "!old-dm:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice",
		Domain: "remote.example", Status: "pending_inbound",
	}
	if err := service.saveContact(context.Background(), existing); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"room_id": existing.RoomID, "peer_mxid": existing.PeerMXID}
	first := mustHandle[contactRecord](t, service, "contacts.requests.accept", cloneParams(params))
	transport.roomMembers = append(transport.roomMembers, memberRecord{
		RoomID: first.RoomID, UserID: service.OwnerMXID(), Membership: "join", Role: "member",
	})

	replayed, apiErr := service.Handle(context.Background(), "contacts.requests.accept", cloneParams(params))
	if apiErr != nil {
		t.Fatalf("contacts.requests.accept replay failed: %#v", apiErr)
	}
	second := replayed.(contactRecord)
	if first.Status != "accepted" || second.Status != "accepted" || second.RoomID != first.RoomID {
		t.Fatalf("contact accept responses are not equivalent: first=%#v replay=%#v", first, second)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("replay created duplicate replacement rooms: %#v", transport.createRooms)
	}
}
