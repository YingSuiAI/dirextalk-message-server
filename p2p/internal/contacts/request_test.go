package contacts

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type requestHarness struct {
	module             *Module
	store              *saveStore
	conversation       *saveConversationPort
	log                *operationLog
	invites            []DirectRoomInviteRequest
	inviteErr          *actionbase.Error
	creates            []DirectRoomCreateRequest
	createRoomID       string
	createErr          *actionbase.Error
	reactivations      []PeerReactivationRequest
	reactivationResult PeerReactivationResult
	reactivationErr    *actionbase.Error
	profile            LocalProfileSnapshot
	joins              []DirectRoomJoinRequest
	joinOutcomes       []DirectRoomJoinOutcome
}

func newRequestHarness(existing ...dirextalkdomain.ContactRecord) *requestHarness {
	harness := &requestHarness{
		log:          &operationLog{},
		createRoomID: "!created:example.com",
		profile: LocalProfileSnapshot{
			MXID: "@owner:example.com", DisplayName: "Owner", AvatarURL: "mxc://example.com/owner",
		},
	}
	harness.store = &saveStore{log: harness.log, records: append([]dirextalkdomain.ContactRecord(nil), existing...)}
	var existingRoomID string
	var existingStatus string
	if len(existing) > 0 {
		existingRoomID = existing[0].RoomID
		existingStatus = existing[0].Status
	}
	harness.conversation = &saveConversationPort{
		log: harness.log, operation: map[string]any{"action": "contacts.request", "status": existingStatus},
		operationView: &dirextalkdomain.ConversationView{MatrixRoomID: existingRoomID, Kind: dirextalkdomain.ConversationKindDirect},
	}
	harness.module = New(harness.store, harness.conversation, Config{
		ServerName: "example.com",
		NewDirectRoomID: func() string {
			harness.log.add("new-room-id")
			return "!fallback:example.com"
		},
		CreateDirectRoom: func(_ context.Context, request DirectRoomCreateRequest) (string, *actionbase.Error) {
			harness.log.add("create:" + request.PeerMXID)
			harness.creates = append(harness.creates, request)
			return harness.createRoomID, harness.createErr
		},
		InviteDirectRoom: func(_ context.Context, request DirectRoomInviteRequest) *actionbase.Error {
			harness.log.add("invite:" + request.Contact.RoomID)
			harness.invites = append(harness.invites, request)
			return harness.inviteErr
		},
		JoinDirectRoom: func(_ context.Context, request DirectRoomJoinRequest) DirectRoomJoinOutcome {
			harness.log.add("join:" + request.RoomID)
			harness.joins = append(harness.joins, request)
			index := len(harness.joins) - 1
			if index < len(harness.joinOutcomes) {
				return harness.joinOutcomes[index]
			}
			return DirectRoomJoinOutcome{}
		},
		LocalProfile: func() LocalProfileSnapshot {
			return harness.profile
		},
		ReactivatePeer: func(_ context.Context, request PeerReactivationRequest) (PeerReactivationResult, *actionbase.Error) {
			harness.log.add("reactivate:" + request.Contact.PeerMXID)
			harness.reactivations = append(harness.reactivations, request)
			return harness.reactivationResult, harness.reactivationErr
		},
	})
	return harness
}

func TestResendPendingOutboundMergesFieldsAndPreservesWorkflowOrder(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!direct:example.com", DisplayName: "Old Alice",
		DisplayNameOverride: true, AvatarURL: "mxc://example.com/old", Status: " pending_OUTBOUND ", Remark: "old remark",
	}
	harness := newRequestHarness(existing)

	view, apiErr := harness.module.ResendPendingOutbound(context.Background(), existing, map[string]any{
		"display_name":    " New Alice ",
		"avatar_url":      " mxc://example.com/new ",
		"domain":          " ",
		"remark":          " primary ",
		"request_message": "secondary",
		"message":         "tertiary",
		"reason":          "last",
	}, "derived.example")
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	want := existing
	want.DisplayName = "New Alice"
	want.AvatarURL = "mxc://example.com/new"
	want.Domain = "derived.example"
	want.Remark = "primary"
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
		t.Fatalf("resend contact = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(harness.invites, []DirectRoomInviteRequest{{Contact: want}}) {
		t.Fatalf("invite requests = %#v, want %#v", harness.invites, want)
	}
	if !reflect.DeepEqual(view.Operation, harness.conversation.operation) || view.Conversation == nil || view.Conversation.MatrixRoomID != existing.RoomID {
		t.Fatalf("resend response = %#v", view)
	}
	wantCalls := []string{
		"invite:!direct:example.com",
		"list",
		"list-conversations",
		"upsert:!direct:example.com",
		"delete-conversation:group:!direct:example.com",
		"save-conversation:direct:!direct:example.com",
		"operation:contacts.request: pending_OUTBOUND :!direct:example.com",
	}
	if got := harness.log.snapshot(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("workflow calls = %#v, want %#v", got, wantCalls)
	}
}

func TestResendPendingOutboundKeepsExistingOptionalFields(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!direct:example.com", DisplayName: "Alice",
		AvatarURL: "mxc://example.com/alice", Domain: "stored.example", Status: "pending_outbound", Remark: "stored remark",
	}
	harness := newRequestHarness(existing)

	view, apiErr := harness.module.ResendPendingOutbound(context.Background(), existing, map[string]any{
		"display_name": " ", "avatar_url": " ", "domain": " ", "remark": " ", "reason": " fallback reason ",
	}, "derived.example")
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	want := existing
	want.Remark = "fallback reason"
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
		t.Fatalf("resend contact = %#v, want %#v", got, want)
	}
}

func TestResendPendingOutboundInviteBoundaryAndFailures(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_outbound"}
	tests := []struct {
		name          string
		roomID        string
		nilInviter    bool
		inviteErr     *actionbase.Error
		storeErr      error
		operationErr  error
		wantInvites   int
		wantStatus    int
		wantError     string
		wantPersisted bool
	}{
		{name: "nil inviter", nilInviter: true, roomID: existing.RoomID, wantPersisted: true},
		{name: "empty room skips invite", roomID: "", wantPersisted: true},
		{name: "blank room is forwarded", roomID: "  ", wantInvites: 1, wantPersisted: true},
		{name: "invite error stops before save", roomID: existing.RoomID, inviteErr: actionbase.StatusError(http.StatusForbidden, "invite denied"), wantInvites: 1, wantStatus: http.StatusForbidden, wantError: "invite denied"},
		{name: "save error stops before operation", roomID: existing.RoomID, storeErr: errors.New("save failed"), wantInvites: 1, wantStatus: http.StatusInternalServerError, wantError: "internal error: save failed"},
		{name: "operation error follows persistence", roomID: existing.RoomID, operationErr: errors.New("operation failed"), wantInvites: 1, wantStatus: http.StatusInternalServerError, wantError: "internal error: operation failed", wantPersisted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contact := existing
			contact.RoomID = tt.roomID
			harness := newRequestHarness(contact)
			harness.inviteErr = tt.inviteErr
			harness.store.upsertErr = tt.storeErr
			harness.conversation.operationErr = tt.operationErr
			if tt.nilInviter {
				harness.module.inviteRoom = nil
			}

			view, apiErr := harness.module.ResendPendingOutbound(context.Background(), contact, nil, "derived.example")
			if tt.wantStatus == 0 {
				want := contact
				if want.Domain == "" {
					want.Domain = "derived.example"
				}
				if apiErr != nil || !reflect.DeepEqual(RecordFromView(view), want) {
					t.Fatalf("resend = (%#v, %#v)", view, apiErr)
				}
			} else if apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("resend error = %#v, want status=%d error=%q", apiErr, tt.wantStatus, tt.wantError)
			}
			if len(harness.invites) != tt.wantInvites {
				t.Fatalf("invite count = %d, want %d", len(harness.invites), tt.wantInvites)
			}
			if got := len(harness.store.upserts()); (got > 0) != tt.wantPersisted {
				t.Fatalf("persisted = %v, want %v; calls=%#v", got > 0, tt.wantPersisted, harness.log.snapshot())
			}
			if tt.inviteErr != nil || tt.storeErr != nil {
				assertNoRequestOperationCall(t, harness.log.snapshot())
			}
		})
	}
}

func TestResendPendingOutboundRunsInsideExistingPeerBoundary(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:example.com", RoomID: "!direct:example.com", Status: "pending_outbound"}
	harness := newRequestHarness(existing)
	done := make(chan *actionbase.Error, 1)
	go harness.module.SerializePeer(existing.PeerMXID, func() {
		_, apiErr := harness.module.ResendPendingOutbound(context.Background(), existing, nil, "")
		done <- apiErr
	})

	select {
	case apiErr := <-done:
		if apiErr != nil {
			t.Fatal(apiErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resend acquired the non-reentrant peer boundary twice")
	}
}

func TestCreateRequestBuildsPendingOutboundAndPreservesWorkflowOrder(t *testing.T) {
	harness := newRequestHarness()
	harness.conversation.operation = map[string]any{"action": "contacts.request", "status": "pending_outbound"}
	harness.conversation.operationView = &dirextalkdomain.ConversationView{MatrixRoomID: "!created:example.com", Kind: dirextalkdomain.ConversationKindDirect}

	view, apiErr := harness.module.CreateRequest(context.Background(), "@alice:remote.example", map[string]any{
		"display_name":    " Alice ",
		"avatar_url":      " mxc://remote.example/alice ",
		"request_message": " hello ",
	}, "remote.example")
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	want := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", DisplayName: "Alice", AvatarURL: "mxc://remote.example/alice",
		Domain: "remote.example", RoomID: "!created:example.com", Status: "pending_outbound", Remark: "hello",
	}
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
		t.Fatalf("created contact = %#v, want %#v", got, want)
	}
	wantCreate := DirectRoomCreateRequest{
		PeerMXID: "@alice:remote.example", DisplayName: "Alice", Remark: "hello", FallbackRoomID: "!fallback:example.com",
	}
	if !reflect.DeepEqual(harness.creates, []DirectRoomCreateRequest{wantCreate}) {
		t.Fatalf("create requests = %#v, want %#v", harness.creates, wantCreate)
	}
	wantCalls := []string{
		"new-room-id",
		"create:@alice:remote.example",
		"list",
		"list-conversations",
		"upsert:!created:example.com",
		"delete-conversation:group:!created:example.com",
		"save-conversation:direct:!created:example.com",
		"operation:contacts.request:pending_outbound:!created:example.com",
	}
	if got := harness.log.snapshot(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("workflow calls = %#v, want %#v", got, wantCalls)
	}
}

func TestCreateRequestCompatibilityFailures(t *testing.T) {
	tests := []struct {
		name          string
		nilCreator    bool
		createdRoomID string
		createErr     *actionbase.Error
		storeErr      error
		operationErr  error
		wantRoomID    string
		wantStatus    int
		wantError     string
		wantPersisted bool
	}{
		{name: "nil creator keeps generated fallback", nilCreator: true, wantRoomID: "!fallback:example.com", wantPersisted: true},
		{name: "empty creator result replaces fallback", createdRoomID: "", wantRoomID: "", wantPersisted: true},
		{name: "create failure stops before save", createdRoomID: "!unused:example.com", createErr: actionbase.StatusError(http.StatusForbidden, "create denied"), wantStatus: http.StatusForbidden, wantError: "create denied"},
		{name: "save failure follows create", createdRoomID: "!created:example.com", storeErr: errors.New("save failed"), wantStatus: http.StatusInternalServerError, wantError: "internal error: save failed"},
		{name: "operation failure follows persistence", createdRoomID: "!created:example.com", operationErr: errors.New("operation failed"), wantStatus: http.StatusInternalServerError, wantError: "internal error: operation failed", wantPersisted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := newRequestHarness()
			harness.createRoomID = tt.createdRoomID
			harness.createErr = tt.createErr
			harness.store.upsertErr = tt.storeErr
			harness.conversation.operationErr = tt.operationErr
			if tt.nilCreator {
				harness.module.createRoom = nil
			}

			view, apiErr := harness.module.CreateRequest(context.Background(), "@alice:remote.example", nil, "remote.example")
			if tt.wantStatus == 0 {
				if apiErr != nil || view.RoomID != tt.wantRoomID || view.Status != "pending_outbound" {
					t.Fatalf("create = (%#v, %#v), want room=%q", view, apiErr, tt.wantRoomID)
				}
			} else if apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("create error = %#v, want status=%d error=%q", apiErr, tt.wantStatus, tt.wantError)
			}
			if got := len(harness.store.upserts()); (got > 0) != tt.wantPersisted {
				t.Fatalf("persisted = %v, want %v; calls=%#v", got > 0, tt.wantPersisted, harness.log.snapshot())
			}
			if tt.createErr != nil || tt.storeErr != nil {
				assertNoRequestOperationCall(t, harness.log.snapshot())
			}
		})
	}
}

func TestCreateReplacementRequestInheritsOnlyCompatibleFieldsWithoutMutatingParams(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:remote.example", RoomID: "!old:example.com", DisplayName: " Stored Alice ",
		DisplayNameOverride: true, AvatarURL: " mxc://example.com/old ", Domain: " stored.example ",
		Status: "accepted", Remark: "old remark",
	}
	harness := newRequestHarness(existing)
	raw := map[string]any{"display_name": " ", "avatar_url": " ", "domain": " ", "remote_node_base_url": "https://remote.example/_p2p"}
	original := make(map[string]any, len(raw))
	for key, value := range raw {
		original[key] = value
	}

	view, apiErr := harness.module.CreateReplacementRequest(context.Background(), existing, raw, "fallback.example")
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	want := dirextalkdomain.ContactRecord{
		PeerMXID: existing.PeerMXID, RoomID: "!created:example.com", DisplayName: "Stored Alice",
		AvatarURL: "mxc://example.com/old", Domain: "stored.example", Status: "pending_outbound",
	}
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
		t.Fatalf("replacement contact = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(raw, original) {
		t.Fatalf("replacement mutated params: got %#v want %#v", raw, original)
	}
	if len(harness.creates) != 1 || harness.creates[0].Remark != "" || harness.creates[0].DisplayName != "Stored Alice" {
		t.Fatalf("replacement create request = %#v", harness.creates)
	}
}

func TestCreateReplacementRequestUsesExplicitFieldsAndFallbackDomain(t *testing.T) {
	existing := dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:example.com", Status: "deleted", Remark: "old"}
	harness := newRequestHarness(existing)

	view, apiErr := harness.module.CreateReplacementRequest(context.Background(), existing, map[string]any{
		"display_name": " New Alice ", "avatar_url": " mxc://example.com/new ", "reason": " retry ",
	}, "fallback.example")
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	want := dirextalkdomain.ContactRecord{
		PeerMXID: existing.PeerMXID, RoomID: "!created:example.com", DisplayName: "New Alice",
		AvatarURL: "mxc://example.com/new", Domain: "fallback.example", Status: "pending_outbound", Remark: "retry",
	}
	if got := RecordFromView(view); !reflect.DeepEqual(got, want) {
		t.Fatalf("replacement contact = %#v, want %#v", got, want)
	}
}

func assertNoRequestOperationCall(t *testing.T, calls []string) {
	t.Helper()
	for _, call := range calls {
		if strings.HasPrefix(call, "operation:") {
			t.Fatalf("unexpected operation call after pre-operation failure: %#v", calls)
		}
	}
}
