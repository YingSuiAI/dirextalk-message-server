package p2p

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

func TestContactDirectRoomAdapterCreatesExactInviteRoomWithCurrentProfile(t *testing.T) {
	transport := &recordingTransport{roomID: " !created:example.com "}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner B",
		"avatar_url":   "mxc://example.com/owner-b",
	})

	roomID, apiErr := service.createContactDirectRoom(context.Background(), contactsmodule.DirectRoomCreateRequest{
		PeerMXID:       "@alice:remote.example",
		DisplayName:    "Alice",
		Remark:         "friend",
		FallbackRoomID: "!fallback:example.com",
	})
	if apiErr != nil || roomID != " !created:example.com " {
		t.Fatalf("createContactDirectRoom = (%q, %#v)", roomID, apiErr)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("create requests = %#v", transport.createRooms)
	}
	request := transport.createRooms[0]
	if request.CreatorMXID != "@owner:example.com" || request.CreatorDisplayName != "Owner B" || request.CreatorAvatarURL != "mxc://example.com/owner-b" ||
		request.Name != "Alice" || request.Visibility != "private" || request.RoomType != DirextalkRoomTypeDirect || !request.IsDirect ||
		!reflect.DeepEqual(request.InviteMXIDs, []string{"@alice:remote.example"}) {
		t.Fatalf("create request = %#v", request)
	}
	wantState := []RoomStateEvent{roomProfileForDirect(
		"Alice", "@owner:example.com", "@alice:remote.example", "Owner B", "mxc://example.com/owner-b", "friend", false,
	)}
	if !reflect.DeepEqual(request.InitialState, wantState) {
		t.Fatalf("initial state = %#v, want %#v", request.InitialState, wantState)
	}
}

func TestContactDirectRoomAdapterCreateFallbackAndErrorMapping(t *testing.T) {
	withoutTransport := NewService(Config{ServerName: "example.com"})
	roomID, apiErr := withoutTransport.createContactDirectRoom(context.Background(), contactsmodule.DirectRoomCreateRequest{
		PeerMXID:       "@alice:remote.example",
		FallbackRoomID: "!fallback:example.com",
	})
	if apiErr != nil || roomID != "!fallback:example.com" {
		t.Fatalf("transportless create = (%q, %#v)", roomID, apiErr)
	}
	emptyResultTransport := &failingAcceptReplacementTransport{}
	emptyResultService := NewServiceWithTransport(Config{ServerName: "example.com"}, emptyResultTransport)
	bootstrapService(t, emptyResultService)
	roomID, apiErr = emptyResultService.createContactDirectRoom(context.Background(), contactsmodule.DirectRoomCreateRequest{
		PeerMXID: "@alice:remote.example", FallbackRoomID: "!fallback:example.com",
	})
	if apiErr != nil || roomID != "" {
		t.Fatalf("empty transport result = (%q, %#v), want empty room without fallback", roomID, apiErr)
	}

	transport := &failingAcceptReplacementTransport{createErr: productpolicy.Forbidden("create denied")}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	roomID, apiErr = service.createContactDirectRoom(context.Background(), contactsmodule.DirectRoomCreateRequest{
		PeerMXID:       "@alice:remote.example",
		FallbackRoomID: "!fallback:example.com",
	})
	if roomID != "" || apiErr == nil || apiErr.Status != http.StatusForbidden || apiErr.Error != "create denied" {
		t.Fatalf("failed create = (%q, %#v)", roomID, apiErr)
	}
}

func TestContactDirectRoomAdapterUsesProvidedProfileSnapshot(t *testing.T) {
	transport := &recordingTransport{roomID: "!replacement:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	profile := contactsmodule.LocalProfileSnapshot{
		MXID: "@captured:example.com", DisplayName: "Captured Owner", AvatarURL: "mxc://example.com/captured",
	}

	roomID, apiErr := service.createContactDirectRoomWithProfile(context.Background(), contactsmodule.DirectRoomCreateRequest{
		PeerMXID: "@alice:remote.example",
	}, profile)
	if apiErr != nil || roomID != "!replacement:example.com" {
		t.Fatalf("create with profile = (%q, %#v)", roomID, apiErr)
	}
	request := transport.createRooms[0]
	if request.CreatorMXID != profile.MXID || request.CreatorDisplayName != profile.DisplayName || request.CreatorAvatarURL != profile.AvatarURL {
		t.Fatalf("create request profile = %#v, want %#v", request, profile)
	}
}

func TestContactDirectRoomAdapterInvitesWithCurrentProfile(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner B",
		"avatar_url":   "mxc://example.com/owner-b",
	})
	contact := dirextalkdomain.ContactRecord{
		RoomID: "!direct:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice", Remark: "friend",
	}
	if apiErr := service.inviteContactDirectRoom(context.Background(), contactsmodule.DirectRoomInviteRequest{Contact: contact}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if len(transport.inviteRequests) != 1 {
		t.Fatalf("invite requests = %#v", transport.inviteRequests)
	}
	request := transport.inviteRequests[0]
	if request.RoomID != contact.RoomID || request.InviterMXID != "@owner:example.com" || request.InviteeMXID != contact.PeerMXID || !request.IsDirect {
		t.Fatalf("invite request = %#v", request)
	}
	wantState := []RoomStateEvent{roomProfileForDirect(
		"Alice", "@owner:example.com", "@alice:remote.example", "Owner B", "mxc://example.com/owner-b", "friend", false,
	)}
	if !reflect.DeepEqual(request.InviteRoomState, wantState) {
		t.Fatalf("invite state = %#v, want %#v", request.InviteRoomState, wantState)
	}
}

func TestContactDirectRoomAdapterInviteCompatibilityErrors(t *testing.T) {
	tests := []struct {
		name       string
		transport  Transport
		roomID     string
		wantStatus int
		wantError  string
	}{
		{name: "no transport", roomID: "!direct:example.com"},
		{name: "empty room", transport: &recordingTransport{}},
		{name: "sender already left is tolerated", transport: &failingInviteTransport{err: productpolicy.Forbidden("sender is not joined to the dirextalk room")}, roomID: "!direct:example.com"},
		{name: "other policy error is preserved", transport: &failingInviteTransport{err: productpolicy.Forbidden("invite denied")}, roomID: "!direct:example.com", wantStatus: http.StatusForbidden, wantError: "invite denied"},
		{name: "ordinary error becomes internal", transport: &failingInviteTransport{err: errors.New("invite failed")}, roomID: "!direct:example.com", wantStatus: http.StatusInternalServerError, wantError: "internal error: invite failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewServiceWithTransport(Config{ServerName: "example.com"}, tt.transport)
			bootstrapService(t, service)
			apiErr := service.inviteContactDirectRoom(context.Background(), contactsmodule.DirectRoomInviteRequest{
				Contact: dirextalkdomain.ContactRecord{RoomID: tt.roomID, PeerMXID: "@alice:remote.example"},
			})
			if apiErr == nil {
				if tt.wantStatus != 0 {
					t.Fatalf("invite error = nil, want status=%d", tt.wantStatus)
				}
			} else if apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("invite error = %#v, want status=%d error=%q", apiErr, tt.wantStatus, tt.wantError)
			}
		})
	}
}

func TestContactDirectRoomAdapterPreservesBlankExistingRoom(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if apiErr := service.inviteContactDirectRoom(context.Background(), contactsmodule.DirectRoomInviteRequest{
		Contact: dirextalkdomain.ContactRecord{RoomID: "  ", PeerMXID: "@alice:remote.example"},
	}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if len(transport.inviteRequests) != 1 || transport.inviteRequests[0].RoomID != "  " {
		t.Fatalf("blank-room invite requests = %#v", transport.inviteRequests)
	}
}
