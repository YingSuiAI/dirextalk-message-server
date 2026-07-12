package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestContactRequestCreatesDirectInviteRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{roomID: "!dm:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Requester Nick",
		"avatar_url":   "mxc://example.com/requester",
	})

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})

	if contact.RoomID != "!dm:example.com" || contact.Status != "pending_outbound" {
		t.Fatalf("expected pending outbound contact with transport room, got %#v", contact)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected one transport room create, got %#v", transport.createRooms)
	}
	room := transport.createRooms[0]
	if room.CreatorMXID != "@owner:example.com" || !room.IsDirect || room.Visibility != "private" {
		t.Fatalf("expected private direct invite room, got %#v", room)
	}
	if room.CreatorDisplayName != "Requester Nick" {
		t.Fatalf("expected direct invite room to carry requester nickname, got %#v", room)
	}
	if room.CreatorAvatarURL != "mxc://example.com/requester" {
		t.Fatalf("expected direct invite room to carry requester avatar, got %#v", room)
	}
	var directProfile map[string]any
	for _, state := range room.InitialState {
		if state.Type == DirextalkRoomProfileEventType && state.Content["room_type"] == DirextalkRoomTypeDirect {
			directProfile = state.Content
		}
		if strings.HasPrefix(state.Type, "p2p.") {
			t.Fatalf("new direct rooms must not write legacy P2P product state, got %#v", room.InitialState)
		}
	}
	if directProfile == nil {
		t.Fatalf("expected native direct profile in direct invite room, got %#v", room.InitialState)
	}
	if directProfile["requester_mxid"] != "@owner:example.com" || directProfile["target_mxid"] != "@alice:remote.example" || directProfile["display_name"] != "Requester Nick" || directProfile["avatar_url"] != "mxc://example.com/requester" || directProfile["domain"] != "example.com" {
		t.Fatalf("expected requester profile in native direct profile state, got %#v", directProfile)
	}
	if len(room.InviteMXIDs) != 1 || room.InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("expected contact request to invite target user, got %#v", room.InviteMXIDs)
	}
}

func TestContactRequestRejectsSelfContact(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "contacts.request", map[string]any{
		"mxid":         "@owner:example.com",
		"display_name": "Me",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected self contact request to be rejected, got %#v", apiErr)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("self contact request must not create a direct room, got %#v", transport.createRooms)
	}
}

func TestContactRequestIsIdempotentForExistingDirectContact(t *testing.T) {
	transport := &recordingTransport{roomID: "!dm:example.com"}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)

	first := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})
	second := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Duplicate",
	})

	if second.RoomID != first.RoomID || second.Status != "pending_outbound" {
		t.Fatalf("expected duplicate contact request to reuse pending outbound room, got first=%#v second=%#v", first, second)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("duplicate contact request must not create another direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 1 {
		t.Fatalf("duplicate pending contact request must resend direct invite, got %#v", transport.inviteRequests)
	}
	if invite := transport.inviteRequests[0]; invite.RoomID != first.RoomID || invite.InviterMXID != "@owner:example.com" || invite.InviteeMXID != "@alice:remote.example" || !invite.IsDirect {
		t.Fatalf("unexpected repeated direct invite request: %#v", invite)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      first.RoomID,
		"peer_mxid":    first.PeerMXID,
		"display_name": first.DisplayName,
		"domain":       first.Domain,
	})
	afterAccept := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Again",
	})

	if afterAccept.RoomID != first.RoomID || afterAccept.Status != accepted.Status {
		t.Fatalf("expected duplicate request after accept to return accepted contact, got %#v", afterAccept)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("duplicate accepted contact request must not create another direct room, got %#v", transport.createRooms)
	}
}

func TestPendingOutboundContactRequestKeepsOldRoomWhenSenderLeft(t *testing.T) {
	transport := &failingInviteTransport{
		err: productpolicy.Forbidden("sender is not joined to the dirextalk room"),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_outbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Again",
		"domain":       "remote.example",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected pending outbound retry to keep old room, got %#v", contact)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("pending outbound retry must not create a replacement direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 1 || transport.inviteRequests[0].RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected retry to attempt old-room invite once, got %#v", transport.inviteRequests)
	}
}

func TestAcceptedContactRequestCreatesPendingInviteWhenPeerNoLongerRetainsOldRoom(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	transport := &recordingTransport{roomID: "!fresh-dm:example.com"}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!fresh-dm:example.com" {
		t.Fatalf("expected peer-deleted re-request to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("peer-deleted re-request must not join the old room before approval, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirextalkRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("peer-deleted re-request must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestAcceptedContactRequestCreatesReplacementWhenOldRoomInviteSenderLeft(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	transport := &failingInviteTransport{
		err: productpolicy.Forbidden("sender is not joined to the dirextalk room"),
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!recorded:example.com" {
		t.Fatalf("expected left-sender re-request to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirextalkRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("left-sender re-request must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestContactRequestAcceptsPendingInboundDirectInvite(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected outbound request to accept pending inbound invite, got %#v", contact)
	}
	if len(transport.joinRequests) != 1 || transport.joinRequests[0].RoomIDOrAlias != "!old-dm:remote.example" {
		t.Fatalf("expected pending inbound invite to be joined, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("accepting pending inbound invite must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestContactRequestReactivatesStalePendingInboundDirectInvite(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "invited",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		err: productpolicy.Forbidden("direct room join requires invite"),
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected stale inbound invite to reactivate old direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 2 {
		t.Fatalf("expected join retry after peer reactivation, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("stale inbound invite reactivation must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestContactRequestKeepsOldRoomWhenPendingInboundInviteIsGone(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "retained contact not found"})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!fresh-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected stale pending inbound to wait for approval in the old room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("stale pending inbound retry must preserve the old direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 1 ||
		transport.inviteRequests[0].RoomID != "!old-dm:remote.example" ||
		transport.inviteRequests[0].InviteeMXID != "@alice:remote.example" {
		t.Fatalf("expected pending invite in old direct room, got %#v", transport.inviteRequests)
	}
}

func TestContactRequestKeepsOldRoomWhenPeerRecordsPendingFromStaleInboundInvite(t *testing.T) {
	remoteActions := []string{}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		remoteActions = append(remoteActions, req.Action)
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "pending_inbound",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!fresh-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice Again",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected stale inbound request to become pending outbound in old room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("stale inbound retry must preserve the old direct room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("peer-recorded pending request must not invite from a left sender, got %#v", transport.inviteRequests)
	}
}

func TestContactAcceptJoinsDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner B",
		"avatar_url":   "mxc://example.com/owner-b",
	})
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      "!dm:remote.example",
		"peer_mxid":    "@alice:remote.example",
		"display_name": "Wrong Param",
		"domain":       "remote.example",
	})

	if accepted.Status != "accepted" {
		t.Fatalf("expected accepted contact, got %#v", accepted)
	}
	if accepted.DisplayName != "Alice" {
		t.Fatalf("expected accept to preserve stored requester nickname, got %#v", accepted)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:example.com in !dm:remote.example" {
		t.Fatalf("expected accept to join direct room through transport, got %#v", transport.joins)
	}
	if transport.joinRequests[0].DisplayName != "Owner B" || transport.joinRequests[0].AvatarURL != "mxc://example.com/owner-b" {
		t.Fatalf("expected accept join to carry accepting owner profile, got %#v", transport.joinRequests[0])
	}
	if !transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected accept join to be marked as direct contact reactivation, got %#v", transport.joinRequests[0])
	}
}

func TestContactAcceptRetriesFederatedJoinInProgress(t *testing.T) {
	transport := &failOnceJoinTransport{
		err: errors.New(`contents=[] msg={
				"errcode": "M_LIMIT_EXCEEDED",
				"error": "There is already a federated join to this room in progress. Please wait for it to finish."
			} code=429 wrapped=`),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":   "!dm:remote.example",
		"peer_mxid": "@alice:remote.example",
		"domain":    "remote.example",
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!dm:remote.example" {
		t.Fatalf("expected accepted contact after retry, got %#v", accepted)
	}
	if transport.attempts != 2 || len(transport.joins) != 2 {
		t.Fatalf("expected one retry for federated join in progress, attempts=%d joins=%#v", transport.attempts, transport.joins)
	}
}

func TestContactAcceptUsesDirectReactivationJoinWhenInviteIsGone(t *testing.T) {
	transport := &directReactivationJoinTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      "!old-dm:remote.example",
		"peer_mxid":    "@alice:remote.example",
		"server_names": []string{"remote.example"},
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected accept to restore pending inbound contact in old room, got %#v", accepted)
	}
	if len(transport.joinRequests) != 1 || !transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected accept to use direct reactivation join, got %#v", transport.joinRequests)
	}
	if transport.joinRequests[0].ServerNames[0] != "remote.example" {
		t.Fatalf("expected accept to preserve remote server names, got %#v", transport.joinRequests[0])
	}
}

func TestContactAcceptCreatesReplacementDirectRoomWhenOldRoomCannotBeRejoined(t *testing.T) {
	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!replacement-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           100,
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	mustHandle[ownerProfile](t, service, "profile.update", map[string]any{
		"display_name": "Owner B",
		"avatar_url":   "mxc://example.com/owner-b",
	})
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":   "!old-dm:remote.example",
		"peer_mxid": "@alice:remote.example",
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!replacement-dm:example.com" {
		t.Fatalf("expected accept to create replacement direct room when old room cannot be rejoined, got %#v", accepted)
	}
	if len(transport.joinRequests) != 1 || !transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected accept to try old-room direct reactivation first, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 {
		t.Fatalf("expected one replacement direct room, got %#v", transport.createRooms)
	}
	room := transport.createRooms[0]
	if room.CreatorMXID != "@owner:example.com" || len(room.InviteMXIDs) != 1 || room.InviteMXIDs[0] != "@alice:remote.example" || !room.IsDirect || room.RoomType != DirextalkRoomTypeDirect {
		t.Fatalf("unexpected replacement direct room request: %#v", room)
	}
}

func TestContactAcceptAlreadyAcceptedDoesNotJoinDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	accepted := mustHandle[contactRecord](t, service, "contacts.requests.accept", map[string]any{
		"room_id":      "!dm:remote.example",
		"peer_mxid":    "@alice:remote.example",
		"display_name": "Wrong Param",
		"domain":       "remote.example",
	})

	if accepted.Status != "accepted" || accepted.RoomID != "!dm:remote.example" {
		t.Fatalf("expected existing accepted contact, got %#v", accepted)
	}
	if len(transport.joins) != 0 {
		t.Fatalf("accepted contact accept must not join direct room again, got %#v", transport.joins)
	}
}

func TestContactDeleteLeavesDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})

	if result["status"] != "ok" {
		t.Fatalf("expected delete status ok, got %#v", result)
	}
	if len(transport.leaves) != 1 || transport.leaves[0] != "@owner:example.com from !dm:remote.example" {
		t.Fatalf("expected contact delete to leave direct room through transport, got %#v", transport.leaves)
	}
}

func TestContactDeleteMarksDeletedWhenMatrixMembershipAlreadyLeft(t *testing.T) {
	transport := &failingLeaveTransport{err: errors.New(`user "@owner:example.com" is not joined to the room (membership is "leave"`)}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})

	if result["status"] != "ok" {
		t.Fatalf("expected delete ok when Matrix membership is already leave, got %#v", result)
	}
	contact, ok, err := service.lookupContactByRoom(context.Background(), "!dm:remote.example")
	if err != nil || !ok || contact.Status != "deleted" {
		t.Fatalf("expected contact to be marked deleted, ok=%v contact=%#v err=%v", ok, contact, err)
	}
}

func TestContactDeleteDoesNotPersistWhenLeaveFails(t *testing.T) {
	tests := []struct {
		name       string
		leaveErr   error
		wantStatus int
		wantError  string
	}{
		{name: "ordinary error", leaveErr: errors.New("leave failed"), wantStatus: http.StatusInternalServerError, wantError: "internal error: leave failed"},
		{name: "policy error", leaveErr: productpolicy.Forbidden("leave forbidden"), wantStatus: http.StatusForbidden, wantError: "leave forbidden"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &failingLeaveTransport{err: tt.leaveErr}
			service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
			bootstrapService(t, service)
			existing := contactRecord{
				RoomID: "!dm:remote.example", PeerMXID: "@alice:remote.example", DisplayName: "Alice", Status: "accepted",
			}
			if err := service.saveContact(context.Background(), existing); err != nil {
				t.Fatal(err)
			}

			result, apiErr := service.Handle(context.Background(), "contacts.delete", map[string]any{"room_id": existing.RoomID})
			if result != nil || apiErr == nil || apiErr.Status != tt.wantStatus || apiErr.Error != tt.wantError {
				t.Fatalf("contacts.delete leave failure = (%#v, %#v)", result, apiErr)
			}
			contact, ok, err := service.lookupContactByRoom(context.Background(), existing.RoomID)
			if err != nil || !ok || contact.Status != existing.Status {
				t.Fatalf("contact after failed leave = %#v ok=%v err=%v, want accepted snapshot", contact, ok, err)
			}
		})
	}
}

func TestContactRequestRestoresDeletedDirectRoomThroughTransport(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	restored := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":         "@alice:remote.example",
		"display_name": "Alice Updated",
		"domain":       "remote.example",
	})

	if restored.Status != "accepted" || restored.RoomID != "!dm:remote.example" {
		t.Fatalf("expected deleted contact request to restore original room, got %#v", restored)
	}
	if restored.DisplayName != "Alice Updated" {
		t.Fatalf("expected re-request to refresh contact display name, got %#v", restored)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("deleted contact request must not create a new direct room, got %#v", transport.createRooms)
	}
	if len(transport.joins) != 1 || transport.joins[0] != "@owner:example.com in !dm:remote.example" {
		t.Fatalf("expected deleted contact request to rejoin original direct room, got %#v", transport.joins)
	}
}
