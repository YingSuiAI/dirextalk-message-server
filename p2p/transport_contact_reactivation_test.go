package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDeletedContactRequestReactivatesOldDirectRoomThroughPeerNode(t *testing.T) {
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
		if trimString(req.Params["room_id"]) != "!old-dm:remote.example" ||
			trimString(req.Params["requester_mxid"]) != "@owner:example.com" {
			t.Fatalf("unexpected reactivation params %#v", req.Params)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "invited",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &recordingTransport{roomID: "!new-dm:example.com"}
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
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice New",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected re-add to restore the old direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 1 ||
		transport.joinRequests[0].RoomIDOrAlias != "!old-dm:remote.example" {
		t.Fatalf("expected rejoin of original room after peer reactivation, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("re-adding a retained peer must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestDeletedContactRequestWaitsForFederatedReactivationInvite(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
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
		recordingTransport: recordingTransport{roomID: "!new-dm:example.com"},
		err:                productpolicy.Forbidden("direct room join requires invite"),
		failures:           3,
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
		Status:      "deleted",
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
		t.Fatalf("expected delayed invite reactivation to restore old direct room, got %#v", contact)
	}
	if len(transport.joinRequests) != 4 {
		t.Fatalf("expected join retries until invite is visible, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("reactivation retry must not create a replacement direct room, got %#v", transport.createRooms)
	}
}

func TestFreshContactRequestRestoresPeerRetainedDirectRoom(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		if trimString(req.Params["room_id"]) != "" ||
			trimString(req.Params["requester_mxid"]) != "@owner:example.com" {
			t.Fatalf("unexpected fresh reactivation params %#v", req.Params)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "invited",
			"room_id": "!old-dm:remote.example",
		})
	}))
	defer remote.Close()

	transport := &recordingTransport{roomID: "!new-dm:example.com"}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "accepted" || contact.RoomID != "!old-dm:remote.example" {
		t.Fatalf("expected fresh request to restore peer-retained old room, got %#v", contact)
	}
	if len(transport.joinRequests) != 1 ||
		transport.joinRequests[0].RoomIDOrAlias != "!old-dm:remote.example" ||
		!transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected direct reactivation join of old room, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 0 {
		t.Fatalf("fresh retained-peer restore must not create a new direct room, got %#v", transport.createRooms)
	}
}

func TestFreshContactRequestCreatesReplacementWhenRetainedRoomCannotRejoin(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
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
		recordingTransport: recordingTransport{roomID: "!new-dm:example.com"},
		err:                errors.New("eventauth: user is not allowed to change their membership from \"leave\" to \"join\" as join rule \"invite\" forbids it"),
		failures:           6,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!new-dm:example.com" {
		t.Fatalf("expected fresh replacement direct request, got %#v", contact)
	}
	if len(transport.joinRequests) != 1 ||
		transport.joinRequests[0].RoomIDOrAlias != "!old-dm:remote.example" ||
		!transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected direct reactivation retries against old room, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 || len(transport.createRooms[0].InviteMXIDs) != 1 || transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("expected replacement direct room invite, got %#v", transport.createRooms)
	}
}

func TestFreshContactRequestCreatesReplacementWhenRetainedLocalRoomVersionIsMissing(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req envelope
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Action != "contacts.reactivate" {
			t.Fatalf("expected contacts.reactivate, got %#v", req)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "invited",
			"room_id": "!old-dm:example.com",
		})
	}))
	defer remote.Close()

	transport := &failOnceJoinTransport{
		recordingTransport: recordingTransport{roomID: "!new-dm:example.com"},
		err:                errors.New(`error joining local room: "gomatrixserverlib: unsupported room version ''"`),
		failures:           6,
	}
	service := NewServiceWithTransport(Config{
		ServerName:                     "example.com",
		RemoteNodeAllowPrivateBaseURLs: true,
	}, transport)
	bootstrapService(t, service)

	contact := mustHandle[contactRecord](t, service, "contacts.request", map[string]any{
		"mxid":                 "@alice:remote.example",
		"display_name":         "Alice",
		"domain":               "remote.example",
		"remote_node_base_url": remote.URL + "/_p2p",
	})

	if contact.Status != "pending_outbound" || contact.RoomID != "!new-dm:example.com" {
		t.Fatalf("expected missing local room version to create replacement direct request, got %#v", contact)
	}
	if len(transport.joinRequests) != 1 ||
		transport.joinRequests[0].RoomIDOrAlias != "!old-dm:example.com" ||
		!transport.joinRequests[0].DirectContactReactivation {
		t.Fatalf("expected direct reactivation attempt against old local room, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 || len(transport.createRooms[0].InviteMXIDs) != 1 || transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("expected replacement direct room invite, got %#v", transport.createRooms)
	}
}

func TestAcceptedContactFreshInviteUsesReplacementWhenRetainedRoomAlreadyJoined(t *testing.T) {
	transport := &failingInviteTransport{
		recordingTransport: recordingTransport{},
		err:                errors.New("user is already joined to room"),
	}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:              "!old-dm:example.com",
		PeerMXID:            "@alice:remote.example",
		DisplayName:         "Local Remark",
		DisplayNameOverride: true,
		AvatarURL:           "mxc://example.com/old",
		Domain:              "remote.example",
		Status:              "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	if err := service.savePendingInboundContact(context.Background(), contactRecord{
		RoomID:      "!new-dm:remote.example",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice Remote",
		AvatarURL:   "mxc://remote.example/new",
		Domain:      "remote.example",
		Status:      "pending_inbound",
	}); err != nil {
		t.Fatal(err)
	}

	contact, ok, err := service.lookupContactByPeer(context.Background(), "@alice:remote.example")
	if err != nil || !ok {
		t.Fatalf("expected accepted replacement contact, ok=%v err=%v", ok, err)
	}
	if contact.RoomID != "!new-dm:remote.example" || contact.Status != "accepted" {
		t.Fatalf("expected accepted contact to move to replacement room, got %#v", contact)
	}
	if contact.DisplayName != "Local Remark" || !contact.DisplayNameOverride {
		t.Fatalf("replacement must preserve local contact remark, got %#v", contact)
	}
	if len(transport.inviteRequests) != 1 || transport.inviteRequests[0].RoomID != "!old-dm:example.com" {
		t.Fatalf("expected retained-room invite attempt, got %#v", transport.inviteRequests)
	}
	if len(transport.joinRequests) != 1 || transport.joinRequests[0].RoomIDOrAlias != "!new-dm:remote.example" {
		t.Fatalf("expected owner to join replacement direct room, got %#v", transport.joinRequests)
	}
}

func TestContactReactivateTreatsAlreadyJoinedAsRestored(t *testing.T) {
	transport := &failingInviteTransport{err: errors.New("user is already joined to room")}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:example.com",
		PeerMXID:    "@alice:remote.example",
		DisplayName: "Alice",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.reactivate", map[string]any{
		"requester_mxid": "@alice:remote.example",
	})

	if result["status"] != "invited" || result["room_id"] != "!old-dm:example.com" {
		t.Fatalf("expected already-joined reactivation to return old room, got %#v", result)
	}
	if len(transport.inviteRequests) != 1 || transport.inviteRequests[0].RoomID != "!old-dm:example.com" {
		t.Fatalf("expected one retained-room invite attempt, got %#v", transport.inviteRequests)
	}
}

func TestDeletedContactRequestCreatesFreshRequestWhenPeerNoLongerRetainsOldRoom(t *testing.T) {
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
		Status:      "deleted",
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
		t.Fatalf("expected both-deleted re-add to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation probe, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("expected retained-contact probe to avoid old-room join before pending invite, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirextalkRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("both-deleted re-add must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestDeletedContactRequestCreatesReplacementRoomWhenPeerRecordsInboundRequest(t *testing.T) {
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
		Status:      "deleted",
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
		t.Fatalf("expected peer-recorded re-request to create a replacement direct room, got %#v", contact)
	}
	if len(remoteActions) != 1 || remoteActions[0] != "contacts.reactivate" {
		t.Fatalf("expected one peer reactivation call, got %#v", remoteActions)
	}
	if len(transport.joinRequests) != 0 {
		t.Fatalf("peer-recorded pending request must not join old direct room, got %#v", transport.joinRequests)
	}
	if len(transport.createRooms) != 1 ||
		transport.createRooms[0].RoomType != DirextalkRoomTypeDirect ||
		len(transport.createRooms[0].InviteMXIDs) != 1 ||
		transport.createRooms[0].InviteMXIDs[0] != "@alice:remote.example" {
		t.Fatalf("peer-recorded pending request must create a replacement direct invite room, got %#v", transport.createRooms)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("replacement room creation carries the invite, must not send old-room invite, got %#v", transport.inviteRequests)
	}
}

func TestContactReactivateInvitesOnlyRetainedAcceptedPeer(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:example.com",
		PeerMXID:    "@owner:example.com",
		DisplayName: "Owner",
		Domain:      "example.com",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	result := mustHandle[map[string]any](t, service, "contacts.reactivate", map[string]any{
		"room_id":        "!old-dm:example.com",
		"requester_mxid": "@owner:example.com",
	})

	if result["status"] != "invited" || result["room_id"] != "!old-dm:example.com" {
		t.Fatalf("expected retained contact reactivation invite, got %#v", result)
	}
	if len(transport.invites) != 1 ||
		transport.invites[0] != "@owner:remote.example -> @owner:example.com in !old-dm:example.com" {
		t.Fatalf("expected peer node to invite requester back to old direct room, got %#v", transport.invites)
	}

	if _, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id":        "!other:example.com",
		"requester_mxid": "@owner:example.com",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected mismatched room reactivation to be rejected, got %#v", apiErr)
	}
}

func TestContactReactivateTreatsDeletedPeerAsNotRetained(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!old-dm:example.com",
		PeerMXID:    "@owner:example.com",
		DisplayName: "Owner",
		Domain:      "example.com",
		Status:      "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	if _, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id":        "!old-dm:example.com",
		"requester_mxid": "@owner:example.com",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected deleted retained room to be unavailable for reactivation, got %#v", apiErr)
	}
	contact, ok, err := service.lookupContactByPeer(context.Background(), "@owner:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || contact.Status != "deleted" || contact.RoomID != "!old-dm:example.com" {
		t.Fatalf("expected deleted retained contact to remain unchanged, got ok=%v contact=%#v", ok, contact)
	}
	if len(transport.inviteRequests) != 0 {
		t.Fatalf("deleted peer request must not invite from a left sender, got %#v", transport.inviteRequests)
	}
}

func TestContactReactivateDoesNotTrustCallerProfileFields(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:   "!old-dm:example.com",
		PeerMXID: "@owner:example.com",
		Status:   "deleted",
	}); err != nil {
		t.Fatal(err)
	}

	if _, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id":        "!old-dm:example.com",
		"requester_mxid": "@owner:example.com",
		"display_name":   "Spoofed Owner",
		"avatar_url":     "mxc://evil/avatar",
		"domain":         "evil.example",
	}); apiErr == nil || apiErr.Status != http.StatusNotFound {
		t.Fatalf("expected deleted retained room to be unavailable for reactivation, got %#v", apiErr)
	}
	contact, ok, err := service.lookupContactByPeer(context.Background(), "@owner:example.com")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("expected retained contact")
	}
	if contact.DisplayName != "" || contact.AvatarURL != "" || contact.Domain != "" || contact.Status != "deleted" {
		t.Fatalf("contacts.reactivate must not trust caller-supplied profile fields, got %#v", contact)
	}
}

func TestContactReactivateRejectsSelfInvite(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "remote.example"}, transport)
	bootstrapService(t, service)
	if err := service.saveContact(context.Background(), contactRecord{
		RoomID:      "!self-dm:remote.example",
		PeerMXID:    "@owner:remote.example",
		DisplayName: "Self",
		Domain:      "remote.example",
		Status:      "accepted",
	}); err != nil {
		t.Fatal(err)
	}

	if _, apiErr := service.Handle(context.Background(), "contacts.reactivate", map[string]any{
		"room_id":        "!self-dm:remote.example",
		"requester_mxid": "@owner:remote.example",
	}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected self reactivation to be rejected, got %#v", apiErr)
	}
	if len(transport.invites) != 0 {
		t.Fatalf("self reactivation must not create an invite, got %#v", transport.invites)
	}
}

func TestAcceptedContactRequestMutationsDoNotBypassDeleteLeave(t *testing.T) {
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

	rejected := mustHandle[contactRecord](t, service, "contacts.requests.reject", map[string]any{
		"room_id": "!dm:remote.example",
	})
	if rejected.Status != "accepted" {
		t.Fatalf("request reject must not downgrade accepted contact, got %#v", rejected)
	}
	mustHandle[map[string]any](t, service, "contacts.requests.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})
	contact, ok, err := service.lookupContactByRoom(context.Background(), "!dm:remote.example")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || contact.Status != "accepted" {
		t.Fatalf("request delete must not delete accepted contact, got %#v ok=%v", contact, ok)
	}

	result := mustHandle[map[string]any](t, service, "contacts.delete", map[string]any{
		"room_id": "!dm:remote.example",
	})

	if result["status"] != "ok" {
		t.Fatalf("expected delete status ok, got %#v", result)
	}
	if len(transport.leaves) != 1 || transport.leaves[0] != "@owner:example.com from !dm:remote.example" {
		t.Fatalf("expected contact delete to leave direct room after request mutations, got %#v", transport.leaves)
	}
}
