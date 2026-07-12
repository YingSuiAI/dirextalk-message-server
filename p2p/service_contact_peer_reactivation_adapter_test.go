package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

type peerReactivationRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn peerReactivationRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type peerReactivationRemoteRequest struct {
	method   string
	path     string
	envelope envelope
	err      error
}

func TestPeerReactivationAdapterMapsRemoteResponsesAndStripsRoutingParam(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantResult contactsmodule.PeerReactivationResult
		wantStatus int
		wantError  string
	}{
		{name: "pending inbound", status: http.StatusOK, body: `{"status":"pending_inbound","room_id":" !old:remote.example "}`, wantResult: contactsmodule.PeerReactivationResult{PendingInbound: true, RoomID: "!old:remote.example"}},
		{name: "unknown success status remains retained", status: http.StatusOK, body: `{"status":"future_status","room_id":" !old:remote.example "}`, wantResult: contactsmodule.PeerReactivationResult{RoomID: "!old:remote.example"}},
		{name: "not found canonicalized", status: http.StatusNotFound, body: `{}`, wantStatus: http.StatusNotFound, wantError: "retained contact not found"},
		{name: "forbidden preserves remote error", status: http.StatusForbidden, body: `{"error":"reactivation forbidden"}`, wantStatus: http.StatusForbidden, wantError: "target node public action failed: status=403 error=reactivation forbidden"},
		{name: "server error preserves status", status: http.StatusInternalServerError, body: `{"error":"remote failure"}`, wantStatus: http.StatusInternalServerError, wantError: "target node public action failed: status=500 error=remote failure"},
		{name: "invalid success JSON becomes bad gateway", status: http.StatusOK, body: `{`, wantStatus: http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan peerReactivationRemoteRequest, 1)
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var received envelope
				err := json.NewDecoder(r.Body).Decode(&received)
				requests <- peerReactivationRemoteRequest{method: r.Method, path: r.URL.Path, envelope: received, err: err}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer remote.Close()

			service := NewService(Config{ServerName: "example.com", RemoteNodeAllowPrivateBaseURLs: true})
			result, apiErr := service.reactivatePeerContact(context.Background(), contactsmodule.PeerReactivationRequest{
				Contact: dirextalkdomain.ContactRecord{
					PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example", Remark: "stored remark is not forwarded",
				},
				RequesterMXID:     "@owner:example.com",
				RemoteNodeBaseURL: remote.URL + "/_p2p",
				DisplayName:       "Alice",
				AvatarURL:         "mxc://remote.example/alice",
				Domain:            "remote.example",
				Remark:            "friend",
			})
			if apiErr == nil {
				if tt.wantStatus != 0 || result != tt.wantResult {
					t.Fatalf("reactivatePeerContact = (%#v, nil), want (%#v, status=%d)", result, tt.wantResult, tt.wantStatus)
				}
			} else if apiErr.Status != tt.wantStatus || (tt.wantError != "" && apiErr.Error != tt.wantError) {
				t.Fatalf("reactivatePeerContact error = %#v, want status=%d error=%q", apiErr, tt.wantStatus, tt.wantError)
			}
			received := <-requests
			if received.err != nil {
				t.Fatal(received.err)
			}
			if received.path != "/_p2p/query" || received.method != http.MethodPost {
				t.Fatalf("remote request = %s %s", received.method, received.path)
			}
			if received.envelope.Action != "contacts.reactivate" {
				t.Fatalf("remote action = %q", received.envelope.Action)
			}
			if len(received.envelope.Params) != 6 {
				t.Fatalf("forwarded params = %#v, want exactly 6 fields", received.envelope.Params)
			}
			if _, exists := received.envelope.Params["remote_node_base_url"]; exists {
				t.Fatalf("remote routing parameter leaked into forwarded params: %#v", received.envelope.Params)
			}
			for key, want := range map[string]string{
				"room_id": "!old:remote.example", "requester_mxid": "@owner:example.com",
				"display_name": "Alice", "avatar_url": "mxc://remote.example/alice",
				"domain": "remote.example", "remark": "friend",
			} {
				if got := trimString(received.envelope.Params[key]); got != want {
					t.Fatalf("forwarded %s = %q, want %q; params=%#v", key, got, want, received.envelope.Params)
				}
			}
		})
	}
}

func TestPeerReactivationAdapterUsesLegacyDefaultURLAndMapsNetworkFailure(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	requests := 0
	service.remoteHTTPClient = &http.Client{Transport: peerReactivationRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		if got := req.URL.String(); got != "https://remote.example/_p2p/query" {
			t.Fatalf("default peer URL = %q", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"status":"invited","room_id":"!old:remote.example"}`)),
		}, nil
	})}
	result, apiErr := service.reactivatePeerContact(context.Background(), contactsmodule.PeerReactivationRequest{
		Contact:       dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example", RoomID: "!old:remote.example"},
		RequesterMXID: "@owner:example.com",
	})
	if apiErr != nil || result.RoomID != "!old:remote.example" || requests != 1 {
		t.Fatalf("default URL reactivation = (%#v, %#v), requests=%d", result, apiErr, requests)
	}

	service.remoteHTTPClient = &http.Client{Transport: peerReactivationRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network unavailable")
	})}
	result, apiErr = service.reactivatePeerContact(context.Background(), contactsmodule.PeerReactivationRequest{
		Contact:       dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example"},
		RequesterMXID: "@owner:example.com",
	})
	if result != (contactsmodule.PeerReactivationResult{}) || apiErr == nil || apiErr.Status != http.StatusBadGateway || !strings.Contains(apiErr.Error, "network unavailable") {
		t.Fatalf("network failure = (%#v, %#v)", result, apiErr)
	}
}

func TestPeerReactivationAdapterRejectsLocalOrMissingPeerNode(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	for _, peerMXID := range []string{"@alice:example.com", "alice"} {
		result, apiErr := service.reactivatePeerContact(context.Background(), contactsmodule.PeerReactivationRequest{
			Contact: dirextalkdomain.ContactRecord{PeerMXID: peerMXID},
		})
		if result != (contactsmodule.PeerReactivationResult{}) || apiErr == nil || apiErr.Status != http.StatusForbidden || apiErr.Error != "peer node is required to reactivate direct room" {
			t.Fatalf("peer %q reactivation = (%#v, %#v)", peerMXID, result, apiErr)
		}
	}
}

func TestPeerReactivationAdapterPreservesRemoteURLValidation(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	request := contactsmodule.PeerReactivationRequest{
		Contact:       dirextalkdomain.ContactRecord{PeerMXID: "@alice:remote.example"},
		RequesterMXID: "@owner:example.com",
	}

	for _, tt := range []struct {
		name       string
		remoteBase string
		wantError  string
	}{
		{name: "malformed", remoteBase: "://bad", wantError: "valid remote_node_base_url is required"},
		{name: "private host", remoteBase: "http://127.0.0.1/_p2p", wantError: "remote_node_base_url host must be public"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request.RemoteNodeBaseURL = tt.remoteBase
			result, apiErr := service.reactivatePeerContact(context.Background(), request)
			if result != (contactsmodule.PeerReactivationResult{}) || apiErr == nil || apiErr.Status != http.StatusBadRequest || apiErr.Error != tt.wantError {
				t.Fatalf("remote base %q = (%#v, %#v)", tt.remoteBase, result, apiErr)
			}
		})
	}
}
