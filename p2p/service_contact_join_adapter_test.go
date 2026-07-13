package p2p

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/productpolicy"
	contactsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/contacts"
)

type scriptedContactJoinTransport struct {
	recordingTransport
	results []JoinRoomResult
	errors  []error
	calls   int
}

type cancelingContactJoinTransport struct {
	recordingTransport
	cancel context.CancelFunc
	calls  int
}

func (t *cancelingContactJoinTransport) JoinRoom(_ context.Context, request JoinRoomRequest) (JoinRoomResult, error) {
	t.calls++
	t.joinRequests = append(t.joinRequests, request)
	t.cancel()
	return JoinRoomResult{}, errors.New("already a federated join to this room in progress")
}

func (t *scriptedContactJoinTransport) JoinRoom(_ context.Context, request JoinRoomRequest) (JoinRoomResult, error) {
	t.joins = append(t.joins, request.UserMXID+" in "+request.RoomIDOrAlias)
	t.joinRequests = append(t.joinRequests, request)
	index := t.calls
	t.calls++
	var result JoinRoomResult
	if index < len(t.results) {
		result = t.results[index]
	}
	if index < len(t.errors) {
		return result, t.errors[index]
	}
	return result, nil
}

func TestContactJoinAdapterBuildsExactRequestAndPreservesRoomID(t *testing.T) {
	tests := []struct {
		name            string
		mode            contactsmodule.DirectRoomJoinMode
		resultRoomID    string
		serverNames     []string
		fallbackServer  bool
		wantRoomID      string
		wantServerNames []string
	}{
		{name: "normal raw result", mode: contactsmodule.DirectRoomJoinNormal, resultRoomID: " !joined:example.com ", serverNames: []string{"one.example"}, wantRoomID: " !joined:example.com ", wantServerNames: []string{"one.example"}},
		{name: "reactivation blank result", mode: contactsmodule.DirectRoomJoinReactivation, resultRoomID: "  ", wantRoomID: "!direct:remote.example"},
		{name: "reactivation derives room server", mode: contactsmodule.DirectRoomJoinReactivation, fallbackServer: true, wantRoomID: "!direct:remote.example", wantServerNames: []string{"remote.example"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &scriptedContactJoinTransport{results: []JoinRoomResult{{RoomID: tt.resultRoomID}}}
			service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
			profile := contactsmodule.LocalProfileSnapshot{
				MXID: "@owner:example.com", DisplayName: "Owner", AvatarURL: "mxc://example.com/owner",
			}
			outcome := service.joinContactDirectRoomTransport(context.Background(), contactsmodule.DirectRoomJoinRequest{
				RoomID: "!direct:remote.example", Profile: profile, ServerNames: tt.serverNames,
				Mode: tt.mode, UseRoomServerFallback: tt.fallbackServer,
			})
			if outcome.Kind != contactsmodule.DirectRoomJoinSucceeded || outcome.RoomID != tt.wantRoomID || outcome.Failure != nil {
				t.Fatalf("join outcome = %#v", outcome)
			}
			if len(transport.joinRequests) != 1 {
				t.Fatalf("join requests = %#v", transport.joinRequests)
			}
			request := transport.joinRequests[0]
			if request.RoomIDOrAlias != "!direct:remote.example" || request.UserMXID != profile.MXID || request.DisplayName != profile.DisplayName || request.AvatarURL != profile.AvatarURL ||
				request.DirectContactReactivation != (tt.mode == contactsmodule.DirectRoomJoinReactivation) || !reflect.DeepEqual(request.ServerNames, tt.wantServerNames) {
				t.Fatalf("join request = %#v, want servers=%#v", request, tt.wantServerNames)
			}
		})
	}
}

func TestContactJoinAdapterClassifiesFailures(t *testing.T) {
	tests := []struct {
		name       string
		mode       contactsmodule.DirectRoomJoinMode
		err        error
		wantKind   contactsmodule.DirectRoomJoinKind
		wantStatus int
		wantError  string
	}{
		{name: "normal invite required", mode: contactsmodule.DirectRoomJoinNormal, err: productpolicy.Forbidden("direct room join requires invite"), wantKind: contactsmodule.DirectRoomJoinInviteRequired},
		{name: "normal failure", mode: contactsmodule.DirectRoomJoinNormal, err: errors.New("join failed"), wantKind: contactsmodule.DirectRoomJoinFailed, wantStatus: http.StatusInternalServerError, wantError: "internal error: join failed"},
		{name: "reactivation unavailable", mode: contactsmodule.DirectRoomJoinReactivation, err: errors.New("InputWasRejected"), wantKind: contactsmodule.DirectRoomJoinRetainedUnavailable, wantStatus: http.StatusInternalServerError, wantError: "internal error: InputWasRejected"},
		{name: "reactivation failure", mode: contactsmodule.DirectRoomJoinReactivation, err: errors.New("join failed"), wantKind: contactsmodule.DirectRoomJoinFailed, wantStatus: http.StatusInternalServerError, wantError: "internal error: join failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &scriptedContactJoinTransport{errors: []error{tt.err}}
			service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
			outcome := service.joinContactDirectRoomTransport(context.Background(), contactsmodule.DirectRoomJoinRequest{
				RoomID: "!direct:remote.example", Profile: contactsmodule.LocalProfileSnapshot{MXID: "@owner:example.com"}, Mode: tt.mode,
			})
			if outcome.Kind != tt.wantKind || transport.calls != 1 {
				t.Fatalf("join outcome = %#v, calls=%d", outcome, transport.calls)
			}
			if tt.wantStatus == 0 {
				if outcome.Failure != nil {
					t.Fatalf("join failure = %#v", outcome.Failure)
				}
			} else if outcome.Failure == nil || outcome.Failure.Status != tt.wantStatus || outcome.Failure.Error != tt.wantError {
				t.Fatalf("join failure = %#v, want status=%d error=%q", outcome.Failure, tt.wantStatus, tt.wantError)
			}
		})
	}
}

func TestContactJoinAdapterUsesModeSpecificRetry(t *testing.T) {
	tests := []struct {
		name string
		mode contactsmodule.DirectRoomJoinMode
		err  error
	}{
		{name: "normal retries federated join", mode: contactsmodule.DirectRoomJoinNormal, err: errors.New("already a federated join to this room in progress")},
		{name: "reactivation retries invite required", mode: contactsmodule.DirectRoomJoinReactivation, err: productpolicy.Forbidden("direct room join requires invite")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &failOnceJoinTransport{err: tt.err}
			service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
			outcome := service.joinContactDirectRoomTransport(context.Background(), contactsmodule.DirectRoomJoinRequest{
				RoomID: "!direct:remote.example", Profile: contactsmodule.LocalProfileSnapshot{MXID: "@owner:example.com"}, Mode: tt.mode,
			})
			if outcome.Kind != contactsmodule.DirectRoomJoinSucceeded || transport.attempts != 2 {
				t.Fatalf("join outcome = %#v, attempts=%d", outcome, transport.attempts)
			}
		})
	}
}

func TestContactJoinAdapterTransportAndEmptyRoomNoop(t *testing.T) {
	withoutTransport := NewService(Config{ServerName: "example.com"})
	outcome := withoutTransport.joinContactDirectRoomTransport(context.Background(), contactsmodule.DirectRoomJoinRequest{RoomID: "!direct:example.com"})
	if outcome.Kind != contactsmodule.DirectRoomJoinSucceeded || outcome.RoomID != "!direct:example.com" {
		t.Fatalf("transportless join = %#v", outcome)
	}

	transport := &scriptedContactJoinTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	outcome = service.joinContactDirectRoomTransport(context.Background(), contactsmodule.DirectRoomJoinRequest{})
	if outcome.Kind != contactsmodule.DirectRoomJoinSucceeded || outcome.RoomID != "" || transport.calls != 0 {
		t.Fatalf("empty-room join = %#v, calls=%d", outcome, transport.calls)
	}
	outcome = service.joinContactDirectRoomTransport(context.Background(), contactsmodule.DirectRoomJoinRequest{RoomID: "  "})
	if outcome.Kind != contactsmodule.DirectRoomJoinSucceeded || outcome.RoomID != "  " || transport.calls != 1 {
		t.Fatalf("blank-room join = %#v, calls=%d", outcome, transport.calls)
	}
}

func TestContactJoinAdapterStopsRetryWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := &cancelingContactJoinTransport{cancel: cancel}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)

	outcome := service.joinContactDirectRoomTransport(ctx, contactsmodule.DirectRoomJoinRequest{
		RoomID: "!direct:remote.example", Mode: contactsmodule.DirectRoomJoinNormal,
	})
	if outcome.Kind != contactsmodule.DirectRoomJoinFailed || outcome.Failure == nil ||
		outcome.Failure.Status != http.StatusInternalServerError || outcome.Failure.Error != "internal error: context canceled" || transport.calls != 1 {
		t.Fatalf("canceled join = %#v, calls=%d", outcome, transport.calls)
	}
}
