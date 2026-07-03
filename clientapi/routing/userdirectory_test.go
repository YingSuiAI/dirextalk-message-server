package routing

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/clientapi/auth/authtypes"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type userDirectoryRoomserver struct {
	roomserverAPI.ClientRoomserverAPI
	users []authtypes.FullyQualifiedProfile
}

func (r *userDirectoryRoomserver) QueryKnownUsers(ctx context.Context, req *roomserverAPI.QueryKnownUsersRequest, res *roomserverAPI.QueryKnownUsersResponse) error {
	res.Users = append([]authtypes.FullyQualifiedProfile{}, r.users...)
	return nil
}

type userDirectoryProfileProvider struct {
	userapi.QuerySearchProfilesAPI
}

func (p *userDirectoryProfileProvider) QuerySearchProfiles(ctx context.Context, req *userapi.QuerySearchProfilesRequest, res *userapi.QuerySearchProfilesResponse) error {
	return nil
}

type userDirectoryFederation struct {
	fclient.FederationClient
	profiles map[string]fclient.RespProfile
	lookups  []string
}

func (f *userDirectoryFederation) LookupProfile(ctx context.Context, origin, serverName spec.ServerName, userID, field string) (fclient.RespProfile, error) {
	f.lookups = append(f.lookups, userID)
	return f.profiles[userID], nil
}

func TestSearchUserDirectoryRefreshesRemoteProfileWhenMXIDMatches(t *testing.T) {
	rsAPI := &userDirectoryRoomserver{users: []authtypes.FullyQualifiedProfile{{
		UserID:      "@owner:dm1.dirextalk.ai",
		DisplayName: "Original Nick",
		AvatarURL:   "mxc://dm1/original",
	}}}
	federation := &userDirectoryFederation{profiles: map[string]fclient.RespProfile{
		"@owner:dm1.dirextalk.ai": {
			DisplayName: "Custom Nick",
			AvatarURL:   "mxc://dm1/custom",
		},
	}}

	res := SearchUserDirectory(
		context.Background(),
		&userapi.Device{UserID: "@owner:im1.dirextalk.ai"},
		rsAPI,
		&userDirectoryProfileProvider{},
		"owner",
		10,
		federation,
		spec.ServerName("im1.dirextalk.ai"),
	)

	if res.Code != 200 {
		t.Fatalf("expected successful user directory response, got %#v", res)
	}
	body := res.JSON.(*UserDirectoryResponse)
	if len(body.Results) != 1 {
		t.Fatalf("expected one user directory result, got %#v", body.Results)
	}
	got := body.Results[0]
	if got.DisplayName != "Custom Nick" || got.AvatarURL != "mxc://dm1/custom" {
		t.Fatalf("expected refreshed remote profile in search result, got %#v", got)
	}
	if len(federation.lookups) != 1 || federation.lookups[0] != "@owner:dm1.dirextalk.ai" {
		t.Fatalf("expected remote profile lookup, got %#v", federation.lookups)
	}
}

func TestSearchUserDirectoryRefreshesRemoteProfileWhenDomainMatches(t *testing.T) {
	rsAPI := &userDirectoryRoomserver{users: []authtypes.FullyQualifiedProfile{{
		UserID:      "@owner:dm1.dirextalk.ai",
		DisplayName: "Original Nick",
		AvatarURL:   "mxc://dm1/original",
	}}}
	federation := &userDirectoryFederation{profiles: map[string]fclient.RespProfile{
		"@owner:dm1.dirextalk.ai": {
			DisplayName: "Custom Nick",
			AvatarURL:   "mxc://dm1/custom",
		},
	}}

	res := SearchUserDirectory(
		context.Background(),
		&userapi.Device{UserID: "@owner:im1.dirextalk.ai"},
		rsAPI,
		&userDirectoryProfileProvider{},
		"dm1.dirextalk.ai",
		10,
		federation,
		spec.ServerName("im1.dirextalk.ai"),
	)

	if res.Code != 200 {
		t.Fatalf("expected successful user directory response, got %#v", res)
	}
	body := res.JSON.(*UserDirectoryResponse)
	if len(body.Results) != 1 {
		t.Fatalf("expected one user directory result, got %#v", body.Results)
	}
	got := body.Results[0]
	if got.DisplayName != "Custom Nick" || got.AvatarURL != "mxc://dm1/custom" {
		t.Fatalf("expected refreshed remote profile in domain search result, got %#v", got)
	}
	if len(federation.lookups) != 1 || federation.lookups[0] != "@owner:dm1.dirextalk.ai" {
		t.Fatalf("expected remote profile lookup, got %#v", federation.lookups)
	}
}
