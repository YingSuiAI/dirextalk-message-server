package dirextalksession

import (
	"context"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/clientapi/auth/authtypes"
	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type fakeUserAPI struct {
	userapi.UserInternalAPI

	createdDeviceID          string
	createdDeviceAccessToken string
	deletedUserID            string
	deletedExceptDeviceID    string
}

func (f *fakeUserAPI) PerformAccountCreation(ctx context.Context, req *userapi.PerformAccountCreationRequest, res *userapi.PerformAccountCreationResponse) error {
	res.AccountCreated = true
	return nil
}

func (f *fakeUserAPI) SetDisplayName(ctx context.Context, localpart string, serverName spec.ServerName, displayName string) (*authtypes.Profile, bool, error) {
	return nil, true, nil
}

func (f *fakeUserAPI) SetAvatarURL(ctx context.Context, localpart string, serverName spec.ServerName, avatarURL string) (*authtypes.Profile, bool, error) {
	return nil, true, nil
}

func (f *fakeUserAPI) PerformDeviceCreation(ctx context.Context, req *userapi.PerformDeviceCreationRequest, res *userapi.PerformDeviceCreationResponse) error {
	deviceID := ""
	if req.DeviceID != nil {
		deviceID = *req.DeviceID
	}
	userID := "@" + req.Localpart + ":" + string(req.ServerName)
	f.createdDeviceID = deviceID
	f.createdDeviceAccessToken = req.AccessToken
	res.DeviceCreated = true
	res.Device = &userapi.Device{
		ID:          deviceID,
		UserID:      userID,
		AccessToken: req.AccessToken,
	}
	return nil
}

func (f *fakeUserAPI) PerformDeviceDeletion(ctx context.Context, req *userapi.PerformDeviceDeletionRequest, res *userapi.PerformDeviceDeletionResponse) error {
	f.deletedUserID = req.UserID
	f.deletedExceptDeviceID = req.ExceptDeviceID
	return nil
}

func TestIssuerRevokesOtherPortalDevices(t *testing.T) {
	userAPI := &fakeUserAPI{}
	issuer := NewIssuer(userAPI, "example.com", "PORTAL_DEFAULT", "P2P Portal")

	token, err := issuer.EnsureMatrixSession(
		context.Background(),
		"@owner:example.com",
		"Owner",
		"mxc://example.com/avatar",
		"DEVICE_B",
		true,
	)
	if err != nil {
		t.Fatalf("EnsureMatrixSession failed: %v", err)
	}

	if token == "" || token != userAPI.createdDeviceAccessToken {
		t.Fatalf("expected returned token to be the created device token, got %q created %q", token, userAPI.createdDeviceAccessToken)
	}
	if userAPI.createdDeviceID != "DEVICE_B" {
		t.Fatalf("expected DEVICE_B to be created, got %q", userAPI.createdDeviceID)
	}
	if userAPI.deletedUserID != "@owner:example.com" {
		t.Fatalf("expected owner devices to be deleted, got user %q", userAPI.deletedUserID)
	}
	if userAPI.deletedExceptDeviceID != "DEVICE_B" {
		t.Fatalf("expected current device to be preserved, got except %q", userAPI.deletedExceptDeviceID)
	}
}

func TestIssuerKeepsDevicesForAgentSessions(t *testing.T) {
	userAPI := &fakeUserAPI{}
	issuer := NewIssuer(userAPI, "example.com", "PORTAL_DEFAULT", "P2P Portal")

	if _, err := issuer.EnsureMatrixSession(
		context.Background(),
		"@owner:example.com",
		"Owner",
		"",
		"DIREXTALK_CLI",
		false,
	); err != nil {
		t.Fatalf("EnsureMatrixSession failed: %v", err)
	}

	if userAPI.deletedUserID != "" || userAPI.deletedExceptDeviceID != "" {
		t.Fatalf("expected agent session to keep portal devices, deleted user=%q except=%q", userAPI.deletedUserID, userAPI.deletedExceptDeviceID)
	}
}

func TestIssuerUsesDefaultDeviceID(t *testing.T) {
	userAPI := &fakeUserAPI{}
	issuer := NewIssuer(userAPI, "example.com", "PORTAL_DEFAULT", "P2P Portal")

	if _, err := issuer.EnsureMatrixSession(
		context.Background(),
		"@owner:example.com",
		"Owner",
		"",
		"  ",
		false,
	); err != nil {
		t.Fatalf("EnsureMatrixSession failed: %v", err)
	}

	if userAPI.createdDeviceID != "PORTAL_DEFAULT" {
		t.Fatalf("expected default device id, got %q", userAPI.createdDeviceID)
	}
}
