package p2p

import (
	"context"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/clientapi/auth"
	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type MatrixSessionIssuer interface {
	EnsureMatrixSession(ctx context.Context, userID, displayName, avatarURL, deviceID string, revokeExistingDevices bool) (string, error)
}

type MatrixProfileUpdater interface {
	UpdateMatrixProfile(ctx context.Context, userID, displayName, avatarURL string) error
}

type DendriteMatrixSessionIssuer struct {
	userAPI    userapi.UserInternalAPI
	serverName spec.ServerName
}

func NewDendriteMatrixSessionIssuer(userAPI userapi.UserInternalAPI, serverName spec.ServerName) *DendriteMatrixSessionIssuer {
	return &DendriteMatrixSessionIssuer{userAPI: userAPI, serverName: serverName}
}

func NewDendriteAccountDeactivator(userAPI userapi.UserInternalAPI, serverName spec.ServerName) *DendriteMatrixSessionIssuer {
	return &DendriteMatrixSessionIssuer{userAPI: userAPI, serverName: serverName}
}

func (i *DendriteMatrixSessionIssuer) DeactivateAccount(ctx context.Context, localpart string) error {
	res := &userapi.PerformAccountDeactivationResponse{}
	return i.userAPI.PerformAccountDeactivation(ctx, &userapi.PerformAccountDeactivationRequest{
		Localpart:  strings.TrimSpace(localpart),
		ServerName: i.serverName,
	}, res)
}

func (i *DendriteMatrixSessionIssuer) EnsureMatrixSession(ctx context.Context, userID, displayName, avatarURL, requestedDeviceID string, revokeExistingDevices bool) (string, error) {
	localpart, serverName, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return "", err
	}
	if serverName == "" {
		serverName = i.serverName
	}
	accountRes := &userapi.PerformAccountCreationResponse{}
	if err = i.userAPI.PerformAccountCreation(ctx, &userapi.PerformAccountCreationRequest{
		AccountType: userapi.AccountTypeUser,
		Localpart:   localpart,
		ServerName:  serverName,
		OnConflict:  userapi.ConflictUpdate,
	}, accountRes); err != nil {
		return "", err
	}
	if err = i.updateMatrixProfile(ctx, localpart, serverName, displayName, avatarURL); err != nil {
		return "", err
	}
	deviceID := cleanMatrixDeviceID(requestedDeviceID)
	deviceName := "P2P Portal"
	accessToken, err := auth.GenerateAccessToken()
	if err != nil {
		return "", err
	}
	deviceRes := &userapi.PerformDeviceCreationResponse{}
	if err := i.userAPI.PerformDeviceCreation(ctx, &userapi.PerformDeviceCreationRequest{
		Localpart:          localpart,
		ServerName:         serverName,
		AccessToken:        accessToken,
		DeviceID:           &deviceID,
		DeviceDisplayName:  &deviceName,
		NoDeviceListUpdate: true,
	}, deviceRes); err != nil {
		return "", err
	}
	if deviceRes.Device == nil {
		return accessToken, nil
	}
	if revokeExistingDevices {
		deleteRes := &userapi.PerformDeviceDeletionResponse{}
		if err := i.userAPI.PerformDeviceDeletion(ctx, &userapi.PerformDeviceDeletionRequest{
			UserID:         userID,
			ExceptDeviceID: deviceRes.Device.ID,
		}, deleteRes); err != nil {
			return "", err
		}
	}
	return deviceRes.Device.AccessToken, nil
}

func (i *DendriteMatrixSessionIssuer) UpdateMatrixProfile(ctx context.Context, userID, displayName, avatarURL string) error {
	localpart, serverName, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return err
	}
	if serverName == "" {
		serverName = i.serverName
	}
	accountRes := &userapi.PerformAccountCreationResponse{}
	if err := i.userAPI.PerformAccountCreation(ctx, &userapi.PerformAccountCreationRequest{
		AccountType: userapi.AccountTypeUser,
		Localpart:   localpart,
		ServerName:  serverName,
		OnConflict:  userapi.ConflictUpdate,
	}, accountRes); err != nil {
		return err
	}
	return i.updateMatrixProfile(ctx, localpart, serverName, displayName, avatarURL)
}

func (i *DendriteMatrixSessionIssuer) updateMatrixProfile(ctx context.Context, localpart string, serverName spec.ServerName, displayName, avatarURL string) error {
	if strings.TrimSpace(displayName) != "" {
		if _, _, err := i.userAPI.SetDisplayName(ctx, localpart, serverName, strings.TrimSpace(displayName)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(avatarURL) != "" {
		if _, _, err := i.userAPI.SetAvatarURL(ctx, localpart, serverName, strings.TrimSpace(avatarURL)); err != nil {
			return err
		}
	}
	return nil
}
