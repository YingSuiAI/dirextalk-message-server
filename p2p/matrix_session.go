package p2p

import (
	"context"

	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/YingSuiAI/dirextalk-message-server/userapi/dirextalksession"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type MatrixSessionIssuer interface {
	EnsureMatrixSession(ctx context.Context, userID, displayName, avatarURL, deviceID string, revokeExistingDevices bool) (string, error)
}

type MatrixProfileUpdater interface {
	UpdateMatrixProfile(ctx context.Context, userID, displayName, avatarURL string) error
}

type DendriteMatrixSessionIssuer = dirextalksession.Issuer

func NewDendriteMatrixSessionIssuer(userAPI userapi.UserInternalAPI, serverName spec.ServerName) *DendriteMatrixSessionIssuer {
	return dirextalksession.NewIssuer(userAPI, serverName, matrixPortalDeviceID, "P2P Portal")
}

func NewDendriteAccountDeactivator(userAPI userapi.UserInternalAPI, serverName spec.ServerName) *DendriteMatrixSessionIssuer {
	return dirextalksession.NewIssuer(userAPI, serverName, matrixPortalDeviceID, "P2P Portal")
}
