package p2p

import (
	"crypto/ed25519"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalktransport/dendrite"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type DendriteTransport = dendrite.DendriteTransport

func NewDendriteTransport(serverName spec.ServerName, keyID gomatrixserverlib.KeyID, privateKey ed25519.PrivateKey, rsAPI roomserverAPI.ClientRoomserverAPI) *DendriteTransport {
	return dendrite.NewDendriteTransport(serverName, keyID, privateKey, rsAPI)
}
