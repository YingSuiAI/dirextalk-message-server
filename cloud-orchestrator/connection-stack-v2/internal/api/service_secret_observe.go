package api

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func serviceSecretObservation(session commandstore.ServiceSecretSession, now time.Time) (contract.ServiceSecretObservation, error) {
	expiresAt, err := time.Parse("2006-01-02T15:04:05.000Z", session.ExpiresAt)
	if err != nil {
		return contract.ServiceSecretObservation{}, err
	}
	status := session.State
	providerVersion := session.ProviderVersion
	if !now.Before(expiresAt) && status != commandstore.ServiceSecretCompleted {
		status = "expired"
		providerVersion = ""
	}
	if status != commandstore.ServiceSecretUploaded && status != commandstore.ServiceSecretCompleted {
		providerVersion = ""
	}
	markerInput := strings.Join([]string{session.SessionID, session.ContextDigest, status, providerVersion, session.ExpiresAt}, "\n")
	marker := sha256.Sum256([]byte(markerInput))
	return contract.ServiceSecretObservation{
		Schema: contract.ServiceSecretObservationSchema, SessionID: session.SessionID, Status: status,
		ProviderVersion: providerVersion, BindingDigest: session.ContextDigest, UpdatedMarker: hex.EncodeToString(marker[:]),
	}, nil
}
