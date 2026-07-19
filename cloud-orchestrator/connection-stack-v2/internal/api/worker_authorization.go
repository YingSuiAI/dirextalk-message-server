package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type workerAuthorization struct {
	Session     commandstore.WorkerSession
	TokenSHA256 string
	Now         time.Time
}

func (b Broker) authorizeWorker(request *http.Request, bootstrapSessionID string, leaseEpoch int64) (workerAuthorization, error) {
	if b.DeploymentStore == nil || leaseEpoch < 1 {
		return workerAuthorization{}, NewError("worker_session_unauthorized", http.StatusUnauthorized)
	}
	token, ok := workerBearerToken(request.Header.Get("Authorization"))
	if !ok {
		return workerAuthorization{}, NewError("worker_session_unauthorized", http.StatusUnauthorized)
	}
	session, found, err := b.DeploymentStore.LookupWorkerSession(request.Context(), bootstrapSessionID)
	if err != nil {
		return workerAuthorization{}, NewError("worker_session_unavailable", http.StatusServiceUnavailable)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if b.Now != nil {
		now = b.Now().UTC().Truncate(time.Millisecond)
	}
	if !found || session.State != "active" || session.BootstrapSessionID != bootstrapSessionID || session.LeaseEpoch != leaseEpoch {
		return workerAuthorization{}, NewError("worker_session_unauthorized", http.StatusUnauthorized)
	}
	leaseExpiresAt, parseErr := time.Parse("2006-01-02T15:04:05.000Z", session.LeaseExpiresAt)
	if parseErr != nil || !leaseExpiresAt.After(now) {
		return workerAuthorization{}, NewError("worker_session_expired", http.StatusUnauthorized)
	}
	tokenDigest := sha256.Sum256([]byte(token))
	wantDigest, decodeErr := hex.DecodeString(session.TokenSHA256)
	if decodeErr != nil || len(wantDigest) != sha256.Size || subtle.ConstantTimeCompare(tokenDigest[:], wantDigest) != 1 {
		return workerAuthorization{}, NewError("worker_session_unauthorized", http.StatusUnauthorized)
	}
	return workerAuthorization{Session: session, TokenSHA256: hex.EncodeToString(tokenDigest[:]), Now: now}, nil
}

func workerBearerToken(value string) (string, bool) {
	if !strings.HasPrefix(value, "Bearer ") {
		return "", false
	}
	token := strings.TrimPrefix(value, "Bearer ")
	if len(token) < 16 || len(token) > 4096 || strings.ContainsAny(token, " \t\r\n") {
		return "", false
	}
	for _, character := range token {
		if character < 0x21 || character > 0x7e || character == '"' || character == '\\' {
			return "", false
		}
	}
	return token, true
}
