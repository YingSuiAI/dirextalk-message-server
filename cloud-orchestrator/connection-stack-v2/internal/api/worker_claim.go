package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

const (
	workerSessionsPathPrefix = "/v2/worker-sessions/"
	workerClaimPathSuffix    = "/claim"
	workerLeaseDuration      = 5 * time.Minute
	workerReconnectRetention = 24 * time.Hour
)

type WorkerIdentityVerifier interface {
	VerifyWorkerIdentity(context.Context, contract.WorkerSessionClaimRequest, commandstore.WorkerSession) error
}

type WorkerTokenGenerator interface {
	GenerateWorkerToken() (string, error)
}

type CryptoWorkerTokenGenerator struct{}

func (CryptoWorkerTokenGenerator) GenerateWorkerToken() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func workerClaimSessionID(path string) (string, bool) {
	if !strings.HasPrefix(path, workerSessionsPathPrefix) || !strings.HasSuffix(path, workerClaimPathSuffix) {
		return "", false
	}
	value := strings.TrimSuffix(strings.TrimPrefix(path, workerSessionsPathPrefix), workerClaimPathSuffix)
	if strings.Contains(value, "/") || !contract.ValidID(value) {
		return "", false
	}
	return value, true
}

func (b Broker) serveWorkerClaim(response http.ResponseWriter, request *http.Request, bootstrapSessionID string) {
	if request.URL.RawQuery != "" {
		writeError(response, http.StatusNotFound, "not_found")
		return
	}
	if request.Method != http.MethodPost {
		response.Header().Set("Allow", http.MethodPost)
		writeError(response, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		writeError(response, http.StatusUnsupportedMediaType, "unsupported_content_type")
		return
	}
	if !b.DeploymentEnabled || b.DeploymentStore == nil || b.WorkerIdentity == nil {
		writeError(response, http.StatusNotImplemented, "operation_not_enabled")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, contract.MaxWorkerClaimBytes)
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(response, http.StatusRequestEntityTooLarge, "request_too_large")
			return
		}
		writeError(response, http.StatusBadRequest, "invalid_worker_claim")
		return
	}
	claim, err := contract.ParseWorkerSessionClaimRequest(raw)
	if err != nil || claim.BootstrapSessionID != bootstrapSessionID {
		writeError(response, http.StatusBadRequest, "invalid_worker_claim")
		return
	}
	session, found, err := b.DeploymentStore.LookupWorkerSession(request.Context(), bootstrapSessionID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found || claim.ConnectionID != session.ConnectionID || claim.DeploymentID != session.DeploymentID ||
		claim.WorkerImageDigest != session.WorkerImageDigest || claim.ArtifactManifestDigest != session.ArtifactManifestDigest {
		writeError(response, http.StatusForbidden, "worker_session_not_found")
		return
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if b.Now != nil {
		now = b.Now().UTC().Truncate(time.Millisecond)
	}
	if !workerSessionClaimable(session, now) {
		writeError(response, http.StatusConflict, "worker_session_expired")
		return
	}
	if err := b.WorkerIdentity.VerifyWorkerIdentity(request.Context(), claim, session); err != nil {
		writeProviderError(response, err)
		return
	}
	generator := b.WorkerTokens
	if generator == nil {
		generator = CryptoWorkerTokenGenerator{}
	}
	token, err := generator.GenerateWorkerToken()
	if err != nil {
		writeError(response, http.StatusServiceUnavailable, "worker_session_unavailable")
		return
	}
	sum := sha256.Sum256([]byte(token))
	leaseExpiresAt := now.Add(workerLeaseDuration).Format("2006-01-02T15:04:05.000Z")
	activated, err := b.DeploymentStore.ActivateWorkerSession(request.Context(), commandstore.WorkerSessionClaim{
		Session: session, TokenSHA256: hex.EncodeToString(sum[:]), Now: now.Format("2006-01-02T15:04:05.000Z"), LeaseExpiresAt: leaseExpiresAt,
	})
	if err != nil {
		writeStoreError(response, err)
		return
	}
	result, err := contract.NewWorkerSessionClaimResponse(claim, activated.LeaseEpoch, activated.LeaseExpiresAt, token)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "worker_session_unavailable")
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(response).Encode(result)
}

func workerSessionClaimable(session commandstore.WorkerSession, now time.Time) bool {
	switch session.State {
	case "bound":
		expiresAt, err := time.Parse("2006-01-02T15:04:05.000Z", session.ExpiresAt)
		return err == nil && expiresAt.After(now)
	case "active":
		leaseExpiresAt, err := time.Parse("2006-01-02T15:04:05.000Z", session.LeaseExpiresAt)
		return err == nil && leaseExpiresAt.Add(workerReconnectRetention).After(now)
	default:
		return false
	}
}
