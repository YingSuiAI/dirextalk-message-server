package api

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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

const serviceSecretSessionsPath = "/v2/service-secret-sessions"

type ServiceSecretKeySealer interface {
	SealServiceSecretKey(context.Context, []byte, []byte) (string, error)
	UnsealServiceSecretKey(context.Context, string, []byte) ([]byte, error)
	SealServiceSecretToken(context.Context, []byte, []byte) (string, error)
	UnsealServiceSecretToken(context.Context, string, []byte) ([]byte, error)
}

type ServiceSecretProviderBinding struct {
	SessionID, ConnectionID, DeploymentID, TaskID, ExecutionID string
	ManifestDigest, RecipeDigest, ArtifactDigest               string
	SlotID, SecretRef, Purpose, Delivery, EnvelopeDigest       string
}

type ServiceSecretProvider interface {
	// PutServiceSecret must use Binding.EnvelopeDigest as its provider-side
	// idempotency key so retry after a lost finalize response cannot create a
	// second secret version.
	PutServiceSecret(context.Context, ServiceSecretProviderBinding, []byte) (string, error)
	GetServiceSecret(context.Context, ServiceSecretReadBinding) ([]byte, error)
}
type ServiceSecretReadBinding struct{ ConnectionID, DeploymentID, SecretRef, ProviderVersion string }

func serviceSecretRoute(path string) bool {
	return path == serviceSecretSessionsPath || strings.HasPrefix(path, serviceSecretSessionsPath+"/")
}

func (b Broker) serveServiceSecret(w http.ResponseWriter, r *http.Request) {
	if !b.ServiceSecretsEnabled {
		writeError(w, http.StatusNotImplemented, "operation_not_enabled")
		return
	}
	if r.URL.RawQuery != "" {
		writeError(w, http.StatusNotFound, "not_found")
		return
	}
	if b.ServiceSecretStore == nil || b.ServiceSecretProvider == nil || b.ServiceSecretKeySealer == nil || b.ApprovalResolver == nil || b.DeploymentStore == nil || b.RecipeTasks == nil {
		writeError(w, http.StatusServiceUnavailable, "broker_not_configured")
		return
	}
	if r.URL.Path == serviceSecretSessionsPath {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, 405, "method_not_allowed")
			return
		}
		b.createServiceSecretSession(w, r)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, serviceSecretSessionsPath+"/"), "/")
	if len(parts) != 2 || !contract.ValidID(parts[0]) {
		writeError(w, 404, "not_found")
		return
	}
	switch parts[1] {
	case "encrypted-upload":
		if r.Method != http.MethodPut {
			w.Header().Set("Allow", http.MethodPut)
			writeError(w, 405, "method_not_allowed")
			return
		}
		b.uploadServiceSecret(w, r, parts[0])
	case "complete":
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, 405, "method_not_allowed")
			return
		}
		b.completeServiceSecret(w, r, parts[0])
	default:
		writeError(w, 404, "not_found")
	}
}

func readServiceSecretBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, bool) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(media, "application/json") {
		writeError(w, 415, "unsupported_content_type")
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, max)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, 413, "request_too_large")
		} else {
			writeError(w, 400, "invalid_request")
		}
		return nil, false
	}
	return raw, true
}

func (b Broker) createServiceSecretSession(w http.ResponseWriter, r *http.Request) {
	raw, ok := readServiceSecretBody(w, r, contract.MaxServiceSecretApprovalBytes)
	if !ok {
		return
	}
	proof, err := contract.ParseServiceSecretApprovalProof(raw)
	if err != nil {
		writeError(w, 400, contract.Code(err))
		return
	}
	now := time.Now().UTC()
	if b.Now != nil {
		now = b.Now().UTC()
	}
	key, found := b.ApprovalResolver.LookupApprovalKey(r.Context(), proof.ConnectionID, proof.SignerKeyID)
	if !found {
		writeError(w, 403, "unknown_approval_key")
		return
	}
	if err = proof.Verify(key, now); err != nil {
		writeError(w, 403, contract.Code(err))
		return
	}
	reservation, found, err := b.DeploymentStore.LookupDeployment(r.Context(), proof.ConnectionID, proof.DeploymentID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !found || reservation.State != "finalized" || !reservation.MatchesApprovedServiceSecret(proof.RecipeDigest, proof.SecretRef, proof.Purpose, proof.Delivery) {
		writeError(w, 409, "service_secret_approval_scope_mismatch")
		return
	}
	task, found, err := b.RecipeTasks.LookupRecipeTask(r.Context(), proof.DeploymentID, proof.TaskID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !found || !serviceSecretMatchesRecipeTask(proof, task) {
		writeError(w, 409, "service_secret_task_scope_mismatch")
		return
	}
	contextValue := proof.Context()
	aad, _ := contextValue.CanonicalCBOR()
	if existing, found, lookupErr := b.ServiceSecretStore.LookupServiceSecret(r.Context(), proof.SessionID); lookupErr != nil {
		writeStoreError(w, lookupErr)
		return
	} else if found {
		b.writeServiceSecretCreateReplay(w, r, existing, serviceSecretSessionFromProof(proof), aad)
		return
	}
	random := b.ServiceSecretRandom
	if random == nil {
		random = rand.Reader
	}
	private, err := ecdh.X25519().GenerateKey(random)
	if err != nil {
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	token := make([]byte, 32)
	if _, err = io.ReadFull(random, token); err != nil {
		writeError(w, 503, "service_secret_token_unavailable")
		return
	}
	sealed, err := b.ServiceSecretKeySealer.SealServiceSecretKey(r.Context(), private.Bytes(), aad)
	if err != nil || sealed == "" {
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	tokenSum := sha256.Sum256(token)
	sealedToken, err := b.ServiceSecretKeySealer.SealServiceSecretToken(r.Context(), token, aad)
	if err != nil || sealedToken == "" {
		clear(token)
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	session := serviceSecretSessionFromProof(proof)
	session.TokenSHA256, session.SealedPrivateKey, session.SealedUploadToken, session.State = hex.EncodeToString(tokenSum[:]), sealed, sealedToken, commandstore.ServiceSecretPending
	stored, created, err := b.ServiceSecretStore.CreateServiceSecret(r.Context(), session)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !created {
		clear(token)
		b.writeServiceSecretCreateReplay(w, r, stored, session, aad)
		return
	}
	if !stored.SameBinding(session) {
		writeError(w, 409, "service_secret_session_conflict")
		return
	}
	b.writeServiceSecretCreateResponse(w, stored, private, token)
	clear(token)
}

func serviceSecretSessionFromProof(proof contract.ServiceSecretApprovalProof) commandstore.ServiceSecretSession {
	return commandstore.ServiceSecretSession{SessionID: proof.SessionID, ConnectionID: proof.ConnectionID, DeploymentID: proof.DeploymentID, TaskID: proof.TaskID, ExecutionID: proof.ExecutionID, ManifestDigest: proof.ManifestDigest, RecipeDigest: proof.RecipeDigest, ArtifactDigest: proof.ArtifactDigest, SlotID: proof.SlotID, SecretRef: proof.SecretRef, Purpose: proof.Purpose, Delivery: proof.Delivery, ContextDigest: proof.ContextDigest, ExpiresAt: contract.CanonicalInstant(proof.ExpiresAt)}
}

func serviceSecretMatchesRecipeTask(proof contract.ServiceSecretApprovalProof, task commandstore.RecipeTaskRecord) bool {
	if task.ConnectionID != proof.ConnectionID || task.DeploymentID != proof.DeploymentID || task.TaskID != proof.TaskID || task.ExecutionID != proof.ExecutionID || task.RecipeExecutionManifestDigest != proof.ManifestDigest || (task.Status != "queued" && task.Status != "running") {
		return false
	}
	manifest, err := contract.ParseRecipeExecutionManifestJSON(task.ManifestJSON)
	if err != nil {
		return false
	}
	digest, err := manifest.Digest()
	if err != nil || digest != proof.ManifestDigest || manifest.ExecutionID != proof.ExecutionID || manifest.DeploymentID != proof.DeploymentID || manifest.RecipeDigest != proof.RecipeDigest || manifest.ArtifactDigest != proof.ArtifactDigest {
		return false
	}
	for _, slot := range manifest.SecretSlots {
		if slot.SlotID == proof.SlotID {
			return slot.SecretRef == proof.SecretRef
		}
	}
	return false
}

func (b Broker) writeServiceSecretCreateReplay(w http.ResponseWriter, r *http.Request, stored, expected commandstore.ServiceSecretSession, aad []byte) {
	if !stored.SameBinding(expected) {
		writeError(w, 409, "service_secret_session_conflict")
		return
	}
	privateBytes, err := b.ServiceSecretKeySealer.UnsealServiceSecretKey(r.Context(), stored.SealedPrivateKey, aad)
	if err != nil {
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	defer clear(privateBytes)
	private, err := ecdh.X25519().NewPrivateKey(privateBytes)
	if err != nil {
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	token, err := b.ServiceSecretKeySealer.UnsealServiceSecretToken(r.Context(), stored.SealedUploadToken, aad)
	if err != nil {
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	defer clear(token)
	sum := sha256.Sum256(token)
	expectedHash, err := hex.DecodeString(stored.TokenSHA256)
	if err != nil || len(token) != 32 || subtle.ConstantTimeCompare(sum[:], expectedHash) != 1 {
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	b.writeServiceSecretCreateResponse(w, stored, private, token)
}

func (b Broker) writeServiceSecretCreateResponse(w http.ResponseWriter, stored commandstore.ServiceSecretSession, private *ecdh.PrivateKey, token []byte) {
	writeServiceSecretJSON(w, 201, map[string]any{"schema": "dirextalk.service-secret-session/v1", "session_id": stored.SessionID, "status": commandstore.ServiceSecretPending, "context_digest": stored.ContextDigest, "server_public_key_b64": base64.RawURLEncoding.EncodeToString(private.PublicKey().Bytes()), "upload_token": base64.RawURLEncoding.EncodeToString(token), "expires_at": stored.ExpiresAt})
}

func (b Broker) uploadServiceSecret(w http.ResponseWriter, r *http.Request, sessionID string) {
	session, found, err := b.ServiceSecretStore.LookupServiceSecret(r.Context(), sessionID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !found {
		writeError(w, 404, "service_secret_session_not_found")
		return
	}
	if !authorizeServiceSecret(r, session.TokenSHA256) {
		writeError(w, 401, "invalid_service_secret_token")
		return
	}
	raw, ok := readServiceSecretBody(w, r, contract.MaxServiceSecretEnvelope)
	if !ok {
		return
	}
	envelope, err := contract.ParseServiceSecretEnvelope(raw)
	if err != nil {
		writeError(w, 400, contract.Code(err))
		return
	}
	contextValue := serviceSecretContext(session)
	if err = envelope.ValidateForContext(contextValue); err != nil {
		writeError(w, 400, contract.Code(err))
		return
	}
	digest, _ := envelope.Digest()
	if session.EnvelopeDigest != "" && session.EnvelopeDigest != digest {
		writeError(w, 409, "service_secret_envelope_conflict")
		return
	}
	if session.State != commandstore.ServiceSecretCompleted && serviceSecretExpired(session, b.Now) {
		writeError(w, http.StatusGone, "service_secret_session_expired")
		return
	}
	if session.State == commandstore.ServiceSecretUploaded || session.State == commandstore.ServiceSecretCompleted {
		writeServiceSecretReceipt(w, session)
		return
	}
	aad, _ := contextValue.CanonicalCBOR()
	private, err := b.ServiceSecretKeySealer.UnsealServiceSecretKey(r.Context(), session.SealedPrivateKey, aad)
	if err != nil {
		writeError(w, 503, "service_secret_key_unavailable")
		return
	}
	plaintext, err := contract.DecryptServiceSecret(contextValue, private, envelope)
	clear(private)
	if err != nil {
		writeError(w, 400, contract.Code(err))
		return
	}
	claimed, _, err := b.ServiceSecretStore.ClaimServiceSecretEnvelope(r.Context(), sessionID, digest)
	if err != nil {
		clear(plaintext)
		writeStoreError(w, err)
		return
	}
	binding := ServiceSecretProviderBinding{session.SessionID, session.ConnectionID, session.DeploymentID, session.TaskID, session.ExecutionID, session.ManifestDigest, session.RecipeDigest, session.ArtifactDigest, session.SlotID, session.SecretRef, session.Purpose, session.Delivery, digest}
	version, err := b.ServiceSecretProvider.PutServiceSecret(r.Context(), binding, plaintext)
	clear(plaintext)
	if err != nil {
		writeError(w, 503, "service_secret_provider_unavailable")
		return
	}
	stored, err := b.ServiceSecretStore.FinalizeServiceSecretUpload(r.Context(), claimed.SessionID, digest, version)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeServiceSecretReceipt(w, stored)
}

func (b Broker) completeServiceSecret(w http.ResponseWriter, r *http.Request, sessionID string) {
	session, found, err := b.ServiceSecretStore.LookupServiceSecret(r.Context(), sessionID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !found {
		writeError(w, 404, "service_secret_session_not_found")
		return
	}
	if !authorizeServiceSecret(r, session.TokenSHA256) {
		writeError(w, 401, "invalid_service_secret_token")
		return
	}
	if session.State != commandstore.ServiceSecretCompleted && serviceSecretExpired(session, b.Now) {
		writeError(w, http.StatusGone, "service_secret_session_expired")
		return
	}
	raw, ok := readServiceSecretBody(w, r, 1024)
	if !ok {
		return
	}
	var body struct {
		Schema         string `json:"schema"`
		EnvelopeDigest string `json:"envelope_digest"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil || body.Schema != "dirextalk.service-secret-complete/v1" {
		writeError(w, 400, "invalid_service_secret_complete")
		return
	}
	canonical, _ := json.Marshal(body)
	if !bytes.Equal(raw, canonical) {
		writeError(w, 400, "noncanonical_payload")
		return
	}
	stored, err := b.ServiceSecretStore.CompleteServiceSecret(r.Context(), sessionID, body.EnvelopeDigest)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeServiceSecretReceipt(w, stored)
}

func authorizeServiceSecret(r *http.Request, expected string) bool {
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") {
		return false
	}
	token, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "Bearer "))
	if err != nil || len(token) != 32 {
		return false
	}
	sum := sha256.Sum256(token)
	expectedBytes, err := hex.DecodeString(expected)
	return err == nil && subtle.ConstantTimeCompare(sum[:], expectedBytes) == 1
}

func serviceSecretContext(s commandstore.ServiceSecretSession) contract.ServiceSecretContextV1 {
	return contract.ServiceSecretContextV1{SchemaVersion: contract.ServiceSecretContextSchema, SessionID: s.SessionID, ConnectionID: s.ConnectionID, DeploymentID: s.DeploymentID, TaskID: s.TaskID, ExecutionID: s.ExecutionID, ManifestDigest: s.ManifestDigest, RecipeDigest: s.RecipeDigest, ArtifactDigest: s.ArtifactDigest, SlotID: s.SlotID, SecretRef: s.SecretRef, Purpose: s.Purpose, Delivery: s.Delivery, ExpiresAt: s.ExpiresAt}
}
func serviceSecretExpired(session commandstore.ServiceSecretSession, nowFn func() time.Time) bool {
	now := time.Now().UTC()
	if nowFn != nil {
		now = nowFn().UTC()
	}
	expires, err := time.Parse("2006-01-02T15:04:05.000Z", session.ExpiresAt)
	return err != nil || !expires.After(now)
}
func writeServiceSecretReceipt(w http.ResponseWriter, s commandstore.ServiceSecretSession) {
	writeServiceSecretJSON(w, 200, map[string]any{"schema": "dirextalk.service-secret-receipt/v1", "session_id": s.SessionID, "status": s.State, "envelope_digest": s.EnvelopeDigest, "provider_version": s.ProviderVersion})
}
func writeServiceSecretJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
