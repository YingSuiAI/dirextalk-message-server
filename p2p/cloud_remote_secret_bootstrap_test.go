package p2p

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
)

type productSecretBootstrapClient struct {
	created         cloudmodule.CreateAgentSecretBootstrapRequest
	uploaded        cloudmodule.UploadAgentEncryptedSecretRequest
	createCalls     int
	uploadCalls     int
	now             time.Time
	sessionID       string
	serverPublicKey []byte
	uploadToken     []byte
	replayUploaded  bool
}

func (client *productSecretBootstrapClient) CreateAgentSecretBootstrap(_ context.Context, request cloudmodule.CreateAgentSecretBootstrapRequest) (cloudmodule.AgentSecretBootstrapSession, error) {
	client.createCalls++
	client.created = request
	session := cloudmodule.AgentSecretBootstrapSession{
		SessionSchemaVersion: cloudmodule.AgentSecretBootstrapSessionSchemaV1, EnvelopeSchemaVersion: cloudmodule.AgentSecretBootstrapEnvelopeSchemaV1,
		SessionID: client.sessionID, AgentInstanceID: "agent-instance-1", OwnerID: "dirextalk-project:example.com",
		Purpose: request.Purpose, TargetID: request.TargetID, ServerPublicKey: append([]byte(nil), client.serverPublicKey...),
		UploadToken: append([]byte(nil), client.uploadToken...), CreatedAt: client.now.Format(time.RFC3339Nano),
		ExpiresAt: client.now.Add(10 * time.Minute).Format(time.RFC3339Nano), Status: "awaiting_upload", Revision: 1,
	}
	if client.replayUploaded {
		session.UploadToken = nil
		session.Status = "uploaded"
		session.Revision = 2
	}
	return session, nil
}

func (client *productSecretBootstrapClient) UploadAgentEncryptedSecret(_ context.Context, request cloudmodule.UploadAgentEncryptedSecretRequest) (cloudmodule.AgentSecretBootstrapSession, error) {
	client.uploadCalls++
	client.uploaded = cloudmodule.UploadAgentEncryptedSecretRequest{
		SessionID: request.SessionID, UploadToken: append([]byte(nil), request.UploadToken...),
		ClientPublicKey: append([]byte(nil), request.ClientPublicKey...), Nonce: append([]byte(nil), request.Nonce...),
		Ciphertext: append([]byte(nil), request.Ciphertext...), IdempotencyKey: request.IdempotencyKey,
		ExpectedRevision: request.ExpectedRevision,
	}
	return cloudmodule.AgentSecretBootstrapSession{
		SessionSchemaVersion: cloudmodule.AgentSecretBootstrapSessionSchemaV1, EnvelopeSchemaVersion: cloudmodule.AgentSecretBootstrapEnvelopeSchemaV1,
		SessionID: request.SessionID, AgentInstanceID: "agent-instance-1", OwnerID: "dirextalk-project:example.com",
		Purpose: cloudmodule.AgentSecretBootstrapPurposeAWSConnection, TargetID: client.created.TargetID,
		ServerPublicKey: append([]byte(nil), client.serverPublicKey...), CreatedAt: client.now.Format(time.RFC3339Nano),
		ExpiresAt: client.now.Add(10 * time.Minute).Format(time.RFC3339Nano), Status: "uploaded", Revision: 2,
	}, nil
}

func TestRemoteAgentSecretBootstrapUsesDurableRolePlanAndDedicatedOwnerOnlyUpload(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	client := &productSecretBootstrapClient{
		now: now, sessionID: uuid.NewString(), serverPublicKey: append(make([]byte, 31), 1), uploadToken: append(make([]byte, 31), 2),
	}
	service := NewService(Config{
		ServerName: "example.com", CloudSecretBootstrapClient: client,
		CloudConnectionStack: CloudConnectionStackConfig{
			TemplateDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ConnectionTemplate: testPublishIntentConnectionTemplate(),
			SourceTreeDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", NodeKeyID: "node-key-1",
			NodePublicKeySPKIBase64: testEd25519SPKIBase64(t), RolePlanTTL: 15 * time.Minute,
		},
	})
	router := newP2PTestRouter(service)
	rolePlanKey := uuid.NewString()
	rolePlanParams := map[string]any{
		"provider": "aws", "region": "ap-northeast-1", "device_approval_key_id": "device-key-root-1",
		"device_approval_public_key_spki_base64": testEd25519SPKIBase64(t), "allow_root_credential_bootstrap": true,
		"idempotency_key": rolePlanKey,
	}
	injectedParams := make(map[string]any, len(rolePlanParams)+1)
	for key, value := range rolePlanParams {
		injectedParams[key] = value
	}
	injectedParams["cloud_connection_id"] = uuid.NewString()
	injected := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.role_plan", "params": injectedParams})
	injected.Header.Set("Authorization", "Bearer "+service.AccessToken())
	injectedRecorder := httptest.NewRecorder()
	router.ServeHTTP(injectedRecorder, injected)
	if injectedRecorder.Code != http.StatusBadRequest {
		t.Fatalf("client-supplied connection target status=%d body=%s", injectedRecorder.Code, injectedRecorder.Body.String())
	}

	roleResult := cloudCommand(t, router, service, "cloud.connections.role_plan", rolePlanParams)
	rolePlan := roleResult["role_plan"].(map[string]any)
	connectionID := rolePlan["cloud_connection_id"].(string)
	parsedConnectionID, err := uuid.Parse(connectionID)
	if err != nil || parsedConnectionID.String() != connectionID {
		t.Fatalf("remote Agent connection target is not a canonical UUID: %q err=%v", connectionID, err)
	}
	replayedRolePlan := cloudCommand(t, router, service, "cloud.connections.role_plan", rolePlanParams)["role_plan"].(map[string]any)
	if replayedRolePlan["cloud_connection_id"] != connectionID || replayedRolePlan["bootstrap_id"] != rolePlan["bootstrap_id"] {
		t.Fatalf("remote role-plan replay changed its durable target: first=%#v replay=%#v", rolePlan, replayedRolePlan)
	}
	bootstrapParams := map[string]any{
		"bootstrap_id": rolePlan["bootstrap_id"], "expected_revision": rolePlan["revision"], "idempotency_key": uuid.NewString(),
	}

	agentAttempt := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.credential_bootstrap.create", "params": bootstrapParams})
	agentAttempt.Header.Set("Authorization", "Bearer "+service.AgentToken())
	agentRecorder := httptest.NewRecorder()
	router.ServeHTTP(agentRecorder, agentAttempt)
	if agentRecorder.Code != http.StatusUnauthorized || client.createCalls != 0 {
		t.Fatalf("agent create status=%d calls=%d", agentRecorder.Code, client.createCalls)
	}

	created := cloudCommand(t, router, service, "cloud.connections.credential_bootstrap.create", bootstrapParams)
	session := created["session"].(map[string]any)
	if client.createCalls != 1 || client.created.TargetID != rolePlan["cloud_connection_id"] || client.created.Purpose != cloudmodule.AgentSecretBootstrapPurposeAWSConnection ||
		client.created.IdempotencyKey != bootstrapParams["idempotency_key"] || session["upload_url"] != cloudmodule.AgentSecretBootstrapUploadPath ||
		session["session_schema_version"] != cloudmodule.AgentSecretBootstrapSessionSchemaV1 || session["envelope_schema_version"] != cloudmodule.AgentSecretBootstrapEnvelopeSchemaV1 ||
		session["server_x25519_public_key"] != base64.RawURLEncoding.EncodeToString(client.serverPublicKey) || session["upload_token"] != base64.RawURLEncoding.EncodeToString(client.uploadToken) ||
		session["created_at"] != now.Format(time.RFC3339Nano) || session["expires_at"] != now.Add(10*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("remote create request=%#v response=%#v", client.created, session)
	}
	for _, forbidden := range []string{"upload_bearer", "receipt", "stack_id", "aad", "hkdf"} {
		if _, present := session[forbidden]; present {
			t.Fatalf("legacy bootstrap field %q leaked: %#v", forbidden, session)
		}
	}
	client.replayUploaded = true
	replayed := cloudCommand(t, router, service, "cloud.connections.credential_bootstrap.create", bootstrapParams)["session"].(map[string]any)
	if replayed["status"] != "uploaded" || replayed["revision"] != float64(2) || client.createCalls != 2 {
		t.Fatalf("uploaded replay = %#v calls=%d", replayed, client.createCalls)
	}
	for _, capability := range []string{"server_x25519_public_key", "upload_url", "upload_token"} {
		if _, present := replayed[capability]; present {
			t.Fatalf("uploaded replay reissued capability %q: %#v", capability, replayed)
		}
	}

	clientPublicKey, nonce, ciphertext := append(make([]byte, 31), 3), append(make([]byte, 11), 4), append(make([]byte, 16), 5)
	uploadIdempotency := uuid.NewString()
	uploadBody := map[string]any{
		"session_id": client.sessionID, "upload_token": base64.RawURLEncoding.EncodeToString(client.uploadToken),
		"client_public_key": base64.RawURLEncoding.EncodeToString(clientPublicKey), "nonce": base64.RawURLEncoding.EncodeToString(nonce),
		"ciphertext": base64.RawURLEncoding.EncodeToString(ciphertext), "idempotency_key": uploadIdempotency, "expected_revision": 1,
	}
	encoded, _ := json.Marshal(uploadBody)
	agentUpload := httptest.NewRequest(http.MethodPost, cloudmodule.AgentSecretBootstrapUploadPath, strings.NewReader(string(encoded)))
	agentUpload.Header.Set("Authorization", "Bearer "+service.AgentToken())
	agentUploadRecorder := httptest.NewRecorder()
	router.ServeHTTP(agentUploadRecorder, agentUpload)
	if agentUploadRecorder.Code != http.StatusUnauthorized || client.uploadCalls != 0 {
		t.Fatalf("agent upload status=%d calls=%d", agentUploadRecorder.Code, client.uploadCalls)
	}

	ownerUpload := httptest.NewRequest(http.MethodPost, cloudmodule.AgentSecretBootstrapUploadPath, strings.NewReader(string(encoded)))
	ownerUpload.Header.Set("Authorization", "Bearer "+service.AccessToken())
	ownerUploadRecorder := httptest.NewRecorder()
	router.ServeHTTP(ownerUploadRecorder, ownerUpload)
	if ownerUploadRecorder.Code != http.StatusOK || client.uploadCalls != 1 {
		t.Fatalf("owner upload status=%d calls=%d body=%s", ownerUploadRecorder.Code, client.uploadCalls, ownerUploadRecorder.Body.String())
	}
	if client.uploaded.SessionID != client.sessionID || client.uploaded.IdempotencyKey != uploadIdempotency || client.uploaded.ExpectedRevision != 1 ||
		!sameProductBytes(client.uploaded.UploadToken, client.uploadToken) || !sameProductBytes(client.uploaded.ClientPublicKey, clientPublicKey) ||
		!sameProductBytes(client.uploaded.Nonce, nonce) || !sameProductBytes(client.uploaded.Ciphertext, ciphertext) {
		t.Fatalf("forwarded upload = %#v", client.uploaded)
	}
	response := ownerUploadRecorder.Body.String()
	if strings.Contains(response, base64.RawURLEncoding.EncodeToString(client.uploadToken)) || strings.Contains(response, base64.RawURLEncoding.EncodeToString(ciphertext)) ||
		strings.Contains(response, "upload_token") || strings.Contains(response, "ciphertext") || strings.Contains(response, "server_x25519_public_key") ||
		strings.Contains(response, "upload_url") || !strings.Contains(response, `"status":"uploaded"`) {
		t.Fatalf("unsafe upload response: %s", response)
	}

	foundationParams := map[string]any{
		"bootstrap_id": rolePlan["bootstrap_id"], "expected_revision": rolePlan["revision"],
		"lifecycle_action": "establish", "idempotency_key": uuid.NewString(),
	}
	foundationSession := cloudCommand(t, router, service, "cloud.connections.credential_bootstrap.create", foundationParams)["session"].(map[string]any)
	if client.created.Purpose != cloudmodule.AgentSecretBootstrapPurposeAWSFoundationEstablish ||
		client.created.TargetID != rolePlan["cloud_connection_id"] ||
		foundationSession["purpose"] != cloudmodule.AgentSecretBootstrapPurposeAWSFoundationEstablish {
		t.Fatalf("Foundation establish bootstrap request=%#v response=%#v", client.created, foundationSession)
	}
}

func sameProductBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
