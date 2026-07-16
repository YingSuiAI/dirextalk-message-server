package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
)

type productIdentityPreviewClient struct {
	request  cloudmodule.IdentityPreviewRequest
	evidence cloudmodule.IdentityPreviewEvidence
	calls    int
	err      error
}

func (client *productIdentityPreviewClient) PreviewAgentAWSIdentity(_ context.Context, request cloudmodule.IdentityPreviewRequest) (cloudmodule.IdentityPreviewEvidence, error) {
	client.calls++
	client.request = request
	return client.evidence, client.err
}

func TestRemoteIdentityPreviewBindsDurableRolePlanAndDoesNotCreateConnection(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	sessionID := uuid.NewString()
	client := &productIdentityPreviewClient{}
	service := NewService(Config{
		ServerName: "example.com", CloudSecretBootstrapClient: &productSecretBootstrapClient{}, CloudIdentityPreviewClient: client,
		CloudConnectionStack: CloudConnectionStackConfig{
			TemplateDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ConnectionTemplate: testPublishIntentConnectionTemplate(),
			SourceTreeDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", NodeKeyID: "node-key-1",
			NodePublicKeySPKIBase64: testEd25519SPKIBase64(t), RolePlanTTL: 15 * time.Minute,
		},
	})
	router := newP2PTestRouter(service)
	roleResult := cloudCommand(t, router, service, "cloud.connections.role_plan", map[string]any{
		"provider": "aws", "region": "ap-northeast-1", "device_approval_key_id": "device-key-root-1",
		"device_approval_public_key_spki_base64": testEd25519SPKIBase64(t), "allow_root_credential_bootstrap": true,
		"idempotency_key": uuid.NewString(),
	})
	rolePlan := roleResult["role_plan"].(map[string]any)
	connectionID := rolePlan["cloud_connection_id"].(string)
	parsedConnectionID, err := uuid.Parse(connectionID)
	if err != nil || parsedConnectionID.String() != connectionID {
		t.Fatalf("identity preview target is not the canonical Agent connection UUID: %q err=%v", connectionID, err)
	}
	client.evidence = cloudmodule.IdentityPreviewEvidence{
		BootstrapSessionID: sessionID, SessionRevision: 2, OwnerID: "dirextalk-project:example.com", TargetID: connectionID,
		AccountID: "123456789012", PrincipalARN: "arn:aws:iam::123456789012:root", PrincipalID: "123456789012",
		Region: "ap-northeast-1", RootIdentity: true, ObservedAt: now.Format(time.RFC3339Nano),
		ExpiresAt: now.Add(5 * time.Minute).Format(time.RFC3339Nano),
	}
	params := map[string]any{
		"bootstrap_id": rolePlan["bootstrap_id"], "expected_revision": rolePlan["revision"],
		"session_id": sessionID, "expected_session_revision": 2,
	}

	agentAttempt := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.identity.preview", "params": params})
	agentAttempt.Header.Set("Authorization", "Bearer "+service.AgentToken())
	agentRecorder := httptest.NewRecorder()
	router.ServeHTTP(agentRecorder, agentAttempt)
	if agentRecorder.Code != http.StatusUnauthorized || client.calls != 0 {
		t.Fatalf("agent identity preview status=%d calls=%d", agentRecorder.Code, client.calls)
	}

	injected := make(map[string]any, len(params)+1)
	for key, value := range params {
		injected[key] = value
	}
	injected["region"] = "us-east-1"
	request := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.identity.preview", "params": injected})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || client.calls != 0 {
		t.Fatalf("client-supplied Region status=%d calls=%d body=%s", recorder.Code, client.calls, recorder.Body.String())
	}

	result := cloudCommand(t, router, service, "cloud.connections.identity.preview", params)
	identity := result["identity"].(map[string]any)
	if client.calls != 1 || client.request.BootstrapSessionID != sessionID || client.request.ExpectedSessionRevision != 2 ||
		client.request.TargetID != connectionID || client.request.Region != "ap-northeast-1" {
		t.Fatalf("identity preview request=%#v calls=%d", client.request, client.calls)
	}
	if len(result) != 7 || identity["account_id"] != "123456789012" || identity["principal_arn"] != "arn:aws:iam::123456789012:root" ||
		identity["principal_id"] != "123456789012" || identity["region"] != "ap-northeast-1" || identity["root_identity"] != true ||
		result["cloud_connection_id"] != connectionID || result["bootstrap_session_id"] != sessionID || result["session_revision"] != float64(2) ||
		result["verification_status"] != "identity_verified" || result["observed_at"] != now.Format(time.RFC3339Nano) ||
		result["expires_at"] != now.Add(5*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("identity preview result=%#v", result)
	}
	connections := cloudCommand(t, router, service, "cloud.connections.list", map[string]any{})["connections"].([]any)
	if len(connections) != 0 {
		t.Fatalf("identity preview created an active connection: %#v", connections)
	}
}

func TestRemoteIdentityPreviewRejectsInvalidEvidenceAndDefaultsUnavailable(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	client := &productIdentityPreviewClient{}
	service := NewService(Config{
		ServerName: "example.com", CloudIdentityPreviewClient: client,
		CloudConnectionStack: CloudConnectionStackConfig{
			TemplateDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ConnectionTemplate: testPublishIntentConnectionTemplate(),
			SourceTreeDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", NodeKeyID: "node-key-1",
			NodePublicKeySPKIBase64: testEd25519SPKIBase64(t), RolePlanTTL: 15 * time.Minute,
		},
	})
	router := newP2PTestRouter(service)
	rolePlan := cloudCommand(t, router, service, "cloud.connections.role_plan", map[string]any{
		"provider": "aws", "region": "us-east-1", "device_approval_key_id": "device-key-root-1",
		"device_approval_public_key_spki_base64": testEd25519SPKIBase64(t), "allow_root_credential_bootstrap": true,
		"idempotency_key": uuid.NewString(),
	})["role_plan"].(map[string]any)
	sessionID := uuid.NewString()
	client.evidence = cloudmodule.IdentityPreviewEvidence{
		BootstrapSessionID: sessionID, SessionRevision: 2, OwnerID: "dirextalk-project:example.com", TargetID: rolePlan["cloud_connection_id"].(string),
		AccountID: "123456789012", PrincipalARN: "arn:aws:iam::999999999999:root", PrincipalID: "123456789012",
		Region: "us-east-1", RootIdentity: true, ObservedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
	}
	params := map[string]any{"bootstrap_id": rolePlan["bootstrap_id"], "expected_revision": rolePlan["revision"], "session_id": sessionID, "expected_session_revision": 2}
	request := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.identity.preview", "params": params})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("invalid identity evidence status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	unavailable := NewService(Config{ServerName: "example.com"})
	unavailableRouter := newP2PTestRouter(unavailable)
	unavailableRequest := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.identity.preview", "params": params})
	unavailableRequest.Header.Set("Authorization", "Bearer "+unavailable.AccessToken())
	unavailableRecorder := httptest.NewRecorder()
	unavailableRouter.ServeHTTP(unavailableRecorder, unavailableRequest)
	if unavailableRecorder.Code != http.StatusServiceUnavailable || unavailableRecorder.Body.String() == "" {
		t.Fatalf("default identity preview status=%d body=%s", unavailableRecorder.Code, unavailableRecorder.Body.String())
	}
	if !strings.Contains(unavailableRecorder.Body.String(), `"code":"cloud_connection_identity_preview_unavailable"`) {
		t.Fatalf("default identity preview returned unstable error: %s", unavailableRecorder.Body.String())
	}

	client.evidence.PrincipalARN = "arn:aws:iam::123456789012:root"
	for name, test := range map[string]struct {
		err    error
		status int
		code   string
	}{
		"stale session":        {err: cloudmodule.ErrIdentityPreviewConflict, status: http.StatusConflict, code: "cloud_connection_identity_preview_conflict"},
		"rejected credential":  {err: cloudmodule.ErrIdentityPreviewRejected, status: http.StatusForbidden, code: "cloud_connection_identity_preview_rejected"},
		"provider unavailable": {err: cloudmodule.ErrIdentityPreviewUnavailable, status: http.StatusServiceUnavailable, code: "cloud_connection_identity_preview_unavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			client.err = test.err
			request := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.identity.preview", "params": params})
			request.Header.Set("Authorization", "Bearer "+service.AccessToken())
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != test.status || !strings.Contains(recorder.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("identity preview error status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}
