package cloud

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestConnectionCredentialBootstrapUsesOnlyDurableRolePlanAndPassesSessionState(t *testing.T) {
	now := time.Date(2026, time.July, 16, 5, 0, 0, 0, time.UTC)
	publicKey := testCredentialBootstrapSPKI(t)
	connectionID := "connection-bootstrap-test-0001"
	plan := ConnectionRolePlan{
		BootstrapID: "bootstrap-credential-test-0001", CloudConnectionID: connectionID, Provider: "aws", Region: "us-east-1",
		Status: ConnectionBootstrapAwaitingStack, Revision: 1, ExpiresAt: now.Add(15 * time.Minute).UnixMilli(),
		ConnectionTemplate:           credentialBootstrapPublishIntent(),
		TemplateURL:                  "",
		TemplateDigest:               "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SourceTreeDigest:             "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		StackName:                    connectionStackName(connectionID),
		AllowRootCredentialBootstrap: true,
		CloudFormationParams: map[string]string{
			"ConnectionId": connectionID, "ConnectionGeneration": "1", "NodeKeyId": "node-key-bootstrap-0001",
			"NodePublicKeySpkiBase64": publicKey, "DeviceApprovalKeyId": "device-key-bootstrap-0001",
			"DeviceApprovalPublicKeySpkiBase64": publicKey, "StageName": "prod",
		},
	}
	store := &credentialBootstrapModuleStore{plan: plan}
	client := &credentialBootstrapModuleClient{now: now}
	module := New(store, Config{
		OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now },
		CredentialBootstrapClient: client,
	})
	params := map[string]any{"bootstrap_id": plan.BootstrapID, "expected_revision": float64(1), "idempotency_key": "019f6a80-1234-7abc-8def-0123456789ab"}
	result, apiErr := module.Handlers()[actionConnectionsCredentialBootstrapCreate](t.Context(), params)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if store.calls != 2 || client.calls != 1 {
		t.Fatalf("load calls=%d client calls=%d", store.calls, client.calls)
	}
	if client.request.RequestID != params["idempotency_key"] || client.request.RolePlan.BootstrapID != plan.BootstrapID || client.request.RolePlan.ConnectionID != connectionID ||
		client.request.RolePlan.NodeEd25519PublicKey != publicKey || client.request.RolePlan.DeviceEd25519PublicKey != publicKey ||
		!client.request.RolePlan.AllowRootCredentialBootstrap || !reflect.DeepEqual(client.request.RolePlan.FixedParameters, plan.CloudFormationParams) || !reflect.DeepEqual(client.request.RolePlan.ConnectionTemplate, plan.ConnectionTemplate) {
		t.Fatalf("request was not derived from durable role plan: %#v", client.request)
	}
	encodedRequest, err := json.Marshal(client.request)
	if err != nil || !strings.Contains(string(encodedRequest), `"connection_template"`) || strings.Contains(string(encodedRequest), `"template_url"`) || strings.Contains(string(encodedRequest), `"template_digest"`) {
		t.Fatalf("credential bootstrap wire must carry only the typed template reference: %s err=%v", encodedRequest, err)
	}
	response := result.(map[string]any)["session"].(map[string]any)
	if response["status"] != "awaiting_upload" || response["upload_bearer"] == "" || response["session_id"] != "aws-bootstrap-session-test-0001" {
		t.Fatalf("unexpected awaiting response: %#v", response)
	}
	for _, forbidden := range []string{"schema", "request_id", "connection_id", "hkdf", "aad"} {
		if _, present := response[forbidden]; present {
			t.Fatalf("internal upstream field %q leaked: %#v", forbidden, response)
		}
	}

	client.accepted = true
	result, apiErr = module.Handlers()[actionConnectionsCredentialBootstrapCreate](t.Context(), params)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	response = result.(map[string]any)["session"].(map[string]any)
	if response["status"] != "accepted" || response["upload_bearer"] != "" || response["receipt"] == nil {
		t.Fatalf("accepted replay must pass status/receipt without bearer: %#v", response)
	}

	injected := map[string]any{"bootstrap_id": plan.BootstrapID, "expected_revision": float64(1), "idempotency_key": params["idempotency_key"], "region": "eu-west-1"}
	if _, apiErr = module.Handlers()[actionConnectionsCredentialBootstrapCreate](t.Context(), injected); apiErr == nil || client.calls != 2 {
		t.Fatalf("client-supplied role-plan material was accepted: err=%v calls=%d", apiErr, client.calls)
	}
}

func credentialBootstrapPublishIntent() ConnectionTemplateReference {
	return ConnectionTemplateReference{
		Schema: connectionTemplateReferenceSchema,
		Mode:   connectionTemplateModePublishIntent,
		PublishIntent: &ConnectionTemplatePublishIntent{
			Kind: connectionTemplateArtifactKind, Version: "v1.1.0-cloud-mvp.20260716.1",
			SHA256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 512, ContentType: connectionTemplateContentType,
		},
	}
}

func TestConnectionCredentialBootstrapMissingClientIsUnavailable(t *testing.T) {
	module := New(&credentialBootstrapModuleStore{}, Config{OwnerMXID: func() string { return "@owner:example.com" }})
	_, apiErr := module.Handlers()[actionConnectionsCredentialBootstrapCreate](t.Context(), map[string]any{
		"bootstrap_id": "bootstrap-credential-test-0001", "expected_revision": float64(1), "idempotency_key": "019f6a80-1234-7abc-8def-0123456789ab",
	})
	if apiErr == nil || apiErr.Status != 503 || apiErr.Code != cloudConnectionCredentialBootstrapUnavailableCode {
		t.Fatalf("missing client error=%#v", apiErr)
	}
}

type credentialBootstrapModuleStore struct {
	Store
	plan         ConnectionRolePlan
	calls        int
	loadRequests []LoadConnectionCredentialBootstrapRequest
}

func (store *credentialBootstrapModuleStore) LoadCloudConnectionCredentialBootstrap(_ context.Context, request LoadConnectionCredentialBootstrapRequest) (ConnectionRolePlan, error) {
	store.calls++
	store.loadRequests = append(store.loadRequests, request)
	if request.OwnerMXID != "@owner:example.com" || request.BootstrapID != store.plan.BootstrapID || request.ExpectedRevision != store.plan.Revision {
		return ConnectionRolePlan{}, ErrConnectionBootstrapConflict
	}
	return store.plan, nil
}

type credentialBootstrapModuleClient struct {
	now      time.Time
	request  ConnectionCredentialBootstrapRequest
	calls    int
	accepted bool
}

func (client *credentialBootstrapModuleClient) CreateSession(_ context.Context, request ConnectionCredentialBootstrapRequest) (ConnectionCredentialBootstrapSession, error) {
	client.calls++
	client.request = request
	expiresAt := client.now.Add(10 * time.Minute).Format(time.RFC3339Nano)
	session := ConnectionCredentialBootstrapSession{
		Schema: connectionCredentialBootstrapResponseSchema, Status: "awaiting_upload", RequestID: request.RequestID,
		SessionID: "aws-bootstrap-session-test-0001", ConnectionID: request.RolePlan.ConnectionID,
		ServerX25519PublicKey: base64.StdEncoding.EncodeToString(append(make([]byte, 31), 1)), UploadBearer: base64.RawURLEncoding.EncodeToString(append(make([]byte, 31), 2)),
		UploadURL: "https://bootstrap.example.invalid/v1/aws-bootstrap/sessions/aws-bootstrap-session-test-0001",
		ExpiresAt: expiresAt, HKDF: connectionCredentialBootstrapHKDF,
	}
	aad, _ := json.Marshal(struct {
		Schema       string `json:"schema"`
		SessionID    string `json:"session_id"`
		ConnectionID string `json:"connection_id"`
		ExpiresAt    string `json:"expires_at"`
	}{connectionCredentialUploadEnvelopeSchema, session.SessionID, session.ConnectionID, session.ExpiresAt})
	session.AAD = string(aad)
	if client.accepted {
		session.Status = "accepted"
		session.UploadBearer = ""
		session.Receipt = &ConnectionCredentialBootstrapReceipt{
			Schema: connectionCredentialBootstrapReceiptSchema, Status: "accepted",
			StackID:      "arn:aws:cloudformation:us-east-1:123456789012:stack/dirextalk/test-stack-id",
			ConnectionID: session.ConnectionID, AcceptedAt: client.now.Add(time.Minute).Format(time.RFC3339Nano),
		}
	}
	return session, nil
}

func testCredentialBootstrapSPKI(t *testing.T) string {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(der)
}
