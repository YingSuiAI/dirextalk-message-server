package cloud

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type foundationSecretBootstrapClient struct {
	request CreateAgentSecretBootstrapRequest
	session AgentSecretBootstrapSession
	calls   int
}

func (client *foundationSecretBootstrapClient) CreateAgentSecretBootstrap(_ context.Context, request CreateAgentSecretBootstrapRequest) (AgentSecretBootstrapSession, error) {
	client.calls++
	client.request = request
	return client.session, nil
}

func (*foundationSecretBootstrapClient) UploadAgentEncryptedSecret(context.Context, UploadAgentEncryptedSecretRequest) (AgentSecretBootstrapSession, error) {
	return AgentSecretBootstrapSession{}, ErrAgentSecretBootstrapUnavailable
}

type foundationIdentityPreviewClient struct {
	request  IdentityPreviewRequest
	evidence IdentityPreviewEvidence
}

func (client *foundationIdentityPreviewClient) PreviewAgentAWSIdentity(_ context.Context, request IdentityPreviewRequest) (IdentityPreviewEvidence, error) {
	client.request = request
	return client.evidence, nil
}

func TestAgentFoundationFacadePreservesSignedScopeAndRecoversApprovalReadback(t *testing.T) {
	now := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	operationID, challengeID, approvalID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	connectionID, sessionID, agentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	scope := AgentCloudFoundationScope{SchemaVersion: "dirextalk.agent.aws-foundation-operation-scope/v1", AgentInstanceID: agentID, OwnerID: "@owner:example.com",
		Action: "upgrade", ConnectionID: connectionID, ExpectedConnectionRevision: 7, AccountID: "123456789012", Region: "ap-south-1",
		BootstrapSessionID: sessionID, ExpectedBootstrapRevision: 2, ExpectedCredentialGeneration: 3,
		FoundationTemplateDigest: foundationTestDigest("a"), ReaperImageURI: "repo/reaper:v1@" + foundationTestDigest("b"),
		ReleaseEnvironment: AgentCloudFoundationReleaseEnvironment{PrivateSubnetCIDR: "10.255.0.0/26", ZeroIngress: true, ArtifactBucket: "dtx-agent-artifacts", KMSAlias: "alias/dtx-agent-test", BucketVersioned: true, BucketSSEKMS: true},
		IdentityObservedAt: now.Add(-time.Minute), IdentityExpiresAt: now.Add(time.Minute)}
	challenge := AgentCloudFoundationChallenge{OperationID: operationID, ChallengeID: challengeID, ApprovalID: approvalID, SignerKeyID: "device-key-1",
		ScopeDigest: foundationTestDigest("c"), Scope: scope, ExpiresAt: now.Add(5 * time.Minute), SigningPayloadCBOR: []byte{0xa1, 0x01, 0x02}, Revision: 1}
	approved := AgentCloudFoundationOperation{OperationID: operationID, OwnerID: scope.OwnerID, ConnectionID: connectionID, Action: scope.Action,
		ApprovalID: approvalID, ScopeDigest: challenge.ScopeDigest, Status: "running", Revision: 3, CreatedAt: now, UpdatedAt: now.Add(time.Second)}
	client := &agentControlModuleClient{foundationChallenge: challenge, foundationApproveErr: ErrAgentCloudControlUnavailable,
		foundationOperation: approved, foundationOperationFound: true}
	module := New(nil, Config{OwnerMXID: func() string { return scope.OwnerID }, Now: func() time.Time { return now }, AgentCloudControlClient: client})

	prepared, apiErr := module.Handlers()[actionConnectionsFoundationConfirmationPrepare](t.Context(), map[string]any{
		"action": scope.Action, "cloud_connection_id": connectionID, "bootstrap_session_id": sessionID, "expected_bootstrap_revision": int64(2),
		"signer_key_id": challenge.SignerKeyID, "idempotency_key": uuid.NewString(),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	confirmation := prepared.(map[string]any)["confirmation"].(map[string]any)
	approval := confirmation["approval"].(agentFoundationApprovalV1)
	if approval.Scope != scope || approval.OwnerID != scope.OwnerID || client.foundationChallengeRequest.ConnectionID != connectionID ||
		confirmation["signing_payload_cbor"] != base64.RawURLEncoding.EncodeToString(challenge.SigningPayloadCBOR) {
		t.Fatalf("confirmation lost signed scope: approval=%#v request=%#v", approval, client.foundationChallengeRequest)
	}
	approval.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	result, apiErr := module.Handlers()[actionConnectionsFoundationApprove](t.Context(), map[string]any{"approval": approval, "idempotency_key": uuid.NewString()})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	operation := result.(map[string]any)["operation"].(map[string]any)
	if operation["operation_id"] != operationID || operation["status"] != "running" || client.foundationApproveRequest.ExpectedScopeDigest != challenge.ScopeDigest {
		t.Fatalf("approval/readback mismatch: result=%#v request=%#v", result, client.foundationApproveRequest)
	}
	read, apiErr := module.Handlers()[actionConnectionsFoundationOperationsGet](t.Context(), map[string]any{"operation_id": operationID})
	if apiErr != nil || read.(map[string]any)["operation"].(map[string]any)["scope_digest"] != challenge.ScopeDigest {
		t.Fatalf("get result=%#v error=%v", read, apiErr)
	}
}

func TestAgentFoundationLifecycleBootstrapAndIdentityDeriveOwnerScopedConnection(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	ownerID, connectionID, sessionID := "dirextalk-project:example.com", uuid.NewString(), uuid.NewString()
	connection := AgentCloudConnection{ConnectionID: connectionID, OwnerID: ownerID, AccountID: "123456789012", Region: "ap-south-1",
		ControlRoleARN: "arn:aws:iam::123456789012:role/dirextalk-agent-control", FoundationStackID: "foundation-stack",
		Status: "active", Revision: 7, CredentialGeneration: 3, CreatedAt: now.Add(-time.Hour), UpdatedAt: now}
	control := &agentControlModuleClient{plan: AgentCloudPlan{ConnectionID: connectionID}, recoveredConnection: connection, recoveredFound: true}
	secret := &foundationSecretBootstrapClient{session: AgentSecretBootstrapSession{
		SessionSchemaVersion: AgentSecretBootstrapSessionSchemaV1, EnvelopeSchemaVersion: AgentSecretBootstrapEnvelopeSchemaV1,
		SessionID: sessionID, AgentInstanceID: uuid.NewString(), OwnerID: ownerID, Purpose: AgentSecretBootstrapPurposeAWSFoundationUpgrade,
		TargetID: connectionID, ServerPublicKey: append(make([]byte, 31), 1), UploadToken: append(make([]byte, 31), 2),
		CreatedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(10 * time.Minute).Format(time.RFC3339Nano), Status: "awaiting_upload", Revision: 1,
	}}
	identity := &foundationIdentityPreviewClient{evidence: IdentityPreviewEvidence{
		BootstrapSessionID: sessionID, SessionRevision: 2, OwnerID: ownerID, TargetID: connectionID,
		AccountID: connection.AccountID, PrincipalARN: "arn:aws:iam::123456789012:root", PrincipalID: connection.AccountID,
		Region: connection.Region, RootIdentity: true, ObservedAt: now.Format(time.RFC3339Nano), ExpiresAt: now.Add(5 * time.Minute).Format(time.RFC3339Nano),
	}}
	module := New(nil, Config{OwnerMXID: func() string { return ownerID }, Now: func() time.Time { return now },
		AgentCloudControlClient: control, SecretBootstrapClient: secret, IdentityPreviewClient: identity})

	bootstrapParams := map[string]any{"lifecycle_action": "upgrade", "cloud_connection_id": connectionID,
		"expected_connection_revision": int64(7), "idempotency_key": uuid.NewString()}
	created, apiErr := module.Handlers()[actionConnectionsCredentialBootstrapCreate](t.Context(), bootstrapParams)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	session := created.(map[string]any)["session"].(map[string]any)
	if secret.request.Purpose != AgentSecretBootstrapPurposeAWSFoundationUpgrade || secret.request.TargetID != connectionID ||
		session["purpose"] != AgentSecretBootstrapPurposeAWSFoundationUpgrade || control.getConnectionCalls != 2 || secret.calls != 1 {
		t.Fatalf("bootstrap request=%#v session=%#v connection_reads=%d", secret.request, session, control.getConnectionCalls)
	}
	if _, apiErr = module.Handlers()[actionConnectionsCredentialBootstrapCreate](t.Context(), map[string]any{
		"bootstrap_id": "legacy-plan", "expected_revision": int64(1), "lifecycle_action": "upgrade",
		"cloud_connection_id": connectionID, "expected_connection_revision": int64(7), "idempotency_key": uuid.NewString(),
	}); apiErr == nil || secret.calls != 1 {
		t.Fatalf("mixed legacy/lifecycle shape was accepted: error=%v", apiErr)
	}

	preview, apiErr := module.Handlers()[actionConnectionsIdentityPreview](t.Context(), map[string]any{
		"lifecycle_action": "upgrade", "cloud_connection_id": connectionID, "expected_connection_revision": int64(7),
		"session_id": sessionID, "expected_session_revision": int64(2),
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if identity.request.TargetID != connectionID || identity.request.Region != connection.Region ||
		preview.(map[string]any)["lifecycle_action"] != "upgrade" || preview.(map[string]any)["connection_revision"] != int64(7) {
		t.Fatalf("preview request=%#v result=%#v", identity.request, preview)
	}
	control.recoveredConnection.Status = "tearing_down"
	if validateReadableAgentCloudConnection(control.recoveredConnection, connectionID) != nil {
		t.Fatal("tearing_down must remain readable while provider teardown is pending")
	}
	if _, apiErr = module.loadAgentFoundationConnection(t.Context(), "teardown", connectionID, 7); apiErr == nil {
		t.Fatal("tearing_down must not be treated as an active lifecycle input")
	}
}

func foundationTestDigest(value string) string { return "sha256:" + strings.Repeat(value, 64) }
