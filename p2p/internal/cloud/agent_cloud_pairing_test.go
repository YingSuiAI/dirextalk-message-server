package cloud

import (
	"context"
	"encoding/base64"
	"reflect"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
)

type pairingModuleClient struct {
	*agentControlModuleClient
	result           AgentCloudPairingPayloadResult
	request          AgentCloudPairingPayloadRequest
	err              error
	calls            int
	session          AgentCloudPairingSession
	sessionFound     bool
	sessionErr       error
	challenge        AgentCloudPairingResumeChallenge
	challengeRequest AgentCloudPairingResumeChallengeRequest
	challengeErr     error
	resumed          AgentCloudPairingSession
	approveRequest   AgentCloudPairingResumeApproveRequest
	approveErr       error
}

func (client *pairingModuleClient) RetrieveAgentCloudPairingPayload(_ context.Context, request AgentCloudPairingPayloadRequest) (AgentCloudPairingPayloadResult, error) {
	client.calls++
	client.request = request
	return client.result, client.err
}

func (client *pairingModuleClient) GetAgentCloudPairing(context.Context, AgentCloudPairingGetRequest) (AgentCloudPairingSession, bool, error) {
	return client.session, client.sessionFound, client.sessionErr
}

func (client *pairingModuleClient) CreateAgentCloudPairingResumeChallenge(_ context.Context, request AgentCloudPairingResumeChallengeRequest) (AgentCloudPairingResumeChallenge, error) {
	client.challengeRequest = request
	return client.challenge, client.challengeErr
}

func (client *pairingModuleClient) ApproveAgentCloudPairingResume(_ context.Context, request AgentCloudPairingResumeApproveRequest) (AgentCloudPairingSession, error) {
	client.approveRequest = request
	return client.resumed, client.approveErr
}

func TestAgentPairingPayloadIsOwnerBoundOpaqueAndNeverPublished(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	recipient := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	serverKey := base64.RawURLEncoding.EncodeToString(bytesFilled(32, 1))
	nonce := base64.RawURLEncoding.EncodeToString(bytesFilled(12, 2))
	ciphertext := base64.RawURLEncoding.EncodeToString(bytesFilled(33, 3))
	deploymentID := "11111111-1111-4111-8111-111111111111"
	pairingID := "22222222-2222-4222-8222-222222222222"
	session := AgentCloudPairingSession{
		PairingID: pairingID, OwnerID: "@owner:example.com", DeploymentID: deploymentID,
		TaskID: "33333333-3333-4333-8333-333333333333", StepID: "44444444-4444-4444-8444-444444444444",
		PlanID: "55555555-5555-4555-8555-555555555555", ConnectionID: "66666666-6666-4666-8666-666666666666",
		RecipeID: "recipe-pairing", RecipeDigest: "sha256:" + strings.Repeat("a", 64), RecipeRevision: 2,
		BeginCommandID: "pairing.begin", ResumeCommandID: "pairing.resume",
		ExecutionManifestDigest: "sha256:" + strings.Repeat("b", 64),
		Status:                  "waiting_payload", PayloadReady: false, DeploymentRevision: 7, Revision: 8,
		ExpiresAt: now.Add(10 * time.Minute), CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
	}
	retrieved := session
	retrieved.Revision = 9
	retrieved.Status = "waiting_user"
	retrieved.PayloadReady = true
	retrieved.PayloadScopeRevision = 8
	retrieved.UpdatedAt = now.Add(time.Second)
	client := &pairingModuleClient{
		agentControlModuleClient: &agentControlModuleClient{},
		session:                  session,
		sessionFound:             true,
		result: AgentCloudPairingPayloadResult{
			Pairing: retrieved,
			Payload: AgentCloudPairingPayload{
				SchemaVersion: pairingPayloadEnvelopeSchemaV1, ServerEphemeralPublicKey: serverKey,
				Nonce: nonce, Ciphertext: ciphertext, AssociatedDataCBOR: []byte{0xa0},
				PayloadDigest: "sha256:" + strings.Repeat("c", 64), ExpiresAt: session.ExpiresAt,
			},
		},
	}
	published := 0
	module := New(nil, Config{
		OwnerMXID: func() string { return session.OwnerID }, Now: func() time.Time { return now },
		AgentCloudControlClient: client,
		Publish:                 func(context.Context, string, string, map[string]any) error { published++; return nil },
	})
	idempotencyKey := "77777777-7777-4777-8777-777777777777"
	result, apiErr := module.Handlers()[serviceapi.CloudDeploymentPairingPayloadRetrieveAction](t.Context(), map[string]any{
		"deployment_id": deploymentID, "recipient_public_key": recipient, "idempotency_key": idempotencyKey,
	})
	if apiErr != nil || client.calls != 1 || published != 0 {
		t.Fatalf("result=%#v err=%#v calls=%d published=%d", result, apiErr, client.calls, published)
	}
	if client.request != (AgentCloudPairingPayloadRequest{
		DeploymentID: deploymentID, PairingID: pairingID, ExpectedRevision: 8,
		RecipientPublicKey: recipient, IdempotencyKey: idempotencyKey,
	}) {
		t.Fatalf("request=%#v", client.request)
	}
	view := result.(map[string]any)
	payload := view["payload"].(map[string]any)
	wantPayload := map[string]any{
		"schema_version": pairingPayloadEnvelopeSchemaV1, "server_ephemeral_public_key_b64": serverKey,
		"nonce_b64": nonce, "ciphertext_b64": ciphertext,
		"associated_data_cbor_b64": base64.RawURLEncoding.EncodeToString([]byte{0xa0}),
	}
	if !reflect.DeepEqual(payload, wantPayload) {
		t.Fatalf("payload=%#v want=%#v", payload, wantPayload)
	}
	if view["payload_digest"] != client.result.Payload.PayloadDigest || view["payload_scope_revision"] != int64(8) ||
		view["expires_at"] != client.result.Payload.ExpiresAt {
		t.Fatalf("payload binding metadata missing: %#v", view)
	}
	client.session = retrieved
	replayed, replayErr := module.Handlers()[serviceapi.CloudDeploymentPairingPayloadRetrieveAction](t.Context(), map[string]any{
		"deployment_id": deploymentID, "recipient_public_key": recipient, "idempotency_key": idempotencyKey,
	})
	if replayErr != nil || client.request.ExpectedRevision != 8 ||
		replayed.(map[string]any)["payload_scope_revision"] != int64(8) {
		t.Fatalf("response-loss replay=%#v request=%#v err=%#v", replayed, client.request, replayErr)
	}
}

func TestCanonicalDeploymentPairingResumeUsesAgentWithoutLocalMutationOrPublish(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	deployment := Deployment{
		DeploymentID: "11111111-1111-4111-8111-111111111111",
		PlanID:       "55555555-5555-4555-8555-555555555555", ConnectionID: "66666666-6666-4666-8666-666666666666",
		Execution: "finished", Outcome: "succeeded", Resource: "active", Revision: 7,
		CreatedAt: now.Add(-time.Hour).UnixMilli(), UpdatedAt: now.Add(-time.Minute).UnixMilli(),
	}
	session := AgentCloudPairingSession{
		PairingID: "22222222-2222-4222-8222-222222222222", OwnerID: "@owner:example.com", DeploymentID: deployment.DeploymentID,
		TaskID: "33333333-3333-4333-8333-333333333333", StepID: "44444444-4444-4444-8444-444444444444",
		PlanID: deployment.PlanID, ConnectionID: deployment.ConnectionID,
		RecipeID: "recipe-pairing", RecipeDigest: "sha256:" + strings.Repeat("a", 64), RecipeRevision: 2,
		BeginCommandID: "pairing.begin", ResumeCommandID: "pairing.resume",
		ExecutionManifestDigest: "sha256:" + strings.Repeat("b", 64),
		Status:                  "waiting_user", PayloadReady: true, DeploymentRevision: 7, PayloadScopeRevision: 7, Revision: 8,
		ExpiresAt: now.Add(10 * time.Minute), CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
	}
	challengeApproval, err := cloudcontracts.NewPairingResumeApprovalV1(cloudcontracts.PairingResumeTargetV1{
		DeploymentID: deployment.DeploymentID, DeploymentRevision: uint64(deployment.Revision),
		PlanID: deployment.PlanID, CloudConnectionID: deployment.ConnectionID, ExecutionID: session.TaskID,
		RecipeExecutionManifestDigest: session.ExecutionManifestDigest,
		JobID:                         session.PairingID, JobRevision: uint64(session.Revision),
	}, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "device-pairing", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	signing, err := challengeApproval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	resumed := session
	resumed.Status, resumed.Revision, resumed.UpdatedAt = "succeeded", 9, now.Add(time.Minute)
	client := &pairingModuleClient{
		agentControlModuleClient: &agentControlModuleClient{}, session: session, sessionFound: true,
		challenge: AgentCloudPairingResumeChallenge{Approval: challengeApproval, SigningPayloadCBOR: signing},
		resumed:   resumed,
	}
	reader := &agentDestroyDeploymentReader{deployment: deployment, found: true}
	published := 0
	module := New(nil, Config{
		OwnerMXID: func() string { return session.OwnerID }, Now: func() time.Time { return now },
		DeploymentReader: reader, AgentCloudControlClient: client,
		Publish: func(context.Context, string, string, map[string]any) error { published++; return nil },
	})
	handler := module.Handlers()[serviceapi.CloudDeploymentPairingResumeAction]
	prepared, apiErr := handler(t.Context(), map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"idempotency_key": "77777777-7777-4777-8777-777777777777",
	})
	if apiErr != nil || published != 0 || client.challengeRequest.PairingID != session.PairingID ||
		client.challengeRequest.ExpectedPairingRevision != session.Revision || client.challengeRequest.SignerKeyID != "" {
		t.Fatalf("prepared=%#v err=%#v request=%#v published=%d", prepared, apiErr, client.challengeRequest, published)
	}
	confirmation := prepared.(map[string]any)["confirmation"].(PairingResumeConfirmation)
	if confirmation.Deployment != deployment || confirmation.Job.JobID != session.PairingID ||
		confirmation.Job.Execution != "waiting_user" || confirmation.Approval != challengeApproval {
		t.Fatalf("confirmation=%#v", confirmation)
	}
	signed := challengeApproval
	signed.Signature = base64.RawURLEncoding.EncodeToString(bytesFilled(64, 9))
	approved, apiErr := handler(t.Context(), map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"approval": signed, "idempotency_key": "88888888-8888-4888-8888-888888888888",
	})
	if apiErr != nil || published != 0 || client.approveRequest.PairingID != session.PairingID ||
		client.approveRequest.ExpectedPairingRevision != session.Revision {
		t.Fatalf("approved=%#v err=%#v request=%#v published=%d", approved, apiErr, client.approveRequest, published)
	}
	result := approved.(map[string]any)
	if result["deployment"].(Deployment) != deployment || result["job"].(Job).Revision != resumed.Revision ||
		result["job"].(Job).Execution != "finished" {
		t.Fatalf("result=%#v", result)
	}
	client.session = resumed
	replayed, apiErr := handler(t.Context(), map[string]any{
		"deployment_id": deployment.DeploymentID, "expected_revision": float64(deployment.Revision),
		"approval": signed, "idempotency_key": "88888888-8888-4888-8888-888888888888",
	})
	if apiErr != nil || replayed.(map[string]any)["job"].(Job).Revision != resumed.Revision ||
		client.approveRequest.ExpectedPairingRevision != int64(challengeApproval.JobRevision) {
		t.Fatalf("response-loss replay=%#v err=%#v request=%#v", replayed, apiErr, client.approveRequest)
	}
}

func TestAgentPairingPayloadRejectsUnknownMaterialAndInvalidAgentResponse(t *testing.T) {
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	session := AgentCloudPairingSession{
		PairingID: "22222222-2222-4222-8222-222222222222", OwnerID: "@owner:example.com",
		DeploymentID: "11111111-1111-4111-8111-111111111111",
		TaskID:       "33333333-3333-4333-8333-333333333333", StepID: "44444444-4444-4444-8444-444444444444",
		PlanID: "55555555-5555-4555-8555-555555555555", ConnectionID: "66666666-6666-4666-8666-666666666666",
		RecipeID: "recipe-pairing", RecipeDigest: "sha256:" + strings.Repeat("a", 64), RecipeRevision: 2,
		BeginCommandID: "pairing.begin", ResumeCommandID: "pairing.resume",
		ExecutionManifestDigest: "sha256:" + strings.Repeat("b", 64),
		Status:                  "waiting_user", PayloadReady: true, DeploymentRevision: 7, PayloadScopeRevision: 7, Revision: 8,
		ExpiresAt: now.Add(10 * time.Minute), CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
	}
	client := &pairingModuleClient{agentControlModuleClient: &agentControlModuleClient{}, session: session, sessionFound: true}
	module := New(nil, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, AgentCloudControlClient: client})
	base := map[string]any{
		"deployment_id":        "11111111-1111-4111-8111-111111111111",
		"recipient_public_key": base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		"idempotency_key":      "77777777-7777-4777-8777-777777777777",
	}
	withMaterial := mapsClone(base)
	withMaterial["pairing_url"] = "https://secret.invalid"
	if _, apiErr := module.Handlers()[serviceapi.CloudDeploymentPairingPayloadRetrieveAction](t.Context(), withMaterial); apiErr == nil || apiErr.Code != cloudInvalidParamsCode || client.calls != 0 {
		t.Fatalf("unknown material accepted: err=%#v calls=%d", apiErr, client.calls)
	}
	client.result = AgentCloudPairingPayloadResult{}
	if _, apiErr := module.Handlers()[serviceapi.CloudDeploymentPairingPayloadRetrieveAction](t.Context(), base); apiErr == nil || apiErr.Status != 502 || client.calls != 1 {
		t.Fatalf("invalid Agent response accepted: err=%#v calls=%d", apiErr, client.calls)
	}
}

func bytesFilled(size int, value byte) []byte {
	result := make([]byte, size)
	for index := range result {
		result[index] = value
	}
	return result
}

func mapsClone(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}
