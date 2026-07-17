package agentgrpc

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCloudPairingPayloadAdapterBindsOwnerAndPreservesOpaqueEnvelope(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	request := cloudmodule.AgentCloudPairingPayloadRequest{
		IdempotencyKey: "77777777-7777-4777-8777-777777777777",
		PairingID:      "22222222-2222-4222-8222-222222222222",
		DeploymentID:   "11111111-1111-4111-8111-111111111111", ExpectedRevision: 7,
		RecipientPublicKey: base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
	}
	var captured *agentv1.RetrieveCloudPairingPayloadRequest
	server.cloud.retrievePairing = func(value *agentv1.RetrieveCloudPairingPayloadRequest) (*agentv1.RetrieveCloudPairingPayloadResponse, error) {
		captured = value
		return &agentv1.RetrieveCloudPairingPayloadResponse{
			Pairing: pairingSessionProto(now, request),
			Payload: &agentv1.EncryptedPairingPayload{
				SchemaVersion:      "dirextalk.agent.recipient-envelope/v1",
				ServerPublicKey:    base64.RawURLEncoding.EncodeToString(bytesOf(32, 1)),
				Nonce:              base64.RawURLEncoding.EncodeToString(bytesOf(12, 2)),
				Ciphertext:         base64.RawURLEncoding.EncodeToString(bytesOf(33, 3)),
				AssociatedDataCbor: []byte{0xa0}, PayloadDigest: "sha256:" + strings.Repeat("c", 64),
				ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)),
			},
		}, nil
	}
	result, err := runner.RetrieveAgentCloudPairingPayload(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if captured.GetOwnerId() != "owner-from-config" || captured.GetPairingId() != request.PairingID ||
		captured.GetDeploymentId() != request.DeploymentID || captured.GetExpectedRevision() != request.ExpectedRevision ||
		captured.GetRecipientPublicKey() != request.RecipientPublicKey || captured.GetIdempotencyKey() != request.IdempotencyKey {
		t.Fatalf("request=%#v", captured)
	}
	if result.Pairing.OwnerID != "owner-from-config" || result.Pairing.Status != "waiting_user" ||
		result.Pairing.DeploymentRevision != 7 || result.Pairing.PayloadScopeRevision != 7 ||
		result.Payload.SchemaVersion != "dirextalk.agent.recipient-envelope/v1" ||
		!reflect.DeepEqual(result.Payload.AssociatedDataCBOR, []byte{0xa0}) {
		t.Fatalf("result=%#v", result)
	}
	captured = nil
	result.Payload.AssociatedDataCBOR[0] = 0
	if captured != nil {
		t.Fatal("result mutation affected the RPC request")
	}
}

func TestCloudPairingResumeAdapterPreservesLegacySigningBytesAndAllowsAgentSignerResolution(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	payloadRequest := cloudmodule.AgentCloudPairingPayloadRequest{
		PairingID:    "22222222-2222-4222-8222-222222222222",
		DeploymentID: "11111111-1111-4111-8111-111111111111", ExpectedRevision: 7,
	}
	session := pairingSessionProto(now, payloadRequest)
	server.cloud.getPairing = func(request *agentv1.GetCloudPairingRequest) (*agentv1.GetCloudPairingResponse, error) {
		if request.GetOwnerId() != "owner-from-config" || request.GetPairingId() != "" ||
			request.GetDeploymentId() != payloadRequest.DeploymentID {
			t.Fatalf("get request=%#v", request)
		}
		return &agentv1.GetCloudPairingResponse{Pairing: session}, nil
	}
	got, found, err := runner.GetAgentCloudPairing(t.Context(), cloudmodule.AgentCloudPairingGetRequest{DeploymentID: payloadRequest.DeploymentID})
	if err != nil || !found || got.PairingID != payloadRequest.PairingID {
		t.Fatalf("get=%#v found=%v err=%v", got, found, err)
	}

	challengeRequest := cloudmodule.AgentCloudPairingResumeChallengeRequest{
		IdempotencyKey: "77777777-7777-4777-8777-777777777777",
		PairingID:      payloadRequest.PairingID, DeploymentID: payloadRequest.DeploymentID,
		ExpectedPairingRevision: 8,
	}
	approval, err := cloudcontracts.NewPairingResumeApprovalV1(cloudcontracts.PairingResumeTargetV1{
		DeploymentID: payloadRequest.DeploymentID, DeploymentRevision: 7,
		PlanID: session.GetPlanId(), CloudConnectionID: session.GetConnectionId(), ExecutionID: session.GetTaskId(),
		RecipeExecutionManifestDigest: session.GetExecutionManifestDigest(),
		JobID:                         session.GetPairingId(), JobRevision: 8,
	}, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "device-pairing", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	signing, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	server.cloud.createPairingResume = func(request *agentv1.CreateCloudPairingResumeChallengeRequest) (*agentv1.CreateCloudPairingResumeChallengeResponse, error) {
		if request.GetSignerKeyId() != "" || request.GetOwnerId() != "owner-from-config" ||
			request.GetExpectedPairingRevision() != 8 {
			t.Fatalf("challenge request=%#v", request)
		}
		return &agentv1.CreateCloudPairingResumeChallengeResponse{Challenge: &agentv1.CloudPairingResumeChallenge{
			SchemaVersion: "dirextalk.agent.pairing-resume-challenge/v1",
			ChallengeId:   approval.ChallengeID, ApprovalId: approval.ApprovalID, SignerKeyId: approval.SignerKeyID,
			Scope: &agentv1.CloudPairingResumeScope{
				SchemaVersion: "dirextalk.agent.pairing-resume-scope/v1", Intent: cloudcontracts.PairingResumeIntent,
				PairingId: payloadRequest.PairingID, OwnerId: "owner-from-config", DeploymentId: payloadRequest.DeploymentID,
				DeploymentRevision: 7, PlanId: session.GetPlanId(), ConnectionId: session.GetConnectionId(),
				TaskId: session.GetTaskId(), StepId: session.GetStepId(), RecipeDigest: session.GetRecipeDigest(),
				ExecutionManifestDigest: session.GetExecutionManifestDigest(), PairingRevision: 8,
			},
			ScopeDigest: "sha256:" + strings.Repeat("d", 64),
			IssuedAt:    timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(5 * time.Minute)),
			SigningPayloadCbor: signing,
		}}, nil
	}
	challenge, err := runner.CreateAgentCloudPairingResumeChallenge(t.Context(), challengeRequest)
	if err != nil || challenge.Approval != approval || !bytes.Equal(challenge.SigningPayloadCBOR, signing) {
		t.Fatalf("challenge=%#v err=%v", challenge, err)
	}

	signed := approval
	signed.Signature = base64.RawURLEncoding.EncodeToString(bytesOf(64, 9))
	resumed := pairingSessionProto(now, payloadRequest)
	resumed.Revision = 9
	resumed.Status = agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_SUCCEEDED
	resumed.UpdatedAt = timestamppb.New(now.Add(time.Minute))
	server.cloud.approvePairingResume = func(request *agentv1.ApproveCloudPairingResumeRequest) (*agentv1.ApproveCloudPairingResumeResponse, error) {
		if request.GetScopeDigest() != "" || request.GetApproval().GetApprovalId() != approval.ApprovalID ||
			request.GetApproval().GetSignerKeyId() != approval.SignerKeyID ||
			!bytes.Equal(request.GetApproval().GetSignature(), bytesOf(64, 9)) {
			t.Fatalf("approve request=%#v", request)
		}
		return &agentv1.ApproveCloudPairingResumeResponse{Pairing: resumed}, nil
	}
	result, err := runner.ApproveAgentCloudPairingResume(t.Context(), cloudmodule.AgentCloudPairingResumeApproveRequest{
		IdempotencyKey: "88888888-8888-4888-8888-888888888888",
		PairingID:      payloadRequest.PairingID, DeploymentID: payloadRequest.DeploymentID,
		ExpectedPairingRevision: 8, Approval: signed,
	})
	if err != nil || result.Revision != 9 || result.Status != "succeeded" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestCloudPairingPayloadAdapterRejectsInvalidRecipientAndResponse(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	request := cloudmodule.AgentCloudPairingPayloadRequest{
		IdempotencyKey: "77777777-7777-4777-8777-777777777777",
		PairingID:      "22222222-2222-4222-8222-222222222222",
		DeploymentID:   "11111111-1111-4111-8111-111111111111", ExpectedRevision: 7,
		RecipientPublicKey: "not-a-key",
	}
	if _, err := runner.RetrieveAgentCloudPairingPayload(t.Context(), request); err != cloudmodule.ErrAgentCloudControlInvalid {
		t.Fatalf("invalid recipient err=%v", err)
	}
	request.RecipientPublicKey = base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	server.cloud.retrievePairing = func(*agentv1.RetrieveCloudPairingPayloadRequest) (*agentv1.RetrieveCloudPairingPayloadResponse, error) {
		return &agentv1.RetrieveCloudPairingPayloadResponse{}, nil
	}
	if _, err := runner.RetrieveAgentCloudPairingPayload(t.Context(), request); err != cloudmodule.ErrAgentCloudControlInvalidResponse {
		t.Fatalf("invalid response err=%v", err)
	}
}

func pairingSessionProto(now time.Time, request cloudmodule.AgentCloudPairingPayloadRequest) *agentv1.CloudPairingSession {
	return &agentv1.CloudPairingSession{
		PairingId: request.PairingID, OwnerId: "owner-from-config", DeploymentId: request.DeploymentID,
		TaskId: "33333333-3333-4333-8333-333333333333", StepId: "44444444-4444-4444-8444-444444444444",
		PlanId: "55555555-5555-4555-8555-555555555555", ConnectionId: "66666666-6666-4666-8666-666666666666",
		RecipeId: "recipe-pairing", RecipeDigest: "sha256:" + strings.Repeat("a", 64), RecipeRevision: 2,
		BeginCommandId: "pairing.begin", ResumeCommandId: "pairing.resume",
		ExecutionManifestDigest: "sha256:" + strings.Repeat("b", 64),
		Status:                  agentv1.CloudPairingStatus_CLOUD_PAIRING_STATUS_WAITING_USER, PayloadReady: true,
		DeploymentRevision: 7, PayloadScopeRevision: 7, Revision: 8, ExpiresAt: timestamppb.New(now.Add(10 * time.Minute)),
		CreatedAt: timestamppb.New(now.Add(-time.Minute)), UpdatedAt: timestamppb.New(now),
	}
}

func bytesOf(size int, value byte) []byte {
	result := make([]byte, size)
	for index := range result {
		result[index] = value
	}
	return result
}
