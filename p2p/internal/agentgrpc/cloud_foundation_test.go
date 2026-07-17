package agentgrpc

import (
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAgentFoundationTransportBindsConfiguredOwnerAndExactScope(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	operationID, challengeID, approvalID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	connectionID, sessionID := uuid.NewString(), uuid.NewString()
	var gotPrepare *agentv1.CreateAwsFoundationOperationChallengeRequest
	var gotApprove *agentv1.ApproveAwsFoundationOperationRequest
	challenge := &agentv1.AwsFoundationOperationChallenge{OperationId: operationID, ChallengeId: challengeID, ApprovalId: approvalID, SignerKeyId: "device-key-1",
		ScopeDigest: digestFor("c"), ExpiresAt: timestamppb.New(now.Add(5 * time.Minute)), SigningPayloadCbor: []byte{0xa1, 0x01, 0x02}, Revision: 1,
		Scope: &agentv1.AwsFoundationOperationScope{SchemaVersion: agentFoundationScopeSchema, AgentInstanceId: runner.agentInstanceID, OwnerId: runner.ownerID,
			Action: agentv1.AwsFoundationOperationAction_AWS_FOUNDATION_OPERATION_ACTION_UPGRADE, ConnectionId: connectionID, ExpectedConnectionRevision: 7,
			AccountId: "123456789012", Region: "ap-south-1", BootstrapSessionId: sessionID, ExpectedBootstrapRevision: 2, ExpectedCredentialGeneration: 3,
			FoundationTemplateDigest: digestFor("a"), ReaperImageUri: "repo/reaper:v1@" + digestFor("b"), IdentityObservedAt: timestamppb.New(now.Add(-time.Minute)), IdentityExpiresAt: timestamppb.New(now.Add(time.Minute)),
			ReleaseEnvironment: &agentv1.AwsFoundationReleaseEnvironment{PrivateSubnetCidr: "10.255.0.0/26", ZeroIngress: true, ArtifactBucket: "dtx-agent-artifacts", KmsAlias: "alias/dtx-agent-test", BucketVersioned: true, BucketSseKms: true}}}
	operation := &agentv1.AwsFoundationOperation{OperationId: operationID, OwnerId: runner.ownerID, ConnectionId: connectionID,
		Action: challenge.Scope.Action, ApprovalId: approvalID, ScopeDigest: challenge.ScopeDigest, Status: agentv1.AwsFoundationOperationStatus_AWS_FOUNDATION_OPERATION_STATUS_APPROVED,
		Revision: 2, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now.Add(time.Second))}
	server.cloud.createFoundation = func(request *agentv1.CreateAwsFoundationOperationChallengeRequest) (*agentv1.CreateAwsFoundationOperationChallengeResponse, error) {
		gotPrepare = request
		return &agentv1.CreateAwsFoundationOperationChallengeResponse{Challenge: challenge}, nil
	}
	server.cloud.approveFoundation = func(request *agentv1.ApproveAwsFoundationOperationRequest) (*agentv1.ApproveAwsFoundationOperationResponse, error) {
		gotApprove = request
		return &agentv1.ApproveAwsFoundationOperationResponse{Operation: operation}, nil
	}
	server.cloud.getFoundation = func(request *agentv1.GetAwsFoundationOperationRequest) (*agentv1.GetAwsFoundationOperationResponse, error) {
		if request.GetOwnerId() != runner.ownerID || request.GetOperationId() != operationID {
			t.Fatalf("get request=%+v", request)
		}
		return &agentv1.GetAwsFoundationOperationResponse{Operation: operation}, nil
	}
	prepared, err := runner.CreateAgentAWSFoundationChallenge(t.Context(), cloudmodule.AgentCloudFoundationChallengeRequest{IdempotencyKey: uuid.NewString(), Action: "upgrade",
		ConnectionID: connectionID, BootstrapSessionID: sessionID, ExpectedBootstrapRevision: 2, SignerKeyID: "device-key-1"})
	if err != nil || prepared.Scope.OwnerID != runner.ownerID || gotPrepare.GetOwnerId() != runner.ownerID {
		t.Fatalf("prepared=%#v request=%+v error=%v", prepared, gotPrepare, err)
	}
	approval := cloudmodule.AgentCloudApprovalSignature{ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: challenge.SignerKeyId, ExpiresAt: now.Add(5 * time.Minute), Signature: make([]byte, 64)}
	approved, err := runner.ApproveAgentAWSFoundation(t.Context(), cloudmodule.AgentCloudFoundationApproveRequest{IdempotencyKey: uuid.NewString(), ExpectedOperationID: operationID,
		ExpectedAction: "upgrade", ExpectedConnectionID: connectionID, ExpectedScopeDigest: challenge.ScopeDigest, ExpectedRevision: 1, Approval: approval})
	if err != nil || approved.Status != "approved" || gotApprove.GetOwnerId() != runner.ownerID || gotApprove.GetOperationId() != operationID ||
		gotApprove.GetExpectedRevision() != 1 || gotApprove.GetConnectionId() != connectionID || gotApprove.GetAction() != challenge.Scope.Action || gotApprove.GetScopeDigest() != challenge.ScopeDigest {
		t.Fatalf("approved=%#v request=%+v error=%v", approved, gotApprove, err)
	}
	read, found, err := runner.GetAgentAWSFoundationOperation(t.Context(), cloudmodule.AgentCloudFoundationOperationRequest{OperationID: operationID})
	if err != nil || !found || read != approved {
		t.Fatalf("read=%#v found=%v error=%v", read, found, err)
	}
}
