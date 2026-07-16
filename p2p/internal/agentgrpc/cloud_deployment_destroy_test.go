package agentgrpc

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAgentCloudDeploymentDestroyAdapterBindsOwnerFullResourceGraphAndDurableOperation(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	remoteDeployment := cloudDeployment(testDeploymentID1, testPlanID1, testConnectionID1, 7)
	expectedDeployment := cloudmodule.Deployment{
		DeploymentID: testDeploymentID1, PlanID: testPlanID1, ConnectionID: testConnectionID1,
		Execution: "running", Outcome: "pending", Resource: "active", Revision: 7,
		CreatedAt: time.Unix(1, 0).UnixMilli(), UpdatedAt: time.Unix(2, 0).UnixMilli(),
	}
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	challenge := remoteAgentDestroyChallenge(now)
	server.cloud.createDestroy = func(*agentv1.CreateCloudDeploymentDestroyChallengeRequest) (*agentv1.CreateCloudDeploymentDestroyChallengeResponse, error) {
		return &agentv1.CreateCloudDeploymentDestroyChallengeResponse{Challenge: challenge}, nil
	}

	prepared, err := runner.CreateAgentCloudDeploymentDestroyChallenge(t.Context(), cloudmodule.AgentCloudDeploymentDestroyChallengeRequest{
		IdempotencyKey: "77777777-7777-4777-8777-777777777777", DeploymentID: testDeploymentID1,
		ExpectedRevision: 7, SignerKeyID: "cloud-device-test", ExpectedDeployment: expectedDeployment,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.cloud.mu.Lock()
	createRequest := server.cloud.createDestroyRequest
	auth := append([]string(nil), server.cloud.auth...)
	server.cloud.mu.Unlock()
	if createRequest.GetOwnerId() != "owner-from-config" || createRequest.GetDeploymentId() != testDeploymentID1 ||
		createRequest.GetExpectedRevision() != 7 || len(auth) == 0 || auth[len(auth)-1] == "" ||
		len(prepared.Scope.Resources) != 4 || prepared.Scope.Resources[3].Type != "security_group" ||
		prepared.Scope.Resources[3].ProviderID != "sg-0123456789abcdef0" || !prepared.Scope.Resources[3].AutoDestroyApproved {
		t.Fatalf("destroy prepare request=%#v result=%#v auth=%v", createRequest, prepared, auth)
	}
	encodedScope, err := json.Marshal(prepared.Scope)
	if err != nil || prepared.Scope.Resources[0].DependsOnResourceIDs == nil ||
		!strings.Contains(string(encodedScope), `"depends_on_resource_ids":[]`) || strings.Contains(string(encodedScope), `"depends_on_resource_ids":null`) {
		t.Fatalf("empty dependency encoding=%s err=%v", encodedScope, err)
	}

	operation := remoteAgentDestroyOperation(now, agentv1.CloudDestroyOperationStatus_CLOUD_DESTROY_OPERATION_STATUS_APPROVED)
	server.cloud.approveDestroy = func(*agentv1.ApproveCloudDeploymentDestroyRequest) (*agentv1.ApproveCloudDeploymentDestroyResponse, error) {
		return &agentv1.ApproveCloudDeploymentDestroyResponse{Operation: operation, Deployment: remoteDeployment}, nil
	}
	approval := cloudmodule.AgentCloudApprovalSignature{
		ApprovalID: challenge.GetApprovalId(), ChallengeID: challenge.GetChallengeId(), SignerKeyID: challenge.GetSignerKeyId(),
		ExpiresAt: challenge.GetExpiresAt().AsTime().UTC(), Signature: make([]byte, 64),
	}
	approved, err := runner.ApproveAgentCloudDeploymentDestroy(t.Context(), cloudmodule.AgentCloudDeploymentDestroyApproveRequest{
		IdempotencyKey: "88888888-8888-4888-8888-888888888888", DeploymentID: testDeploymentID1,
		ExpectedOperationID: challenge.GetOperationId(), ExpectedRevision: 7, ExpectedDeployment: expectedDeployment, Approval: approval,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.cloud.mu.Lock()
	approveRequest := server.cloud.approveDestroyRequest
	server.cloud.mu.Unlock()
	if approveRequest.GetOwnerId() != "owner-from-config" || approveRequest.GetApproval().GetApprovalId() != challenge.GetApprovalId() ||
		approved.Operation.Status != "approved" || approved.Deployment.Resource != "active" {
		t.Fatalf("destroy approve request=%#v result=%#v", approveRequest, approved)
	}

	server.cloud.getDestroy = func(*agentv1.GetCloudDestroyOperationRequest) (*agentv1.GetCloudDestroyOperationResponse, error) {
		return &agentv1.GetCloudDestroyOperationResponse{Operation: operation}, nil
	}
	recovered, found, err := runner.GetAgentCloudDestroyOperation(t.Context(), cloudmodule.AgentCloudDestroyOperationRequest{OperationID: operation.GetOperationId()})
	if err != nil || !found || recovered.OperationID != operation.GetOperationId() {
		t.Fatalf("destroy operation found=%v result=%#v err=%v", found, recovered, err)
	}
	server.cloud.mu.Lock()
	getRequest := server.cloud.getDestroyRequest
	server.cloud.mu.Unlock()
	if getRequest.GetOwnerId() != "owner-from-config" {
		t.Fatalf("destroy operation owner was not fixed: %#v", getRequest)
	}
}

func TestAgentCloudDeploymentDestroyAdapterRejectsScopeSubstitutionAndSanitizesUnknownResult(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	expected := cloudmodule.Deployment{
		DeploymentID: testDeploymentID1, PlanID: testPlanID1, ConnectionID: testConnectionID1,
		Execution: "running", Outcome: "pending", Resource: "active", Revision: 7,
		CreatedAt: time.Unix(1, 0).UnixMilli(), UpdatedAt: time.Unix(2, 0).UnixMilli(),
	}
	challenge := remoteAgentDestroyChallenge(time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC))
	challenge.Scope.OwnerId = "attacker"
	server.cloud.createDestroy = func(*agentv1.CreateCloudDeploymentDestroyChallengeRequest) (*agentv1.CreateCloudDeploymentDestroyChallengeResponse, error) {
		return &agentv1.CreateCloudDeploymentDestroyChallengeResponse{Challenge: challenge}, nil
	}
	if _, err := runner.CreateAgentCloudDeploymentDestroyChallenge(t.Context(), cloudmodule.AgentCloudDeploymentDestroyChallengeRequest{
		IdempotencyKey: "77777777-7777-4777-8777-777777777777", DeploymentID: testDeploymentID1,
		ExpectedRevision: 7, SignerKeyID: "cloud-device-test", ExpectedDeployment: expected,
	}); err != cloudmodule.ErrAgentCloudControlInvalidResponse {
		t.Fatalf("substituted owner error=%v", err)
	}

	server.cloud.getDestroy = func(*agentv1.GetCloudDestroyOperationRequest) (*agentv1.GetCloudDestroyOperationResponse, error) {
		return nil, status.Error(codes.Unavailable, cloudErrorCanary)
	}
	if _, found, err := runner.GetAgentCloudDestroyOperation(t.Context(), cloudmodule.AgentCloudDestroyOperationRequest{OperationID: "55555555-5555-4555-8555-555555555555"}); found || err != cloudmodule.ErrAgentCloudControlUnavailable {
		t.Fatalf("unknown result found=%v err=%v", found, err)
	}

	server.cloud.getDestroy = func(*agentv1.GetCloudDestroyOperationRequest) (*agentv1.GetCloudDestroyOperationResponse, error) {
		return nil, status.Error(codes.NotFound, cloudErrorCanary)
	}
	if _, found, err := runner.GetAgentCloudDestroyOperation(t.Context(), cloudmodule.AgentCloudDestroyOperationRequest{OperationID: "55555555-5555-4555-8555-555555555555"}); err != nil || found {
		t.Fatalf("not-found operation found=%v err=%v", found, err)
	}
}

func remoteAgentDestroyChallenge(now time.Time) *agentv1.CloudDeploymentDestroyChallenge {
	resourceIDs := []string{
		"10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002",
		"10000000-0000-4000-8000-000000000003", "10000000-0000-4000-8000-000000000004",
	}
	providers := []string{"i-0123456789abcdef0", "vol-0123456789abcdef0", "eni-0123456789abcdef0", "sg-0123456789abcdef0"}
	types := []agentv1.CloudResourceType{
		agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EC2, agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_EBS,
		agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_ENI, agentv1.CloudResourceType_CLOUD_RESOURCE_TYPE_SECURITY_GROUP,
	}
	resources := make([]*agentv1.CloudDestroyResourceScope, 0, len(resourceIDs))
	for index := range resourceIDs {
		dependencies := []string{}
		if index > 0 {
			dependencies = []string{resourceIDs[0]}
		}
		resources = append(resources, &agentv1.CloudDestroyResourceScope{
			ResourceId: resourceIDs[index], Type: types[index], ProviderId: providers[index], Revision: 3,
			DependsOnResourceIds: dependencies, RetentionPolicy: agentv1.RetentionPolicy_RETENTION_POLICY_EPHEMERAL_AUTO_DESTROY,
			Status: agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_ACTIVE, Region: "us-east-1",
			SpecDigest:         "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			ApprovedPlanHash:   "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			OriginalApprovalId: "99999999-9999-4999-8999-999999999999", DestroyDeadline: timestamppb.New(now.Add(time.Hour)), AutoDestroyApproved: true,
			ReadBack: &agentv1.CloudResourceReadBack{Observed: true, Exists: true, ProviderId: providers[index], ObservedAt: timestamppb.New(now.Add(-time.Minute)), TagDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333"},
		})
	}
	return &agentv1.CloudDeploymentDestroyChallenge{
		OperationId: "55555555-5555-4555-8555-555555555555", ChallengeId: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
		ApprovalId: "66666666-6666-4666-8666-666666666666", SignerKeyId: "cloud-device-test",
		ExpiresAt: timestamppb.New(now.Add(5 * time.Minute)), SigningPayloadCbor: []byte{0xa1, 0x01, 0x01}, Revision: 1,
		Scope: &agentv1.CloudDeploymentDestroyScope{
			SchemaVersion: agentCloudDeploymentDestroyScopeSchema, AgentInstanceId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", OwnerId: "owner-from-config",
			DeploymentId: testDeploymentID1, DeploymentRevision: 7, TaskId: "44444444-4444-4444-8444-444444444444",
			PlanId: testPlanID1, PlanHash: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
			ConnectionId: testConnectionID1, Resources: resources,
		},
	}
}

func remoteAgentDestroyOperation(now time.Time, operationStatus agentv1.CloudDestroyOperationStatus) *agentv1.CloudDestroyOperation {
	return &agentv1.CloudDestroyOperation{
		OperationId: "55555555-5555-4555-8555-555555555555", OwnerId: "owner-from-config", DeploymentId: testDeploymentID1,
		ApprovalId: "66666666-6666-4666-8666-666666666666", ScopeDigest: "sha256:5555555555555555555555555555555555555555555555555555555555555555",
		Status: operationStatus, Revision: 2, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now.Add(time.Second)),
	}
}
