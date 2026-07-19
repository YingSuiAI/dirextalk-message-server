package agentgrpc

import (
	"errors"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const agentCloudControlCanary = "AKIA-AGENT-CLOUD-CONTROL-CANARY"

func TestAgentCloudControlBindsOwnerPlanScopeAndConnection(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	plan := validAgentCloudPlanProto()
	connection := validAgentCloudConnectionProto(plan.GetConnectionId(), plan.GetResource().GetRegion())
	var gotChallenge *agentv1.CreateApprovalChallengeRequest
	var gotApprove *agentv1.ApproveCloudPlanRequest
	var gotEstablish *agentv1.EstablishAwsConnectionRequest
	server.cloud.getPlan = func(request *agentv1.GetCloudPlanRequest) (*agentv1.GetCloudPlanResponse, error) {
		if request.GetOwnerId() != "owner-from-config" || request.GetPlanId() != plan.GetPlanId() {
			t.Fatalf("get plan request=%+v", request)
		}
		return &agentv1.GetCloudPlanResponse{Plan: proto.Clone(plan).(*agentv1.CloudPlan)}, nil
	}
	server.cloud.createChallenge = func(request *agentv1.CreateApprovalChallengeRequest) (*agentv1.CreateApprovalChallengeResponse, error) {
		gotChallenge = proto.Clone(request).(*agentv1.CreateApprovalChallengeRequest)
		return &agentv1.CreateApprovalChallengeResponse{Challenge: validAgentCloudChallengeProto(plan, request.GetSignerKeyId())}, nil
	}
	server.cloud.approvePlan = func(request *agentv1.ApproveCloudPlanRequest) (*agentv1.ApproveCloudPlanResponse, error) {
		gotApprove = proto.Clone(request).(*agentv1.ApproveCloudPlanRequest)
		approved := proto.Clone(plan).(*agentv1.CloudPlan)
		approved.Status = agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_APPROVED
		approved.Revision++
		approved.PlanHash = "sha256:" + strings.Repeat("f", 64)
		return &agentv1.ApproveCloudPlanResponse{Plan: approved}, nil
	}
	server.cloud.establish = func(request *agentv1.EstablishAwsConnectionRequest) (*agentv1.EstablishAwsConnectionResponse, error) {
		gotEstablish = proto.Clone(request).(*agentv1.EstablishAwsConnectionRequest)
		return &agentv1.EstablishAwsConnectionResponse{Connection: proto.Clone(connection).(*agentv1.CloudConnection)}, nil
	}
	server.cloud.getConnection = func(request *agentv1.GetCloudConnectionRequest) (*agentv1.GetCloudConnectionResponse, error) {
		if request.GetOwnerId() != "owner-from-config" || request.GetConnectionId() != connection.GetConnectionId() {
			t.Fatalf("get connection request=%+v", request)
		}
		return &agentv1.GetCloudConnectionResponse{Connection: proto.Clone(connection).(*agentv1.CloudConnection)}, nil
	}
	server.cloud.listConnections = func(request *agentv1.ListCloudConnectionsRequest) (*agentv1.ListCloudConnectionsResponse, error) {
		if request.GetOwnerId() != "owner-from-config" || request.GetPageSize() != agentCloudConnectionPageSize {
			t.Fatalf("list connections request=%+v", request)
		}
		return &agentv1.ListCloudConnectionsResponse{Connections: []*agentv1.CloudConnection{proto.Clone(connection).(*agentv1.CloudConnection)}}, nil
	}
	secondPlan := proto.Clone(plan).(*agentv1.CloudPlan)
	secondPlan.PlanId = uuid.NewString()
	secondPlan.QuoteId = uuid.NewString()
	secondPlan.PlanHash = "sha256:" + strings.Repeat("d", 64)
	server.cloud.listPlans = func(request *agentv1.ListCloudPlansRequest) (*agentv1.ListCloudPlansResponse, error) {
		if request.GetOwnerId() != "owner-from-config" || request.GetPageSize() != agentCloudPlanPageSize {
			t.Fatalf("list plans request=%+v", request)
		}
		switch request.GetPageToken() {
		case "":
			return &agentv1.ListCloudPlansResponse{Plans: []*agentv1.CloudPlan{proto.Clone(plan).(*agentv1.CloudPlan)}, NextPageToken: "next-plan-page"}, nil
		case "next-plan-page":
			return &agentv1.ListCloudPlansResponse{Plans: []*agentv1.CloudPlan{proto.Clone(secondPlan).(*agentv1.CloudPlan)}}, nil
		default:
			t.Fatalf("unexpected plan page token=%q", request.GetPageToken())
			return nil, nil
		}
	}

	mapped, found, err := runner.GetAgentCloudPlan(t.Context(), cloudmodule.AgentCloudPlanRequest{PlanID: plan.GetPlanId()})
	if err != nil || !found || mapped.OwnerID != "owner-from-config" || mapped.Resource.PurchaseOption != "on_demand" || mapped.Network.EntryPoint != "none" || mapped.Retention.Class != "ephemeral" {
		t.Fatalf("mapped plan=%+v found=%v err=%v", mapped, found, err)
	}
	listedPlans, err := runner.ListAgentCloudPlans(t.Context())
	if err != nil || len(listedPlans) != 2 || listedPlans[0].PlanID != mapped.PlanID || listedPlans[0].PlanHash != mapped.PlanHash || listedPlans[1].PlanID != secondPlan.GetPlanId() {
		t.Fatalf("listed plans=%+v err=%v", listedPlans, err)
	}
	challenge, err := runner.CreateAgentCloudApprovalChallenge(t.Context(), cloudmodule.AgentCloudChallengeRequest{IdempotencyKey: uuid.NewString(), PlanID: mapped.PlanID, ExpectedRevision: mapped.Revision, SignerKeyID: "device-key-1", ExpectedPlan: mapped})
	if err != nil || challenge.OwnerID != mapped.OwnerID || challenge.ConnectionID != mapped.ConnectionID || challenge.QuoteScopeDigest != mapped.QuoteScopeDigest || len(challenge.SigningPayloadCBOR) == 0 {
		t.Fatalf("challenge=%+v err=%v", challenge, err)
	}
	approval := cloudmodule.AgentCloudApprovalSignature{ApprovalID: challenge.ApprovalID, ChallengeID: challenge.ChallengeID, SignerKeyID: challenge.SignerKeyID, ExpiresAt: challenge.ExpiresAt, Signature: make([]byte, 64)}
	approved, err := runner.ApproveAgentCloudPlan(t.Context(), cloudmodule.AgentCloudApproveRequest{IdempotencyKey: uuid.NewString(), PlanID: mapped.PlanID, ExpectedRevision: mapped.Revision, ExpectedPlan: mapped, Approval: approval})
	if err != nil || approved.Status != "approved" || approved.Revision != mapped.Revision+1 || approved.PlanHash == mapped.PlanHash {
		t.Fatalf("approved=%+v err=%v", approved, err)
	}
	established, err := runner.EstablishAgentAWSConnection(t.Context(), cloudmodule.AgentCloudEstablishRequest{IdempotencyKey: uuid.NewString(), BootstrapSessionID: uuid.NewString(), ExpectedSessionRevision: 2, PlanID: approved.PlanID, ExpectedPlanRevision: approved.Revision, Approval: approval, ExpectedConnectionID: approved.ConnectionID, ExpectedRegion: approved.Resource.Region})
	if err != nil || established.ConnectionID != approved.ConnectionID || established.OwnerID != mapped.OwnerID {
		t.Fatalf("connection=%+v err=%v", established, err)
	}
	readBack, found, err := runner.GetAgentCloudConnection(t.Context(), cloudmodule.AgentCloudConnectionRequest{ConnectionID: approved.ConnectionID})
	if err != nil || !found || readBack != established {
		t.Fatalf("readback=%+v found=%v err=%v", readBack, found, err)
	}
	listed, err := runner.ListAgentCloudConnections(t.Context())
	if err != nil || len(listed) != 1 || listed[0] != established {
		t.Fatalf("listed connections=%+v err=%v", listed, err)
	}
	for name, owner := range map[string]string{"challenge": gotChallenge.GetOwnerId(), "approve": gotApprove.GetOwnerId(), "establish": gotEstablish.GetOwnerId()} {
		if owner != "owner-from-config" {
			t.Fatalf("%s owner=%q", name, owner)
		}
	}
	if gotChallenge.GetPlanId() != mapped.PlanID || gotChallenge.GetExpectedRevision() != mapped.Revision || gotApprove.GetApproval().GetApprovalId() != approval.ApprovalID || gotEstablish.GetBootstrapSessionId() == "" {
		t.Fatalf("requests lost bindings challenge=%+v approve=%+v establish=%+v", gotChallenge, gotApprove, gotEstablish)
	}
}

func TestAgentCloudChallengeRejectsEveryStructuredBindingDrift(t *testing.T) {
	t.Parallel()
	planProto := validAgentCloudPlanProto()
	tests := map[string]func(*agentv1.ApprovalChallenge){
		"agent instance": func(v *agentv1.ApprovalChallenge) { v.AgentInstanceId = "agent-instance-1" },
		"owner":          func(v *agentv1.ApprovalChallenge) { v.OwnerId = "owner-other" },
		"connection":     func(v *agentv1.ApprovalChallenge) { v.ConnectionId = uuid.NewString() },
		"recipe":         func(v *agentv1.ApprovalChallenge) { v.RecipeDigest = digestFor("9") },
		"quote id":       func(v *agentv1.ApprovalChallenge) { v.QuoteId = uuid.NewString() },
		"quote digest":   func(v *agentv1.ApprovalChallenge) { v.QuoteDigest = digestFor("8") },
		"quote scope":    func(v *agentv1.ApprovalChallenge) { v.QuoteScopeDigest = digestFor("7") },
		"candidate":      func(v *agentv1.ApprovalChallenge) { v.QuoteCandidateId = "performance" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			server := startRuntimeServer(t)
			runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
			mapped, err := runner.mapAgentCloudPlan(planProto, planProto.GetPlanId())
			if err != nil {
				t.Fatal(err)
			}
			server.cloud.createChallenge = func(request *agentv1.CreateApprovalChallengeRequest) (*agentv1.CreateApprovalChallengeResponse, error) {
				value := validAgentCloudChallengeProto(planProto, request.GetSignerKeyId())
				mutate(value)
				return &agentv1.CreateApprovalChallengeResponse{Challenge: value}, nil
			}
			_, err = runner.CreateAgentCloudApprovalChallenge(t.Context(), cloudmodule.AgentCloudChallengeRequest{IdempotencyKey: uuid.NewString(), PlanID: mapped.PlanID, ExpectedRevision: mapped.Revision, SignerKeyID: "device-key-1", ExpectedPlan: mapped})
			if !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestAgentCloudControlValidatesRequestsAndSanitizesRPCFailures(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	if _, _, err := runner.GetAgentCloudPlan(t.Context(), cloudmodule.AgentCloudPlanRequest{PlanID: "not-a-uuid"}); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalid) {
		t.Fatalf("invalid error=%v", err)
	}
	server.cloud.getPlan = func(*agentv1.GetCloudPlanRequest) (*agentv1.GetCloudPlanResponse, error) {
		return nil, status.Error(codes.Internal, agentCloudControlCanary)
	}
	_, _, err := runner.GetAgentCloudPlan(t.Context(), cloudmodule.AgentCloudPlanRequest{PlanID: uuid.NewString()})
	if !errors.Is(err, cloudmodule.ErrAgentCloudControlUnavailable) || strings.Contains(err.Error(), agentCloudControlCanary) {
		t.Fatalf("unsanitized error=%v", err)
	}
	missing := uuid.NewString()
	server.cloud.getConnection = func(*agentv1.GetCloudConnectionRequest) (*agentv1.GetCloudConnectionResponse, error) {
		return nil, status.Error(codes.NotFound, agentCloudControlCanary)
	}
	if _, found, err := runner.GetAgentCloudConnection(t.Context(), cloudmodule.AgentCloudConnectionRequest{ConnectionID: missing}); err != nil || found {
		t.Fatalf("notfound found=%v err=%v", found, err)
	}
}

func TestAgentCloudApprovalRejectsStructuredScopeDriftAndConnectionARNDrift(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	planProto := validAgentCloudPlanProto()
	mapped, err := runner.mapAgentCloudPlan(planProto, planProto.GetPlanId())
	if err != nil {
		t.Fatal(err)
	}
	approval := cloudmodule.AgentCloudApprovalSignature{ApprovalID: uuid.NewString(), ChallengeID: "challenge_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", SignerKeyID: "device-key-1", ExpiresAt: time.Date(2026, 7, 16, 8, 5, 0, 0, time.UTC), Signature: make([]byte, 64)}
	server.cloud.approvePlan = func(*agentv1.ApproveCloudPlanRequest) (*agentv1.ApproveCloudPlanResponse, error) {
		value := proto.Clone(planProto).(*agentv1.CloudPlan)
		value.Status = agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_APPROVED
		value.Revision++
		value.Resource.DiskGib++
		return &agentv1.ApproveCloudPlanResponse{Plan: value}, nil
	}
	_, err = runner.ApproveAgentCloudPlan(t.Context(), cloudmodule.AgentCloudApproveRequest{IdempotencyKey: uuid.NewString(), PlanID: mapped.PlanID, ExpectedRevision: mapped.Revision, ExpectedPlan: mapped, Approval: approval})
	if !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("scope drift error=%v", err)
	}
	server.cloud.approvePlan = func(*agentv1.ApproveCloudPlanRequest) (*agentv1.ApproveCloudPlanResponse, error) {
		value := proto.Clone(planProto).(*agentv1.CloudPlan)
		value.Status = agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_APPROVED
		value.Revision++
		return &agentv1.ApproveCloudPlanResponse{Plan: value}, nil
	}
	_, err = runner.ApproveAgentCloudPlan(t.Context(), cloudmodule.AgentCloudApproveRequest{IdempotencyKey: uuid.NewString(), PlanID: mapped.PlanID, ExpectedRevision: mapped.Revision, ExpectedPlan: mapped, Approval: approval})
	if !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("approved plan with stale revision-bound hash error=%v", err)
	}
	connection := validAgentCloudConnectionProto(mapped.ConnectionID, mapped.Resource.Region)
	connection.FoundationStackId = strings.Replace(connection.FoundationStackId, "123456789012", "210987654321", 1)
	server.cloud.getConnection = func(*agentv1.GetCloudConnectionRequest) (*agentv1.GetCloudConnectionResponse, error) {
		return &agentv1.GetCloudConnectionResponse{Connection: connection}, nil
	}
	_, _, err = runner.GetAgentCloudConnection(t.Context(), cloudmodule.AgentCloudConnectionRequest{ConnectionID: mapped.ConnectionID})
	if !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("stack drift error=%v", err)
	}
	if candidate, ok := agentCloudCandidate(agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_ECONOMY); !ok || candidate != "economic" {
		t.Fatalf("economic candidate=%q ok=%v", candidate, ok)
	}
}

func validAgentCloudPlanProto() *agentv1.CloudPlan {
	now := time.Date(2026, 7, 16, 8, 0, 0, 0, time.UTC)
	return &agentv1.CloudPlan{PlanId: uuid.NewString(), OwnerId: "owner-from-config", ConnectionId: uuid.NewString(), SchemaVersion: cloudmodule.AgentCloudPlanSchemaV1, Recipe: &agentv1.CloudRecipeBinding{RecipeId: "recipe-1", Digest: digestFor("a"), Maturity: "experimental"}, QuoteId: uuid.NewString(), QuoteDigest: digestFor("b"), QuoteScopeDigest: digestFor("c"), CandidateProfile: agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_RECOMMENDED, QuoteValidUntil: timestamppb.New(now.Add(15 * time.Minute)), Resource: &agentv1.CloudResourceScope{Region: "us-east-1", AvailabilityZones: []string{"us-east-1a"}, InstanceType: "m7i.xlarge", InstanceCount: 1, Architecture: "amd64", Vcpu: 4, MemoryMib: 16384, DiskGib: 80, VolumeType: "gp3", VolumeEncrypted: true, PurchaseOption: agentv1.CloudPurchaseOption_CLOUD_PURCHASE_OPTION_ON_DEMAND, WorkerImageId: "ami-0123456789abcdef0", WorkerImageDigest: digestFor("d")}, Network: &agentv1.CloudNetworkScope{VpcId: "vpc-0123456789abcdef0", SubnetId: "subnet-0123456789abcdef0", SecurityGroupId: "sg-0123456789abcdef0", EntryPoint: agentv1.CloudEntryPointKind_CLOUD_ENTRY_POINT_KIND_NONE}, SecretScope: []*agentv1.CloudSecretScope{{SecretRef: "secret_ref:model/token", Purpose: "model access", Delivery: "file"}}, IntegrationScope: []*agentv1.CloudIntegrationScope{{Kind: "mcp", Name: "knowledge", Scopes: []string{"query"}}}, Retention: &agentv1.CloudRetentionScope{RetentionClass: agentv1.CloudRetentionClass_CLOUD_RETENTION_CLASS_EPHEMERAL, AutoDestroy: true, GracePeriodSeconds: 1800, MaxLifetimeSeconds: 86400}, Status: agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_READY_FOR_CONFIRMATION, PlanHash: digestFor("e"), Revision: 7}
}

func validAgentCloudChallengeProto(plan *agentv1.CloudPlan, signer string) *agentv1.ApprovalChallenge {
	return &agentv1.ApprovalChallenge{ChallengeId: "challenge_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", SignerKeyId: signer, PlanId: plan.GetPlanId(), PlanRevision: plan.GetRevision(), PlanHash: plan.GetPlanHash(), ExpiresAt: timestamppb.New(time.Date(2026, 7, 16, 8, 5, 0, 0, time.UTC)), SigningPayloadCbor: []byte{0xa1, 0x01, 0x02}, Revision: 1, ApprovalId: uuid.NewString(), AgentInstanceId: uuid.NewString(), OwnerId: plan.GetOwnerId(), ConnectionId: plan.GetConnectionId(), RecipeDigest: plan.GetRecipe().GetDigest(), QuoteId: plan.GetQuoteId(), QuoteDigest: plan.GetQuoteDigest(), QuoteScopeDigest: plan.GetQuoteScopeDigest(), QuoteCandidateId: "recommended"}
}

func validAgentCloudConnectionProto(connectionID, region string) *agentv1.CloudConnection {
	now := time.Date(2026, 7, 16, 8, 6, 0, 0, time.UTC)
	return &agentv1.CloudConnection{ConnectionId: connectionID, OwnerId: "owner-from-config", AccountId: "123456789012", Region: region, ControlRoleArn: "arn:aws:iam::123456789012:role/dirextalk-control", FoundationStackId: "arn:aws:cloudformation:us-east-1:123456789012:stack/dirextalk-foundation/12345678-1234-1234-1234-123456789abc", Status: "active", Revision: 1, CredentialGeneration: 1, CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now)}
}

func digestFor(value string) string { return "sha256:" + strings.Repeat(value, 64) }
