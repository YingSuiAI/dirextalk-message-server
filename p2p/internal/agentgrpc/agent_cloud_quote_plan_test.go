package agentgrpc

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testQuoteID = "019f6a80-1234-7abc-8def-012345678911"
	testPlanID  = "019f6a80-1234-7abc-8def-012345678912"
)

func TestAgentCloudPlanningBindsOwnerAndAcceptsServerOwnedWorkerRelease(t *testing.T) {
	now := time.Date(2026, time.July, 17, 8, 0, 0, 0, time.UTC)
	server := startRuntimeServer(t)
	quote := planningQuoteProto(now)
	server.cloud.createQuote = func(request *agentv1.CreateCloudQuoteRequest) (*agentv1.CreateCloudQuoteResponse, error) {
		if request.GetIdempotencyKey() != "019f6a80-1234-7abc-8def-012345678913" || len(request.GetScopes()) != 3 {
			t.Fatalf("unexpected create quote request: %#v", request)
		}
		for _, scope := range request.GetScopes() {
			if scope.GetOwnerId() != "owner-from-config" || scope.GetResource().GetWorkerImageId() != "" || scope.GetResource().GetWorkerImageDigest() != "" ||
				len(scope.GetResource().GetVolumeScopes()) != 1 || scope.GetResource().GetVolumeScopes()[0].GetSlotId() != "knowledge" ||
				scope.GetResource().GetVolumeScopes()[0].GetMountPath() != "/srv/knowledge" || scope.GetResource().GetVolumeScopes()[0].GetKmsKeyId() != "alias/dirextalk-agent-test" ||
				scope.GetNetwork().GetSecurityGroupMode() != agentv1.CloudSecurityGroupMode_CLOUD_SECURITY_GROUP_MODE_CREATE_DEDICATED ||
				scope.GetNetwork().GetSecurityGroupId() != "" || !scope.GetNetwork().GetPublicIpv4() {
				t.Fatalf("owner or server-owned Worker release was changed: %#v", scope)
			}
		}
		return &agentv1.CreateCloudQuoteResponse{Quote: quote}, nil
	}
	server.cloud.getQuote = func(request *agentv1.GetCloudQuoteRequest) (*agentv1.GetCloudQuoteResponse, error) {
		if request.GetQuoteId() != testQuoteID || request.GetOwnerId() != "owner-from-config" {
			t.Fatalf("quote read was not owner-bound: %#v", request)
		}
		return &agentv1.GetCloudQuoteResponse{Quote: quote}, nil
	}
	server.cloud.createPlan = func(request *agentv1.CreateCloudPlanRequest) (*agentv1.CreateCloudPlanResponse, error) {
		if request.GetQuoteId() != testQuoteID || request.GetCandidateProfile() != agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_RECOMMENDED ||
			request.GetCurrentScope().GetOwnerId() != "owner-from-config" || request.GetCurrentScope().GetResource().GetWorkerImageId() == "" {
			t.Fatalf("plan did not preserve the bound quote scope: %#v", request)
		}
		return &agentv1.CreateCloudPlanResponse{Plan: planningPlanProto(now, quote.GetCandidates()[1])}, nil
	}
	runner := newTestRunner(t, server, Config{})

	scopes := planningQuoteScopes(false)
	created, err := runner.CreateAgentCloudQuote(t.Context(), cloudmodule.AgentCloudQuoteCreateRequest{
		IdempotencyKey: "019f6a80-1234-7abc-8def-012345678913", Scopes: scopes,
		Usage:              cloudmodule.AgentCloudUsageEstimate{RuntimeHoursPerMonth: 730, PublicIPv4Hours: 730},
		BootstrapSessionID: "019f6a80-1234-7abc-8def-012345678914", ExpectedSessionRevision: 1,
	})
	if err != nil || created.QuoteID != testQuoteID || len(created.Candidates) != 3 || created.Candidates[1].Scope.Resource.WorkerImageID == "" ||
		len(created.Candidates[1].Scope.Resource.VolumeScopes) != 1 || created.Candidates[1].Scope.Resource.VolumeScopes[0].SlotID != "knowledge" ||
		created.Candidates[1].Scope.Network.SecurityGroupMode != "create_dedicated" || !created.Candidates[1].Scope.Network.PublicIPv4 {
		t.Fatalf("created quote=%#v err=%v", created, err)
	}
	read, found, err := runner.GetAgentCloudQuote(t.Context(), cloudmodule.AgentCloudQuoteRequest{QuoteID: testQuoteID})
	if err != nil || !found || !reflect.DeepEqual(read, created) {
		t.Fatalf("read quote=%#v found=%v err=%v", read, found, err)
	}
	plan, err := runner.CreateAgentCloudPlan(t.Context(), cloudmodule.AgentCloudPlanCreateRequest{
		IdempotencyKey: "019f6a80-1234-7abc-8def-012345678915", QuoteID: testQuoteID,
		CandidateProfile: "recommended", CurrentScope: created.Candidates[1].Scope,
	})
	if err != nil || plan.PlanID != testPlanID || plan.Resource.WorkerImageID == "" {
		t.Fatalf("created plan=%#v err=%v", plan, err)
	}

	quote.Assumptions = []string{"AWS_SECRET_ACCESS_KEY=not-a-real-secret-value"}
	if _, err := runner.CreateAgentCloudQuote(t.Context(), cloudmodule.AgentCloudQuoteCreateRequest{
		IdempotencyKey: "019f6a80-1234-7abc-8def-012345678913", Scopes: scopes,
		Usage:              cloudmodule.AgentCloudUsageEstimate{RuntimeHoursPerMonth: 730, PublicIPv4Hours: 730},
		BootstrapSessionID: "019f6a80-1234-7abc-8def-012345678914", ExpectedSessionRevision: 1,
	}); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("credential-shaped quote text was not rejected: %v", err)
	}
}

func TestAgentCloudResourceMappingPreservesAbsentDataVolumeScope(t *testing.T) {
	t.Parallel()
	resource := planningQuoteScopes(true)[0].Resource
	resource.VolumeScopes = nil
	remote, ok := agentCloudResourceToProto(resource, false)
	if !ok || len(remote.GetVolumeScopes()) != 0 {
		t.Fatalf("resource without data volumes did not encode as absent: %#v ok=%v", remote, ok)
	}
	mapped, ok := mapAgentCloudResource(remote)
	expected := resource
	// Candidate profile belongs to CloudQuoteCandidate, not CloudResourceScope
	// when the generic resource mapper is used directly.
	expected.CandidateProfile = ""
	if !ok || mapped.VolumeScopes != nil || !reflect.DeepEqual(mapped, expected) {
		t.Fatalf("resource without data volumes changed across protobuf mapping: %#v ok=%v", mapped, ok)
	}
}

func TestAgentCloudResourceMappingNormalizesDataVolumeScopeOrder(t *testing.T) {
	t.Parallel()
	resource := planningQuoteScopes(true)[0].Resource
	knowledge := resource.VolumeScopes[0]
	cache := knowledge
	cache.SlotID, cache.DeviceName, cache.MountPath = "cache", "/dev/sdg", "/srv/cache"
	resource.VolumeScopes = []cloudmodule.AgentCloudVolumeScope{knowledge, cache}
	remote, ok := agentCloudResourceToProto(resource, false)
	if !ok {
		t.Fatal("resource with valid data volumes did not encode")
	}
	mapped, ok := mapAgentCloudResource(remote)
	if !ok || len(mapped.VolumeScopes) != 2 || mapped.VolumeScopes[0].SlotID != "cache" || mapped.VolumeScopes[1].SlotID != "knowledge" {
		t.Fatalf("data-volume scope order was not normalized: %#v ok=%v", mapped.VolumeScopes, ok)
	}
}

func planningQuoteScopes(workerBound bool) []cloudmodule.AgentCloudQuoteScope {
	profiles := []string{"economic", "recommended", "performance"}
	result := make([]cloudmodule.AgentCloudQuoteScope, 0, len(profiles))
	for index, profile := range profiles {
		resource := cloudmodule.AgentCloudResourceScope{
			CandidateProfile: profile, Region: "ap-northeast-1", AvailabilityZones: []string{"ap-northeast-1a"},
			InstanceType: []string{"t3.large", "m7i.large", "c7i.xlarge"}[index], InstanceCount: 1,
			Architecture: "amd64", VCPU: uint32(2 + index*2), MemoryMiB: uint64(8192 + index*8192),
			DiskGiB: 40, VolumeType: "gp3", VolumeEncrypted: true, PurchaseOption: "on_demand",
			VolumeScopes: []cloudmodule.AgentCloudVolumeScope{{
				SlotID: "knowledge", SizeGiB: 80, VolumeType: "gp3", IOPS: 3_000, ThroughputMiBPS: 125,
				Encrypted: true, KMSKeyID: "alias/dirextalk-agent-test", DeviceName: "/dev/sdf", MountPath: "/srv/knowledge",
				Persistent: true, Disposition: "delete_with_deployment",
			}},
		}
		if workerBound {
			resource.WorkerImageID = "ami-0123456789abcdef0"
			resource.WorkerImageDigest = "sha256:" + strings.Repeat("4", 64)
		}
		result = append(result, cloudmodule.AgentCloudQuoteScope{
			ConnectionID:     "019f6a80-1234-7abc-8def-012345678902",
			Recipe:           cloudmodule.AgentCloudRecipeBinding{RecipeID: "recipe-openclaw-0001", Digest: "sha256:" + strings.Repeat("1", 64), Maturity: "experimental"},
			Resource:         resource,
			Network:          cloudmodule.AgentCloudNetworkScope{VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", SecurityGroupMode: "create_dedicated", EntryPoint: "none", PublicIPv4: true},
			SecretScope:      []cloudmodule.AgentCloudSecretScope{{SecretRef: "secret_ref:openclaw/token", Purpose: "service token", Delivery: "file"}},
			IntegrationScope: []cloudmodule.AgentCloudIntegrationScope{{Kind: "grpc", Name: "worker-control", Scopes: []string{"checkpoint"}}},
			Retention:        cloudmodule.AgentCloudRetentionScope{Class: "ephemeral", AutoDestroy: true, GracePeriodSeconds: 1800, MaxLifetimeSeconds: 86400},
		})
	}
	return result
}

func planningQuoteProto(now time.Time) *agentv1.CloudQuote {
	scopes := planningQuoteScopes(true)
	candidates := make([]*agentv1.CloudQuoteCandidate, 0, len(scopes))
	for index, scope := range scopes {
		remote, _ := agentCloudQuoteScopeToProto(scope, "owner-from-config", true)
		micros := uint64((index + 1) * 1_000_000)
		candidates = append(candidates, &agentv1.CloudQuoteCandidate{
			CandidateProfile: remote.GetResource().GetCandidateProfile(), Scope: remote,
			ScopeDigest: "sha256:" + strings.Repeat(string(rune('5'+index)), 64), OfferedAvailabilityZones: []string{"ap-northeast-1a"},
			Quotas: []*agentv1.CloudQuotaEvidence{{ServiceCode: "ec2", QuotaCode: "L-1216C47A", LimitUnits: 32, RequiredUnits: 4}},
			CostItems: []*agentv1.CloudCostItem{
				{Category: "compute", Description: "EC2 On-Demand", SourceId: "price-list", HourlyEstimateMicros: micros, MonthlyEstimateMicros: micros * 730, MaximumLaunchAmountMicros: micros},
				{Category: "public_ipv4", Description: "Public IPv4", SourceId: "price-list", HourlyEstimateMicros: 5_000, MonthlyEstimateMicros: 3_650_000},
			},
			HourlyEstimateMicros: micros, MonthlyEstimateMicros: micros * 730, MaximumLaunchAmountMicros: micros,
		})
	}
	return &agentv1.CloudQuote{
		QuoteId: testQuoteID, QuotedAt: timestamppb.New(now), ValidUntil: timestamppb.New(now.Add(15 * time.Minute)), Currency: "USD",
		Candidates: candidates, Usage: &agentv1.CloudUsageEstimate{RuntimeHoursPerMonth: 730, PublicIpv4Hours: 730},
		Assumptions: []string{"On-Demand pricing"}, Exclusions: []string{"tax"}, Digest: "sha256:" + strings.Repeat("8", 64),
	}
}

func planningPlanProto(now time.Time, selected *agentv1.CloudQuoteCandidate) *agentv1.CloudPlan {
	scope := selected.GetScope()
	return &agentv1.CloudPlan{
		PlanId: testPlanID, OwnerId: "owner-from-config", ConnectionId: scope.GetConnectionId(), Recipe: scope.GetRecipe(),
		QuoteId: testQuoteID, QuoteDigest: "sha256:" + strings.Repeat("8", 64), QuoteScopeDigest: selected.GetScopeDigest(),
		CandidateProfile: selected.GetCandidateProfile(), QuoteValidUntil: timestamppb.New(now.Add(15 * time.Minute)),
		Resource: scope.GetResource(), Network: scope.GetNetwork(), SecretScope: scope.GetSecretScope(), IntegrationScope: scope.GetIntegrationScope(),
		Retention: scope.GetRetention(), Status: agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_READY_FOR_CONFIRMATION,
		PlanHash: "sha256:" + strings.Repeat("9", 64), Revision: 1,
	}
}
