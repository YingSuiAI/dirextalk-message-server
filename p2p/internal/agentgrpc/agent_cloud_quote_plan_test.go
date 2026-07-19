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

func TestAgentCloudPlanningMapsV2ServiceOperationsAndRejectsUsageDrift(t *testing.T) {
	now := time.Date(2026, time.July, 18, 8, 0, 0, 0, time.UTC)
	server := startRuntimeServer(t)
	usage := cloudmodule.AgentCloudUsageEstimate{RuntimeHoursPerMonth: 730, PrivateEndpointHours: 720, PrivateEndpointDataMiB: 96}
	quote := planningQuoteProtoForScopes(now, planningServiceOperationScopes(true), usage)
	server.cloud.createQuote = func(request *agentv1.CreateCloudQuoteRequest) (*agentv1.CreateCloudQuoteResponse, error) {
		if request.GetUsage().GetPrivateEndpointHours() != 720 || request.GetUsage().GetPrivateEndpointDataMib() != 96 {
			t.Fatalf("v2 usage was not forwarded: %#v", request.GetUsage())
		}
		for _, scope := range request.GetScopes() {
			operations := scope.GetServiceOperations()
			if scope.GetSchemaVersion() != cloudmodule.AgentCloudQuoteScopeSchemaV2 || operations == nil || len(operations.GetPrivateEndpoints()) != 1 ||
				operations.GetPrivateEndpoints()[0].GetService() != agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_S3 ||
				operations.GetPrivateEndpoints()[0].GetSecurityGroupSource() != agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_WORKER_DEDICATED {
				t.Fatalf("v2 scope was not forwarded exactly: %#v", scope)
			}
		}
		return &agentv1.CreateCloudQuoteResponse{Quote: quote}, nil
	}
	server.cloud.createPlan = func(request *agentv1.CreateCloudPlanRequest) (*agentv1.CreateCloudPlanResponse, error) {
		return &agentv1.CreateCloudPlanResponse{Plan: planningPlanProto(now, quote.GetCandidates()[1])}, nil
	}
	server.cloud.getQuote = func(*agentv1.GetCloudQuoteRequest) (*agentv1.GetCloudQuoteResponse, error) {
		return &agentv1.GetCloudQuoteResponse{Quote: quote}, nil
	}
	runner := newTestRunner(t, server, Config{})

	created, err := runner.CreateAgentCloudQuote(t.Context(), cloudmodule.AgentCloudQuoteCreateRequest{
		IdempotencyKey: "019f6a80-1234-7abc-8def-012345678913", Scopes: planningServiceOperationScopes(false), Usage: usage,
	})
	if err != nil || created.Usage.PrivateEndpointHours != 720 || created.Candidates[0].Scope.SchemaVersion != cloudmodule.AgentCloudQuoteScopeSchemaV2 ||
		len(created.Candidates[0].Scope.ServiceOperations.PrivateEndpoints) != 1 {
		t.Fatalf("v2 quote mapping=%#v err=%v", created, err)
	}
	plan, err := runner.CreateAgentCloudPlan(t.Context(), cloudmodule.AgentCloudPlanCreateRequest{
		IdempotencyKey: "019f6a80-1234-7abc-8def-012345678915", QuoteID: testQuoteID,
		CandidateProfile: "recommended", CurrentScope: created.Candidates[1].Scope,
	})
	if err != nil || plan.SchemaVersion != cloudmodule.AgentCloudPlanSchemaV2 || len(plan.ServiceOperations.PrivateEndpoints) != 1 {
		t.Fatalf("v2 plan mapping=%#v err=%v", plan, err)
	}

	quote.Usage.PrivateEndpointDataMib++
	if _, _, err := runner.GetAgentCloudQuote(t.Context(), cloudmodule.AgentCloudQuoteRequest{QuoteID: testQuoteID}); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("usage drift was accepted: %v", err)
	}

	wire, ok := agentCloudQuoteScopeToProto(planningServiceOperationScopes(true)[0], "owner-from-config", true)
	if !ok {
		t.Fatal("valid v2 scope did not marshal")
	}
	wire.SchemaVersion = cloudmodule.AgentCloudQuoteScopeSchemaV1
	if _, ok := mapAgentCloudQuoteScope(wire, "owner-from-config"); ok {
		t.Fatal("v1 wire schema with service operations was accepted")
	}
	wrongPlanSchema := planningPlanProto(now, quote.GetCandidates()[1])
	wrongPlanSchema.SchemaVersion = cloudmodule.AgentCloudQuoteScopeSchemaV2
	if _, err := runner.mapAgentCloudPlan(wrongPlanSchema, ""); !errors.Is(err, cloudmodule.ErrAgentCloudControlInvalidResponse) {
		t.Fatalf("plan accepted a quote-scope schema: %v", err)
	}
}

func TestAgentCloudPlanningForwardsWorkerControlPrivateLinkScope(t *testing.T) {
	now := time.Date(2026, time.July, 19, 8, 0, 0, 0, time.UTC)
	server := startRuntimeServer(t)
	usage := cloudmodule.AgentCloudUsageEstimate{RuntimeHoursPerMonth: 730, PrivateEndpointHours: 1460, PrivateEndpointDataMiB: 2}
	scopes := planningWorkerControlScopes(true)
	quote := planningQuoteProtoForScopes(now, scopes, usage)
	server.cloud.createQuote = func(request *agentv1.CreateCloudQuoteRequest) (*agentv1.CreateCloudQuoteResponse, error) {
		for _, scope := range request.GetScopes() {
			operations := scope.GetServiceOperations()
			if scope.GetNetwork().GetPrivateConnectivity() != "no_nat_endpoints_v1" || scope.GetNetwork().GetControlPlaneEndpoint() != "grpcs://worker-control.y1.dirextalk.ai:443" ||
				operations == nil || len(operations.GetPrivateEndpoints()) != 3 || request.GetUsage().GetPrivateEndpointHours() != 1460 || request.GetUsage().GetPrivateEndpointDataMib() != 2 {
				t.Fatalf("worker-control scope was not forwarded: %#v", request)
			}
			endpoints := operations.GetPrivateEndpoints()
			if endpoints[0].GetOperationKey() != "worker-s3-gateway" || endpoints[0].GetService() != agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_S3 || endpoints[0].GetEndpointType() != agentv1.CloudPrivateEndpointType_CLOUD_PRIVATE_ENDPOINT_TYPE_GATEWAY ||
				endpoints[1].GetOperationKey() != "worker-secretsmanager-interface" || endpoints[1].GetService() != agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_SECRETS_MANAGER || endpoints[1].GetServiceName() != "" ||
				endpoints[2].GetOperationKey() != "worker-worker-control-interface" || endpoints[2].GetService() != agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_WORKER_CONTROL || endpoints[2].GetServiceName() != "com.amazonaws.vpce.ap-northeast-3.vpce-svc-0123456789abcdef0" {
				t.Fatalf("worker-control endpoint bytes changed: %#v", endpoints)
			}
		}
		return &agentv1.CreateCloudQuoteResponse{Quote: quote}, nil
	}
	runner := newTestRunner(t, server, Config{})
	created, err := runner.CreateAgentCloudQuote(t.Context(), cloudmodule.AgentCloudQuoteCreateRequest{
		IdempotencyKey: "019f6a80-1234-7abc-8def-012345678913", Scopes: planningWorkerControlScopes(false), Usage: usage,
	})
	if err != nil || len(created.Candidates) != 3 || len(created.Candidates[0].Scope.ServiceOperations.PrivateEndpoints) != 3 {
		t.Fatalf("worker-control quote mapping=%#v err=%v", created, err)
	}
	if created.Candidates[0].Scope.ServiceOperations.PrivateEndpoints[2].ServiceName != "com.amazonaws.vpce.ap-northeast-3.vpce-svc-0123456789abcdef0" {
		t.Fatalf("worker-control service identity was not round-tripped: %#v", created.Candidates[0].Scope.ServiceOperations)
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
			SchemaVersion:    cloudmodule.AgentCloudQuoteScopeSchemaV1,
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

func planningServiceOperationScopes(workerBound bool) []cloudmodule.AgentCloudQuoteScope {
	result := planningQuoteScopes(workerBound)
	for index := range result {
		result[index].SchemaVersion = cloudmodule.AgentCloudQuoteScopeSchemaV2
		result[index].ServiceOperations = cloudmodule.AgentCloudServiceOperationScope{
			PrivateEndpoints: []cloudmodule.AgentCloudPrivateEndpointOperation{{
				OperationKey: "endpoint-s3", Service: "s3", SecurityGroupSource: "worker_dedicated",
				PrivateDNSEnabled: true, MonthlyHours: 720, DataMiBPerMonth: 96,
			}},
		}
	}
	return result
}

func planningWorkerControlScopes(workerBound bool) []cloudmodule.AgentCloudQuoteScope {
	result := planningQuoteScopes(workerBound)
	for index := range result {
		result[index].SchemaVersion = cloudmodule.AgentCloudQuoteScopeSchemaV2
		result[index].Resource.Region = "ap-northeast-3"
		result[index].Resource.AvailabilityZones = []string{"ap-northeast-3a"}
		result[index].Network = cloudmodule.AgentCloudNetworkScope{
			VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", SecurityGroupMode: "create_dedicated", EntryPoint: "none",
			RouteTableID: "rtb-0123456789abcdef0", ControlPlaneEndpoint: "grpcs://worker-control.y1.dirextalk.ai:443", PrivateConnectivity: "no_nat_endpoints_v1",
		}
		result[index].ServiceOperations = cloudmodule.AgentCloudServiceOperationScope{PrivateEndpoints: []cloudmodule.AgentCloudPrivateEndpointOperation{
			{OperationKey: "worker-worker-control-interface", Service: "worker_control", ServiceName: "com.amazonaws.vpce.ap-northeast-3.vpce-svc-0123456789abcdef0", SecurityGroupSource: "endpoint_dedicated_from_worker", EndpointType: "interface", PrivateDNSEnabled: true, MonthlyHours: 730, DataMiBPerMonth: 1},
			{OperationKey: "worker-s3-gateway", Service: "s3", EndpointType: "gateway"},
			{OperationKey: "worker-secretsmanager-interface", Service: "secretsmanager", SecurityGroupSource: "endpoint_dedicated_from_worker", EndpointType: "interface", PrivateDNSEnabled: true, MonthlyHours: 730, DataMiBPerMonth: 1},
		}}
	}
	return result
}

func planningQuoteProto(now time.Time) *agentv1.CloudQuote {
	return planningQuoteProtoForScopes(now, planningQuoteScopes(true), cloudmodule.AgentCloudUsageEstimate{RuntimeHoursPerMonth: 730, PublicIPv4Hours: 730})
}

func planningQuoteProtoForScopes(now time.Time, scopes []cloudmodule.AgentCloudQuoteScope, usage cloudmodule.AgentCloudUsageEstimate) *agentv1.CloudQuote {
	candidates := make([]*agentv1.CloudQuoteCandidate, 0, len(scopes))
	for index, scope := range scopes {
		remote, _ := agentCloudQuoteScopeToProto(scope, "owner-from-config", true)
		micros := uint64((index + 1) * 1_000_000)
		candidates = append(candidates, &agentv1.CloudQuoteCandidate{
			CandidateProfile: remote.GetResource().GetCandidateProfile(), Scope: remote,
			ScopeDigest: "sha256:" + strings.Repeat(string(rune('5'+index)), 64), OfferedAvailabilityZones: append([]string(nil), scope.Resource.AvailabilityZones...),
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
		Candidates: candidates, Usage: agentCloudUsageToProto(usage),
		Assumptions: []string{"On-Demand pricing"}, Exclusions: []string{"tax"}, Digest: "sha256:" + strings.Repeat("8", 64),
	}
}

func planningPlanProto(now time.Time, selected *agentv1.CloudQuoteCandidate) *agentv1.CloudPlan {
	scope := selected.GetScope()
	return &agentv1.CloudPlan{
		PlanId: testPlanID, OwnerId: "owner-from-config", ConnectionId: scope.GetConnectionId(), Recipe: scope.GetRecipe(),
		SchemaVersion: agentCloudPlanSchemaForQuoteScope(scope.GetSchemaVersion()), ServiceOperations: scope.GetServiceOperations(),
		QuoteId: testQuoteID, QuoteDigest: "sha256:" + strings.Repeat("8", 64), QuoteScopeDigest: selected.GetScopeDigest(),
		CandidateProfile: selected.GetCandidateProfile(), QuoteValidUntil: timestamppb.New(now.Add(15 * time.Minute)),
		Resource: scope.GetResource(), Network: scope.GetNetwork(), SecretScope: scope.GetSecretScope(), IntegrationScope: scope.GetIntegrationScope(),
		Retention: scope.GetRetention(), Status: agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_READY_FOR_CONFIRMATION,
		PlanHash: "sha256:" + strings.Repeat("9", 64), Revision: 1,
	}
}
