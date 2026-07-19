package agentgrpc

import (
	"context"
	"reflect"
	"regexp"
	"strings"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var agentCloudCurrencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

var _ cloudmodule.AgentCloudPlanningClient = (*Runner)(nil)

// CreateAgentCloudQuote forwards only the typed pricing contract. Owner is
// always taken from Runner configuration, and an omitted Worker AMI pair is
// preserved so the Agent can bind its active immutable Worker release.
func (runner *Runner) CreateAgentCloudQuote(ctx context.Context, request cloudmodule.AgentCloudQuoteCreateRequest) (cloudmodule.AgentCloudQuote, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || len(request.Scopes) != 3 || !validAgentCloudUsage(request.Usage) ||
		(request.BootstrapSessionID == "") != (request.ExpectedSessionRevision == 0) ||
		(request.BootstrapSessionID != "" && (!validUUID(request.BootstrapSessionID) || request.ExpectedSessionRevision < 1)) {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	scopes := make([]*agentv1.CloudQuoteScope, 0, len(request.Scopes))
	seen := make(map[string]struct{}, len(request.Scopes))
	var usageScope *cloudmodule.AgentCloudQuoteScope
	for _, value := range request.Scopes {
		converted, ok := agentCloudQuoteScopeToProto(value, runner.ownerID, true)
		if !ok {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
		}
		if usageScope == nil {
			copy := value
			copy.ServiceOperations = cloudmodule.NormalizeAgentCloudServiceOperations(value.ServiceOperations)
			usageScope = &copy
		} else if !sameAgentCloudQuoteServiceOperationIdentity(*usageScope, value) {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
		}
		if err := cloudmodule.ValidateAgentCloudServiceOperationUsage(value.SchemaVersion, value.ServiceOperations, value.Resource, value.Network, value.Retention, request.Usage); err != nil {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
		}
		profile, _ := agentCloudCandidate(converted.GetResource().GetCandidateProfile())
		if _, duplicate := seen[profile]; duplicate {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
		}
		seen[profile] = struct{}{}
		scopes = append(scopes, converted)
	}
	for _, required := range []string{"economic", "recommended", "performance"} {
		if _, exists := seen[required]; !exists {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
		}
	}
	if usageScope == nil {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	spot, ok := agentCloudSpotToProto(request.SpotQualification)
	if !ok {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateCloudQuote(callContext, &agentv1.CreateCloudQuoteRequest{
		IdempotencyKey: request.IdempotencyKey, Scopes: scopes, Usage: agentCloudUsageToProto(request.Usage),
		SpotQualification: spot, BootstrapSessionId: request.BootstrapSessionID, ExpectedSessionRevision: request.ExpectedSessionRevision,
	})
	if err != nil {
		return cloudmodule.AgentCloudQuote{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetQuote() == nil {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	value, mapErr := runner.mapAgentCloudQuote(response.GetQuote(), "")
	if mapErr != nil || !sameAgentCloudQuoteRequest(value, request) {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return value, nil
}

func (runner *Runner) GetAgentCloudQuote(ctx context.Context, request cloudmodule.AgentCloudQuoteRequest) (cloudmodule.AgentCloudQuote, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudQuote{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.QuoteID) {
		return cloudmodule.AgentCloudQuote{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudQuote(callContext, &agentv1.GetCloudQuoteRequest{QuoteId: request.QuoteID, OwnerId: runner.ownerID})
	if err != nil {
		if status.Code(err) == codes.NotFound && callContext.Err() == nil {
			return cloudmodule.AgentCloudQuote{}, false, nil
		}
		return cloudmodule.AgentCloudQuote{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetQuote() == nil {
		return cloudmodule.AgentCloudQuote{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	value, mapErr := runner.mapAgentCloudQuote(response.GetQuote(), request.QuoteID)
	if mapErr != nil {
		return cloudmodule.AgentCloudQuote{}, false, mapErr
	}
	return value, true, nil
}

func (runner *Runner) CreateAgentCloudPlan(ctx context.Context, request cloudmodule.AgentCloudPlanCreateRequest) (cloudmodule.AgentCloudPlan, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	profile, ok := agentCloudCandidateToProto(request.CandidateProfile)
	if !validUUID(request.IdempotencyKey) || !validUUID(request.QuoteID) || !ok || request.CurrentScope.Resource.CandidateProfile != request.CandidateProfile {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	scope, ok := agentCloudQuoteScopeToProto(request.CurrentScope, runner.ownerID, false)
	if !ok {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateCloudPlan(callContext, &agentv1.CreateCloudPlanRequest{
		IdempotencyKey: request.IdempotencyKey, QuoteId: request.QuoteID, CandidateProfile: profile, CurrentScope: scope,
	})
	if err != nil {
		return cloudmodule.AgentCloudPlan{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetPlan() == nil {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	value, mapErr := runner.mapAgentCloudPlan(response.GetPlan(), "")
	if mapErr != nil || value.QuoteID != request.QuoteID || value.CandidateProfile != request.CandidateProfile ||
		value.SchemaVersion != agentCloudPlanSchemaForQuoteScope(request.CurrentScope.SchemaVersion) ||
		!reflect.DeepEqual(value.Resource, request.CurrentScope.Resource) || value.ConnectionID != request.CurrentScope.ConnectionID ||
		value.Recipe != request.CurrentScope.Recipe || !sameAgentCloudNetwork(value.Network, request.CurrentScope.Network) ||
		!reflect.DeepEqual(value.SecretScope, request.CurrentScope.SecretScope) || !reflect.DeepEqual(value.IntegrationScope, request.CurrentScope.IntegrationScope) ||
		value.Retention != request.CurrentScope.Retention || !sameAgentCloudServiceOperations(value.ServiceOperations, request.CurrentScope.ServiceOperations) {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return value, nil
}

func (runner *Runner) mapAgentCloudQuote(remote *agentv1.CloudQuote, expectedQuoteID string) (cloudmodule.AgentCloudQuote, error) {
	if remote == nil || (expectedQuoteID != "" && remote.GetQuoteId() != expectedQuoteID) || !validUUID(remote.GetQuoteId()) ||
		!agentCloudCurrencyPattern.MatchString(remote.GetCurrency()) || !agentCloudDigestPattern.MatchString(remote.GetDigest()) || len(remote.GetCandidates()) != 3 {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	quotedAt, quotedErr := exactAgentCloudTimestamp(remote.GetQuotedAt())
	validUntil, validErr := exactAgentCloudTimestamp(remote.GetValidUntil())
	usage := mapAgentCloudUsage(remote.GetUsage())
	if quotedErr != nil || validErr != nil || !validUntil.Equal(quotedAt.Add(15*time.Minute)) || !validAgentCloudUsage(usage) ||
		len(remote.GetAssumptions()) == 0 || len(remote.GetExclusions()) == 0 || !validAgentCloudStringSet(remote.GetAssumptions(), 32) || !validAgentCloudStringSet(remote.GetExclusions(), 32) {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	candidates := make([]cloudmodule.AgentCloudQuoteCandidate, 0, 3)
	seen := make(map[string]struct{}, 3)
	hasSpot := false
	var usageScope *cloudmodule.AgentCloudQuoteScope
	for _, value := range remote.GetCandidates() {
		profile, ok := agentCloudCandidate(value.GetCandidateProfile())
		if !ok || value.GetScope() == nil || value.GetScope().GetResource().GetCandidateProfile() != value.GetCandidateProfile() ||
			!agentCloudDigestPattern.MatchString(value.GetScopeDigest()) || len(value.GetOfferedAvailabilityZones()) == 0 || len(value.GetOfferedAvailabilityZones()) > 16 ||
			len(value.GetQuotas()) == 0 || len(value.GetQuotas()) > 16 || len(value.GetCostItems()) == 0 || len(value.GetCostItems()) > 64 ||
			value.GetHourlyEstimateMicros() == 0 || value.GetMonthlyEstimateMicros() == 0 || value.GetMaximumLaunchAmountMicros() == 0 {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		if _, duplicate := seen[profile]; duplicate {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		seen[profile] = struct{}{}
		scope, ok := mapAgentCloudQuoteScope(value.GetScope(), runner.ownerID)
		if !ok || scope.Resource.CandidateProfile != profile || !validOfferedZones(scope.Resource.Region, value.GetOfferedAvailabilityZones()) ||
			!zonesIntersect(scope.Resource.AvailabilityZones, value.GetOfferedAvailabilityZones()) {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		if usageScope == nil {
			copy := scope
			usageScope = &copy
		} else if !sameAgentCloudQuoteServiceOperationIdentity(*usageScope, scope) {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		if err := cloudmodule.ValidateAgentCloudServiceOperationUsage(scope.SchemaVersion, scope.ServiceOperations, scope.Resource, scope.Network, scope.Retention, usage); err != nil {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		quotas := make([]cloudmodule.AgentCloudQuotaEvidence, 0, len(value.GetQuotas()))
		for _, quota := range value.GetQuotas() {
			if quota == nil || !agentCloudIdentifierPattern.MatchString(quota.GetServiceCode()) || !agentCloudIdentifierPattern.MatchString(quota.GetQuotaCode()) || quota.GetRequiredUnits() == 0 {
				return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
			quotas = append(quotas, cloudmodule.AgentCloudQuotaEvidence{ServiceCode: quota.GetServiceCode(), QuotaCode: quota.GetQuotaCode(), LimitUnits: quota.GetLimitUnits(), UsedUnits: quota.GetUsedUnits(), RequiredUnits: quota.GetRequiredUnits()})
		}
		costs := make([]cloudmodule.AgentCloudCostItem, 0, len(value.GetCostItems()))
		for _, cost := range value.GetCostItems() {
			if cost == nil || !agentCloudIdentifierPattern.MatchString(cost.GetCategory()) || !validAgentCloudText(cost.GetDescription(), 512) || !agentCloudIdentifierPattern.MatchString(cost.GetSourceId()) {
				return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
			costs = append(costs, cloudmodule.AgentCloudCostItem{Category: cost.GetCategory(), Description: cost.GetDescription(), SourceID: cost.GetSourceId(), HourlyEstimateMicros: cost.GetHourlyEstimateMicros(), MonthlyEstimateMicros: cost.GetMonthlyEstimateMicros(), MaximumLaunchAmountMicros: cost.GetMaximumLaunchAmountMicros()})
		}
		hasSpot = hasSpot || scope.Resource.PurchaseOption == "spot"
		candidates = append(candidates, cloudmodule.AgentCloudQuoteCandidate{CandidateProfile: profile, Scope: scope, ScopeDigest: value.GetScopeDigest(), OfferedAvailabilityZones: append([]string(nil), value.GetOfferedAvailabilityZones()...), Quotas: quotas, CostItems: costs, HourlyEstimateMicros: value.GetHourlyEstimateMicros(), MonthlyEstimateMicros: value.GetMonthlyEstimateMicros(), MaximumLaunchAmountMicros: value.GetMaximumLaunchAmountMicros()})
	}
	for _, required := range []string{"economic", "recommended", "performance"} {
		if _, exists := seen[required]; !exists {
			return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
	}
	if usageScope == nil {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	spot, ok := mapAgentCloudSpot(remote.GetSpotEvidence())
	if !ok || hasSpot != (spot != nil) {
		return cloudmodule.AgentCloudQuote{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudQuote{QuoteID: remote.GetQuoteId(), Currency: remote.GetCurrency(), Digest: remote.GetDigest(), QuotedAt: quotedAt, ValidUntil: validUntil, Candidates: candidates, Usage: usage, Assumptions: append([]string(nil), remote.GetAssumptions()...), Exclusions: append([]string(nil), remote.GetExclusions()...), SpotQualification: spot}, nil
}

func agentCloudQuoteScopeToProto(value cloudmodule.AgentCloudQuoteScope, owner string, allowUnboundWorker bool) (*agentv1.CloudQuoteScope, bool) {
	if !agentCloudIdentifierPattern.MatchString(owner) || !validUUID(value.ConnectionID) || !agentCloudIdentifierPattern.MatchString(value.Recipe.RecipeID) ||
		!agentCloudDigestPattern.MatchString(value.Recipe.Digest) || (value.Recipe.Maturity != "experimental" && value.Recipe.Maturity != "managed") || !validAgentCloudQuoteScopeSchema(value.SchemaVersion) {
		return nil, false
	}
	resource, ok := agentCloudResourceToProto(value.Resource, allowUnboundWorker)
	if !ok {
		return nil, false
	}
	network, ok := agentCloudNetworkToProto(value.Network)
	if !ok {
		return nil, false
	}
	retention, ok := agentCloudRetentionToProto(value.Retention)
	if !ok {
		return nil, false
	}
	if !validAgentCloudVolumeRetention(value.Resource.VolumeScopes, value.Retention) {
		return nil, false
	}
	if err := cloudmodule.ValidateAgentCloudServiceOperations(value.SchemaVersion, value.ServiceOperations, value.Resource, value.Network, value.Retention); err != nil {
		return nil, false
	}
	serviceOperations, ok := agentCloudServiceOperationsToProto(value.ServiceOperations, value.SchemaVersion)
	if !ok {
		return nil, false
	}
	secrets := make([]*agentv1.CloudSecretScope, 0, len(value.SecretScope))
	for _, secret := range value.SecretScope {
		if !agentCloudSecretRefPattern.MatchString(secret.SecretRef) || !validAgentCloudText(secret.Purpose, 256) || (secret.Delivery != "file" && secret.Delivery != "environment") {
			return nil, false
		}
		secrets = append(secrets, &agentv1.CloudSecretScope{SecretRef: secret.SecretRef, Purpose: secret.Purpose, Delivery: secret.Delivery})
	}
	integrations := make([]*agentv1.CloudIntegrationScope, 0, len(value.IntegrationScope))
	for _, integration := range value.IntegrationScope {
		if !validAgentCloudIntegration(integration.Kind) || !validAgentCloudText(integration.Name, 160) || !validAgentCloudStringSet(integration.Scopes, 32) {
			return nil, false
		}
		integrations = append(integrations, &agentv1.CloudIntegrationScope{Kind: integration.Kind, Name: integration.Name, Scopes: append([]string(nil), integration.Scopes...)})
	}
	return &agentv1.CloudQuoteScope{OwnerId: owner, ConnectionId: value.ConnectionID, Recipe: &agentv1.CloudRecipeBinding{RecipeId: value.Recipe.RecipeID, Digest: value.Recipe.Digest, Maturity: value.Recipe.Maturity}, Resource: resource, Network: network, SecretScope: secrets, IntegrationScope: integrations, Retention: retention, SchemaVersion: value.SchemaVersion, ServiceOperations: serviceOperations}, true
}

func mapAgentCloudQuoteScope(value *agentv1.CloudQuoteScope, expectedOwner string) (cloudmodule.AgentCloudQuoteScope, bool) {
	if value == nil || value.GetOwnerId() != expectedOwner || !validUUID(value.GetConnectionId()) || value.GetRecipe() == nil ||
		!agentCloudIdentifierPattern.MatchString(value.GetRecipe().GetRecipeId()) || !agentCloudDigestPattern.MatchString(value.GetRecipe().GetDigest()) ||
		(value.GetRecipe().GetMaturity() != "experimental" && value.GetRecipe().GetMaturity() != "managed") {
		return cloudmodule.AgentCloudQuoteScope{}, false
	}
	requiresServiceOperations, knownSchema := cloudmodule.AgentCloudServiceOperationsRequired(value.GetSchemaVersion())
	if !validAgentCloudQuoteScopeSchema(value.GetSchemaVersion()) || !knownSchema || requiresServiceOperations != (value.GetServiceOperations() != nil) {
		return cloudmodule.AgentCloudQuoteScope{}, false
	}
	resource, ok := mapAgentCloudResource(value.GetResource())
	profile, profileOK := agentCloudCandidate(value.GetResource().GetCandidateProfile())
	if !ok || !profileOK {
		return cloudmodule.AgentCloudQuoteScope{}, false
	}
	resource.CandidateProfile = profile
	network, ok := mapAgentCloudNetwork(value.GetNetwork())
	if !ok {
		return cloudmodule.AgentCloudQuoteScope{}, false
	}
	retention, ok := mapAgentCloudRetention(value.GetRetention())
	if !ok || !validAgentCloudVolumeRetention(resource.VolumeScopes, retention) {
		return cloudmodule.AgentCloudQuoteScope{}, false
	}
	secrets := make([]cloudmodule.AgentCloudSecretScope, 0, len(value.GetSecretScope()))
	for _, secret := range value.GetSecretScope() {
		if secret == nil || !agentCloudSecretRefPattern.MatchString(secret.GetSecretRef()) || !validAgentCloudText(secret.GetPurpose(), 256) || (secret.GetDelivery() != "file" && secret.GetDelivery() != "environment") {
			return cloudmodule.AgentCloudQuoteScope{}, false
		}
		secrets = append(secrets, cloudmodule.AgentCloudSecretScope{SecretRef: secret.GetSecretRef(), Purpose: secret.GetPurpose(), Delivery: secret.GetDelivery()})
	}
	integrations := make([]cloudmodule.AgentCloudIntegrationScope, 0, len(value.GetIntegrationScope()))
	for _, integration := range value.GetIntegrationScope() {
		if integration == nil || !validAgentCloudIntegration(integration.GetKind()) || !validAgentCloudText(integration.GetName(), 160) || !validAgentCloudStringSet(integration.GetScopes(), 32) {
			return cloudmodule.AgentCloudQuoteScope{}, false
		}
		integrations = append(integrations, cloudmodule.AgentCloudIntegrationScope{Kind: integration.GetKind(), Name: integration.GetName(), Scopes: append([]string(nil), integration.GetScopes()...)})
	}
	serviceOperations, ok := mapAgentCloudServiceOperations(value.GetServiceOperations())
	if !ok {
		return cloudmodule.AgentCloudQuoteScope{}, false
	}
	result := cloudmodule.AgentCloudQuoteScope{SchemaVersion: value.GetSchemaVersion(), ConnectionID: value.GetConnectionId(), Recipe: cloudmodule.AgentCloudRecipeBinding{RecipeID: value.GetRecipe().GetRecipeId(), Digest: value.GetRecipe().GetDigest(), Maturity: value.GetRecipe().GetMaturity()}, Resource: resource, Network: network, SecretScope: secrets, IntegrationScope: integrations, Retention: retention, ServiceOperations: serviceOperations}
	if err := cloudmodule.ValidateAgentCloudServiceOperations(result.SchemaVersion, result.ServiceOperations, result.Resource, result.Network, result.Retention); err != nil {
		return cloudmodule.AgentCloudQuoteScope{}, false
	}
	return result, true
}

func agentCloudServiceOperationsToProto(value cloudmodule.AgentCloudServiceOperationScope, schemaVersion string) (*agentv1.CloudServiceOperationScope, bool) {
	required, known := cloudmodule.AgentCloudServiceOperationsRequired(schemaVersion)
	if !known {
		return nil, false
	}
	if !required {
		return nil, len(value.PrivateEndpoints) == 0 && len(value.Snapshots) == 0
	}
	if len(value.PrivateEndpoints) == 0 && len(value.Snapshots) == 0 {
		return nil, false
	}
	normalized := cloudmodule.NormalizeAgentCloudServiceOperations(value)
	result := &agentv1.CloudServiceOperationScope{
		PrivateEndpoints: make([]*agentv1.CloudPrivateEndpointOperation, 0, len(normalized.PrivateEndpoints)),
		Snapshots:        make([]*agentv1.CloudSnapshotOperation, 0, len(normalized.Snapshots)),
	}
	for _, endpoint := range normalized.PrivateEndpoints {
		service := agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_UNSPECIFIED
		switch endpoint.Service {
		case "s3":
			service = agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_S3
		case "secretsmanager":
			service = agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_SECRETS_MANAGER
		case "worker_control":
			service = agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_WORKER_CONTROL
		default:
			return nil, false
		}
		securityGroupSource := agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_UNSPECIFIED
		switch endpoint.SecurityGroupSource {
		case "":
			// Gateway endpoints have no security group.
		case "plan_existing":
			securityGroupSource = agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_PLAN_EXISTING
		case "worker_dedicated":
			securityGroupSource = agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_WORKER_DEDICATED
		case "endpoint_dedicated_from_worker":
			securityGroupSource = agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_ENDPOINT_DEDICATED_FROM_WORKER
		default:
			return nil, false
		}
		endpointType := agentv1.CloudPrivateEndpointType_CLOUD_PRIVATE_ENDPOINT_TYPE_UNSPECIFIED
		switch endpoint.EndpointType {
		case "":
			// The frozen V2 interface endpoint compatibility form omits this field.
		case "gateway":
			endpointType = agentv1.CloudPrivateEndpointType_CLOUD_PRIVATE_ENDPOINT_TYPE_GATEWAY
		case "interface":
			endpointType = agentv1.CloudPrivateEndpointType_CLOUD_PRIVATE_ENDPOINT_TYPE_INTERFACE
		default:
			return nil, false
		}
		result.PrivateEndpoints = append(result.PrivateEndpoints, &agentv1.CloudPrivateEndpointOperation{
			OperationKey: endpoint.OperationKey, Service: service, ServiceName: endpoint.ServiceName, SecurityGroupSource: securityGroupSource,
			PrivateDnsEnabled: endpoint.PrivateDNSEnabled, MonthlyHours: endpoint.MonthlyHours, DataMibPerMonth: endpoint.DataMiBPerMonth, EndpointType: endpointType,
		})
	}
	for _, snapshot := range normalized.Snapshots {
		disposition := agentv1.CloudSnapshotOperationDisposition_CLOUD_SNAPSHOT_OPERATION_DISPOSITION_UNSPECIFIED
		switch snapshot.Disposition {
		case "delete_with_deployment":
			disposition = agentv1.CloudSnapshotOperationDisposition_CLOUD_SNAPSHOT_OPERATION_DISPOSITION_DELETE_WITH_DEPLOYMENT
		case "retain_with_managed_service":
			disposition = agentv1.CloudSnapshotOperationDisposition_CLOUD_SNAPSHOT_OPERATION_DISPOSITION_RETAIN_WITH_MANAGED_SERVICE
		default:
			return nil, false
		}
		result.Snapshots = append(result.Snapshots, &agentv1.CloudSnapshotOperation{
			OperationKey: snapshot.OperationKey, SourceVolumeSlotId: snapshot.SourceVolumeSlotID,
			SourceVolumeSpecDigest: snapshot.SourceVolumeSpecDigest, Disposition: disposition,
			MaxRetentionSeconds: snapshot.MaxRetentionSeconds,
		})
	}
	return result, true
}

func mapAgentCloudServiceOperations(value *agentv1.CloudServiceOperationScope) (cloudmodule.AgentCloudServiceOperationScope, bool) {
	if value == nil {
		return cloudmodule.AgentCloudServiceOperationScope{}, true
	}
	result := cloudmodule.AgentCloudServiceOperationScope{
		PrivateEndpoints: make([]cloudmodule.AgentCloudPrivateEndpointOperation, 0, len(value.GetPrivateEndpoints())),
		Snapshots:        make([]cloudmodule.AgentCloudSnapshotOperation, 0, len(value.GetSnapshots())),
	}
	for _, endpoint := range value.GetPrivateEndpoints() {
		if endpoint == nil {
			return cloudmodule.AgentCloudServiceOperationScope{}, false
		}
		service := ""
		switch endpoint.GetService() {
		case agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_S3:
			service = "s3"
		case agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_SECRETS_MANAGER:
			service = "secretsmanager"
		case agentv1.CloudPrivateEndpointService_CLOUD_PRIVATE_ENDPOINT_SERVICE_WORKER_CONTROL:
			service = "worker_control"
		default:
			return cloudmodule.AgentCloudServiceOperationScope{}, false
		}
		securityGroupSource := ""
		switch endpoint.GetSecurityGroupSource() {
		case agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_UNSPECIFIED:
			securityGroupSource = ""
		case agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_PLAN_EXISTING:
			securityGroupSource = "plan_existing"
		case agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_WORKER_DEDICATED:
			securityGroupSource = "worker_dedicated"
		case agentv1.CloudEndpointSecurityGroupSource_CLOUD_ENDPOINT_SECURITY_GROUP_SOURCE_ENDPOINT_DEDICATED_FROM_WORKER:
			securityGroupSource = "endpoint_dedicated_from_worker"
		default:
			return cloudmodule.AgentCloudServiceOperationScope{}, false
		}
		endpointType := ""
		switch endpoint.GetEndpointType() {
		case agentv1.CloudPrivateEndpointType_CLOUD_PRIVATE_ENDPOINT_TYPE_UNSPECIFIED:
		case agentv1.CloudPrivateEndpointType_CLOUD_PRIVATE_ENDPOINT_TYPE_GATEWAY:
			endpointType = "gateway"
		case agentv1.CloudPrivateEndpointType_CLOUD_PRIVATE_ENDPOINT_TYPE_INTERFACE:
			endpointType = "interface"
		default:
			return cloudmodule.AgentCloudServiceOperationScope{}, false
		}
		result.PrivateEndpoints = append(result.PrivateEndpoints, cloudmodule.AgentCloudPrivateEndpointOperation{
			OperationKey: endpoint.GetOperationKey(), Service: service, ServiceName: endpoint.GetServiceName(), SecurityGroupSource: securityGroupSource,
			PrivateDNSEnabled: endpoint.GetPrivateDnsEnabled(), MonthlyHours: endpoint.GetMonthlyHours(), DataMiBPerMonth: endpoint.GetDataMibPerMonth(), EndpointType: endpointType,
		})
	}
	for _, snapshot := range value.GetSnapshots() {
		if snapshot == nil {
			return cloudmodule.AgentCloudServiceOperationScope{}, false
		}
		disposition := ""
		switch snapshot.GetDisposition() {
		case agentv1.CloudSnapshotOperationDisposition_CLOUD_SNAPSHOT_OPERATION_DISPOSITION_DELETE_WITH_DEPLOYMENT:
			disposition = "delete_with_deployment"
		case agentv1.CloudSnapshotOperationDisposition_CLOUD_SNAPSHOT_OPERATION_DISPOSITION_RETAIN_WITH_MANAGED_SERVICE:
			disposition = "retain_with_managed_service"
		default:
			return cloudmodule.AgentCloudServiceOperationScope{}, false
		}
		result.Snapshots = append(result.Snapshots, cloudmodule.AgentCloudSnapshotOperation{
			OperationKey: snapshot.GetOperationKey(), SourceVolumeSlotID: snapshot.GetSourceVolumeSlotId(),
			SourceVolumeSpecDigest: snapshot.GetSourceVolumeSpecDigest(), Disposition: disposition,
			MaxRetentionSeconds: snapshot.GetMaxRetentionSeconds(),
		})
	}
	return cloudmodule.NormalizeAgentCloudServiceOperations(result), true
}

func agentCloudPlanSchemaForQuoteScope(schemaVersion string) string {
	switch schemaVersion {
	case cloudmodule.AgentCloudQuoteScopeSchemaV1:
		return cloudmodule.AgentCloudPlanSchemaV1
	case cloudmodule.AgentCloudQuoteScopeSchemaV2:
		return cloudmodule.AgentCloudPlanSchemaV2
	default:
		return ""
	}
}

func validAgentCloudQuoteScopeSchema(value string) bool {
	return value == cloudmodule.AgentCloudQuoteScopeSchemaV1 || value == cloudmodule.AgentCloudQuoteScopeSchemaV2
}

func validAgentCloudPlanSchema(value string) bool {
	return value == cloudmodule.AgentCloudPlanSchemaV1 || value == cloudmodule.AgentCloudPlanSchemaV2
}

func sameAgentCloudServiceOperations(left, right cloudmodule.AgentCloudServiceOperationScope) bool {
	return reflect.DeepEqual(cloudmodule.NormalizeAgentCloudServiceOperations(left), cloudmodule.NormalizeAgentCloudServiceOperations(right))
}

func sameAgentCloudQuoteServiceOperationIdentity(left, right cloudmodule.AgentCloudQuoteScope) bool {
	return left.SchemaVersion == right.SchemaVersion && sameAgentCloudServiceOperations(left.ServiceOperations, right.ServiceOperations)
}

func agentCloudResourceToProto(value cloudmodule.AgentCloudResourceScope, allowUnboundWorker bool) (*agentv1.CloudResourceScope, bool) {
	profile, ok := agentCloudCandidateToProto(value.CandidateProfile)
	if !ok || (value.WorkerImageID == "") != (value.WorkerImageDigest == "") || (!allowUnboundWorker && value.WorkerImageID == "") {
		return nil, false
	}
	resource := &agentv1.CloudResourceScope{CandidateProfile: profile, Region: value.Region, AvailabilityZones: append([]string(nil), value.AvailabilityZones...), InstanceType: value.InstanceType, InstanceCount: value.InstanceCount, Architecture: value.Architecture, Vcpu: value.VCPU, MemoryMib: value.MemoryMiB, GpuType: value.GPUType, GpuCount: value.GPUCount, GpuMemoryMib: value.GPUMemoryMiB, DiskGib: value.DiskGiB, VolumeType: value.VolumeType, VolumeIops: value.VolumeIOPS, VolumeThroughputMibps: value.VolumeThroughputMiBPS, VolumeEncrypted: value.VolumeEncrypted, WorkerImageId: value.WorkerImageID, WorkerImageDigest: value.WorkerImageDigest}
	if len(value.VolumeScopes) > 0 {
		resource.VolumeScopes = make([]*agentv1.CloudVolumeScope, 0, len(value.VolumeScopes))
		for _, volume := range value.VolumeScopes {
			resource.VolumeScopes = append(resource.VolumeScopes, &agentv1.CloudVolumeScope{
				SlotId: volume.SlotID, SizeGib: volume.SizeGiB, VolumeType: volume.VolumeType, Iops: volume.IOPS,
				ThroughputMibps: volume.ThroughputMiBPS, Encrypted: volume.Encrypted, KmsKeyId: volume.KMSKeyID,
				DeviceName: volume.DeviceName, MountPath: volume.MountPath, ReadOnly: volume.ReadOnly,
				Persistent: volume.Persistent, Disposition: volume.Disposition,
			})
		}
	}
	switch value.PurchaseOption {
	case "on_demand":
		resource.PurchaseOption = agentv1.CloudPurchaseOption_CLOUD_PURCHASE_OPTION_ON_DEMAND
	case "spot":
		resource.PurchaseOption = agentv1.CloudPurchaseOption_CLOUD_PURCHASE_OPTION_SPOT
	default:
		return nil, false
	}
	validation := proto.Clone(resource).(*agentv1.CloudResourceScope)
	if validation.WorkerImageId == "" {
		validation.WorkerImageId = "ami-00000000000000000"
		validation.WorkerImageDigest = "sha256:" + strings.Repeat("0", 64)
	}
	if _, ok := mapAgentCloudResource(validation); !ok {
		return nil, false
	}
	return resource, true
}

func agentCloudNetworkToProto(value cloudmodule.AgentCloudNetworkScope) (*agentv1.CloudNetworkScope, bool) {
	value = normalizeAgentCloudNetwork(value)
	network := &agentv1.CloudNetworkScope{VpcId: value.VPCID, SubnetId: value.SubnetID, SecurityGroupId: value.SecurityGroupID, PublicIpv4: value.PublicIPv4, PublicExposure: value.PublicExposure, IngressPorts: append([]uint32(nil), value.IngressPorts...), Hostname: value.Hostname, TlsRequired: value.TLSRequired, AuthenticationRequired: value.AuthenticationRequired, RouteTableId: value.RouteTableID, ControlPlaneEndpoint: value.ControlPlaneEndpoint, PrivateConnectivity: value.PrivateConnectivity}
	switch value.SecurityGroupMode {
	case "existing":
		network.SecurityGroupMode = agentv1.CloudSecurityGroupMode_CLOUD_SECURITY_GROUP_MODE_EXISTING
	case "create_dedicated":
		network.SecurityGroupMode = agentv1.CloudSecurityGroupMode_CLOUD_SECURITY_GROUP_MODE_CREATE_DEDICATED
	default:
		return nil, false
	}
	switch value.EntryPoint {
	case "none":
		network.EntryPoint = agentv1.CloudEntryPointKind_CLOUD_ENTRY_POINT_KIND_NONE
	case "alb":
		network.EntryPoint = agentv1.CloudEntryPointKind_CLOUD_ENTRY_POINT_KIND_ALB
	case "cloudfront":
		network.EntryPoint = agentv1.CloudEntryPointKind_CLOUD_ENTRY_POINT_KIND_CLOUDFRONT
	default:
		return nil, false
	}
	_, ok := mapAgentCloudNetwork(network)
	return network, ok
}

func agentCloudRetentionToProto(value cloudmodule.AgentCloudRetentionScope) (*agentv1.CloudRetentionScope, bool) {
	retention := &agentv1.CloudRetentionScope{AutoDestroy: value.AutoDestroy, GracePeriodSeconds: value.GracePeriodSeconds, MaxLifetimeSeconds: value.MaxLifetimeSeconds}
	switch value.Class {
	case "ephemeral":
		retention.RetentionClass = agentv1.CloudRetentionClass_CLOUD_RETENTION_CLASS_EPHEMERAL
	case "managed":
		retention.RetentionClass = agentv1.CloudRetentionClass_CLOUD_RETENTION_CLASS_MANAGED
	default:
		return nil, false
	}
	_, ok := mapAgentCloudRetention(retention)
	return retention, ok
}

func agentCloudCandidateToProto(value string) (agentv1.CloudCandidateProfile, bool) {
	switch value {
	case "economic":
		return agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_ECONOMY, true
	case "recommended":
		return agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_RECOMMENDED, true
	case "performance":
		return agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_PERFORMANCE, true
	default:
		return agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_UNSPECIFIED, false
	}
}

func validAgentCloudUsage(value cloudmodule.AgentCloudUsageEstimate) bool {
	return cloudmodule.ValidAgentCloudUsageEstimate(value)
}

func agentCloudUsageToProto(value cloudmodule.AgentCloudUsageEstimate) *agentv1.CloudUsageEstimate {
	return &agentv1.CloudUsageEstimate{RuntimeHoursPerMonth: value.RuntimeHoursPerMonth, PublicIpv4Hours: value.PublicIPv4Hours, LogIngestMib: value.LogIngestMiB, LogStoredMibMonths: value.LogStoredMiBMonths, SnapshotGibMonths: value.SnapshotGiBMonths, EntryHours: value.EntryHours, InternetEgressMib: value.InternetEgressMiB, PrivateEndpointHours: value.PrivateEndpointHours, PrivateEndpointDataMib: value.PrivateEndpointDataMiB}
}

func mapAgentCloudUsage(value *agentv1.CloudUsageEstimate) cloudmodule.AgentCloudUsageEstimate {
	if value == nil {
		return cloudmodule.AgentCloudUsageEstimate{}
	}
	return cloudmodule.AgentCloudUsageEstimate{RuntimeHoursPerMonth: value.GetRuntimeHoursPerMonth(), PublicIPv4Hours: value.GetPublicIpv4Hours(), LogIngestMiB: value.GetLogIngestMib(), LogStoredMiBMonths: value.GetLogStoredMibMonths(), SnapshotGiBMonths: value.GetSnapshotGibMonths(), EntryHours: value.GetEntryHours(), InternetEgressMiB: value.GetInternetEgressMib(), PrivateEndpointHours: value.GetPrivateEndpointHours(), PrivateEndpointDataMiB: value.GetPrivateEndpointDataMib()}
}

func agentCloudSpotToProto(value *cloudmodule.AgentCloudSpotQualification) (*agentv1.CloudSpotQualification, bool) {
	if value == nil {
		return nil, true
	}
	if !agentCloudIdentifierPattern.MatchString(value.EvidenceID) || !agentCloudDigestPattern.MatchString(value.RecipeDigest) || !agentCloudIdentifierPattern.MatchString(value.CheckpointName) ||
		!agentCloudIdentifierPattern.MatchString(value.ResumeAction) || value.MaxRetries == 0 || value.MaxRetries > 100 || value.CheckpointVerifiedAt.IsZero() ||
		value.InterruptionTestedAt.Before(value.CheckpointVerifiedAt) {
		return nil, false
	}
	return &agentv1.CloudSpotQualification{EvidenceId: value.EvidenceID, RecipeDigest: value.RecipeDigest, CheckpointName: value.CheckpointName, ResumeAction: value.ResumeAction, MaxRetries: value.MaxRetries, CheckpointVerifiedAt: timestamppb.New(value.CheckpointVerifiedAt), InterruptionTestedAt: timestamppb.New(value.InterruptionTestedAt)}, true
}

func mapAgentCloudSpot(value *agentv1.CloudSpotQualification) (*cloudmodule.AgentCloudSpotQualification, bool) {
	if value == nil {
		return nil, true
	}
	checkpoint, err1 := exactAgentCloudTimestamp(value.GetCheckpointVerifiedAt())
	interruption, err2 := exactAgentCloudTimestamp(value.GetInterruptionTestedAt())
	result := &cloudmodule.AgentCloudSpotQualification{EvidenceID: value.GetEvidenceId(), RecipeDigest: value.GetRecipeDigest(), CheckpointName: value.GetCheckpointName(), ResumeAction: value.GetResumeAction(), MaxRetries: value.GetMaxRetries(), CheckpointVerifiedAt: checkpoint, InterruptionTestedAt: interruption}
	_, ok := agentCloudSpotToProto(result)
	return result, err1 == nil && err2 == nil && ok
}

func validOfferedZones(region string, zones []string) bool {
	seen := make(map[string]struct{}, len(zones))
	for _, zone := range zones {
		if !agentCloudAZPattern.MatchString(zone) || !strings.HasPrefix(zone, region) {
			return false
		}
		if _, duplicate := seen[zone]; duplicate {
			return false
		}
		seen[zone] = struct{}{}
	}
	return true
}

func zonesIntersect(left, right []string) bool {
	set := make(map[string]struct{}, len(left))
	for _, value := range left {
		set[value] = struct{}{}
	}
	for _, value := range right {
		if _, exists := set[value]; exists {
			return true
		}
	}
	return false
}

func sameAgentCloudQuoteRequest(value cloudmodule.AgentCloudQuote, request cloudmodule.AgentCloudQuoteCreateRequest) bool {
	if !reflect.DeepEqual(value.Usage, request.Usage) || !sameAgentCloudSpot(value.SpotQualification, request.SpotQualification) || len(value.Candidates) != len(request.Scopes) {
		return false
	}
	expected := make(map[string]cloudmodule.AgentCloudQuoteScope, len(request.Scopes))
	for _, scope := range request.Scopes {
		expected[scope.Resource.CandidateProfile] = scope
	}
	for _, candidate := range value.Candidates {
		scope, exists := expected[candidate.CandidateProfile]
		if !exists {
			return false
		}
		actualResource, expectedResource := candidate.Scope.Resource, scope.Resource
		actualResource.WorkerImageID, actualResource.WorkerImageDigest = "", ""
		expectedResource.WorkerImageID, expectedResource.WorkerImageDigest = "", ""
		if candidate.Scope.SchemaVersion != scope.SchemaVersion || candidate.Scope.ConnectionID != scope.ConnectionID || candidate.Scope.Recipe != scope.Recipe ||
			!reflect.DeepEqual(actualResource, expectedResource) || !sameAgentCloudNetwork(candidate.Scope.Network, scope.Network) ||
			!reflect.DeepEqual(candidate.Scope.SecretScope, scope.SecretScope) || !reflect.DeepEqual(candidate.Scope.IntegrationScope, scope.IntegrationScope) ||
			candidate.Scope.Retention != scope.Retention || !sameAgentCloudServiceOperations(candidate.Scope.ServiceOperations, scope.ServiceOperations) {
			return false
		}
		delete(expected, candidate.CandidateProfile)
	}
	return len(expected) == 0
}

func normalizeAgentCloudNetwork(value cloudmodule.AgentCloudNetworkScope) cloudmodule.AgentCloudNetworkScope {
	if value.SecurityGroupMode == "" && value.SecurityGroupID != "" {
		value.SecurityGroupMode = "existing"
	}
	return value
}

func sameAgentCloudNetwork(left, right cloudmodule.AgentCloudNetworkScope) bool {
	return reflect.DeepEqual(normalizeAgentCloudNetwork(left), normalizeAgentCloudNetwork(right))
}

func sameAgentCloudSpot(left, right *cloudmodule.AgentCloudSpotQualification) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.EvidenceID == right.EvidenceID && left.RecipeDigest == right.RecipeDigest && left.CheckpointName == right.CheckpointName &&
		left.ResumeAction == right.ResumeAction && left.MaxRetries == right.MaxRetries && left.CheckpointVerifiedAt.Equal(right.CheckpointVerifiedAt) &&
		left.InterruptionTestedAt.Equal(right.InterruptionTestedAt)
}
