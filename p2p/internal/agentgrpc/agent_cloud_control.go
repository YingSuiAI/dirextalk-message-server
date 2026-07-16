package agentgrpc

import (
	"context"
	"crypto/ed25519"
	"reflect"
	"regexp"
	"strings"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	agentCloudConnectionPageSize = int32(100)
	maxAgentCloudConnectionPages = 16
	maxAgentCloudConnections     = 1000
	agentCloudPlanPageSize       = int32(100)
	maxAgentCloudPlanPages       = 16
	maxAgentCloudPlans           = 1000
)

var (
	agentCloudDigestPattern     = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	agentCloudIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	agentCloudAccountPattern    = regexp.MustCompile(`^[0-9]{12}$`)
	agentCloudRoleARNPattern    = regexp.MustCompile(`^arn:(aws|aws-cn|aws-us-gov):iam::([0-9]{12}):role/([A-Za-z0-9+=,.@_/-]{1,512})$`)
	agentCloudStackARNPattern   = regexp.MustCompile(`^arn:(aws|aws-cn|aws-us-gov):cloudformation:([a-z0-9-]+):([0-9]{12}):stack/([^\x00-\x20\x7f]{1,256})/([0-9a-f-]{36})$`)
	agentCloudAZPattern         = regexp.MustCompile(`^[a-z]{2}(?:-[a-z0-9]+)+-[0-9]+[a-z]$`)
	agentCloudInstancePattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*\.[a-z0-9]+$`)
	agentCloudAMIPattern        = regexp.MustCompile(`^ami-[0-9a-f]{8,17}$`)
	agentCloudVPCPattern        = regexp.MustCompile(`^vpc-[0-9a-f]{8,17}$`)
	agentCloudSubnetPattern     = regexp.MustCompile(`^subnet-[0-9a-f]{8,17}$`)
	agentCloudSGPattern         = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)
	agentCloudSecretRefPattern  = regexp.MustCompile(`^secret_ref:[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$`)
)

// ListAgentCloudPlans adapts Agent's owner-scoped cursor API to the existing
// complete ProductCore list while preserving strict owner and scope mapping.
func (runner *Runner) ListAgentCloudPlans(ctx context.Context) ([]cloudmodule.AgentCloudPlan, error) {
	if runner == nil || runner.cloud == nil {
		return nil, cloudmodule.ErrAgentCloudControlUnavailable
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()

	items := make([]cloudmodule.AgentCloudPlan, 0)
	seenTokens := make(map[string]struct{})
	seenPlans := make(map[string]struct{})
	pageToken := ""
	for page := 0; page < maxAgentCloudPlanPages; page++ {
		response, err := runner.cloud.ListCloudPlans(callContext, &agentv1.ListCloudPlansRequest{
			OwnerId: runner.ownerID, PageSize: agentCloudPlanPageSize, PageToken: pageToken,
		})
		if err != nil {
			return nil, mapAgentCloudControlRPCError(callContext, err)
		}
		if response == nil || len(items)+len(response.GetPlans()) > maxAgentCloudPlans {
			return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		for _, remote := range response.GetPlans() {
			item, mapErr := runner.mapAgentCloudPlan(remote, "")
			if mapErr != nil {
				return nil, mapErr
			}
			if _, duplicate := seenPlans[item.PlanID]; duplicate {
				return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
			seenPlans[item.PlanID] = struct{}{}
			items = append(items, item)
		}
		next := strings.TrimSpace(response.GetNextPageToken())
		if next == "" {
			return items, nil
		}
		if next == pageToken {
			return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}
	return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
}

// ListAgentCloudConnections adapts Agent's owner-scoped cursor API to the
// existing complete ProductCore list. Traversal is bounded and every item is
// validated through the same strict projection used by Get.
func (runner *Runner) ListAgentCloudConnections(ctx context.Context) ([]cloudmodule.AgentCloudConnection, error) {
	if runner == nil || runner.cloud == nil {
		return nil, cloudmodule.ErrAgentCloudControlUnavailable
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()

	items := make([]cloudmodule.AgentCloudConnection, 0)
	seenTokens := make(map[string]struct{})
	seenConnections := make(map[string]struct{})
	pageToken := ""
	for page := 0; page < maxAgentCloudConnectionPages; page++ {
		response, err := runner.cloud.ListCloudConnections(callContext, &agentv1.ListCloudConnectionsRequest{
			OwnerId: runner.ownerID, PageSize: agentCloudConnectionPageSize, PageToken: pageToken,
		})
		if err != nil {
			return nil, mapAgentCloudControlRPCError(callContext, err)
		}
		if response == nil || len(items)+len(response.GetConnections()) > maxAgentCloudConnections {
			return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		for _, remote := range response.GetConnections() {
			item, mapErr := runner.mapAgentCloudConnection(remote, "", "")
			if mapErr != nil {
				return nil, mapErr
			}
			if _, duplicate := seenConnections[item.ConnectionID]; duplicate {
				return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
			}
			seenConnections[item.ConnectionID] = struct{}{}
			items = append(items, item)
		}
		next := strings.TrimSpace(response.GetNextPageToken())
		if next == "" {
			return items, nil
		}
		if next == pageToken {
			return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}
	return nil, cloudmodule.ErrAgentCloudControlInvalidResponse
}

func (runner *Runner) GetAgentCloudPlan(ctx context.Context, request cloudmodule.AgentCloudPlanRequest) (cloudmodule.AgentCloudPlan, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudPlan{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.PlanID) {
		return cloudmodule.AgentCloudPlan{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudPlan(callContext, &agentv1.GetCloudPlanRequest{PlanId: request.PlanID, OwnerId: runner.ownerID})
	if err != nil {
		if status.Code(err) == codes.NotFound && callContext.Err() == nil {
			return cloudmodule.AgentCloudPlan{}, false, nil
		}
		return cloudmodule.AgentCloudPlan{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetPlan() == nil {
		return cloudmodule.AgentCloudPlan{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	value, mapErr := runner.mapAgentCloudPlan(response.GetPlan(), request.PlanID)
	if mapErr != nil {
		return cloudmodule.AgentCloudPlan{}, false, mapErr
	}
	return value, true, nil
}

func (runner *Runner) CreateAgentCloudApprovalChallenge(ctx context.Context, request cloudmodule.AgentCloudChallengeRequest) (cloudmodule.AgentCloudChallenge, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudChallenge{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.PlanID) || request.ExpectedRevision <= 0 || !agentCloudIdentifierPattern.MatchString(request.SignerKeyID) ||
		!sameExpectedPlanRequest(request.ExpectedPlan, request.PlanID, request.ExpectedRevision, runner.ownerID, "ready_for_confirmation") {
		return cloudmodule.AgentCloudChallenge{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.CreateApprovalChallenge(callContext, &agentv1.CreateApprovalChallengeRequest{
		IdempotencyKey: request.IdempotencyKey, PlanId: request.PlanID, ExpectedRevision: request.ExpectedRevision,
		SignerKeyId: request.SignerKeyID, OwnerId: runner.ownerID,
	})
	if err != nil {
		return cloudmodule.AgentCloudChallenge{}, mapAgentCloudControlRPCError(callContext, err)
	}
	remote := response.GetChallenge()
	if response == nil || remote == nil || !validUUID(remote.GetApprovalId()) || !agentCloudIdentifierPattern.MatchString(remote.GetChallengeId()) ||
		remote.GetSignerKeyId() != request.SignerKeyID || !validUUID(remote.GetAgentInstanceId()) ||
		remote.GetOwnerId() != runner.ownerID || remote.GetPlanId() != request.PlanID || remote.GetPlanRevision() != request.ExpectedRevision ||
		remote.GetPlanHash() != request.ExpectedPlan.PlanHash || remote.GetConnectionId() != request.ExpectedPlan.ConnectionID ||
		remote.GetRecipeDigest() != request.ExpectedPlan.Recipe.Digest || remote.GetQuoteId() != request.ExpectedPlan.QuoteID ||
		remote.GetQuoteDigest() != request.ExpectedPlan.QuoteDigest || remote.GetQuoteScopeDigest() != request.ExpectedPlan.QuoteScopeDigest ||
		remote.GetQuoteCandidateId() != request.ExpectedPlan.CandidateProfile || remote.GetRevision() <= 0 ||
		len(remote.GetSigningPayloadCbor()) == 0 || len(remote.GetSigningPayloadCbor()) > 64*1024 {
		return cloudmodule.AgentCloudChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	expiresAt, timestampErr := exactAgentCloudTimestamp(remote.GetExpiresAt())
	if timestampErr != nil {
		return cloudmodule.AgentCloudChallenge{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudChallenge{
		ApprovalID: remote.GetApprovalId(), ChallengeID: remote.GetChallengeId(), SignerKeyID: remote.GetSignerKeyId(),
		AgentInstanceID: remote.GetAgentInstanceId(), OwnerID: remote.GetOwnerId(), PlanID: remote.GetPlanId(),
		PlanRevision: remote.GetPlanRevision(), PlanHash: remote.GetPlanHash(), ConnectionID: remote.GetConnectionId(),
		RecipeDigest: remote.GetRecipeDigest(), QuoteID: remote.GetQuoteId(), QuoteDigest: remote.GetQuoteDigest(),
		QuoteScopeDigest: remote.GetQuoteScopeDigest(), QuoteCandidateID: remote.GetQuoteCandidateId(),
		ExpiresAt: expiresAt, SigningPayloadCBOR: append([]byte(nil), remote.GetSigningPayloadCbor()...), Revision: remote.GetRevision(),
	}, nil
}

func (runner *Runner) ApproveAgentCloudPlan(ctx context.Context, request cloudmodule.AgentCloudApproveRequest) (cloudmodule.AgentCloudPlan, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.PlanID) || request.ExpectedRevision <= 0 ||
		!sameExpectedPlanRequest(request.ExpectedPlan, request.PlanID, request.ExpectedRevision, runner.ownerID, "ready_for_confirmation") || !validAgentCloudApproval(request.Approval) {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.ApproveCloudPlan(callContext, &agentv1.ApproveCloudPlanRequest{
		IdempotencyKey: request.IdempotencyKey, PlanId: request.PlanID, ExpectedRevision: request.ExpectedRevision,
		Approval: agentCloudApprovalToProto(request.Approval), OwnerId: runner.ownerID,
	})
	if err != nil {
		return cloudmodule.AgentCloudPlan{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetPlan() == nil {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	value, mapErr := runner.mapAgentCloudPlan(response.GetPlan(), request.PlanID)
	if mapErr != nil || value.Status != cloudmodule.AgentCloudPlanStatusApproved || value.Revision != request.ExpectedRevision+1 ||
		value.PlanHash == request.ExpectedPlan.PlanHash || !sameAgentCloudPlanApprovalScope(value, request.ExpectedPlan) {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return value, nil
}

func (runner *Runner) EstablishAgentAWSConnection(ctx context.Context, request cloudmodule.AgentCloudEstablishRequest) (cloudmodule.AgentCloudConnection, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudConnection{}, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !validUUID(request.BootstrapSessionID) || request.ExpectedSessionRevision <= 0 ||
		!validUUID(request.PlanID) || request.ExpectedPlanRevision <= 0 || !validUUID(request.ExpectedConnectionID) ||
		!remoteAWSRegionPattern.MatchString(request.ExpectedRegion) || !validAgentCloudApproval(request.Approval) {
		return cloudmodule.AgentCloudConnection{}, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.EstablishAwsConnection(callContext, &agentv1.EstablishAwsConnectionRequest{
		IdempotencyKey: request.IdempotencyKey, BootstrapSessionId: request.BootstrapSessionID,
		ExpectedSessionRevision: request.ExpectedSessionRevision, PlanId: request.PlanID,
		ExpectedPlanRevision: request.ExpectedPlanRevision, Approval: agentCloudApprovalToProto(request.Approval), OwnerId: runner.ownerID,
	})
	if err != nil {
		return cloudmodule.AgentCloudConnection{}, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetConnection() == nil {
		return cloudmodule.AgentCloudConnection{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return runner.mapAgentCloudConnection(response.GetConnection(), request.ExpectedConnectionID, request.ExpectedRegion)
}

func (runner *Runner) GetAgentCloudConnection(ctx context.Context, request cloudmodule.AgentCloudConnectionRequest) (cloudmodule.AgentCloudConnection, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.AgentCloudConnection{}, false, cloudmodule.ErrAgentCloudControlUnavailable
	}
	if !validUUID(request.ConnectionID) {
		return cloudmodule.AgentCloudConnection{}, false, cloudmodule.ErrAgentCloudControlInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudConnection(callContext, &agentv1.GetCloudConnectionRequest{OwnerId: runner.ownerID, ConnectionId: request.ConnectionID})
	if err != nil {
		if status.Code(err) == codes.NotFound && callContext.Err() == nil {
			return cloudmodule.AgentCloudConnection{}, false, nil
		}
		return cloudmodule.AgentCloudConnection{}, false, mapAgentCloudControlRPCError(callContext, err)
	}
	if response == nil || response.GetConnection() == nil {
		return cloudmodule.AgentCloudConnection{}, false, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	value, mapErr := runner.mapAgentCloudConnection(response.GetConnection(), request.ConnectionID, "")
	if mapErr != nil {
		return cloudmodule.AgentCloudConnection{}, false, mapErr
	}
	return value, true, nil
}

func (runner *Runner) mapAgentCloudPlan(remote *agentv1.CloudPlan, expectedPlanID string) (cloudmodule.AgentCloudPlan, error) {
	if remote == nil || (expectedPlanID != "" && remote.GetPlanId() != expectedPlanID) || remote.GetOwnerId() != runner.ownerID || !validUUID(remote.GetPlanId()) ||
		!validUUID(remote.GetConnectionId()) || !validUUID(remote.GetQuoteId()) || remote.GetRevision() <= 0 ||
		!agentCloudDigestPattern.MatchString(remote.GetQuoteDigest()) || !agentCloudDigestPattern.MatchString(remote.GetQuoteScopeDigest()) ||
		!agentCloudDigestPattern.MatchString(remote.GetPlanHash()) || remote.GetRecipe() == nil || remote.GetResource() == nil || remote.GetNetwork() == nil || remote.GetRetention() == nil {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	quoteValidUntil, err := exactAgentCloudTimestamp(remote.GetQuoteValidUntil())
	if err != nil {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	candidate, ok := agentCloudCandidate(remote.GetCandidateProfile())
	if !ok {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	planStatus, ok := agentCloudPlanStatus(remote.GetStatus())
	if !ok {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	resource, ok := mapAgentCloudResource(remote.GetResource())
	if !ok || resource.Region == "" {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	resource.CandidateProfile = candidate
	network, ok := mapAgentCloudNetwork(remote.GetNetwork())
	if !ok {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	retention, ok := mapAgentCloudRetention(remote.GetRetention())
	if !ok {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	recipe := remote.GetRecipe()
	if !agentCloudIdentifierPattern.MatchString(recipe.GetRecipeId()) || !agentCloudDigestPattern.MatchString(recipe.GetDigest()) || (recipe.GetMaturity() != "experimental" && recipe.GetMaturity() != "managed") {
		return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	secrets := make([]cloudmodule.AgentCloudSecretScope, 0, len(remote.GetSecretScope()))
	for _, value := range remote.GetSecretScope() {
		if value == nil || !agentCloudSecretRefPattern.MatchString(value.GetSecretRef()) || !validAgentCloudText(value.GetPurpose(), 256) || (value.GetDelivery() != "file" && value.GetDelivery() != "environment") {
			return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		secrets = append(secrets, cloudmodule.AgentCloudSecretScope{SecretRef: value.GetSecretRef(), Purpose: value.GetPurpose(), Delivery: value.GetDelivery()})
	}
	integrations := make([]cloudmodule.AgentCloudIntegrationScope, 0, len(remote.GetIntegrationScope()))
	for _, value := range remote.GetIntegrationScope() {
		if value == nil || !validAgentCloudIntegration(value.GetKind()) || !validAgentCloudText(value.GetName(), 160) || !validAgentCloudStringSet(value.GetScopes(), 32) {
			return cloudmodule.AgentCloudPlan{}, cloudmodule.ErrAgentCloudControlInvalidResponse
		}
		integrations = append(integrations, cloudmodule.AgentCloudIntegrationScope{Kind: value.GetKind(), Name: value.GetName(), Scopes: append([]string(nil), value.GetScopes()...)})
	}
	return cloudmodule.AgentCloudPlan{
		PlanID: remote.GetPlanId(), OwnerID: remote.GetOwnerId(), ConnectionID: remote.GetConnectionId(),
		Recipe:  cloudmodule.AgentCloudRecipeBinding{RecipeID: recipe.GetRecipeId(), Digest: recipe.GetDigest(), Maturity: recipe.GetMaturity()},
		QuoteID: remote.GetQuoteId(), QuoteDigest: remote.GetQuoteDigest(), QuoteScopeDigest: remote.GetQuoteScopeDigest(),
		CandidateProfile: candidate, QuoteValidUntil: quoteValidUntil, Resource: resource, Network: network,
		SecretScope: secrets, IntegrationScope: integrations, Retention: retention, Status: planStatus,
		PlanHash: remote.GetPlanHash(), Revision: remote.GetRevision(),
	}, nil
}

func (runner *Runner) mapAgentCloudConnection(remote *agentv1.CloudConnection, expectedID, expectedRegion string) (cloudmodule.AgentCloudConnection, error) {
	if remote == nil || (expectedID != "" && remote.GetConnectionId() != expectedID) || remote.GetOwnerId() != runner.ownerID || !validUUID(remote.GetConnectionId()) ||
		!agentCloudAccountPattern.MatchString(remote.GetAccountId()) || !remoteAWSRegionPattern.MatchString(remote.GetRegion()) ||
		(expectedRegion != "" && remote.GetRegion() != expectedRegion) || !validAgentCloudConnectionARNs(remote) ||
		!agentCloudIdentifierPattern.MatchString(remote.GetStatus()) ||
		remote.GetRevision() <= 0 || remote.GetCredentialGeneration() <= 0 {
		return cloudmodule.AgentCloudConnection{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	createdAt, createErr := exactAgentCloudTimestamp(remote.GetCreatedAt())
	updatedAt, updateErr := exactAgentCloudTimestamp(remote.GetUpdatedAt())
	if createErr != nil || updateErr != nil || updatedAt.Before(createdAt) {
		return cloudmodule.AgentCloudConnection{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return cloudmodule.AgentCloudConnection{ConnectionID: remote.GetConnectionId(), OwnerID: remote.GetOwnerId(), AccountID: remote.GetAccountId(), Region: remote.GetRegion(), ControlRoleARN: remote.GetControlRoleArn(), FoundationStackID: remote.GetFoundationStackId(), Status: remote.GetStatus(), Revision: remote.GetRevision(), CredentialGeneration: remote.GetCredentialGeneration(), CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
}

func validAgentCloudApproval(value cloudmodule.AgentCloudApprovalSignature) bool {
	return validUUID(value.ApprovalID) && agentCloudIdentifierPattern.MatchString(value.ChallengeID) && agentCloudIdentifierPattern.MatchString(value.SignerKeyID) &&
		value.ExpiresAt.Location() == time.UTC && value.ExpiresAt.Unix() > 0 && len(value.Signature) == ed25519.SignatureSize
}

func agentCloudApprovalToProto(value cloudmodule.AgentCloudApprovalSignature) *agentv1.DeviceApprovalSignature {
	return &agentv1.DeviceApprovalSignature{ApprovalId: value.ApprovalID, ChallengeId: value.ChallengeID, SignerKeyId: value.SignerKeyID, ExpiresAt: timestamppb.New(value.ExpiresAt), Signature: append([]byte(nil), value.Signature...)}
}

func exactAgentCloudTimestamp(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || value.CheckValid() != nil {
		return time.Time{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	result := value.AsTime().UTC()
	if result.Unix() <= 0 {
		return time.Time{}, cloudmodule.ErrAgentCloudControlInvalidResponse
	}
	return result, nil
}

func mapAgentCloudControlRPCError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return cloudmodule.ErrAgentCloudControlUnavailable
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return cloudmodule.ErrAgentCloudControlInvalid
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition, codes.NotFound:
		return cloudmodule.ErrAgentCloudControlConflict
	case codes.PermissionDenied:
		return cloudmodule.ErrAgentCloudControlRejected
	default:
		return cloudmodule.ErrAgentCloudControlUnavailable
	}
}

func agentCloudCandidate(value agentv1.CloudCandidateProfile) (string, bool) {
	switch value {
	case agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_ECONOMY:
		return "economic", true
	case agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_RECOMMENDED:
		return "recommended", true
	case agentv1.CloudCandidateProfile_CLOUD_CANDIDATE_PROFILE_PERFORMANCE:
		return "performance", true
	}
	return "", false
}
func agentCloudPlanStatus(value agentv1.CloudPlanStatus) (string, bool) {
	switch value {
	case agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_RESEARCHING:
		return "researching", true
	case agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_QUOTING:
		return "quoting", true
	case agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_READY_FOR_CONFIRMATION:
		return "ready_for_confirmation", true
	case agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_APPROVED:
		return "approved", true
	case agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_EXPIRED:
		return "expired", true
	case agentv1.CloudPlanStatus_CLOUD_PLAN_STATUS_SUPERSEDED:
		return "superseded", true
	}
	return "", false
}

func mapAgentCloudResource(value *agentv1.CloudResourceScope) (cloudmodule.AgentCloudResourceScope, bool) {
	purchase := ""
	switch value.GetPurchaseOption() {
	case agentv1.CloudPurchaseOption_CLOUD_PURCHASE_OPTION_ON_DEMAND:
		purchase = "on_demand"
	case agentv1.CloudPurchaseOption_CLOUD_PURCHASE_OPTION_SPOT:
		purchase = "spot"
	default:
		return cloudmodule.AgentCloudResourceScope{}, false
	}
	if !remoteAWSRegionPattern.MatchString(value.GetRegion()) || len(value.GetAvailabilityZones()) == 0 || len(value.GetAvailabilityZones()) > 16 ||
		!agentCloudInstancePattern.MatchString(value.GetInstanceType()) || value.GetInstanceCount() != 1 || value.GetVcpu() == 0 || value.GetVcpu() > 1024 ||
		value.GetMemoryMib() == 0 || value.GetMemoryMib() > 64*1024*1024 || value.GetDiskGib() == 0 || value.GetDiskGib() > 64*1024 ||
		value.GetVolumeType() == "" || !value.GetVolumeEncrypted() || !agentCloudAMIPattern.MatchString(value.GetWorkerImageId()) ||
		!agentCloudDigestPattern.MatchString(value.GetWorkerImageDigest()) {
		return cloudmodule.AgentCloudResourceScope{}, false
	}
	seenZones := make(map[string]struct{}, len(value.GetAvailabilityZones()))
	for _, zone := range value.GetAvailabilityZones() {
		if !agentCloudAZPattern.MatchString(zone) || !strings.HasPrefix(zone, value.GetRegion()) {
			return cloudmodule.AgentCloudResourceScope{}, false
		}
		if _, exists := seenZones[zone]; exists {
			return cloudmodule.AgentCloudResourceScope{}, false
		}
		seenZones[zone] = struct{}{}
	}
	if value.GetArchitecture() != "amd64" && value.GetArchitecture() != "arm64" {
		return cloudmodule.AgentCloudResourceScope{}, false
	}
	if (value.GetGpuCount() == 0 && (value.GetGpuType() != "" || value.GetGpuMemoryMib() != 0)) ||
		(value.GetGpuCount() > 0 && (value.GetGpuCount() > 64 || value.GetGpuType() == "" || value.GetGpuMemoryMib() == 0)) {
		return cloudmodule.AgentCloudResourceScope{}, false
	}
	return cloudmodule.AgentCloudResourceScope{Region: value.GetRegion(), AvailabilityZones: append([]string(nil), value.GetAvailabilityZones()...), InstanceType: value.GetInstanceType(), InstanceCount: value.GetInstanceCount(), Architecture: value.GetArchitecture(), VCPU: value.GetVcpu(), MemoryMiB: value.GetMemoryMib(), GPUType: value.GetGpuType(), GPUCount: value.GetGpuCount(), GPUMemoryMiB: value.GetGpuMemoryMib(), DiskGiB: value.GetDiskGib(), VolumeType: value.GetVolumeType(), VolumeIOPS: value.GetVolumeIops(), VolumeThroughputMiBPS: value.GetVolumeThroughputMibps(), VolumeEncrypted: value.GetVolumeEncrypted(), PurchaseOption: purchase, WorkerImageID: value.GetWorkerImageId(), WorkerImageDigest: value.GetWorkerImageDigest()}, true
}

func mapAgentCloudNetwork(value *agentv1.CloudNetworkScope) (cloudmodule.AgentCloudNetworkScope, bool) {
	if value == nil || !agentCloudVPCPattern.MatchString(value.GetVpcId()) || !agentCloudSubnetPattern.MatchString(value.GetSubnetId()) {
		return cloudmodule.AgentCloudNetworkScope{}, false
	}
	securityGroupMode := ""
	switch value.GetSecurityGroupMode() {
	case agentv1.CloudSecurityGroupMode_CLOUD_SECURITY_GROUP_MODE_UNSPECIFIED:
		if value.GetSecurityGroupId() == "" {
			return cloudmodule.AgentCloudNetworkScope{}, false
		}
		securityGroupMode = "existing"
	case agentv1.CloudSecurityGroupMode_CLOUD_SECURITY_GROUP_MODE_EXISTING:
		securityGroupMode = "existing"
	case agentv1.CloudSecurityGroupMode_CLOUD_SECURITY_GROUP_MODE_CREATE_DEDICATED:
		securityGroupMode = "create_dedicated"
	default:
		return cloudmodule.AgentCloudNetworkScope{}, false
	}
	if (securityGroupMode == "existing" && !agentCloudSGPattern.MatchString(value.GetSecurityGroupId())) ||
		(securityGroupMode == "create_dedicated" && value.GetSecurityGroupId() != "") {
		return cloudmodule.AgentCloudNetworkScope{}, false
	}
	entry := ""
	switch value.GetEntryPoint() {
	case agentv1.CloudEntryPointKind_CLOUD_ENTRY_POINT_KIND_NONE:
		entry = "none"
	case agentv1.CloudEntryPointKind_CLOUD_ENTRY_POINT_KIND_ALB:
		entry = "alb"
	case agentv1.CloudEntryPointKind_CLOUD_ENTRY_POINT_KIND_CLOUDFRONT:
		entry = "cloudfront"
	default:
		return cloudmodule.AgentCloudNetworkScope{}, false
	}
	if entry == "none" && (value.GetPublicExposure() || len(value.GetIngressPorts()) != 0 || value.GetHostname() != "" || value.GetTlsRequired() || value.GetAuthenticationRequired()) {
		return cloudmodule.AgentCloudNetworkScope{}, false
	}
	if entry != "none" && (!value.GetPublicExposure() || len(value.GetIngressPorts()) == 0 || !value.GetTlsRequired() || !value.GetAuthenticationRequired()) {
		return cloudmodule.AgentCloudNetworkScope{}, false
	}
	seenPorts := make(map[uint32]struct{}, len(value.GetIngressPorts()))
	for _, port := range value.GetIngressPorts() {
		if port == 0 || port > 65535 {
			return cloudmodule.AgentCloudNetworkScope{}, false
		}
		if _, exists := seenPorts[port]; exists {
			return cloudmodule.AgentCloudNetworkScope{}, false
		}
		seenPorts[port] = struct{}{}
	}
	if entry != "none" && !validAgentCloudText(value.GetHostname(), 253) {
		return cloudmodule.AgentCloudNetworkScope{}, false
	}
	return cloudmodule.AgentCloudNetworkScope{VPCID: value.GetVpcId(), SubnetID: value.GetSubnetId(), SecurityGroupID: value.GetSecurityGroupId(), SecurityGroupMode: securityGroupMode, EntryPoint: entry, PublicIPv4: value.GetPublicIpv4(), PublicExposure: value.GetPublicExposure(), IngressPorts: append([]uint32(nil), value.GetIngressPorts()...), Hostname: value.GetHostname(), TLSRequired: value.GetTlsRequired(), AuthenticationRequired: value.GetAuthenticationRequired()}, true
}

func mapAgentCloudRetention(value *agentv1.CloudRetentionScope) (cloudmodule.AgentCloudRetentionScope, bool) {
	class := ""
	switch value.GetRetentionClass() {
	case agentv1.CloudRetentionClass_CLOUD_RETENTION_CLASS_EPHEMERAL:
		class = "ephemeral"
		if !value.GetAutoDestroy() || value.GetGracePeriodSeconds() == 0 || value.GetMaxLifetimeSeconds() == 0 || uint64(value.GetGracePeriodSeconds()) > value.GetMaxLifetimeSeconds() || value.GetMaxLifetimeSeconds() > 365*24*60*60 {
			return cloudmodule.AgentCloudRetentionScope{}, false
		}
	case agentv1.CloudRetentionClass_CLOUD_RETENTION_CLASS_MANAGED:
		class = "managed"
		if value.GetAutoDestroy() || value.GetGracePeriodSeconds() != 0 || value.GetMaxLifetimeSeconds() != 0 {
			return cloudmodule.AgentCloudRetentionScope{}, false
		}
	default:
		return cloudmodule.AgentCloudRetentionScope{}, false
	}
	return cloudmodule.AgentCloudRetentionScope{Class: class, AutoDestroy: value.GetAutoDestroy(), GracePeriodSeconds: value.GetGracePeriodSeconds(), MaxLifetimeSeconds: value.GetMaxLifetimeSeconds()}, true
}

func validAgentCloudIntegration(value string) bool {
	switch value {
	case "mcp", "acp", "grpc", "web":
		return true
	}
	return false
}

func sameExpectedPlanRequest(plan cloudmodule.AgentCloudPlan, planID string, revision int64, owner, status string) bool {
	return plan.PlanID == planID && plan.OwnerID == owner && plan.Revision == revision && plan.Status == status && validUUID(plan.ConnectionID) &&
		agentCloudDigestPattern.MatchString(plan.PlanHash) && validAgentCloudPlanDomain(plan)
}

// sameAgentCloudPlanApprovalScope compares every approval-bound field except
// PlanHash. PlanHash includes revision, so the durable approved projection
// must have a different hash after the single revision increment.
func sameAgentCloudPlanApprovalScope(left, right cloudmodule.AgentCloudPlan) bool {
	return left.PlanID == right.PlanID && left.OwnerID == right.OwnerID && left.ConnectionID == right.ConnectionID &&
		left.QuoteID == right.QuoteID && left.QuoteDigest == right.QuoteDigest &&
		left.QuoteScopeDigest == right.QuoteScopeDigest && left.CandidateProfile == right.CandidateProfile &&
		left.Recipe == right.Recipe && left.QuoteValidUntil.Equal(right.QuoteValidUntil) &&
		reflect.DeepEqual(left.Resource, right.Resource) && reflect.DeepEqual(left.Network, right.Network) &&
		reflect.DeepEqual(left.SecretScope, right.SecretScope) && reflect.DeepEqual(left.IntegrationScope, right.IntegrationScope) &&
		left.Retention == right.Retention
}

func validAgentCloudConnectionARNs(value *agentv1.CloudConnection) bool {
	role := agentCloudRoleARNPattern.FindStringSubmatch(value.GetControlRoleArn())
	stack := agentCloudStackARNPattern.FindStringSubmatch(value.GetFoundationStackId())
	if role == nil || stack == nil || role[2] != value.GetAccountId() || stack[3] != value.GetAccountId() ||
		stack[2] != value.GetRegion() || role[1] != stack[1] {
		return false
	}
	switch role[1] {
	case "aws-cn":
		return strings.HasPrefix(value.GetRegion(), "cn-")
	case "aws-us-gov":
		return strings.Contains(value.GetRegion(), "-gov-")
	default:
		return !strings.HasPrefix(value.GetRegion(), "cn-") && !strings.Contains(value.GetRegion(), "-gov-")
	}
}

func validAgentCloudPlanDomain(value cloudmodule.AgentCloudPlan) bool {
	if !validUUID(value.PlanID) || !validUUID(value.ConnectionID) || !validUUID(value.QuoteID) || !agentCloudIdentifierPattern.MatchString(value.OwnerID) ||
		!agentCloudIdentifierPattern.MatchString(value.Recipe.RecipeID) || !agentCloudDigestPattern.MatchString(value.Recipe.Digest) ||
		(value.Recipe.Maturity != "experimental" && value.Recipe.Maturity != "managed") || !agentCloudDigestPattern.MatchString(value.QuoteDigest) ||
		!agentCloudDigestPattern.MatchString(value.QuoteScopeDigest) || !agentCloudDigestPattern.MatchString(value.PlanHash) || value.Revision <= 0 ||
		value.QuoteValidUntil.Location() != time.UTC || value.QuoteValidUntil.Unix() <= 0 {
		return false
	}
	if value.CandidateProfile != "economic" && value.CandidateProfile != "recommended" && value.CandidateProfile != "performance" {
		return false
	}
	resource := value.Resource
	resource.CandidateProfile = value.CandidateProfile
	if _, ok := agentCloudResourceToProto(resource, false); !ok {
		return false
	}
	if _, ok := agentCloudNetworkToProto(value.Network); !ok {
		return false
	}
	if _, ok := agentCloudRetentionToProto(value.Retention); !ok {
		return false
	}
	for _, secret := range value.SecretScope {
		if !agentCloudSecretRefPattern.MatchString(secret.SecretRef) || !validAgentCloudText(secret.Purpose, 256) || (secret.Delivery != "file" && secret.Delivery != "environment") {
			return false
		}
	}
	for _, integration := range value.IntegrationScope {
		if !validAgentCloudIntegration(integration.Kind) || !validAgentCloudText(integration.Name, 160) || !validAgentCloudStringSet(integration.Scopes, 32) {
			return false
		}
	}
	return true
}

func validAgentCloudText(value string, maximum int) bool {
	return value == strings.TrimSpace(value) && value != "" && len(value) <= maximum &&
		!cloudmodule.ContainsSensitiveGoalMaterial(value)
}
func validAgentCloudStringSet(values []string, maximum int) bool {
	if len(values) > maximum {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validAgentCloudText(value, 128) {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}
