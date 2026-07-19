package agentgrpc

import (
	"context"
	"errors"
	"strings"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	cloudDeploymentPageSize = int32(100)
	maxCloudDeploymentPages = 16
	maxCloudDeployments     = 1000
)

// ListCloudDeployments adapts the paginated Agent service into ProductCore's
// existing complete-array response. Traversal is bounded and rejects cyclic
// cursors so a compromised or faulty peer cannot hold the request indefinitely.
func (runner *Runner) ListCloudDeployments(ctx context.Context) ([]cloudmodule.Deployment, error) {
	if runner == nil || runner.cloud == nil {
		return nil, errors.New("agent service client is unavailable")
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()

	items := make([]cloudmodule.Deployment, 0)
	seenTokens := make(map[string]struct{})
	seenDeployments := make(map[string]struct{})
	pageToken := ""
	for page := 0; page < maxCloudDeploymentPages; page++ {
		response, err := runner.cloud.ListCloudDeployments(callContext, &agentv1.ListCloudDeploymentsRequest{
			OwnerId: runner.ownerID, PageSize: cloudDeploymentPageSize, PageToken: pageToken,
		})
		if err != nil {
			return nil, sanitizeRPCError(callContext, err)
		}
		if response == nil || len(items)+len(response.GetDeployments()) > maxCloudDeployments {
			return nil, errors.New("agent service returned an invalid cloud deployment response")
		}
		for _, remote := range response.GetDeployments() {
			item, mapErr := runner.mapCloudDeployment(remote)
			if mapErr != nil {
				return nil, mapErr
			}
			if _, duplicate := seenDeployments[item.DeploymentID]; duplicate {
				return nil, errors.New("agent service returned an invalid cloud deployment response")
			}
			seenDeployments[item.DeploymentID] = struct{}{}
			items = append(items, item)
		}
		next := strings.TrimSpace(response.GetNextPageToken())
		if next == "" {
			return items, nil
		}
		if next == pageToken {
			return nil, errors.New("agent service returned an invalid cloud deployment cursor")
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return nil, errors.New("agent service returned an invalid cloud deployment cursor")
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}
	return nil, errors.New("agent service returned too many cloud deployment pages")
}

// GetCloudDeployment reads one owner-bound deployment. NotFound is represented
// through the existing Store-style bool so the Cloud module preserves its
// stable ProductCore 404 envelope.
func (runner *Runner) GetCloudDeployment(ctx context.Context, deploymentID string) (cloudmodule.Deployment, bool, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.Deployment{}, false, errors.New("agent service client is unavailable")
	}
	deploymentID = strings.TrimSpace(deploymentID)
	if deploymentID == "" {
		return cloudmodule.Deployment{}, false, errors.New("cloud deployment id is required")
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.GetCloudDeployment(callContext, &agentv1.GetCloudDeploymentRequest{
		OwnerId: runner.ownerID, DeploymentId: deploymentID,
	})
	if err != nil {
		if callContext.Err() == nil && status.Code(err) == codes.NotFound {
			return cloudmodule.Deployment{}, false, nil
		}
		return cloudmodule.Deployment{}, false, sanitizeRPCError(callContext, err)
	}
	if response == nil || response.GetDeployment() == nil || response.GetDeployment().GetDeploymentId() != deploymentID {
		return cloudmodule.Deployment{}, false, errors.New("agent service returned an invalid cloud deployment response")
	}
	item, err := runner.mapCloudDeployment(response.GetDeployment())
	if err != nil {
		return cloudmodule.Deployment{}, false, err
	}
	return item, true, nil
}

func (runner *Runner) mapCloudDeployment(remote *agentv1.CloudDeployment) (cloudmodule.Deployment, error) {
	if remote == nil || !validUUID(remote.GetDeploymentId()) || remote.GetOwnerId() != runner.ownerID ||
		!validUUID(remote.GetPlanId()) || !validUUID(remote.GetConnectionId()) ||
		remote.GetResources() == nil || remote.GetRevision() <= 0 {
		return cloudmodule.Deployment{}, errors.New("agent service returned an invalid cloud deployment response")
	}
	execution, ok := cloudDeploymentExecution(remote.GetExecutionStatus())
	if !ok {
		return cloudmodule.Deployment{}, errors.New("agent service returned an invalid cloud deployment response")
	}
	outcome, ok := cloudDeploymentOutcome(remote.GetOutcomeStatus())
	if !ok {
		return cloudmodule.Deployment{}, errors.New("agent service returned an invalid cloud deployment response")
	}
	resourceStatus, ok := cloudDeploymentResource(remote.GetResources().GetStatus())
	if !ok {
		return cloudmodule.Deployment{}, errors.New("agent service returned an invalid cloud deployment response")
	}
	createdAt, err := timestampMillis(remote.GetCreatedAt())
	if err != nil {
		return cloudmodule.Deployment{}, err
	}
	updatedAt, err := timestampMillis(remote.GetUpdatedAt())
	if err != nil || updatedAt < createdAt {
		return cloudmodule.Deployment{}, errors.New("agent service returned an invalid cloud deployment response")
	}
	health, err := mapCloudDeploymentHealth(remote.GetHealth())
	if err != nil {
		return cloudmodule.Deployment{}, err
	}
	return cloudmodule.Deployment{
		DeploymentID: remote.GetDeploymentId(), PlanID: remote.GetPlanId(), ConnectionID: remote.GetConnectionId(),
		Execution: execution, Outcome: outcome, Resource: resourceStatus, Revision: remote.GetRevision(),
		Health: health, CreatedAt: createdAt, UpdatedAt: updatedAt,
	}, nil
}

func mapCloudDeploymentHealth(remote *agentv1.CloudHealthSummary) (*cloudmodule.DeploymentHealthSummary, error) {
	unknown := &cloudmodule.DeploymentHealthSummary{
		Status: "unknown", EvidenceType: "none", ProbeCounts: []cloudmodule.DeploymentHealthProbeCount{},
	}
	// Older Agent peers cannot send field 14. Treat absence exactly like the
	// current Agent's explicit no-monitor summary instead of fabricating a
	// successful observation.
	if remote == nil {
		return unknown, nil
	}
	status, ok := cloudDeploymentHealthStatus(remote.GetStatus())
	if !ok {
		return nil, errors.New("agent service returned an invalid cloud deployment response")
	}
	evidenceType, ok := cloudDeploymentHealthEvidence(remote.GetEvidenceType())
	if !ok {
		return nil, errors.New("agent service returned an invalid cloud deployment response")
	}
	result := &cloudmodule.DeploymentHealthSummary{
		Status: status, Revision: remote.GetRevision(), ProbeCount: remote.GetProbeCount(),
		ProbeCounts: make([]cloudmodule.DeploymentHealthProbeCount, 0, len(remote.GetProbeCounts())),
		ExternalEvidenceDigest: remote.GetExternalEvidenceDigest(), EvidenceType: evidenceType,
	}
	if status == "unknown" {
		if result.Revision != 0 || remote.GetObservedAt() != nil || remote.GetNextDueAt() != nil || result.ProbeCount != 0 ||
			len(remote.GetProbeCounts()) != 0 || result.ExternalEvidenceDigest != "" || evidenceType != "none" {
			return nil, errors.New("agent service returned an invalid cloud deployment response")
		}
		return result, nil
	}
	if result.Revision <= 0 || result.ProbeCount == 0 || remote.GetNextDueAt() == nil {
		return nil, errors.New("agent service returned an invalid cloud deployment response")
	}
	nextDueAt, err := timestampMillis(remote.GetNextDueAt())
	if err != nil {
		return nil, errors.New("agent service returned an invalid cloud deployment response")
	}
	result.NextDueAt = nextDueAt
	seen := make(map[string]struct{}, len(remote.GetProbeCounts()))
	var total uint64
	for _, count := range remote.GetProbeCounts() {
		if count == nil || count.GetCount() == 0 {
			return nil, errors.New("agent service returned an invalid cloud deployment response")
		}
		kind, valid := cloudDeploymentHealthProbeKind(count.GetKind())
		if !valid {
			return nil, errors.New("agent service returned an invalid cloud deployment response")
		}
		if _, duplicate := seen[kind]; duplicate {
			return nil, errors.New("agent service returned an invalid cloud deployment response")
		}
		seen[kind] = struct{}{}
		total += uint64(count.GetCount())
		result.ProbeCounts = append(result.ProbeCounts, cloudmodule.DeploymentHealthProbeCount{Kind: kind, Count: count.GetCount()})
	}
	if total != uint64(result.ProbeCount) {
		return nil, errors.New("agent service returned an invalid cloud deployment response")
	}
	if status == "pending" {
		if remote.GetObservedAt() != nil || result.ExternalEvidenceDigest != "" || evidenceType != "none" {
			return nil, errors.New("agent service returned an invalid cloud deployment response")
		}
		return result, nil
	}
	if remote.GetObservedAt() == nil || evidenceType != "independent_external" || !agentCloudDigestPattern.MatchString(result.ExternalEvidenceDigest) {
		return nil, errors.New("agent service returned an invalid cloud deployment response")
	}
	observedAt, err := timestampMillis(remote.GetObservedAt())
	if err != nil || nextDueAt < observedAt {
		return nil, errors.New("agent service returned an invalid cloud deployment response")
	}
	result.ObservedAt = observedAt
	return result, nil
}

func cloudDeploymentHealthStatus(value agentv1.CloudHealthStatus) (string, bool) {
	switch value {
	case agentv1.CloudHealthStatus_CLOUD_HEALTH_STATUS_UNKNOWN:
		return "unknown", true
	case agentv1.CloudHealthStatus_CLOUD_HEALTH_STATUS_PENDING:
		return "pending", true
	case agentv1.CloudHealthStatus_CLOUD_HEALTH_STATUS_HEALTHY:
		return "healthy", true
	case agentv1.CloudHealthStatus_CLOUD_HEALTH_STATUS_DEGRADED:
		return "degraded", true
	case agentv1.CloudHealthStatus_CLOUD_HEALTH_STATUS_UNHEALTHY:
		return "unhealthy", true
	case agentv1.CloudHealthStatus_CLOUD_HEALTH_STATUS_CANCELED:
		return "canceled", true
	default:
		return "", false
	}
}

func cloudDeploymentHealthProbeKind(value agentv1.CloudHealthProbeKind) (string, bool) {
	switch value {
	case agentv1.CloudHealthProbeKind_CLOUD_HEALTH_PROBE_KIND_LIVENESS:
		return "liveness", true
	case agentv1.CloudHealthProbeKind_CLOUD_HEALTH_PROBE_KIND_READINESS:
		return "readiness", true
	case agentv1.CloudHealthProbeKind_CLOUD_HEALTH_PROBE_KIND_SEMANTIC:
		return "semantic", true
	default:
		return "", false
	}
}

func cloudDeploymentHealthEvidence(value agentv1.CloudHealthEvidenceType) (string, bool) {
	switch value {
	case agentv1.CloudHealthEvidenceType_CLOUD_HEALTH_EVIDENCE_TYPE_NONE:
		return "none", true
	case agentv1.CloudHealthEvidenceType_CLOUD_HEALTH_EVIDENCE_TYPE_INDEPENDENT_EXTERNAL:
		return "independent_external", true
	default:
		return "", false
	}
}

func validUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

func cloudDeploymentExecution(value agentv1.ExecutionStatus) (string, bool) {
	switch value {
	case agentv1.ExecutionStatus_EXECUTION_STATUS_QUEUED:
		return "queued", true
	case agentv1.ExecutionStatus_EXECUTION_STATUS_RUNNING:
		return "running", true
	case agentv1.ExecutionStatus_EXECUTION_STATUS_WAITING_USER:
		return "waiting_user", true
	case agentv1.ExecutionStatus_EXECUTION_STATUS_VERIFYING:
		return "verifying", true
	case agentv1.ExecutionStatus_EXECUTION_STATUS_FINISHED:
		return "finished", true
	default:
		return "", false
	}
}

func cloudDeploymentOutcome(value agentv1.OutcomeStatus) (string, bool) {
	switch value {
	case agentv1.OutcomeStatus_OUTCOME_STATUS_PENDING:
		return "pending", true
	case agentv1.OutcomeStatus_OUTCOME_STATUS_SUCCEEDED:
		return "succeeded", true
	case agentv1.OutcomeStatus_OUTCOME_STATUS_FAILED:
		return "failed", true
	case agentv1.OutcomeStatus_OUTCOME_STATUS_CANCELED:
		return "canceled", true
	case agentv1.OutcomeStatus_OUTCOME_STATUS_TIMED_OUT:
		return "timed_out", true
	case agentv1.OutcomeStatus_OUTCOME_STATUS_INTERRUPTED:
		return "interrupted", true
	default:
		return "", false
	}
}

func cloudDeploymentResource(value agentv1.CloudResourceStatus) (string, bool) {
	switch value {
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_NONE:
		return "none", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_PROVISIONING:
		return "provisioning", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_ACTIVE:
		return "active", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_DESTROY_SCHEDULED:
		return "destroy_scheduled", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_RETAINED_MANAGED:
		return "retained_managed", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_DESTROYING:
		return "destroying", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_VERIFIED_DESTROYED:
		return "verified_destroyed", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_DESTROY_BLOCKED:
		return "destroy_blocked", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_ORPHANED:
		return "orphaned", true
	case agentv1.CloudResourceStatus_CLOUD_RESOURCE_STATUS_MIXED:
		return "mixed", true
	default:
		return "", false
	}
}

func timestampMillis(value *timestamppb.Timestamp) (int64, error) {
	if value == nil || value.CheckValid() != nil {
		return 0, errors.New("agent service returned an invalid cloud deployment response")
	}
	millis := value.AsTime().UnixMilli()
	if millis <= 0 {
		return 0, errors.New("agent service returned an invalid cloud deployment response")
	}
	return millis, nil
}
