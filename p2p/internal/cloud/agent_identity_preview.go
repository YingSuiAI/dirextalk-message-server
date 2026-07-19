package cloud

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

var (
	ErrIdentityPreviewInvalid         = errors.New("AWS identity preview request is invalid")
	ErrIdentityPreviewConflict        = errors.New("AWS identity preview conflicts with current bootstrap state")
	ErrIdentityPreviewRejected        = errors.New("AWS identity preview was rejected")
	ErrIdentityPreviewUnavailable     = errors.New("AWS identity preview is unavailable")
	ErrIdentityPreviewInvalidResponse = errors.New("AWS identity preview returned an invalid response")

	awsAccountIDPattern    = regexp.MustCompile(`^[0-9]{12}$`)
	awsPrincipalIDPattern  = regexp.MustCompile(`^[A-Za-z0-9+=,.@_:/-]{1,256}$`)
	awsPrincipalARNPattern = regexp.MustCompile(`^arn:(aws|aws-cn|aws-us-gov):(iam|sts)::([0-9]{12}):([^\x00-\x20\x7f]{1,384})$`)
)

// IdentityPreviewRequest is derived entirely from an owner-scoped durable
// role plan. Owner, target and Region are never accepted from ProductCore.
type IdentityPreviewRequest struct {
	BootstrapSessionID      string
	ExpectedSessionRevision int64
	TargetID                string
	Region                  string
}

// IdentityPreviewEvidence is a short-lived, persisted Agent observation. It
// proves only GetCallerIdentity; it does not establish an active connection.
type IdentityPreviewEvidence struct {
	BootstrapSessionID string
	SessionRevision    int64
	OwnerID            string
	TargetID           string
	AccountID          string
	PrincipalARN       string
	PrincipalID        string
	Region             string
	RootIdentity       bool
	ObservedAt         string
	ExpiresAt          string
}

type IdentityPreviewClient interface {
	PreviewAgentAWSIdentity(context.Context, IdentityPreviewRequest) (IdentityPreviewEvidence, error)
}

func (m *Module) previewConnectionIdentity(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "bootstrap_id", "expected_revision", "lifecycle_action", "cloud_connection_id", "expected_connection_revision", "session_id", "expected_session_revision"); err != nil {
		return nil, err
	}
	values := actionbase.Params(params)
	_, hasBootstrapID := params["bootstrap_id"]
	_, hasExpectedRevision := params["expected_revision"]
	_, hasLifecycleAction := params["lifecycle_action"]
	_, hasConnectionID := params["cloud_connection_id"]
	_, hasConnectionRevision := params["expected_connection_revision"]
	action := values.String("lifecycle_action")
	rolePlanShape := hasBootstrapID || hasExpectedRevision
	connectionShape := hasConnectionID || hasConnectionRevision
	if (!hasLifecycleAction && (!hasBootstrapID || !hasExpectedRevision || connectionShape)) ||
		(hasLifecycleAction && action == "establish" && (!hasBootstrapID || !hasExpectedRevision || connectionShape)) ||
		(hasLifecycleAction && action != "establish" && (rolePlanShape || !hasConnectionID || !hasConnectionRevision)) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdentityPreviewInvalidCode, "cloud connection identity preview request is invalid")
	}
	if hasLifecycleAction && action != "establish" {
		return m.previewAgentFoundationIdentity(ctx, action, values.String("cloud_connection_id"),
			values.Int64("expected_connection_revision"), values.String("session_id"), values.Int64("expected_session_revision"))
	}
	if m == nil || m.store == nil || m.cfg.IdentityPreviewClient == nil {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudIdentityPreviewUnavailableCode, "cloud connection identity preview is unavailable")
	}
	store, ok := m.store.(ConnectionCredentialBootstrapStore)
	if !ok {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudIdentityPreviewUnavailableCode, "cloud connection identity preview is unavailable")
	}
	bootstrapID := values.String("bootstrap_id")
	expectedRevision := values.Int64("expected_revision")
	sessionID := values.String("session_id")
	expectedSessionRevision := values.Int64("expected_session_revision")
	if !cloudIdentifierPattern.MatchString(bootstrapID) || expectedRevision <= 0 || !canonicalUUID(sessionID) || expectedSessionRevision <= 0 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdentityPreviewInvalidCode, "cloud connection identity preview request is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC()
	load := LoadConnectionCredentialBootstrapRequest{
		OwnerMXID: ownerMXID, BootstrapID: bootstrapID, ExpectedRevision: expectedRevision, Now: now.UnixMilli(),
	}
	rolePlan, err := store.LoadCloudConnectionCredentialBootstrap(ctx, load)
	if err != nil {
		return nil, connectionCredentialBootstrapStoreError(err)
	}
	if rolePlan.Provider != "aws" || !rolePlan.AllowRootCredentialBootstrap || !cloudRegionPattern.MatchString(rolePlan.Region) ||
		!cloudIdentifierPattern.MatchString(rolePlan.CloudConnectionID) || ContainsSensitiveGoalMaterial(rolePlan.CloudConnectionID) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudIdentityPreviewConflictCode, "cloud connection role plan is incompatible with identity preview")
	}
	evidence, err := m.cfg.IdentityPreviewClient.PreviewAgentAWSIdentity(ctx, IdentityPreviewRequest{
		BootstrapSessionID: sessionID, ExpectedSessionRevision: expectedSessionRevision,
		TargetID: rolePlan.CloudConnectionID, Region: rolePlan.Region,
	})
	if err != nil {
		return nil, identityPreviewError(err)
	}
	if validateIdentityPreviewEvidence(evidence, sessionID, expectedSessionRevision, rolePlan.CloudConnectionID, "", rolePlan.Region, now) != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudIdentityPreviewUpstreamCode, "cloud connection identity preview returned an invalid response")
	}
	// Re-read after the network call. A concurrent role-plan revision or expiry
	// must not expose identity evidence bound to an obsolete connection target.
	load.Now = m.now().UTC().UnixMilli()
	if _, err = store.LoadCloudConnectionCredentialBootstrap(ctx, load); err != nil {
		return nil, connectionCredentialBootstrapStoreError(err)
	}
	result := map[string]any{
		"identity": map[string]any{
			"account_id": evidence.AccountID, "principal_arn": evidence.PrincipalARN,
			"principal_id": evidence.PrincipalID, "region": evidence.Region, "root_identity": evidence.RootIdentity,
		},
		"cloud_connection_id": rolePlan.CloudConnectionID, "bootstrap_session_id": evidence.BootstrapSessionID,
		"session_revision": evidence.SessionRevision, "verification_status": "identity_verified",
		"observed_at": evidence.ObservedAt, "expires_at": evidence.ExpiresAt,
	}
	if hasLifecycleAction {
		result["lifecycle_action"] = "establish"
		result["connection_revision"] = int64(0)
	}
	return result, nil
}

func (m *Module) previewAgentFoundationIdentity(
	ctx context.Context,
	action, connectionID string,
	expectedConnectionRevision int64,
	sessionID string,
	expectedSessionRevision int64,
) (any, *actionbase.Error) {
	if m == nil || m.cfg.IdentityPreviewClient == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudIdentityPreviewUnavailableCode, "cloud connection identity preview is unavailable")
	}
	if !validAgentFoundationExistingConnectionAction(action) || !canonicalUUID(connectionID) || expectedConnectionRevision <= 0 ||
		!canonicalUUID(sessionID) || expectedSessionRevision <= 0 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdentityPreviewInvalidCode, "cloud connection identity preview request is invalid")
	}
	connection, apiErr := m.loadAgentFoundationConnection(ctx, action, connectionID, expectedConnectionRevision)
	if apiErr != nil {
		return nil, apiErr
	}
	now := m.now().UTC()
	evidence, err := m.cfg.IdentityPreviewClient.PreviewAgentAWSIdentity(ctx, IdentityPreviewRequest{
		BootstrapSessionID: sessionID, ExpectedSessionRevision: expectedSessionRevision,
		TargetID: connection.ConnectionID, Region: connection.Region,
	})
	if err != nil {
		return nil, identityPreviewError(err)
	}
	if validateIdentityPreviewEvidence(evidence, sessionID, expectedSessionRevision, connection.ConnectionID, connection.AccountID, connection.Region, now) != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudIdentityPreviewUpstreamCode, "cloud connection identity preview returned an invalid response")
	}
	if _, apiErr = m.loadAgentFoundationConnection(ctx, action, connectionID, expectedConnectionRevision); apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"identity": map[string]any{
			"account_id": evidence.AccountID, "principal_arn": evidence.PrincipalARN,
			"principal_id": evidence.PrincipalID, "region": evidence.Region, "root_identity": evidence.RootIdentity,
		},
		"lifecycle_action": action, "cloud_connection_id": connection.ConnectionID, "connection_revision": connection.Revision,
		"bootstrap_session_id": evidence.BootstrapSessionID, "session_revision": evidence.SessionRevision,
		"verification_status": "identity_verified", "observed_at": evidence.ObservedAt, "expires_at": evidence.ExpiresAt,
	}, nil
}

func identityPreviewError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdentityPreviewInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudIdentityPreviewInvalidCode, "cloud connection identity preview request is invalid")
	case errors.Is(err, ErrIdentityPreviewConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdentityPreviewConflictCode, "cloud connection identity preview conflicts with current bootstrap state")
	case errors.Is(err, ErrIdentityPreviewRejected):
		return actionbase.CodedError(http.StatusForbidden, cloudIdentityPreviewRejectedCode, "cloud connection identity preview was rejected")
	case errors.Is(err, ErrIdentityPreviewInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudIdentityPreviewUpstreamCode, "cloud connection identity preview returned an invalid response")
	default:
		return actionbase.CodedError(http.StatusServiceUnavailable, cloudIdentityPreviewUnavailableCode, "cloud connection identity preview is unavailable")
	}
}

func validateIdentityPreviewEvidence(value IdentityPreviewEvidence, sessionID string, sessionRevision int64, targetID, accountID, region string, now time.Time) error {
	if value.BootstrapSessionID != sessionID || value.SessionRevision != sessionRevision || value.SessionRevision <= 0 ||
		!agentSecretIdentifierPattern.MatchString(value.OwnerID) || value.TargetID != targetID || value.Region != region ||
		!awsAccountIDPattern.MatchString(value.AccountID) || !awsPrincipalIDPattern.MatchString(value.PrincipalID) ||
		!cloudRegionPattern.MatchString(value.Region) || (accountID != "" && value.AccountID != accountID) {
		return ErrIdentityPreviewInvalidResponse
	}
	for _, field := range []string{value.OwnerID, value.TargetID, value.AccountID, value.PrincipalARN, value.PrincipalID, value.Region} {
		if ContainsSensitiveGoalMaterial(field) {
			return ErrIdentityPreviewInvalidResponse
		}
	}
	matches := awsPrincipalARNPattern.FindStringSubmatch(value.PrincipalARN)
	if matches == nil || matches[3] != value.AccountID || awsPartitionForRegion(value.Region) != matches[1] {
		return ErrIdentityPreviewInvalidResponse
	}
	isRootARN := matches[2] == "iam" && matches[4] == "root"
	if value.RootIdentity != isRootARN {
		return ErrIdentityPreviewInvalidResponse
	}
	observedAt, observedErr := time.Parse(time.RFC3339Nano, value.ObservedAt)
	expiresAt, expiresErr := time.Parse(time.RFC3339Nano, value.ExpiresAt)
	if observedErr != nil || expiresErr != nil || observedAt.UTC().Format(time.RFC3339Nano) != value.ObservedAt ||
		expiresAt.UTC().Format(time.RFC3339Nano) != value.ExpiresAt || !observedAt.Before(expiresAt) ||
		!now.Before(expiresAt) || observedAt.After(now.Add(30*time.Second)) {
		return ErrIdentityPreviewInvalidResponse
	}
	return nil
}

func awsPartitionForRegion(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.Contains(region, "-gov-"):
		return "aws-us-gov"
	default:
		return "aws"
	}
}
