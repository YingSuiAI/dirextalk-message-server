package agentgrpc

import (
	"context"
	"regexp"
	"strings"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	remoteAWSRegionPattern    = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`)
	remoteAWSAccountPattern   = regexp.MustCompile(`^[0-9]{12}$`)
	remoteAWSPrincipalPattern = regexp.MustCompile(`^[A-Za-z0-9+=,.@_:/-]{1,256}$`)
)

// PreviewAgentAWSIdentity performs only STS caller-identity inspection. The
// trusted Runner owner is implicit and the role-plan target/Region are passed
// by the server module, never by ProductCore.
func (runner *Runner) PreviewAgentAWSIdentity(ctx context.Context, request cloudmodule.IdentityPreviewRequest) (cloudmodule.IdentityPreviewEvidence, error) {
	if runner == nil || runner.cloud == nil {
		return cloudmodule.IdentityPreviewEvidence{}, cloudmodule.ErrIdentityPreviewUnavailable
	}
	if !validUUID(request.BootstrapSessionID) || request.ExpectedSessionRevision <= 0 ||
		!validAgentSecretIdentifier(request.TargetID) || !remoteAWSRegionPattern.MatchString(request.Region) {
		return cloudmodule.IdentityPreviewEvidence{}, cloudmodule.ErrIdentityPreviewInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.cloud.PreviewAwsIdentity(callContext, &agentv1.PreviewAwsIdentityRequest{
		BootstrapSessionId:      request.BootstrapSessionID,
		ExpectedSessionRevision: request.ExpectedSessionRevision,
		Region:                  request.Region,
	})
	if err != nil {
		return cloudmodule.IdentityPreviewEvidence{}, mapIdentityPreviewRPCError(callContext, err)
	}
	if response == nil || response.GetIdentity() == nil ||
		response.GetBootstrapSessionId() != request.BootstrapSessionID ||
		response.GetSessionRevision() != request.ExpectedSessionRevision ||
		response.GetOwnerId() != runner.ownerID || response.GetTargetId() != request.TargetID {
		return cloudmodule.IdentityPreviewEvidence{}, cloudmodule.ErrIdentityPreviewInvalidResponse
	}
	identity := response.GetIdentity()
	if !remoteAWSAccountPattern.MatchString(identity.GetAccountId()) ||
		!remoteAWSPrincipalPattern.MatchString(identity.GetPrincipalId()) || identity.GetRegion() != request.Region ||
		strings.TrimSpace(identity.GetPrincipalArn()) != identity.GetPrincipalArn() || identity.GetPrincipalArn() == "" {
		return cloudmodule.IdentityPreviewEvidence{}, cloudmodule.ErrIdentityPreviewInvalidResponse
	}
	observedAt, observedErr := exactBootstrapTimestamp(response.GetObservedAt())
	expiresAt, expiresErr := exactBootstrapTimestamp(response.GetExpiresAt())
	if observedErr != nil || expiresErr != nil || !observedAt.Before(expiresAt) {
		return cloudmodule.IdentityPreviewEvidence{}, cloudmodule.ErrIdentityPreviewInvalidResponse
	}
	return cloudmodule.IdentityPreviewEvidence{
		BootstrapSessionID: response.GetBootstrapSessionId(), SessionRevision: response.GetSessionRevision(),
		OwnerID: response.GetOwnerId(), TargetID: response.GetTargetId(), AccountID: identity.GetAccountId(),
		PrincipalARN: identity.GetPrincipalArn(), PrincipalID: identity.GetPrincipalId(), Region: identity.GetRegion(),
		RootIdentity: identity.GetRootIdentity(), ObservedAt: observedAt.Format(time.RFC3339Nano),
		ExpiresAt: expiresAt.Format(time.RFC3339Nano),
	}, nil
}

func mapIdentityPreviewRPCError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return cloudmodule.ErrIdentityPreviewUnavailable
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return cloudmodule.ErrIdentityPreviewInvalid
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition, codes.NotFound:
		return cloudmodule.ErrIdentityPreviewConflict
	case codes.PermissionDenied:
		return cloudmodule.ErrIdentityPreviewRejected
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable, codes.Unauthenticated, codes.Internal, codes.ResourceExhausted:
		return cloudmodule.ErrIdentityPreviewUnavailable
	default:
		return cloudmodule.ErrIdentityPreviewUnavailable
	}
}
