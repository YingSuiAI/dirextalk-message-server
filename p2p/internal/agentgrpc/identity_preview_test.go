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

func TestIdentityPreviewBindsTrustedRolePlanScopeAndEvidence(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	now := time.Now().UTC().Truncate(time.Second)
	sessionID := uuid.NewString()
	targetID := "cloud_connection_preview_1"
	response := &agentv1.PreviewAwsIdentityResponse{
		Identity: &agentv1.AwsBootstrapIdentity{
			AccountId: "123456789012", PrincipalArn: "arn:aws:iam::123456789012:root",
			PrincipalId: "123456789012", Region: "us-east-1", RootIdentity: true,
		},
		BootstrapSessionId: sessionID, SessionRevision: 2, OwnerId: "owner-from-config", TargetId: targetID,
		ObservedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(5 * time.Minute)),
	}
	server.cloud.preview = func(*agentv1.PreviewAwsIdentityRequest) (*agentv1.PreviewAwsIdentityResponse, error) {
		return response, nil
	}
	evidence, err := runner.PreviewAgentAWSIdentity(t.Context(), cloudmodule.IdentityPreviewRequest{
		BootstrapSessionID: sessionID, ExpectedSessionRevision: 2, TargetID: targetID, Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	server.cloud.mu.Lock()
	request := server.cloud.previewRequest
	authorization := server.cloud.auth[len(server.cloud.auth)-1]
	server.cloud.mu.Unlock()
	if request.GetBootstrapSessionId() != sessionID || request.GetExpectedSessionRevision() != 2 || request.GetRegion() != "us-east-1" ||
		authorization != authorizationScheme+" "+testServiceKey {
		t.Fatalf("preview request=%#v auth=%q", request, authorization)
	}
	if evidence.BootstrapSessionID != sessionID || evidence.SessionRevision != 2 || evidence.OwnerID != "owner-from-config" ||
		evidence.TargetID != targetID || evidence.AccountID != "123456789012" || evidence.Region != "us-east-1" || !evidence.RootIdentity ||
		evidence.ObservedAt != now.Format(time.RFC3339Nano) || evidence.ExpiresAt != now.Add(5*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("identity evidence=%#v", evidence)
	}
}

func TestIdentityPreviewRejectsMismatchedEvidenceAndSanitizesProviderFailure(t *testing.T) {
	t.Parallel()
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})
	now := time.Now().UTC().Truncate(time.Second)
	sessionID := uuid.NewString()
	targetID := "cloud_connection_preview_2"
	valid := &agentv1.PreviewAwsIdentityResponse{
		Identity: &agentv1.AwsBootstrapIdentity{
			AccountId: "123456789012", PrincipalArn: "arn:aws:iam::123456789012:root",
			PrincipalId: "123456789012", Region: "us-east-1", RootIdentity: true,
		},
		BootstrapSessionId: sessionID, SessionRevision: 2, OwnerId: "owner-from-config", TargetId: targetID,
		ObservedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Minute)),
	}
	for name, mutate := range map[string]func(*agentv1.PreviewAwsIdentityResponse){
		"owner":            func(value *agentv1.PreviewAwsIdentityResponse) { value.OwnerId = "different-owner" },
		"target":           func(value *agentv1.PreviewAwsIdentityResponse) { value.TargetId = "different_target" },
		"session revision": func(value *agentv1.PreviewAwsIdentityResponse) { value.SessionRevision++ },
		"Region":           func(value *agentv1.PreviewAwsIdentityResponse) { value.Identity.Region = "eu-west-1" },
		"timestamps":       func(value *agentv1.PreviewAwsIdentityResponse) { value.ExpiresAt = value.ObservedAt },
	} {
		t.Run(name, func(t *testing.T) {
			response := proto.Clone(valid).(*agentv1.PreviewAwsIdentityResponse)
			mutate(response)
			server.cloud.preview = func(*agentv1.PreviewAwsIdentityRequest) (*agentv1.PreviewAwsIdentityResponse, error) {
				return response, nil
			}
			_, err := runner.PreviewAgentAWSIdentity(t.Context(), cloudmodule.IdentityPreviewRequest{
				BootstrapSessionID: sessionID, ExpectedSessionRevision: 2, TargetID: targetID, Region: "us-east-1",
			})
			if !errors.Is(err, cloudmodule.ErrIdentityPreviewInvalidResponse) {
				t.Fatalf("mismatched evidence error=%v", err)
			}
		})
	}

	const canary = "AWS-IDENTITY-PREVIEW-SECRET-CANARY"
	server.cloud.preview = func(*agentv1.PreviewAwsIdentityRequest) (*agentv1.PreviewAwsIdentityResponse, error) {
		return nil, status.Error(codes.Internal, canary)
	}
	_, err := runner.PreviewAgentAWSIdentity(t.Context(), cloudmodule.IdentityPreviewRequest{
		BootstrapSessionID: sessionID, ExpectedSessionRevision: 2, TargetID: targetID, Region: "us-east-1",
	})
	if !errors.Is(err, cloudmodule.ErrIdentityPreviewUnavailable) || strings.Contains(err.Error(), canary) {
		t.Fatalf("provider failure was not sanitized: %v", err)
	}
}
