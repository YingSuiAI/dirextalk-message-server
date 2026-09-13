package productcapability

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"strings"

	capv1 "github.com/YingSuiAI/dirextalk-capability-api/gen/go/dirextalk/capability/v1"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

const groupAgentCapability = "product.group_agent.v1"

// This private service channel is deliberately absent from the capability
// registry, grants, tool catalogs and external MCP. Its only caller is the
// authenticated Agent poller, never the owner's model/tool execution path.
func (s *Server) validatePrivateGroupAgent(call *capv1.CallContext, permission *capv1.PermissionContext, raw []byte) *capv1.CapabilityError {
	if permission != nil {
		return capabilityError(capv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "private group Agent channel does not accept owner or model permission")
	}
	if call == nil || call.Route != capv1.NodeAgent+capv1.RouteSeparator+capv1.NodeProduct || call.Hop != 2 || call.ParentCallId != "" || capv1.ValidateOperationID(call.RootOperationId) != nil {
		return capabilityError(capv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "private group Agent channel requires a fresh Agent service route")
	}
	if s.config == nil || strings.TrimSpace(s.config.ServiceOwnerID) == "" || s.config.ExpectedAccountGeneration <= 0 || s.config.InvokeGroupAgentCapability == nil {
		return capabilityError(capv1.ErrorCode_ERROR_CODE_NOT_READY, "private group Agent channel is unavailable")
	}
	if len(raw) == 0 || len(raw) > 32<<10 {
		return capabilityError(capv1.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "invalid private group Agent request size")
	}
	canonical, err := capv1.CanonicalizeJSON(raw)
	if err != nil || !bytes.Equal(canonical, raw) {
		return capabilityError(capv1.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "request_json must be canonical JSON")
	}
	return nil
}

func (s *Server) queryGroupAgent(ctx context.Context, req *capv1.QueryRequest) *capv1.QueryResponse {
	if err := s.validatePrivateGroupAgent(req.CallContext, req.Permission, req.RequestJson); err != nil {
		return &capv1.QueryResponse{Error: err}
	}
	switch req.OperationId {
	case "pull", "validate", "history", "members", "bindings", "transcript":
	default:
		return &capv1.QueryResponse{Error: capabilityError(capv1.ErrorCode_ERROR_CODE_NOT_FOUND, "private group Agent query is unavailable")}
	}
	value, err := s.config.InvokeGroupAgentCapability(ctx, req.OperationId, req.RequestJson)
	if err != nil {
		return &capv1.QueryResponse{Error: groupAgentCapabilityError(err)}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return &capv1.QueryResponse{Error: groupAgentCapabilityError(err)}
	}
	raw, err = capv1.CanonicalizeJSON(raw)
	if err != nil {
		return &capv1.QueryResponse{Error: groupAgentCapabilityError(err)}
	}
	return &capv1.QueryResponse{ResultJson: raw}
}

func (s *Server) startGroupAgent(ctx context.Context, req *capv1.StartOperationRequest) *capv1.StartOperationResponse {
	fail := func(err *capv1.CapabilityError) *capv1.StartOperationResponse {
		return &capv1.StartOperationResponse{OperationId: req.OperationId, State: capv1.OperationState_OPERATION_STATE_FAILED, Error: err}
	}
	if err := s.validatePrivateGroupAgent(req.CallContext, req.Permission, req.RequestJson); err != nil {
		return fail(err)
	}
	if capv1.ValidateOperationID(req.OperationId) != nil || req.CallContext.RootOperationId != req.OperationId || req.ExpectedRevision != 0 {
		return fail(capabilityError(capv1.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "invalid private group Agent operation identity"))
	}
	if req.Operation != "publish" && req.Operation != "complete" && req.Operation != "enqueue" {
		return fail(capabilityError(capv1.ErrorCode_ERROR_CODE_NOT_FOUND, "private group Agent mutation is unavailable"))
	}
	digest := sha256.Sum256(req.RequestJson)
	if len(req.RequestDigest) != sha256.Size || subtle.ConstantTimeCompare(req.RequestDigest, digest[:]) != 1 {
		return fail(capabilityError(capv1.ErrorCode_ERROR_CODE_CONFLICT, "request digest mismatch"))
	}
	value, err := s.config.InvokeGroupAgentCapability(ctx, req.Operation, req.RequestJson)
	if err != nil {
		return fail(groupAgentCapabilityError(err))
	}
	result, _ := value.(map[string]any)
	if req.Operation == "publish" && result["status"] == "cancelled" {
		return fail(capabilityError(capv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, "group Agent request was revoked"))
	}
	replayed, _ := result["replayed"].(bool)
	return &capv1.StartOperationResponse{OperationId: req.OperationId, State: capv1.OperationState_OPERATION_STATE_COMPLETED, Replayed: replayed}
}

func groupAgentCapabilityError(err error) *capv1.CapabilityError {
	if errors.Is(err, dirextalkdomain.ErrGroupAgentConflict) {
		return capabilityError(capv1.ErrorCode_ERROR_CODE_CONFLICT, "group Agent binding or request conflict")
	}
	// The Agent is the only caller of this private channel, and it needs the
	// refusal reason to fail a scheduled occurrence honestly instead of
	// reporting an unexplained model error. Keep one bounded, sanitized line.
	return capabilityError(capv1.ErrorCode_ERROR_CODE_UPSTREAM_FAILED, boundedGroupAgentReason(err))
}

func boundedGroupAgentReason(err error) string {
	if err == nil {
		return "group Agent operation failed"
	}
	reason := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		return r
	}, strings.TrimSpace(err.Error()))
	if reason == "" {
		return "group Agent operation failed"
	}
	if len(reason) > 200 {
		reason = reason[:200]
	}
	return reason
}
