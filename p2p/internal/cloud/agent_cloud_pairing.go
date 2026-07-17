package cloud

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const pairingPayloadEnvelopeSchemaV1 = "dirextalk.agent.recipient-envelope/v1"

const (
	cloudPairingPayloadInvalidCode      = "cloud_pairing_payload_invalid"
	cloudPairingPayloadConflictCode     = "cloud_pairing_payload_conflict"
	cloudPairingPayloadAgentInvalidCode = "cloud_pairing_payload_agent_invalid"
)

// AgentCloudPairingClient is the protocol-neutral, owner-bound pairing seam.
// Its payload result is encrypted to the caller's one-time X25519 public key;
// Message Server must never persist, publish, log, or project the envelope.
type AgentCloudPairingClient interface {
	GetAgentCloudPairing(context.Context, AgentCloudPairingGetRequest) (AgentCloudPairingSession, bool, error)
	RetrieveAgentCloudPairingPayload(context.Context, AgentCloudPairingPayloadRequest) (AgentCloudPairingPayloadResult, error)
	CreateAgentCloudPairingResumeChallenge(context.Context, AgentCloudPairingResumeChallengeRequest) (AgentCloudPairingResumeChallenge, error)
	ApproveAgentCloudPairingResume(context.Context, AgentCloudPairingResumeApproveRequest) (AgentCloudPairingSession, error)
}

type AgentCloudPairingGetRequest struct{ PairingID, DeploymentID string }

type AgentCloudPairingPayloadRequest struct {
	IdempotencyKey, PairingID, DeploymentID, RecipientPublicKey string
	ExpectedRevision                                            int64
}

type AgentCloudPairingSession struct {
	PairingID, OwnerID, DeploymentID, TaskID, StepID, PlanID, ConnectionID string
	RecipeID, RecipeDigest, BeginCommandID, ResumeCommandID                string
	ExecutionManifestDigest, Status                                        string
	RecipeRevision, DeploymentRevision, PayloadScopeRevision, Revision     int64
	PayloadReady                                                           bool
	ExpiresAt, CreatedAt, UpdatedAt                                        time.Time
}

type AgentCloudPairingPayload struct {
	SchemaVersion, ServerEphemeralPublicKey, Nonce, Ciphertext string
	AssociatedDataCBOR                                         []byte
	PayloadDigest                                              string
	ExpiresAt                                                  time.Time
}

type AgentCloudPairingPayloadResult struct {
	Pairing AgentCloudPairingSession
	Payload AgentCloudPairingPayload
}

type AgentCloudPairingResumeChallengeRequest struct {
	IdempotencyKey, PairingID, DeploymentID, SignerKeyID string
	ExpectedPairingRevision                              int64
}

type AgentCloudPairingResumeChallenge struct {
	Approval           cloudcontracts.PairingResumeApprovalV1
	SigningPayloadCBOR []byte
}

type AgentCloudPairingResumeApproveRequest struct {
	IdempotencyKey, PairingID, DeploymentID string
	ExpectedPairingRevision                 int64
	Approval                                cloudcontracts.PairingResumeApprovalV1
}

func (m *Module) retrieveDeploymentPairingPayload(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "deployment_id", "recipient_public_key", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentPairingClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	request := AgentCloudPairingPayloadRequest{
		DeploymentID:       values.String("deployment_id"),
		RecipientPublicKey: values.String("recipient_public_key"),
		IdempotencyKey:     values.String("idempotency_key"),
	}
	if !canonicalUUID(request.DeploymentID) || !canonicalUUID(request.IdempotencyKey) ||
		!validPairingRecipient(request.RecipientPublicKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudPairingPayloadInvalidCode, "cloud pairing payload request is invalid")
	}
	session, found, err := client.GetAgentCloudPairing(ctx, AgentCloudPairingGetRequest{DeploymentID: request.DeploymentID})
	if err != nil {
		return nil, agentPairingPayloadError(err)
	}
	if !found || !validAgentPairingSession(session, m.ownerMXID()) || session.DeploymentID != request.DeploymentID ||
		!m.now().UTC().Before(session.ExpiresAt) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudPairingPayloadConflictCode, "cloud pairing payload request conflicts with current state")
	}
	switch {
	case session.Status == "waiting_payload" && !session.PayloadReady && session.PayloadScopeRevision == 0:
		request.ExpectedRevision = session.Revision
	case (session.Status == "payload_ready" || session.Status == "waiting_user") && session.PayloadReady &&
		session.PayloadScopeRevision > 0 && session.PayloadScopeRevision < session.Revision:
		request.ExpectedRevision = session.PayloadScopeRevision
	default:
		return nil, actionbase.CodedError(http.StatusConflict, cloudPairingPayloadConflictCode, "cloud pairing payload request conflicts with current state")
	}
	request.PairingID = session.PairingID
	result, err := client.RetrieveAgentCloudPairingPayload(ctx, request)
	if err != nil {
		return nil, agentPairingPayloadError(err)
	}
	if validateAgentPairingPayloadResult(result, m.ownerMXID(), request, m.now().UTC()) != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudPairingPayloadAgentInvalidCode, "cloud Agent returned an invalid pairing payload response")
	}
	return map[string]any{
		"pairing": agentPairingSessionView(result.Pairing),
		"payload": map[string]any{
			"schema_version":                  result.Payload.SchemaVersion,
			"server_ephemeral_public_key_b64": result.Payload.ServerEphemeralPublicKey,
			"nonce_b64":                       result.Payload.Nonce,
			"ciphertext_b64":                  result.Payload.Ciphertext,
			"associated_data_cbor_b64":        base64.RawURLEncoding.EncodeToString(result.Payload.AssociatedDataCBOR),
		},
		"payload_digest":         result.Payload.PayloadDigest,
		"payload_scope_revision": request.ExpectedRevision,
		"expires_at":             result.Payload.ExpiresAt,
	}, nil
}

func agentPairingPayloadError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudPairingPayloadInvalidCode, "cloud pairing payload request is invalid")
	case errors.Is(err, ErrAgentCloudControlConflict):
		return actionbase.CodedError(http.StatusConflict, cloudPairingPayloadConflictCode, "cloud pairing payload request conflicts with current state")
	case errors.Is(err, ErrAgentCloudControlRejected):
		return actionbase.CodedError(http.StatusForbidden, cloudPairingPayloadConflictCode, "cloud pairing payload request was rejected")
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudPairingPayloadAgentInvalidCode, "cloud Agent returned an invalid pairing payload response")
	case errors.Is(err, ErrAgentCloudControlUnavailable):
		return actionbase.CodedError(http.StatusServiceUnavailable, "cloud_agent_unavailable", "cloud Agent is unavailable")
	default:
		return actionbase.InternalError(err)
	}
}

func (m *Module) resumeAgentDeploymentPairing(ctx context.Context, deploymentID, idempotencyKey string, expectedRevision int64, approval *cloudcontracts.PairingResumeApprovalV1) (any, *actionbase.Error) {
	client, ok := m.agentPairingClient()
	if !ok {
		return nil, unavailableError()
	}
	deployment, apiErr := m.currentAgentPairingDeployment(ctx, deploymentID, expectedRevision)
	if apiErr != nil {
		return nil, apiErr
	}
	session, found, err := client.GetAgentCloudPairing(ctx, AgentCloudPairingGetRequest{DeploymentID: deploymentID})
	if err != nil {
		return nil, agentPairingError(err)
	}
	if !found || !validAgentPairingSession(session, m.ownerMXID()) || session.DeploymentID != deploymentID ||
		session.DeploymentRevision != deployment.Revision || session.PlanID != deployment.PlanID ||
		session.ConnectionID != deployment.ConnectionID {
		return nil, actionbase.CodedError(http.StatusConflict, cloudPairingResumeConflictCode, "cloud pairing resume conflicts with current deployment state")
	}
	if approval == nil {
		if (session.Status != "waiting_user" && session.Status != "payload_ready") || !session.PayloadReady ||
			!m.now().UTC().Before(session.ExpiresAt) {
			return nil, actionbase.CodedError(http.StatusConflict, cloudPairingResumeConflictCode, "cloud pairing resume conflicts with current deployment state")
		}
		challenge, callErr := client.CreateAgentCloudPairingResumeChallenge(ctx, AgentCloudPairingResumeChallengeRequest{
			IdempotencyKey: idempotencyKey, PairingID: session.PairingID, DeploymentID: deploymentID,
			ExpectedPairingRevision: session.Revision,
		})
		if callErr != nil {
			return nil, agentPairingError(callErr)
		}
		if validateAgentPairingResumeChallenge(challenge, deployment, session, m.now().UTC()) != nil {
			return nil, actionbase.CodedError(http.StatusBadGateway, cloudPairingResumeInvalidCode, "cloud Agent returned an invalid pairing resume challenge")
		}
		return map[string]any{"confirmation": PairingResumeConfirmation{
			Deployment: deployment, Job: agentPairingJob(session), Approval: challenge.Approval,
		}}, nil
	}
	if approval.Validate() != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudPairingResumeInvalidCode, "cloud pairing resume approval is invalid")
	}
	if !m.now().UTC().Before(approval.ExpiresAt) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudPairingResumeExpiredCode, "cloud pairing resume approval has expired")
	}
	if approval.IssuedAt.After(m.now().UTC()) || approval.DeploymentID != deploymentID ||
		int64(approval.DeploymentRevision) != expectedRevision || approval.JobID != session.PairingID ||
		int64(approval.JobRevision) > session.Revision || approval.ExecutionID != session.TaskID ||
		approval.PlanID != session.PlanID || approval.CloudConnectionID != session.ConnectionID ||
		approval.RecipeExecutionManifestDigest != session.ExecutionManifestDigest {
		return nil, actionbase.CodedError(http.StatusConflict, cloudPairingResumeConflictCode, "cloud pairing resume conflicts with current deployment state")
	}
	resumed, callErr := client.ApproveAgentCloudPairingResume(ctx, AgentCloudPairingResumeApproveRequest{
		IdempotencyKey: idempotencyKey, PairingID: session.PairingID, DeploymentID: deploymentID,
		ExpectedPairingRevision: int64(approval.JobRevision), Approval: *approval,
	})
	if callErr != nil {
		return nil, agentPairingError(callErr)
	}
	if !validAgentPairingSession(resumed, m.ownerMXID()) || resumed.PairingID != session.PairingID ||
		resumed.DeploymentID != deploymentID || resumed.Revision <= int64(approval.JobRevision) ||
		(resumed.Status != "resuming" && resumed.Status != "succeeded") {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudPairingResumeInvalidCode, "cloud Agent returned an invalid pairing resume result")
	}
	current, apiErr := m.currentAgentPairingDeploymentAtLeast(ctx, deployment, expectedRevision)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{"deployment": current, "job": agentPairingJob(resumed)}, nil
}

func (m *Module) currentAgentPairingDeployment(ctx context.Context, deploymentID string, expectedRevision int64) (Deployment, *actionbase.Error) {
	reader := m.deploymentReader()
	if reader == nil {
		return Deployment{}, unavailableError()
	}
	deployment, found, err := reader.GetCloudDeployment(ctx, deploymentID)
	if err != nil {
		return Deployment{}, actionbase.InternalError(err)
	}
	if !found || deployment.DeploymentID != deploymentID || deployment.Revision != expectedRevision {
		return Deployment{}, actionbase.CodedError(http.StatusConflict, cloudPairingResumeConflictCode, "cloud pairing resume conflicts with current deployment state")
	}
	return deployment, nil
}

func (m *Module) currentAgentPairingDeploymentAtLeast(ctx context.Context, expected Deployment, minimumRevision int64) (Deployment, *actionbase.Error) {
	reader := m.deploymentReader()
	if reader == nil {
		return Deployment{}, unavailableError()
	}
	current, found, err := reader.GetCloudDeployment(ctx, expected.DeploymentID)
	if err != nil {
		return Deployment{}, actionbase.InternalError(err)
	}
	if !found || current.DeploymentID != expected.DeploymentID || current.PlanID != expected.PlanID ||
		current.ConnectionID != expected.ConnectionID || current.Revision < minimumRevision {
		return Deployment{}, actionbase.CodedError(http.StatusConflict, cloudPairingResumeConflictCode, "cloud pairing resume conflicts with current deployment state")
	}
	return current, nil
}

func validateAgentPairingResumeChallenge(value AgentCloudPairingResumeChallenge, deployment Deployment, session AgentCloudPairingSession, now time.Time) error {
	approval := value.Approval
	if approval.Validate() != nil || approval.Signature != "" || approval.DeploymentID != deployment.DeploymentID ||
		int64(approval.DeploymentRevision) != deployment.Revision || approval.PlanID != deployment.PlanID ||
		approval.CloudConnectionID != deployment.ConnectionID || approval.ExecutionID != session.TaskID ||
		approval.RecipeExecutionManifestDigest != session.ExecutionManifestDigest ||
		approval.JobID != session.PairingID || int64(approval.JobRevision) != session.Revision ||
		approval.IssuedAt.After(now) || !now.Before(approval.ExpiresAt) {
		return ErrAgentCloudControlInvalidResponse
	}
	signing, err := approval.SigningPayload()
	if err != nil || len(value.SigningPayloadCBOR) == 0 || !bytes.Equal(signing, value.SigningPayloadCBOR) {
		clear(signing)
		return ErrAgentCloudControlInvalidResponse
	}
	clear(signing)
	return nil
}

func agentPairingJob(value AgentCloudPairingSession) Job {
	job := Job{
		JobID: value.PairingID, PlanID: value.PlanID, DeploymentID: value.DeploymentID,
		Kind: "install", Revision: value.Revision,
		CreatedAt: value.CreatedAt.UnixMilli(), UpdatedAt: value.UpdatedAt.UnixMilli(),
	}
	switch value.Status {
	case "waiting_payload", "payload_ready", "waiting_user":
		job.Execution, job.Outcome, job.Checkpoint = "waiting_user", "pending", "waiting_user_pairing"
	case "resuming":
		job.Execution, job.Outcome, job.Checkpoint = "running", "pending", "pairing_resume"
	case "succeeded":
		job.Execution, job.Outcome, job.Checkpoint = "finished", "succeeded", "pairing_resumed"
	case "timed_out":
		job.Execution, job.Outcome, job.Checkpoint, job.ErrorCode = "finished", "timed_out", "pairing_timed_out", "pairing_timed_out"
	case "failed":
		job.Execution, job.Outcome, job.Checkpoint, job.ErrorCode = "finished", "failed", "pairing_failed", "pairing_failed"
	}
	return job
}

func (m *Module) agentPairingClient() (AgentCloudPairingClient, bool) {
	if m == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, false
	}
	client, ok := m.cfg.AgentCloudControlClient.(AgentCloudPairingClient)
	return client, ok
}

func validPairingRecipient(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(value) == 43 && len(decoded) == 32
}

func validateAgentPairingPayloadResult(result AgentCloudPairingPayloadResult, owner string, request AgentCloudPairingPayloadRequest, now time.Time) error {
	session := result.Pairing
	payload := result.Payload
	if !validAgentPairingSession(session, owner) || session.PairingID != request.PairingID ||
		session.DeploymentID != request.DeploymentID || session.Revision <= request.ExpectedRevision ||
		session.PayloadScopeRevision != request.ExpectedRevision ||
		!session.PayloadReady || (session.Status != "payload_ready" && session.Status != "waiting_user") ||
		payload.SchemaVersion != pairingPayloadEnvelopeSchemaV1 ||
		!validPairingRecipient(payload.ServerEphemeralPublicKey) ||
		!validRawURLBytes(payload.Nonce, 12, 12) ||
		!validRawURLBytes(payload.Ciphertext, 17, 8*1024+16) ||
		len(payload.AssociatedDataCBOR) == 0 || len(payload.AssociatedDataCBOR) > 2048 ||
		!namedSHA256Pattern.MatchString(payload.PayloadDigest) ||
		payload.ExpiresAt.Location() != time.UTC || payload.ExpiresAt != session.ExpiresAt ||
		!now.Before(payload.ExpiresAt) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validAgentPairingSession(value AgentCloudPairingSession, owner string) bool {
	if !canonicalUUID(value.PairingID) || value.OwnerID != owner || !canonicalUUID(value.DeploymentID) ||
		!canonicalUUID(value.TaskID) || !canonicalUUID(value.StepID) || !canonicalUUID(value.PlanID) ||
		!canonicalUUID(value.ConnectionID) || !cloudIdentifierPattern.MatchString(value.RecipeID) ||
		!namedSHA256Pattern.MatchString(value.RecipeDigest) || value.RecipeRevision <= 0 ||
		!cloudIdentifierPattern.MatchString(value.BeginCommandID) || !cloudIdentifierPattern.MatchString(value.ResumeCommandID) ||
		!namedSHA256Pattern.MatchString(value.ExecutionManifestDigest) || value.DeploymentRevision <= 0 || value.Revision <= 0 ||
		value.ExpiresAt.Location() != time.UTC || value.CreatedAt.Location() != time.UTC || value.UpdatedAt.Location() != time.UTC ||
		value.CreatedAt.Unix() <= 0 || !value.ExpiresAt.After(value.CreatedAt) || value.UpdatedAt.Before(value.CreatedAt) {
		return false
	}
	switch value.Status {
	case "waiting_payload":
		return !value.PayloadReady && value.PayloadScopeRevision == 0
	case "payload_ready", "waiting_user", "resuming", "succeeded":
		return value.PayloadReady && value.PayloadScopeRevision > 0 && value.PayloadScopeRevision < value.Revision
	case "timed_out", "failed":
		return !value.PayloadReady || value.PayloadScopeRevision > 0
	default:
		return false
	}
}

// ValidateAgentCloudPairingSession shares the exact protocol-neutral session
// checks with the gRPC adapter before any ciphertext reaches ProductCore.
func ValidateAgentCloudPairingSession(value AgentCloudPairingSession, owner string) bool {
	return validAgentPairingSession(value, owner)
}

func validRawURLBytes(value string, minimum, maximum int) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) >= minimum && len(decoded) <= maximum
}

func agentPairingSessionView(value AgentCloudPairingSession) map[string]any {
	return map[string]any{
		"pairing_id": value.PairingID, "owner_id": value.OwnerID, "deployment_id": value.DeploymentID,
		"deployment_revision": value.DeploymentRevision, "payload_scope_revision": value.PayloadScopeRevision,
		"status": value.Status, "payload_ready": value.PayloadReady,
		"revision": value.Revision, "expires_at": value.ExpiresAt,
	}
}

func agentPairingError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudPairingResumeInvalidCode, "cloud pairing request is invalid")
	case errors.Is(err, ErrAgentCloudControlConflict):
		return actionbase.CodedError(http.StatusConflict, cloudPairingResumeConflictCode, "cloud pairing request conflicts with current state")
	case errors.Is(err, ErrAgentCloudControlRejected):
		return actionbase.CodedError(http.StatusForbidden, cloudPairingResumeSignatureCode, "cloud pairing request was rejected")
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudPairingResumeInvalidCode, "cloud Agent returned an invalid pairing response")
	case errors.Is(err, ErrAgentCloudControlUnavailable):
		return actionbase.CodedError(http.StatusServiceUnavailable, "cloud_agent_unavailable", "cloud Agent is unavailable")
	default:
		return actionbase.InternalError(err)
	}
}
