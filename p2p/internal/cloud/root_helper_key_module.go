package cloud

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/fxamacker/cbor/v2"
)

var (
	rootHelperInstancePattern = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	rootHelperAccountPattern  = regexp.MustCompile(`^[0-9]{12}$`)
	rootHelperRegionPattern   = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z]+-\d$`)
)

func (m *Module) prepareRootHelperKey(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentRootHelperKeyClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || !canonicalUUID(idempotencyKey) {
		return nil, rootHelperKeyRequestError()
	}
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	if compatibility.DeploymentRevision != expectedRevision {
		return nil, rootHelperKeyConflictError()
	}
	approval, err := client.PrepareRootHelperKey(ctx, AgentRootHelperKeyPrepareRequest{
		IdempotencyKey: idempotencyKey, DeploymentID: compatibility.DeploymentID,
		ExpectedDeploymentRevision: expectedRevision, DeviceSignerKeyID: compatibility.SignerKeyID,
	})
	if err != nil {
		return nil, rootHelperKeyAgentError(err, true)
	}
	if validateRootHelperKeyApproval(approval, m.ownerMXID(), compatibility) != nil || approval.Status != "awaiting_approval" {
		return nil, rootHelperKeyInvalidResponseError()
	}
	public := rootHelperKeyApprovalToPublic(approval)
	return map[string]any{"confirmation": map[string]any{
		"service_id": serviceID, "approval": public,
		"signing_payload_cbor":   base64.RawURLEncoding.EncodeToString(approval.SigningPayloadCBOR),
		"signing_payload_digest": approval.SigningPayloadDigest,
	}}, nil
}

func (m *Module) approveRootHelperKey(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentRootHelperKeyClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	public, signature, err := decodeRootHelperKeyPublicApproval(params["approval"])
	if err != nil || !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || !canonicalUUID(idempotencyKey) {
		return nil, rootHelperKeyRequestError()
	}
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	if compatibility.DeploymentRevision != expectedRevision || public.Binding.BindingRevision != expectedRevision ||
		public.Binding.DeploymentID != compatibility.DeploymentID || public.DeviceSignerKeyID != compatibility.SignerKeyID {
		return nil, rootHelperKeyConflictError()
	}
	request := AgentRootHelperKeyApproveRequest{
		IdempotencyKey: idempotencyKey, DeploymentID: compatibility.DeploymentID, DeliveryID: public.Binding.DeliveryID,
		ExpectedRevision: public.Revision, DeviceSignature: signature,
	}
	current, found, getErr := client.GetRootHelperKey(ctx, AgentRootHelperKeyGetRequest{
		DeploymentID: compatibility.DeploymentID, DeliveryID: public.Binding.DeliveryID,
	})
	if getErr != nil {
		return nil, rootHelperKeyAgentError(getErr, true)
	}
	if found {
		if validateRootHelperKeyApproval(current, m.ownerMXID(), compatibility) != nil ||
			!sameRootHelperKeyPublicApproval(current, public) {
			return nil, rootHelperKeyInvalidResponseError()
		}
		if current.Status == "approved" {
			return map[string]any{"approval": rootHelperKeyRedactedView(serviceID, current)}, nil
		}
		if current.Status != "awaiting_approval" {
			return nil, rootHelperKeyConflictError()
		}
	} else {
		return nil, rootHelperKeyConflictError()
	}
	approved, callErr := client.ApproveRootHelperKey(ctx, request)
	if callErr == nil && validateRootHelperKeyApproval(approved, m.ownerMXID(), compatibility) == nil &&
		sameRootHelperKeyPublicApproval(approved, public) && approved.Status == "approved" {
		return map[string]any{"approval": rootHelperKeyRedactedView(serviceID, approved)}, nil
	}
	// The device signature is never sent twice after an unknown result.
	recovered, found, getErr := client.GetRootHelperKey(ctx, AgentRootHelperKeyGetRequest{
		DeploymentID: compatibility.DeploymentID, DeliveryID: public.Binding.DeliveryID,
	})
	if getErr != nil {
		return nil, rootHelperKeyAgentError(getErr, true)
	}
	if found && validateRootHelperKeyApproval(recovered, m.ownerMXID(), compatibility) == nil &&
		sameRootHelperKeyPublicApproval(recovered, public) && recovered.Status == "approved" {
		return map[string]any{"approval": rootHelperKeyRedactedView(serviceID, recovered)}, nil
	}
	if callErr == nil {
		callErr = ErrAgentCloudControlInvalidResponse
	}
	return nil, rootHelperKeyAgentError(callErr, true)
}

func (m *Module) getRootHelperKey(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "delivery_id"); err != nil {
		return nil, err
	}
	client, ok := m.agentRootHelperKeyClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, deliveryID := values.String("service_id"), values.String("delivery_id")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || !canonicalUUID(deliveryID) {
		return nil, rootHelperKeyRequestError()
	}
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	if compatibility.DeploymentRevision != expectedRevision {
		return nil, rootHelperKeyConflictError()
	}
	approval, found, err := client.GetRootHelperKey(ctx, AgentRootHelperKeyGetRequest{
		DeploymentID: compatibility.DeploymentID, DeliveryID: deliveryID,
	})
	if err != nil || !found {
		return nil, rootHelperKeyAgentError(err, found)
	}
	if validateRootHelperKeyApproval(approval, m.ownerMXID(), compatibility) != nil {
		return nil, rootHelperKeyInvalidResponseError()
	}
	return map[string]any{"approval": rootHelperKeyRedactedView(serviceID, approval)}, nil
}

func (m *Module) agentRootHelperKeyClient() (AgentRootHelperKeyClient, bool) {
	if m == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, false
	}
	client, ok := m.cfg.AgentCloudControlClient.(AgentRootHelperKeyClient)
	return client, ok && client != nil
}

func validateRootHelperKeyApproval(value AgentRootHelperKeyApproval, owner string, compatibility ManagedAcceptanceCompatibility) error {
	binding := value.Binding
	payload, err := rootHelperKeyBindingSigningPayload(binding)
	publicDigest, nonceDigest := sha256.Sum256(value.PublicKey), sha256.Sum256(value.Nonce)
	if err != nil || value.SchemaVersion != rootHelperKeyApprovalSchema || !canonicalUUID(value.ChallengeID) ||
		!cloudKeyIDPattern.MatchString(value.DeviceSignerKeyID) || binding.SchemaVersion != rootHelperKeySchema ||
		!canonicalUUID(binding.AgentInstanceID) || binding.OwnerID != owner || !canonicalUUID(binding.DeliveryID) ||
		binding.DeploymentID != compatibility.DeploymentID || binding.BindingRevision != compatibility.DeploymentRevision ||
		!rootHelperInstancePattern.MatchString(binding.InstanceID) || strings.TrimSpace(binding.WorkerRoleARN) == "" ||
		!strings.HasSuffix(binding.WorkerPrincipalID, ":"+binding.InstanceID) || binding.HelperID != "root-helper" ||
		!cloudKeyIDPattern.MatchString(binding.SignerKeyID) ||
		binding.PublicKeyDigest != "sha256:"+hex.EncodeToString(publicDigest[:]) ||
		binding.NonceDigest != "sha256:"+hex.EncodeToString(nonceDigest[:]) ||
		len(value.PublicKey) != 32 || len(value.Nonce) != 32 || !bytes.Equal(payload, value.SigningPayloadCBOR) ||
		value.SigningPayloadDigest != digestRootHelperKeyPayload(payload) ||
		(value.Status != "awaiting_approval" && value.Status != "approved") || value.Revision < 1 ||
		value.CreatedAt.IsZero() || value.CreatedAt.Location() != time.UTC || value.UpdatedAt.Location() != time.UTC ||
		value.UpdatedAt.Before(value.CreatedAt) || !validRootHelperKeySecretCoordinate(binding.Secret, binding.SecretPlan) {
		return ErrAgentCloudControlInvalidResponse
	}
	if (value.Status == "awaiting_approval" && len(value.DeviceSignature) != 0) ||
		(value.Status == "approved" && len(value.DeviceSignature) != 64) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validRootHelperKeySecretCoordinate(value AgentRootHelperKeySecretCoordinate, plan AgentRootHelperKeySecretPlan) bool {
	if value == (AgentRootHelperKeySecretCoordinate{}) {
		return true
	}
	return value.Name == plan.Name && value.VersionID == plan.VersionID && value.KMSKeyARN == plan.KMSKeyARN &&
		strings.Contains(value.ARN, ":"+plan.Region+":"+plan.AccountID+":secret:"+plan.Name+"-") &&
		!strings.ContainsAny(value.ARN+value.Name+value.VersionID+value.KMSKeyARN, "\x00\r\n")
}

// ValidateAgentRootHelperKeyApproval is shared with the gRPC adapter so both
// the transport and ProductCore boundary enforce the same public binding.
func ValidateAgentRootHelperKeyApproval(value AgentRootHelperKeyApproval, owner string, compatibility ManagedAcceptanceCompatibility) error {
	return validateRootHelperKeyApproval(value, owner, compatibility)
}

func rootHelperKeyBindingSigningPayload(value AgentRootHelperKeyBinding) ([]byte, error) {
	type signingScope struct {
		SchemaVersion     string `json:"schema_version"`
		AgentInstanceID   string `json:"agent_instance_id"`
		OwnerID           string `json:"owner_id"`
		DeliveryID        string `json:"delivery_id"`
		DeploymentID      string `json:"deployment_id"`
		BindingRevision   int64  `json:"binding_revision"`
		InstanceID        string `json:"instance_id"`
		WorkerRoleARN     string `json:"worker_role_arn"`
		WorkerPrincipalID string `json:"worker_principal_id"`
		HelperID          string `json:"helper_id"`
		SignerKeyID       string `json:"signer_key_id"`
		PublicKeyDigest   string `json:"public_key_digest"`
		SecretPartition   string `json:"secret_partition"`
		SecretAccountID   string `json:"secret_account_id"`
		SecretRegion      string `json:"secret_region"`
		SecretName        string `json:"secret_name"`
		SecretVersionID   string `json:"secret_version_id"`
		SecretKMSKeyARN   string `json:"secret_kms_key_arn"`
		TargetPath        string `json:"target_path"`
		FileMode          uint32 `json:"file_mode"`
		NonceDigest       string `json:"nonce_digest"`
	}
	plan := value.SecretPlan
	fields := []string{value.SchemaVersion, value.AgentInstanceID, value.OwnerID, value.DeliveryID, value.DeploymentID,
		value.InstanceID, value.WorkerRoleARN, value.WorkerPrincipalID, value.HelperID, value.SignerKeyID,
		value.PublicKeyDigest, plan.Partition, plan.AccountID, plan.Region, plan.Name, plan.VersionID,
		plan.KMSKeyARN, plan.TargetPath, value.NonceDigest}
	for _, field := range fields {
		if field == "" || strings.ContainsAny(field, "\x00\r\n") {
			return nil, ErrAgentCloudControlInvalidResponse
		}
	}
	expectedName := "dtx/" + value.AgentInstanceID + "/deployments/" + value.DeploymentID + "/__dirextalk_root_helper_key"
	if value.BindingRevision < 1 || plan.FileMode != 0o400 ||
		(plan.Partition != "aws" && plan.Partition != "aws-us-gov" && plan.Partition != "aws-cn") ||
		!rootHelperAccountPattern.MatchString(plan.AccountID) || !rootHelperRegionPattern.MatchString(plan.Region) ||
		plan.Name != expectedName || plan.VersionID != value.DeliveryID ||
		!strings.HasPrefix(plan.KMSKeyARN, "arn:"+plan.Partition+":kms:"+plan.Region+":"+plan.AccountID+":key/") ||
		plan.TargetPath != "/etc/dirextalk-root-helper/signing.key" {
		return nil, ErrAgentCloudControlInvalidResponse
	}
	document := signingScope{
		value.SchemaVersion, value.AgentInstanceID, value.OwnerID, value.DeliveryID, value.DeploymentID,
		value.BindingRevision, value.InstanceID, value.WorkerRoleARN, value.WorkerPrincipalID, value.HelperID,
		value.SignerKeyID, value.PublicKeyDigest, plan.Partition, plan.AccountID, plan.Region, plan.Name,
		plan.VersionID, plan.KMSKeyARN, plan.TargetPath, plan.FileMode, value.NonceDigest,
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var projected any
	if err = decoder.Decode(&projected); err != nil {
		return nil, err
	}
	projected, err = normalizeManagedPreparationJSONNumbers(projected)
	if err != nil {
		return nil, err
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return mode.Marshal(projected)
}

// RootHelperKeyBindingSigningPayload reconstructs the Agent's versioned,
// deterministic CBOR without accepting private key material.
func RootHelperKeyBindingSigningPayload(value AgentRootHelperKeyBinding) ([]byte, error) {
	return rootHelperKeyBindingSigningPayload(value)
}

func rootHelperKeyApprovalToPublic(value AgentRootHelperKeyApproval) rootHelperKeyPublicApproval {
	return rootHelperKeyPublicApproval{
		SchemaVersion: value.SchemaVersion, ChallengeID: value.ChallengeID, DeviceSignerKeyID: value.DeviceSignerKeyID,
		Binding: value.Binding, PublicKey: base64.RawURLEncoding.EncodeToString(value.PublicKey),
		Nonce: base64.RawURLEncoding.EncodeToString(value.Nonce), SigningPayloadDigest: value.SigningPayloadDigest,
		Status: value.Status, Revision: value.Revision, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	}
}

func decodeRootHelperKeyPublicApproval(raw any) (rootHelperKeyPublicApproval, []byte, error) {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) > 128*1024 {
		return rootHelperKeyPublicApproval{}, nil, ErrAgentCloudControlInvalid
	}
	var value rootHelperKeyPublicApproval
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&value); err != nil {
		return rootHelperKeyPublicApproval{}, nil, ErrAgentCloudControlInvalid
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return rootHelperKeyPublicApproval{}, nil, ErrAgentCloudControlInvalid
	}
	publicKey, publicErr := base64.RawURLEncoding.DecodeString(value.PublicKey)
	nonce, nonceErr := base64.RawURLEncoding.DecodeString(value.Nonce)
	signature, signatureErr := base64.RawURLEncoding.DecodeString(value.DeviceSignature)
	payload, payloadErr := rootHelperKeyBindingSigningPayload(value.Binding)
	if publicErr != nil || nonceErr != nil || signatureErr != nil || payloadErr != nil ||
		len(publicKey) != 32 || len(nonce) != 32 || len(signature) != 64 ||
		value.SigningPayloadDigest != digestRootHelperKeyPayload(payload) ||
		value.SchemaVersion != rootHelperKeyApprovalSchema || value.Status != "awaiting_approval" || value.Revision != 1 {
		return rootHelperKeyPublicApproval{}, nil, ErrAgentCloudControlInvalid
	}
	publicDigest, nonceDigest := sha256.Sum256(publicKey), sha256.Sum256(nonce)
	if value.Binding.PublicKeyDigest != "sha256:"+hex.EncodeToString(publicDigest[:]) ||
		value.Binding.NonceDigest != "sha256:"+hex.EncodeToString(nonceDigest[:]) {
		return rootHelperKeyPublicApproval{}, nil, ErrAgentCloudControlInvalid
	}
	return value, signature, nil
}

func sameRootHelperKeyPublicApproval(value AgentRootHelperKeyApproval, expected rootHelperKeyPublicApproval) bool {
	actual := rootHelperKeyApprovalToPublic(value)
	return actual.SchemaVersion == expected.SchemaVersion && actual.ChallengeID == expected.ChallengeID &&
		actual.DeviceSignerKeyID == expected.DeviceSignerKeyID && reflect.DeepEqual(actual.Binding, expected.Binding) &&
		actual.PublicKey == expected.PublicKey && actual.Nonce == expected.Nonce &&
		actual.SigningPayloadDigest == expected.SigningPayloadDigest && actual.CreatedAt.Equal(expected.CreatedAt)
}

func rootHelperKeyRedactedView(serviceID string, value AgentRootHelperKeyApproval) map[string]any {
	return map[string]any{
		"service_id": serviceID, "delivery_id": value.Binding.DeliveryID, "deployment_id": value.Binding.DeploymentID,
		"status": value.Status, "revision": value.Revision, "signing_payload_digest": value.SigningPayloadDigest,
		"created_at": value.CreatedAt.UTC().Format(time.RFC3339Nano), "updated_at": value.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func digestRootHelperKeyPayload(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func rootHelperKeyAgentError(err error, found bool) *actionbase.Error {
	switch {
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return rootHelperKeyRequestError()
	case errors.Is(err, ErrAgentCloudControlConflict), errors.Is(err, ErrAgentCloudControlRejected):
		return rootHelperKeyConflictError()
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return rootHelperKeyInvalidResponseError()
	case err != nil:
		return unavailableError()
	case !found:
		return actionbase.CodedError(http.StatusNotFound, cloudServiceRootHelperKeyInvalidCode, "cloud root helper key approval was not found")
	default:
		return unavailableError()
	}
}

func rootHelperKeyRequestError() *actionbase.Error {
	return actionbase.CodedError(http.StatusBadRequest, cloudServiceRootHelperKeyInvalidCode, "cloud root helper key request is invalid")
}

func rootHelperKeyConflictError() *actionbase.Error {
	return actionbase.CodedError(http.StatusConflict, cloudServiceRootHelperKeyConflictCode, "cloud root helper key request conflicts with current state")
}

func rootHelperKeyInvalidResponseError() *actionbase.Error {
	return actionbase.CodedError(http.StatusBadGateway, cloudServiceRootHelperKeyInvalidCode, "cloud Agent returned an invalid root helper key response")
}
