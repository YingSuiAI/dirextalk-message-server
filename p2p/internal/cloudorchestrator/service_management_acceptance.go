package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

const (
	ServiceManagementAcceptanceIntent  = "service_management_acceptance"
	ServiceManagementAcceptancePolicy  = "manual_verified_v1"
	maxServiceManagementAcceptanceLife = 5 * time.Minute
)

// ServiceManagementAcceptanceTargetV1 binds a maturity change to evidence
// already persisted by the control plane. It grants no Worker or AWS action.
type ServiceManagementAcceptanceTargetV1 struct {
	AcceptanceID                    string              `json:"acceptance_id"`
	ServiceID                       string              `json:"service_id"`
	ServiceRevision                 uint64              `json:"service_revision"`
	DeploymentID                    string              `json:"deployment_id"`
	DeploymentRevision              uint64              `json:"deployment_revision"`
	CloudConnectionID               string              `json:"cloud_connection_id"`
	RecipeID                        string              `json:"recipe_id"`
	RecipeDigest                    string              `json:"recipe_digest"`
	RecipeRevision                  uint64              `json:"recipe_revision"`
	RecipeMaturity                  RecipeMaturity      `json:"recipe_maturity"`
	InstalledManifestDigest         string              `json:"installed_manifest_digest"`
	ArtifactDigest                  string              `json:"artifact_digest"`
	ReadinessSemanticEvidenceDigest string              `json:"readiness_semantic_evidence_digest"`
	ReadinessStackObservationDigest string              `json:"readiness_stack_observation_digest"`
	RestartOperationID              string              `json:"restart_operation_id"`
	RestartOperationRevision        uint64              `json:"restart_operation_revision"`
	BackupID                        string              `json:"backup_id"`
	BackupRevision                  uint64              `json:"backup_revision"`
	RestoreID                       string              `json:"restore_id"`
	RestoreRevision                 uint64              `json:"restore_revision"`
	SourceArtifactDigests           []string            `json:"source_artifact_digests"`
	Health                          HealthContractV1    `json:"health"`
	Lifecycle                       LifecycleContractV1 `json:"lifecycle"`
	VolumeSlots                     []VolumeSlotV1      `json:"volume_slots"`
	DataSlots                       []DataSlotV1        `json:"data_slots"`
	SecretSlots                     []SecretSlotV1      `json:"secret_slots"`
	DestroyInstanceID               string              `json:"destroy_instance_id"`
	DestroyVolumeIDs                []string            `json:"destroy_volume_ids"`
	DestroyNetworkInterfaceIDs      []string            `json:"destroy_network_interface_ids"`
	AcceptancePolicy                string              `json:"acceptance_policy"`
}

type ServiceManagementAcceptanceApprovalV1 struct {
	SchemaVersion string `json:"schema_version"`
	Intent        string `json:"intent"`
	ApprovalID    string `json:"approval_id"`
	ChallengeID   string `json:"challenge_id"`
	SignerKeyID   string `json:"signer_key_id"`
	ServiceManagementAcceptanceTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Signature string    `json:"signature,omitempty"`
}

type serviceManagementAcceptancePayloadV1 struct {
	SchemaVersion  string `json:"schema_version"`
	PayloadVersion string `json:"payload_version"`
	HashAlgorithm  string `json:"hash_algorithm"`
	Intent         string `json:"intent"`
	ApprovalID     string `json:"approval_id"`
	ChallengeID    string `json:"challenge_id"`
	SignerKeyID    string `json:"signer_key_id"`
	ServiceManagementAcceptanceTargetV1
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

var ErrServiceManagementAcceptanceBinding = errors.New("service management acceptance does not match verified evidence")

func (t ServiceManagementAcceptanceTargetV1) Validate() error {
	for label, value := range map[string]string{
		"acceptance_id": t.AcceptanceID, "service_id": t.ServiceID, "deployment_id": t.DeploymentID,
		"cloud_connection_id": t.CloudConnectionID, "recipe_id": t.RecipeID, "restart_operation_id": t.RestartOperationID,
		"backup_id": t.BackupID, "restore_id": t.RestoreID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if t.ServiceRevision == 0 || t.DeploymentRevision == 0 || t.RecipeRevision == 0 || t.RestartOperationRevision == 0 || t.BackupRevision == 0 || t.RestoreRevision == 0 {
		return errors.New("service management acceptance revisions must be positive")
	}
	if t.RecipeMaturity != RecipeAwaitingManagementAccept && t.RecipeMaturity != RecipeManaged {
		return errors.New("service management acceptance recipe maturity is invalid")
	}
	for label, digest := range map[string]string{"recipe_digest": t.RecipeDigest, "installed_manifest_digest": t.InstalledManifestDigest, "artifact_digest": t.ArtifactDigest, "readiness_semantic_evidence_digest": t.ReadinessSemanticEvidenceDigest, "readiness_stack_observation_digest": t.ReadinessStackObservationDigest} {
		if err := validateDigest(label, digest); err != nil {
			return err
		}
	}
	if len(t.SourceArtifactDigests) == 0 {
		return errors.New("service management acceptance requires locked sources")
	}
	for _, digest := range t.SourceArtifactDigests {
		if err := validateDigest("source_artifact_digest", digest); err != nil {
			return err
		}
	}
	if t.Health.validate() != nil || t.Lifecycle.validate() != nil {
		return errors.New("service management health or lifecycle contract is invalid")
	}
	if err := validateVolumeSlots(t.VolumeSlots); err != nil {
		return err
	}
	if err := validateDataSlots(t.DataSlots); err != nil {
		return err
	}
	if err := validateSecretSlots(t.SecretSlots); err != nil {
		return err
	}
	if !ec2InstanceIDPattern.MatchString(t.DestroyInstanceID) || validateProviderResourceIDs("destroy_volume_ids", t.DestroyVolumeIDs, ebsVolumeIDPattern) != nil || validateProviderResourceIDs("destroy_network_interface_ids", t.DestroyNetworkInterfaceIDs, ec2NetworkInterfaceIDPattern) != nil {
		return errors.New("service management destroy scope is invalid")
	}
	if t.AcceptancePolicy != ServiceManagementAcceptancePolicy {
		return errors.New("service management acceptance policy is invalid")
	}
	return nil
}

func NewServiceManagementAcceptanceApprovalV1(target ServiceManagementAcceptanceTargetV1, approvalID, challengeID, signerKeyID string, issuedAt, expiresAt time.Time) (ServiceManagementAcceptanceApprovalV1, error) {
	target = normalizeServiceManagementAcceptanceTarget(target)
	if err := target.Validate(); err != nil {
		return ServiceManagementAcceptanceApprovalV1{}, fmt.Errorf("invalid service management acceptance target: %w", err)
	}
	a := ServiceManagementAcceptanceApprovalV1{SchemaVersion: SchemaVersionV1, Intent: ServiceManagementAcceptanceIntent, ApprovalID: approvalID, ChallengeID: challengeID, SignerKeyID: signerKeyID, ServiceManagementAcceptanceTargetV1: target, IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC()}
	if err := a.Validate(); err != nil {
		return ServiceManagementAcceptanceApprovalV1{}, err
	}
	return a, nil
}

func (a ServiceManagementAcceptanceApprovalV1) Validate() error { return a.validate(false) }
func (a ServiceManagementAcceptanceApprovalV1) ValidateAt(now time.Time) error {
	if err := a.validate(true); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) {
		return errors.New("service management acceptance approval has expired")
	}
	return nil
}
func (a ServiceManagementAcceptanceApprovalV1) validate(requireSignature bool) error {
	if validateSchema(a.SchemaVersion) != nil || a.Intent != ServiceManagementAcceptanceIntent {
		return errors.New("service management acceptance schema or intent is invalid")
	}
	for label, value := range map[string]string{"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if err := a.ServiceManagementAcceptanceTargetV1.Validate(); err != nil {
		return err
	}
	if a.IssuedAt.IsZero() || a.ExpiresAt.IsZero() || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > maxServiceManagementAcceptanceLife {
		return errors.New("service management acceptance expiry is invalid")
	}
	if requireSignature || a.Signature != "" {
		signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
		if err != nil || len(signature) != ed25519.SignatureSize {
			return errors.New("service management acceptance signature is invalid")
		}
	}
	return nil
}

func (a ServiceManagementAcceptanceApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	a.ServiceManagementAcceptanceTargetV1 = normalizeServiceManagementAcceptanceTarget(a.ServiceManagementAcceptanceTargetV1)
	return canonicalCBOR(serviceManagementAcceptancePayloadV1{SchemaVersion: a.SchemaVersion, PayloadVersion: "service-management-acceptance-signing-payload/v1", HashAlgorithm: HashAlgorithmDeterministicCBORSHA256, Intent: a.Intent, ApprovalID: a.ApprovalID, ChallengeID: a.ChallengeID, SignerKeyID: a.SignerKeyID, ServiceManagementAcceptanceTargetV1: a.ServiceManagementAcceptanceTargetV1, IssuedAt: a.IssuedAt.UTC(), ExpiresAt: a.ExpiresAt.UTC()})
}

func (a ServiceManagementAcceptanceApprovalV1) Sign(key ed25519.PrivateKey, now time.Time) (ServiceManagementAcceptanceApprovalV1, error) {
	if len(key) != ed25519.PrivateKeySize || a.validate(false) != nil || !a.ExpiresAt.After(now.UTC()) {
		return ServiceManagementAcceptanceApprovalV1{}, errors.New("service management acceptance cannot be signed")
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return ServiceManagementAcceptanceApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	return a, nil
}

func (a ServiceManagementAcceptanceApprovalV1) Verify(key ed25519.PublicKey, now time.Time) error {
	if len(key) != ed25519.PublicKeySize {
		return errors.New("service management acceptance key is invalid")
	}
	if err := a.ValidateAt(now); err != nil {
		return err
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(a.Signature)
	if !ed25519.Verify(key, payload, signature) {
		return errors.New("service management acceptance signature is invalid")
	}
	return nil
}

func (a ServiceManagementAcceptanceApprovalV1) ValidateAgainst(target ServiceManagementAcceptanceTargetV1, now time.Time) error {
	if a.ValidateAt(now) != nil || target.Validate() != nil || !reflect.DeepEqual(normalizeServiceManagementAcceptanceTarget(a.ServiceManagementAcceptanceTargetV1), normalizeServiceManagementAcceptanceTarget(target)) {
		return ErrServiceManagementAcceptanceBinding
	}
	return nil
}

func normalizeServiceManagementAcceptanceTarget(t ServiceManagementAcceptanceTargetV1) ServiceManagementAcceptanceTargetV1 {
	t.SourceArtifactDigests = canonicalSet(t.SourceArtifactDigests)
	t.VolumeSlots = append([]VolumeSlotV1(nil), t.VolumeSlots...)
	t.DataSlots = append([]DataSlotV1(nil), t.DataSlots...)
	t.SecretSlots = append([]SecretSlotV1(nil), t.SecretSlots...)
	sort.Slice(t.VolumeSlots, func(i, j int) bool { return t.VolumeSlots[i].SlotID < t.VolumeSlots[j].SlotID })
	sort.Slice(t.DataSlots, func(i, j int) bool { return t.DataSlots[i].SlotID < t.DataSlots[j].SlotID })
	sort.Slice(t.SecretSlots, func(i, j int) bool { return t.SecretSlots[i].SlotID < t.SecretSlots[j].SlotID })
	t.DestroyVolumeIDs = canonicalSet(t.DestroyVolumeIDs)
	t.DestroyNetworkInterfaceIDs = canonicalSet(t.DestroyNetworkInterfaceIDs)
	return t
}
