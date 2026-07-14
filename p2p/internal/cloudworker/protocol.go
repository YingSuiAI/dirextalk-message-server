package cloudworker

import (
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
	"time"
)

const (
	maxIdentityDocumentBytes  = 64 * 1024
	maxIdentitySignatureBytes = 32 * 1024
)

var safeCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)

// InstanceIdentityProof is the opaque, signed VM identity material obtained
// by a future bootstrap adapter from IMDSv2. It is not an AWS credential and
// this package deliberately does not contact or parse AWS metadata.
type InstanceIdentityProof struct {
	DocumentB64  string `json:"instance_identity_document_b64"`
	SignatureB64 string `json:"instance_identity_signature_b64"`
}

func (proof InstanceIdentityProof) Validate() error {
	if !validCanonicalBase64(proof.DocumentB64, maxIdentityDocumentBytes) ||
		!validCanonicalBase64(proof.SignatureB64, maxIdentitySignatureBytes) {
		return errors.New("worker identity proof is invalid")
	}
	return nil
}

// ClaimRequest binds the one-time session to the immutable manifests before a
// short-lived event token can be returned. No secret or cloud credential is
// accepted in the request body.
type ClaimRequest struct {
	Schema                 string `json:"schema"`
	ConnectionID           string `json:"connection_id"`
	DeploymentID           string `json:"deployment_id"`
	BootstrapSessionID     string `json:"bootstrap_session_id"`
	WorkerImageDigest      string `json:"worker_image_digest"`
	ArtifactManifestDigest string `json:"artifact_manifest_digest"`
	InstanceIdentityProof
}

func NewClaimRequest(manifest BootstrapManifest, proof InstanceIdentityProof) (ClaimRequest, error) {
	if err := proof.Validate(); err != nil {
		return ClaimRequest{}, err
	}
	request := ClaimRequest{
		Schema:                 WorkerSessionClaimV1Schema,
		ConnectionID:           manifest.ConnectionID,
		DeploymentID:           manifest.DeploymentID,
		BootstrapSessionID:     manifest.BootstrapSessionID,
		WorkerImageDigest:      manifest.WorkerImageDigest,
		ArtifactManifestDigest: manifest.ArtifactManifestDigest,
		InstanceIdentityProof:  proof,
	}
	return request, request.Validate()
}

func ParseClaimRequest(raw []byte) (ClaimRequest, error) {
	var request ClaimRequest
	if err := decodeStrictObject(raw, &request); err != nil {
		return ClaimRequest{}, errors.New("worker claim request is invalid")
	}
	if err := request.Validate(); err != nil {
		return ClaimRequest{}, err
	}
	return request, nil
}

func (request ClaimRequest) Validate() error {
	if request.Schema != WorkerSessionClaimV1Schema || !validIdentifier(request.ConnectionID) ||
		!validIdentifier(request.DeploymentID) || !validIdentifier(request.BootstrapSessionID) ||
		!validNamedSHA256(request.WorkerImageDigest) || !validNamedSHA256(request.ArtifactManifestDigest) {
		return errors.New("worker claim request is invalid")
	}
	return request.InstanceIdentityProof.Validate()
}

type EventKind string

const (
	EventKindHeartbeat  EventKind = "heartbeat"
	EventKindCheckpoint EventKind = "checkpoint"
	EventKindReport     EventKind = "report"
)

type ReportStatus string

const (
	ReportStatusInstalling           ReportStatus = "installing"
	ReportStatusWaitingUser          ReportStatus = "waiting_user"
	ReportStatusLocalReadyUnverified ReportStatus = "local_ready_unverified"
	ReportStatusSucceeded            ReportStatus = "succeeded"
	ReportStatusFailed               ReportStatus = "failed"
	ReportStatusInterrupted          ReportStatus = "interrupted"
)

// SessionEvent carries only bounded state. Free-form output, shell commands,
// URLs, logs, pairing material, and credential-bearing fields are absent by
// construction and must use a later, separately authorized artifact path.
type SessionEvent struct {
	Schema             string       `json:"schema"`
	ConnectionID       string       `json:"connection_id"`
	DeploymentID       string       `json:"deployment_id"`
	BootstrapSessionID string       `json:"bootstrap_session_id"`
	LeaseEpoch         uint64       `json:"lease_epoch"`
	Sequence           uint64       `json:"sequence"`
	Kind               EventKind    `json:"kind"`
	Checkpoint         string       `json:"checkpoint,omitempty"`
	ReportStatus       ReportStatus `json:"report_status,omitempty"`
	ErrorCode          string       `json:"error_code,omitempty"`
	EvidenceDigest     string       `json:"evidence_digest,omitempty"`
	OccurredAt         string       `json:"occurred_at"`
}

func ParseSessionEvent(raw []byte) (SessionEvent, error) {
	var event SessionEvent
	if err := decodeStrictObject(raw, &event); err != nil {
		return SessionEvent{}, errors.New("worker event is invalid")
	}
	if err := event.Validate(); err != nil {
		return SessionEvent{}, err
	}
	return event, nil
}

func (event SessionEvent) Validate() error {
	if event.Schema != WorkerEventV1Schema || !validIdentifier(event.ConnectionID) ||
		!validIdentifier(event.DeploymentID) || !validIdentifier(event.BootstrapSessionID) ||
		event.LeaseEpoch == 0 || event.Sequence == 0 {
		return errors.New("worker event is invalid")
	}
	if _, err := parseCanonicalInstant(event.OccurredAt); err != nil {
		return errors.New("worker event is invalid")
	}
	if event.Checkpoint != "" && !safeCodePattern.MatchString(event.Checkpoint) {
		return errors.New("worker event is invalid")
	}
	if event.ErrorCode != "" && !safeCodePattern.MatchString(event.ErrorCode) {
		return errors.New("worker event is invalid")
	}
	if event.EvidenceDigest != "" && !validNamedSHA256(event.EvidenceDigest) {
		return errors.New("worker event is invalid")
	}
	switch event.Kind {
	case EventKindHeartbeat:
		if event.Checkpoint != "" || event.ReportStatus != "" || event.ErrorCode != "" || event.EvidenceDigest != "" {
			return errors.New("worker heartbeat is invalid")
		}
	case EventKindCheckpoint:
		if event.Checkpoint == "" || event.ReportStatus != "" || event.ErrorCode != "" {
			return errors.New("worker checkpoint is invalid")
		}
	case EventKindReport:
		if !validReportStatus(event.ReportStatus) || event.Checkpoint != "" {
			return errors.New("worker report is invalid")
		}
		if event.ReportStatus == ReportStatusFailed && event.ErrorCode == "" {
			return errors.New("worker failed report is invalid")
		}
		if event.ReportStatus != ReportStatusFailed && event.ErrorCode != "" {
			return errors.New("worker report is invalid")
		}
	default:
		return errors.New("worker event kind is invalid")
	}
	return nil
}

// EventReceipt is the only server response accepted to advance the local
// sequence. An indeterminate result leaves the exact pending event available
// for a safe same-sequence retry.
type EventReceipt struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	DeploymentID       string `json:"deployment_id"`
	BootstrapSessionID string `json:"bootstrap_session_id"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
	Sequence           uint64 `json:"sequence"`
	Disposition        string `json:"disposition"`
}

func (receipt EventReceipt) ValidateFor(event SessionEvent) error {
	if receipt.Schema != WorkerEventReceiptV1Schema || receipt.ConnectionID != event.ConnectionID ||
		receipt.DeploymentID != event.DeploymentID || receipt.BootstrapSessionID != event.BootstrapSessionID ||
		receipt.LeaseEpoch != event.LeaseEpoch || receipt.Sequence != event.Sequence ||
		(receipt.Disposition != "accepted" && receipt.Disposition != "idempotent") {
		return errors.New("worker event receipt is invalid")
	}
	return nil
}

type claimResponse struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	DeploymentID       string `json:"deployment_id"`
	BootstrapSessionID string `json:"bootstrap_session_id"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
	LeaseExpiresAt     string `json:"lease_expires_at"`
	AccessToken        string `json:"access_token"`
}

func (response claimResponse) ValidateFor(manifest BootstrapManifest, now time.Time) error {
	if response.Schema != WorkerSessionClaimResponseV1Schema || response.ConnectionID != manifest.ConnectionID ||
		response.DeploymentID != manifest.DeploymentID || response.BootstrapSessionID != manifest.BootstrapSessionID ||
		response.LeaseEpoch == 0 || !validOpaqueToken(response.AccessToken) {
		return errors.New("worker claim response is invalid")
	}
	expiresAt, err := parseCanonicalInstant(response.LeaseExpiresAt)
	if err != nil || !expiresAt.After(now.UTC()) || expiresAt.Sub(now.UTC()) > maxBootstrapManifestLifetime {
		return errors.New("worker claim response is invalid")
	}
	return nil
}

func validCanonicalBase64(value string, maximum int) bool {
	if value == "" || len(value) > maximum*2 || strings.TrimSpace(value) != value {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(value)
	return err == nil && len(decoded) > 0 && len(decoded) <= maximum && base64.StdEncoding.EncodeToString(decoded) == value
}

func validOpaqueToken(value string) bool {
	if len(value) < 16 || len(value) > 4096 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e || character == '"' || character == '\\' {
			return false
		}
	}
	return true
}

func validReportStatus(value ReportStatus) bool {
	switch value {
	case ReportStatusInstalling, ReportStatusWaitingUser, ReportStatusLocalReadyUnverified,
		ReportStatusSucceeded, ReportStatusFailed, ReportStatusInterrupted:
		return true
	default:
		return false
	}
}
