package cloudworker

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/fixedprobe"
)

const (
	ServiceReadinessTaskClaimV1Schema         = "dirextalk.service-readiness-task-claim/v1"
	ServiceReadinessTaskClaimResponseV1Schema = "dirextalk.service-readiness-task-claim-response/v1"
	ServiceReadinessTaskV1Schema              = "dirextalk.service-readiness-task/v1"
	ServiceReadinessChallengeV1Schema         = "dirextalk.service-readiness-challenge/v1"
	ServiceReadinessTaskEventV1Schema         = "dirextalk.service-readiness-task-event/v1"
	ServiceReadinessTaskEventReceiptV1Schema  = "dirextalk.service-readiness-task-event-receipt/v1"
	ServiceReadinessProbeKind                 = "stack_witnessed_oci_semantic_probe_v1"
	maxServiceReadinessChallengeLifetime      = 5 * time.Minute
)

type ServiceReadinessProbeV1 struct {
	Scheme         string `json:"scheme"`
	Port           uint16 `json:"port"`
	Path           string `json:"path"`
	ExpectedStatus uint16 `json:"expected_status"`
	BodySHA256     string `json:"body_sha256"`
}

func (probe ServiceReadinessProbeV1) validate() error {
	if (probe.Scheme != "http" && probe.Scheme != "https") || probe.Port == 0 || probe.ExpectedStatus < 100 || probe.ExpectedStatus > 599 ||
		!validNamedSHA256(probe.BodySHA256) || len(probe.Path) == 0 || len(probe.Path) > 256 || !strings.HasPrefix(probe.Path, "/") ||
		strings.HasPrefix(probe.Path, "//") || strings.ContainsAny(probe.Path, "?#\\") || strings.Contains(probe.Path, "..") {
		return errors.New("service readiness probe is invalid")
	}
	parsed, err := url.ParseRequestURI(probe.Path)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("service readiness probe is invalid")
	}
	return nil
}

type ServiceReadinessTaskClaimRequestV1 struct {
	Schema     string `json:"schema"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

func NewServiceReadinessTaskClaimRequestV1(epoch uint64) (ServiceReadinessTaskClaimRequestV1, error) {
	request := ServiceReadinessTaskClaimRequestV1{Schema: ServiceReadinessTaskClaimV1Schema, LeaseEpoch: epoch}
	if !validTaskPositive(epoch) {
		return ServiceReadinessTaskClaimRequestV1{}, errors.New("service readiness task claim is invalid")
	}
	return request, nil
}

type ServiceReadinessTaskV1 struct {
	Schema                        string                  `json:"schema"`
	TaskID                        string                  `json:"task_id"`
	ExecutionID                   string                  `json:"execution_id"`
	DeploymentID                  string                  `json:"deployment_id"`
	ServiceID                     string                  `json:"service_id"`
	ProbeKind                     string                  `json:"probe_kind"`
	RecipeExecutionManifestDigest string                  `json:"recipe_execution_manifest_digest"`
	InstallEvidenceDigest         string                  `json:"install_evidence_digest"`
	ArtifactDigest                string                  `json:"artifact_digest"`
	SemanticProbe                 ServiceReadinessProbeV1 `json:"semantic_probe"`
	SemanticExpectationDigest     string                  `json:"semantic_expectation_digest"`
	Attempt                       uint64                  `json:"attempt"`
	LastSequence                  uint64                  `json:"last_sequence"`
}

func (task ServiceReadinessTaskV1) validate(manifest BootstrapManifest) error {
	if task.Schema != ServiceReadinessTaskV1Schema || !validIdentifier(task.TaskID) || !validIdentifier(task.ExecutionID) ||
		task.DeploymentID != manifest.DeploymentID || !validIdentifier(task.ServiceID) || task.ProbeKind != ServiceReadinessProbeKind ||
		!validNamedSHA256(task.RecipeExecutionManifestDigest) || !validNamedSHA256(task.InstallEvidenceDigest) || !validNamedSHA256(task.ArtifactDigest) ||
		task.SemanticProbe.validate() != nil || task.SemanticExpectationDigest != task.SemanticProbe.BodySHA256 || !validTaskPositive(task.Attempt) ||
		!validTaskNonnegative(task.LastSequence) {
		return errors.New("service readiness task is invalid")
	}
	return nil
}

type ServiceReadinessChallengeV1 struct {
	Schema          string `json:"schema"`
	ChallengeBase64 string `json:"challenge_b64"`
	ChallengeDigest string `json:"challenge_digest"`
	ExpiresAt       string `json:"expires_at"`
}

func (challenge ServiceReadinessChallengeV1) validate(now time.Time) error {
	decoded, err := base64.StdEncoding.DecodeString(challenge.ChallengeBase64)
	if err != nil || len(decoded) != 32 || base64.StdEncoding.EncodeToString(decoded) != challenge.ChallengeBase64 ||
		challenge.Schema != ServiceReadinessChallengeV1Schema || challenge.ChallengeDigest != namedSHA256(decoded) {
		return errors.New("service readiness challenge is invalid")
	}
	expiresAt, err := parseCanonicalInstant(challenge.ExpiresAt)
	if err != nil || !expiresAt.After(now.UTC()) || expiresAt.After(now.UTC().Add(maxServiceReadinessChallengeLifetime)) {
		return errors.New("service readiness challenge is invalid")
	}
	return nil
}

type ServiceReadinessTaskClaimResponseV1 struct {
	Schema     string                       `json:"schema"`
	Status     string                       `json:"status"`
	LeaseEpoch uint64                       `json:"lease_epoch"`
	Task       *ServiceReadinessTaskV1      `json:"task,omitempty"`
	Challenge  *ServiceReadinessChallengeV1 `json:"challenge,omitempty"`
}

func ParseServiceReadinessTaskClaimResponseV1(raw []byte, manifest BootstrapManifest, epoch uint64, now time.Time) (ServiceReadinessTaskClaimResponseV1, error) {
	var response ServiceReadinessTaskClaimResponseV1
	if err := decodeStrictObject(raw, &response); err != nil || requireTaskFields(raw, "schema", "status", "lease_epoch") != nil ||
		response.Schema != ServiceReadinessTaskClaimResponseV1Schema || response.LeaseEpoch != epoch || !validTaskPositive(epoch) {
		return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
	}
	fields, err := taskRawFields(raw)
	if err != nil {
		return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
	}
	switch response.Status {
	case "none":
		if _, ok := fields["task"]; ok || response.Task != nil {
			return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
		}
		if _, ok := fields["challenge"]; ok || response.Challenge != nil {
			return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
		}
	case "claimed":
		if response.Task == nil || response.Challenge == nil || bytes.Equal(bytes.TrimSpace(fields["task"]), []byte("null")) ||
			bytes.Equal(bytes.TrimSpace(fields["challenge"]), []byte("null")) {
			return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
		}
		task, err := parseServiceReadinessTaskV1(fields["task"], manifest)
		if err != nil {
			return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
		}
		challenge, err := parseServiceReadinessChallengeV1(fields["challenge"], now)
		if err != nil {
			return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
		}
		response.Task, response.Challenge = &task, &challenge
	default:
		return ServiceReadinessTaskClaimResponseV1{}, errors.New("service readiness task claim response is invalid")
	}
	return response, nil
}

func parseServiceReadinessTaskV1(raw []byte, manifest BootstrapManifest) (ServiceReadinessTaskV1, error) {
	var task ServiceReadinessTaskV1
	if err := decodeStrictObject(raw, &task); err != nil || requireTaskFields(raw, "schema", "task_id", "execution_id", "deployment_id", "service_id", "probe_kind",
		"recipe_execution_manifest_digest", "install_evidence_digest", "artifact_digest", "semantic_probe", "semantic_expectation_digest", "attempt", "last_sequence") != nil ||
		requireNonNullTaskFields(raw, "attempt", "last_sequence") != nil || task.validate(manifest) != nil {
		return ServiceReadinessTaskV1{}, errors.New("service readiness task is invalid")
	}
	return task, nil
}

func parseServiceReadinessChallengeV1(raw []byte, now time.Time) (ServiceReadinessChallengeV1, error) {
	var challenge ServiceReadinessChallengeV1
	if err := decodeStrictObject(raw, &challenge); err != nil || requireTaskFields(raw, "schema", "challenge_b64", "challenge_digest", "expires_at") != nil || challenge.validate(now) != nil {
		return ServiceReadinessChallengeV1{}, errors.New("service readiness challenge is invalid")
	}
	return challenge, nil
}

type ServiceReadinessTaskStatus string

const (
	ServiceReadinessTaskSucceeded   ServiceReadinessTaskStatus = "succeeded"
	ServiceReadinessTaskFailed      ServiceReadinessTaskStatus = "failed"
	ServiceReadinessTaskInterrupted ServiceReadinessTaskStatus = "interrupted"
)

type ServiceReadinessTaskEventV1 struct {
	Schema                 string                     `json:"schema"`
	TaskID                 string                     `json:"task_id"`
	Attempt                uint64                     `json:"attempt"`
	LeaseEpoch             uint64                     `json:"lease_epoch"`
	Sequence               uint64                     `json:"sequence"`
	Status                 ServiceReadinessTaskStatus `json:"status"`
	ChallengeDigest        *string                    `json:"challenge_digest"`
	SemanticEvidenceDigest *string                    `json:"semantic_evidence_digest"`
	ErrorCode              *string                    `json:"error_code"`
	OccurredAt             string                     `json:"occurred_at"`
}

func (event ServiceReadinessTaskEventV1) validate(task ServiceReadinessTaskV1, challenge ServiceReadinessChallengeV1, epoch uint64) error {
	if event.Schema != ServiceReadinessTaskEventV1Schema || event.TaskID != task.TaskID || event.Attempt != task.Attempt ||
		event.LeaseEpoch != epoch || !validTaskPositive(epoch) || event.Sequence != task.LastSequence+1 || !validTaskPositive(event.Sequence) {
		return errors.New("service readiness task event is invalid")
	}
	if _, err := parseCanonicalInstant(event.OccurredAt); err != nil {
		return errors.New("service readiness task event is invalid")
	}
	switch event.Status {
	case ServiceReadinessTaskSucceeded:
		if event.ChallengeDigest == nil || *event.ChallengeDigest != challenge.ChallengeDigest || event.SemanticEvidenceDigest == nil ||
			*event.SemanticEvidenceDigest != task.SemanticProbe.BodySHA256 || event.ErrorCode != nil {
			return errors.New("service readiness task event is invalid")
		}
	case ServiceReadinessTaskFailed, ServiceReadinessTaskInterrupted:
		if event.ChallengeDigest != nil || event.SemanticEvidenceDigest != nil || event.ErrorCode == nil || !safeCodePattern.MatchString(*event.ErrorCode) {
			return errors.New("service readiness task event is invalid")
		}
	default:
		return errors.New("service readiness task event is invalid")
	}
	return nil
}

func ParseServiceReadinessTaskEventV1(raw []byte, task ServiceReadinessTaskV1, challenge ServiceReadinessChallengeV1, epoch uint64) (ServiceReadinessTaskEventV1, error) {
	var event ServiceReadinessTaskEventV1
	if err := decodeStrictObject(raw, &event); err != nil || requireTaskFields(raw, "schema", "task_id", "attempt", "lease_epoch", "sequence", "status", "challenge_digest", "semantic_evidence_digest", "error_code", "occurred_at") != nil ||
		requireNonNullTaskFields(raw, "attempt", "lease_epoch", "sequence") != nil || event.validate(task, challenge, epoch) != nil {
		return ServiceReadinessTaskEventV1{}, errors.New("service readiness task event is invalid")
	}
	return event, nil
}

type ServiceReadinessTaskEventReceiptV1 struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"task_id"`
	Attempt     uint64 `json:"attempt"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
	Sequence    uint64 `json:"sequence"`
	Disposition string `json:"disposition"`
}

func ParseServiceReadinessTaskEventReceiptV1(raw []byte, event ServiceReadinessTaskEventV1) (ServiceReadinessTaskEventReceiptV1, error) {
	var receipt ServiceReadinessTaskEventReceiptV1
	if err := decodeStrictObject(raw, &receipt); err != nil || requireTaskFields(raw, "schema", "task_id", "attempt", "lease_epoch", "sequence", "disposition") != nil ||
		receipt.Schema != ServiceReadinessTaskEventReceiptV1Schema || receipt.TaskID != event.TaskID || receipt.Attempt != event.Attempt ||
		receipt.LeaseEpoch != event.LeaseEpoch || receipt.Sequence != event.Sequence || (receipt.Disposition != "accepted" && receipt.Disposition != "idempotent") {
		return ServiceReadinessTaskEventReceiptV1{}, errors.New("service readiness task event receipt is invalid")
	}
	return receipt, nil
}

func FixedReadinessEvidenceDigest() string { return namedSHA256([]byte(fixedprobe.ReadinessBody)) }

func namedSHA256(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])
}
