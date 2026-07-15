package recipeexec

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	TaskClaimV1Schema         = "dirextalk.recipe-execution-task-claim/v1"
	TaskClaimResponseV1Schema = "dirextalk.recipe-execution-task-claim-response/v1"
	EventReceiptV1Schema      = "dirextalk.recipe-execution-task-event-receipt/v1"
	RecipeArtifactMediaTypeV1 = "application/x-tar"
	MaxRecipeArtifactBytesV1  = int64(256 << 20)
)

var (
	recipeArtifactArchiveSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	recipeArtifactVersionPattern       = regexp.MustCompile(`^[A-Za-z0-9._~+=/-]+$`)
)

type TaskClaimRequestV1 struct {
	Schema     string `json:"schema"`
	LeaseEpoch uint64 `json:"lease_epoch"`
}

func NewTaskClaimRequestV1(leaseEpoch uint64) (TaskClaimRequestV1, error) {
	request := TaskClaimRequestV1{Schema: TaskClaimV1Schema, LeaseEpoch: leaseEpoch}
	if request.LeaseEpoch == 0 || request.LeaseEpoch > maxTaskSafeInteger {
		return TaskClaimRequestV1{}, ErrTaskInvalid
	}
	return request, nil
}

type TaskClaimResponseV1 struct {
	Schema         string                                       `json:"schema"`
	Status         string                                       `json:"status"`
	LeaseEpoch     uint64                                       `json:"lease_epoch"`
	Task           *TaskV1                                      `json:"task,omitempty"`
	Manifest       *cloudorchestrator.RecipeExecutionManifestV1 `json:"manifest,omitempty"`
	ArtifactAccess *ArtifactAccessV1                            `json:"artifact_access,omitempty"`
}

// ArtifactAccessV1 is an in-memory, single-claim download grant. Callers must
// never persist or log URL because its query carries temporary authorization.
type ArtifactAccessV1 struct {
	Method        string `json:"method"`
	URL           string `json:"url"`
	ExpiresAt     string `json:"expires_at"`
	VersionID     string `json:"version_id"`
	MediaType     string `json:"media_type"`
	SizeBytes     int64  `json:"size_bytes"`
	ArchiveSHA256 string `json:"archive_sha256"`
}

func (access ArtifactAccessV1) Validate() error {
	expiresAt, timeErr := time.Parse("2006-01-02T15:04:05.000Z", access.ExpiresAt)
	parsed, urlErr := url.ParseRequestURI(access.URL)
	query, queryErr := url.ParseQuery(parsedRawQuery(parsed))
	versions := query["versionId"]
	if access.Method != "GET" || len(access.URL) == 0 || len(access.URL) > 8192 || urlErr != nil || parsed == nil ||
		parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || parsed.Opaque != "" ||
		strings.ContainsAny(access.URL, "\r\n\x00\\") ||
		timeErr != nil || expiresAt.UTC().Format("2006-01-02T15:04:05.000Z") != access.ExpiresAt ||
		len(access.VersionID) == 0 || len(access.VersionID) > 1024 || !recipeArtifactVersionPattern.MatchString(access.VersionID) ||
		queryErr != nil || len(versions) != 1 || versions[0] != access.VersionID ||
		access.MediaType != RecipeArtifactMediaTypeV1 || access.SizeBytes < 1 || access.SizeBytes > MaxRecipeArtifactBytesV1 ||
		!recipeArtifactArchiveSHA256Pattern.MatchString(access.ArchiveSHA256) {
		return ErrTaskInvalid
	}
	return nil
}

func parsedRawQuery(parsed *url.URL) string {
	if parsed == nil {
		return ""
	}
	return parsed.RawQuery
}

func ParseTaskClaimResponseV1(raw []byte, expectedLeaseEpoch uint64) (TaskClaimResponseV1, error) {
	type wireResponse struct {
		Schema         string          `json:"schema"`
		Status         string          `json:"status"`
		LeaseEpoch     uint64          `json:"lease_epoch"`
		Task           json.RawMessage `json:"task,omitempty"`
		Manifest       json.RawMessage `json:"manifest,omitempty"`
		ArtifactAccess json.RawMessage `json:"artifact_access,omitempty"`
	}
	var wire wireResponse
	if err := decodeStrictTaskObject(raw, &wire); err != nil || requireTaskObjectFields(raw, "schema", "status", "lease_epoch") != nil ||
		wire.Schema != TaskClaimResponseV1Schema || wire.LeaseEpoch != expectedLeaseEpoch || !validTaskPositive(wire.LeaseEpoch) {
		return TaskClaimResponseV1{}, ErrTaskInvalid
	}
	fields, err := taskObjectFields(raw)
	if err != nil {
		return TaskClaimResponseV1{}, ErrTaskInvalid
	}
	result := TaskClaimResponseV1{Schema: wire.Schema, Status: wire.Status, LeaseEpoch: wire.LeaseEpoch}
	switch wire.Status {
	case "none":
		if _, taskPresent := fields["task"]; taskPresent {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		if _, manifestPresent := fields["manifest"]; manifestPresent {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		if _, accessPresent := fields["artifact_access"]; accessPresent {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		return result, nil
	case "artifact_pending":
		if _, taskPresent := fields["task"]; taskPresent {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		if _, manifestPresent := fields["manifest"]; manifestPresent {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		if _, accessPresent := fields["artifact_access"]; accessPresent {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		return result, nil
	case "claimed":
		if len(wire.Task) == 0 || len(wire.Manifest) == 0 || bytes.Equal(bytes.TrimSpace(wire.Task), []byte("null")) || bytes.Equal(bytes.TrimSpace(wire.Manifest), []byte("null")) {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		task, err := ParseTaskV1(wire.Task)
		if err != nil {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		var manifest cloudorchestrator.RecipeExecutionManifestV1
		if _, err := taskObjectFields(wire.Manifest); err != nil {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		if err := decodeStrictTaskObject(wire.Manifest, &manifest); err != nil || manifest.Validate() != nil || task.ValidateForManifest(manifest) != nil {
			return TaskClaimResponseV1{}, ErrTaskInvalid
		}
		result.Task, result.Manifest = &task, &manifest
		if _, present := fields["artifact_access"]; present {
			if len(wire.ArtifactAccess) == 0 || bytes.Equal(bytes.TrimSpace(wire.ArtifactAccess), []byte("null")) ||
				requireTaskObjectFields(wire.ArtifactAccess, "method", "url", "expires_at", "version_id", "media_type", "size_bytes", "archive_sha256") != nil {
				return TaskClaimResponseV1{}, ErrTaskInvalid
			}
			var access ArtifactAccessV1
			if err := decodeStrictTaskObject(wire.ArtifactAccess, &access); err != nil || access.Validate() != nil {
				return TaskClaimResponseV1{}, ErrTaskInvalid
			}
			result.ArtifactAccess = &access
		}
		return result, nil
	default:
		return TaskClaimResponseV1{}, ErrTaskInvalid
	}
}

type EventReceiptV1 struct {
	Schema      string `json:"schema"`
	TaskID      string `json:"task_id"`
	Attempt     uint64 `json:"attempt"`
	LeaseEpoch  uint64 `json:"lease_epoch"`
	Sequence    uint64 `json:"sequence"`
	Disposition string `json:"disposition"`
}

func ParseEventReceiptV1(raw []byte, event EventV1) (EventReceiptV1, error) {
	var receipt EventReceiptV1
	if err := decodeStrictTaskObject(raw, &receipt); err != nil || requireTaskObjectFields(raw,
		"schema", "task_id", "attempt", "lease_epoch", "sequence", "disposition") != nil ||
		receipt.Schema != EventReceiptV1Schema || receipt.TaskID != event.TaskID || receipt.Attempt != event.Attempt ||
		receipt.LeaseEpoch != event.LeaseEpoch || receipt.Sequence != event.Sequence ||
		(receipt.Disposition != "accepted" && receipt.Disposition != "idempotent") {
		return EventReceiptV1{}, errors.New("recipe task event receipt is invalid")
	}
	return receipt, nil
}
