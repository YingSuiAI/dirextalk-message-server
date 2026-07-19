// Package stackteardown owns the closed cleanup contract for one user-owned
// Connection Stack. It deliberately accepts a Connection ID plus a reviewed
// plan; callers cannot select AWS ARNs, resource names, or API operations.
package stackteardown

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const (
	PlanSchema             = "dirextalk.connection-stack-teardown-plan/v1"
	defaultKeyDeletionDays = int32(7)
)

var (
	ErrInvalidRequest            = errors.New("connection stack teardown request is invalid")
	ErrStackNotFound             = errors.New("connection stack was not found")
	ErrPlanProvenanceUnavailable = errors.New("connection stack teardown plan provenance is unavailable")
	ErrPlanStale                 = errors.New("connection stack teardown plan is stale")
	ErrProviderUnavailable       = errors.New("connection stack teardown provider is unavailable")
	ErrProviderForbidden         = errors.New("connection stack teardown provider is forbidden")
	ErrUntrackedRetainedResource = errors.New("connection stack has an untracked retained resource")
	identifierPattern            = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	regionPattern                = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]?$`)
	stackARNPattern              = regexp.MustCompile(`^arn:(?:aws|aws-us-gov|aws-cn):cloudformation:([a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]?):[0-9]{12}:stack/([A-Za-z][-A-Za-z0-9]*)/[0-9A-Fa-f-]+$`)
	tableNamePattern             = regexp.MustCompile(`^[A-Za-z0-9_.-]{3,255}$`)
	bucketNamePattern            = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	kmsKeyIDPattern              = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)
	kmsKeyARNPattern             = regexp.MustCompile(`^arn:(?:aws|aws-us-gov|aws-cn):kms:([a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]?):[0-9]{12}:key/([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})$`)
	amiIDPattern                 = regexp.MustCompile(`^ami-[0-9a-f]{8}(?:[0-9a-f]{9})?$`)
	snapshotIDPattern            = regexp.MustCompile(`^snap-[0-9a-f]{8}(?:[0-9a-f]{9})?$`)
)

// Request contains the only caller-selected values. The stack name is always
// derived from ConnectionID; no arbitrary CloudFormation stack identifier is
// accepted here.
type Request struct {
	ConnectionID string `json:"connection_id"`
	Region       string `json:"region"`
}

func (request Request) Validate() error {
	if !identifierPattern.MatchString(request.ConnectionID) || !regionPattern.MatchString(request.Region) {
		return ErrInvalidRequest
	}
	return nil
}

func (request Request) stackName() string {
	// Keep this derivation local so the independently operated cleanup binary
	// has no runtime dependency on credential-bootstrap code. The byte contract
	// is intentionally identical to the reviewed Role Plan derivation.
	sum := sha256.Sum256([]byte("dirextalk-connection-stack/v1\x00" + request.ConnectionID))
	return "dirextalk-connection-" + hex.EncodeToString(sum[:12])
}

type ResourceKind string

const (
	ResourceDynamoDBTable ResourceKind = "dynamodb_table"
	ResourceS3Bucket      ResourceKind = "s3_bucket"
	ResourceKMSKey        ResourceKind = "kms_key"
	ResourceKMSAlias      ResourceKind = "kms_alias"
)

type ResourceRef struct {
	LogicalID  string       `json:"logical_id"`
	Kind       ResourceKind `json:"kind"`
	Identifier string       `json:"identifier"`
}

// Plan is a typed, CloudFormation-derived cleanup manifest. It is never an
// API passthrough: resource identifiers are accepted only when their logical
// IDs belong to this reviewed template and Execute rechecks their provenance
// from CloudFormation before issuing a mutation.
type Plan struct {
	Schema                string        `json:"schema"`
	ConnectionID          string        `json:"connection_id"`
	Region                string        `json:"region"`
	StackName             string        `json:"stack_name"`
	StackID               string        `json:"stack_id"`
	RetainedResources     []ResourceRef `json:"retained_resources"`
	ManagedImageIDs       []string      `json:"managed_image_ids"`
	ManagedSnapshotIDs    []string      `json:"managed_snapshot_ids"`
	KMSDeletionWindowDays int32         `json:"kms_deletion_window_days"`
}

func (plan Plan) request() Request {
	return Request{ConnectionID: plan.ConnectionID, Region: plan.Region}
}

func (plan Plan) Validate() error {
	request := plan.request()
	if plan.Schema != PlanSchema || request.Validate() != nil || plan.StackName != request.stackName() || plan.KMSDeletionWindowDays != defaultKeyDeletionDays {
		return ErrInvalidRequest
	}
	stackParts := stackARNPattern.FindStringSubmatch(plan.StackID)
	if len(stackParts) != 3 || stackParts[1] != plan.Region || stackParts[2] != plan.StackName {
		return ErrInvalidRequest
	}
	if len(plan.RetainedResources) == 0 || len(plan.RetainedResources) > len(retainedTemplateResources) || !resourceRefsSorted(plan.RetainedResources) || !sortedUnique(plan.ManagedImageIDs) || !sortedUnique(plan.ManagedSnapshotIDs) {
		return ErrInvalidRequest
	}
	seen := make(map[string]struct{}, len(plan.RetainedResources))
	for _, resource := range plan.RetainedResources {
		kind, known := retainedTemplateResources[resource.LogicalID]
		if !known || kind != resource.Kind {
			return ErrInvalidRequest
		}
		if _, duplicate := seen[resource.LogicalID]; duplicate || !validResourceIdentifier(plan, resource) {
			return ErrInvalidRequest
		}
		seen[resource.LogicalID] = struct{}{}
	}
	for logicalID := range requiredDynamoTables {
		if _, found := seen[logicalID]; !found {
			return ErrInvalidRequest
		}
	}
	if !optionalResourceGroupsValid(seen) {
		return ErrInvalidRequest
	}
	for _, imageID := range plan.ManagedImageIDs {
		if !amiIDPattern.MatchString(imageID) {
			return ErrInvalidRequest
		}
	}
	for _, snapshotID := range plan.ManagedSnapshotIDs {
		if !snapshotIDPattern.MatchString(snapshotID) {
			return ErrInvalidRequest
		}
	}
	return nil
}

var requiredDynamoTables = map[string]struct{}{
	"CommandReceiptsTable": {}, "ConnectionCountersTable": {}, "IssuedQuotesTable": {},
	"DeploymentReservationsTable": {}, "DeploymentDestroyTable": {}, "ServiceBackupsTable": {},
	"ServiceRestoresTable": {}, "ApprovalUsesTable": {}, "WorkerSessionsTable": {},
	"WorkerTasksTable": {}, "ServiceReadinessTasksTable": {},
}

var retainedTemplateResources = map[string]ResourceKind{
	"CommandReceiptsTable":         ResourceDynamoDBTable,
	"ConnectionCountersTable":      ResourceDynamoDBTable,
	"IssuedQuotesTable":            ResourceDynamoDBTable,
	"DeploymentReservationsTable":  ResourceDynamoDBTable,
	"DeploymentDestroyTable":       ResourceDynamoDBTable,
	"ServiceBackupsTable":          ResourceDynamoDBTable,
	"ServiceRestoresTable":         ResourceDynamoDBTable,
	"ApprovalUsesTable":            ResourceDynamoDBTable,
	"WorkerSessionsTable":          ResourceDynamoDBTable,
	"WorkerTasksTable":             ResourceDynamoDBTable,
	"ServiceReadinessTasksTable":   ResourceDynamoDBTable,
	"ServiceSecretSessionsTable":   ResourceDynamoDBTable,
	"DynamicArtifactsTable":        ResourceDynamoDBTable,
	"ServiceSecretSealingKey":      ResourceKMSKey,
	"ServiceSecretSealingKeyAlias": ResourceKMSAlias,
	"DynamicArtifactKey":           ResourceKMSKey,
	"DynamicArtifactBucket":        ResourceS3Bucket,
}

func optionalResourceGroupsValid(seen map[string]struct{}) bool {
	serviceKey := hasResource(seen, "ServiceSecretSealingKey")
	serviceAlias := hasResource(seen, "ServiceSecretSealingKeyAlias")
	serviceTable := hasResource(seen, "ServiceSecretSessionsTable")
	if serviceKey != serviceAlias || serviceKey != serviceTable {
		return false
	}
	dynamicKey := hasResource(seen, "DynamicArtifactKey")
	dynamicBucket := hasResource(seen, "DynamicArtifactBucket")
	dynamicTable := hasResource(seen, "DynamicArtifactsTable")
	return dynamicKey == dynamicBucket && dynamicKey == dynamicTable
}

func hasResource(resources map[string]struct{}, logicalID string) bool {
	_, found := resources[logicalID]
	return found
}

func validResourceIdentifier(plan Plan, resource ResourceRef) bool {
	switch resource.Kind {
	case ResourceDynamoDBTable:
		return tableNamePattern.MatchString(resource.Identifier) && strings.HasPrefix(resource.Identifier, plan.StackName+"-"+resource.LogicalID+"-")
	case ResourceS3Bucket:
		// CloudFormation truncates generated S3 physical names to the service's
		// 63-character limit, so there is no stable logical-ID prefix to check.
		// Execute re-derives this identifier from the exact Stack ID before any
		// S3 mutation.
		return bucketNamePattern.MatchString(resource.Identifier)
	case ResourceKMSKey:
		if kmsKeyIDPattern.MatchString(resource.Identifier) {
			return true
		}
		parts := kmsKeyARNPattern.FindStringSubmatch(resource.Identifier)
		return len(parts) == 3 && parts[1] == plan.Region
	case ResourceKMSAlias:
		return resource.LogicalID == "ServiceSecretSealingKeyAlias" && resource.Identifier == "alias/dirextalk/"+plan.StackName+"/service-secret-sessions"
	default:
		return false
	}
}

func resourceRefsSorted(values []ResourceRef) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1].LogicalID >= values[index].LogicalID {
			return false
		}
	}
	return true
}

func sortedUnique(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] >= values[index] {
			return false
		}
	}
	return true
}

// ParsePlan is intentionally strict because a persisted plan is an operator
// hand-off artifact. Execute still refreshes CloudFormation provenance before
// mutations, so a syntactically valid edited plan cannot redirect cleanup.
func ParsePlan(raw []byte) (Plan, error) {
	var plan Plan
	if strictDecode(raw, &plan) != nil || plan.Validate() != nil {
		return Plan{}, ErrInvalidRequest
	}
	return plan, nil
}

func planDigest(plan Plan) string {
	raw, _ := json.Marshal(plan)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func strictDecode(raw []byte, target any) error {
	if len(raw) == 0 || rejectDuplicateKeys(raw) != nil {
		return ErrInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

func scanJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		keys := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrInvalidRequest
			}
			if _, duplicate := keys[key]; duplicate {
				return ErrInvalidRequest
			}
			keys[key] = struct{}{}
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("%w: unexpected JSON delimiter", ErrInvalidRequest)
	}
}

func sortedResourceRefs(values []ResourceRef) []ResourceRef {
	result := append([]ResourceRef(nil), values...)
	sort.Slice(result, func(left, right int) bool { return result[left].LogicalID < result[right].LogicalID })
	return result
}

func sortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
