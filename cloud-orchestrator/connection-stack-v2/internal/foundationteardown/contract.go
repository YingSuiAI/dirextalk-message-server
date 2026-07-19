// Package foundationteardown implements the deliberately narrow cleanup
// contract for a Connection Foundation.  It is not a general AWS cleanup
// facility: the only input is the independently persisted Foundation facts.
package foundationteardown

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionfoundation"
)

const (
	RequestSchema = "dirextalk.connection-foundation-teardown-request/v1"
	PlanSchema    = "dirextalk.connection-foundation-teardown-plan/v1"

	// AWS KMS enforces a minimum seven-day waiting period.  A scheduled key is
	// intentionally reported as pending_key_deletion, never destroyed.
	KMSDeletionWindowDays int32 = 7
)

var (
	ErrInvalidRequest       = errors.New("connection foundation teardown request is invalid")
	ErrInvalidPlan          = errors.New("connection foundation teardown plan is invalid")
	ErrPlanStale            = errors.New("connection foundation teardown plan no longer matches the foundation")
	ErrProviderUnavailable  = errors.New("connection foundation teardown provider is unavailable")
	ErrProviderForbidden    = errors.New("connection foundation teardown provider access is denied")
	ErrArtifactBucketBlocked = errors.New("connection foundation artifact bucket contains an unmanaged object")
	ErrTeardownBlocked      = errors.New("connection foundation teardown is blocked")
)

var (
	eipAllocationPattern = regexp.MustCompile(`^eipalloc-[0-9a-z]+$`)
	associationPattern   = regexp.MustCompile(`^rtbassoc-[0-9a-z]+$`)
	releaseVersionRE     = regexp.MustCompile(`^v?(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*$`)
	digestHexRE          = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Request has no independent account, Region, resource ID, route, or bucket
// input.  Every deletion target is bound to the Foundation facts produced by
// the creation read-back and is revalidated against the AWS account.
type Request struct {
	Schema string                      `json:"schema"`
	Facts  connectionfoundation.Facts `json:"facts"`
}

func NewRequest(facts connectionfoundation.Facts) (Request, error) {
	request := Request{Schema: RequestSchema, Facts: facts}
	if request.Validate() != nil {
		return Request{}, ErrInvalidRequest
	}
	return request, nil
}

func (request Request) Validate() error {
	if request.Schema != RequestSchema || request.Facts.Validate() != nil {
		return ErrInvalidRequest
	}
	return nil
}

// ParseRequest is strict so operator hand-off data cannot introduce an
// unreviewed resource identifier or an unknown cleanup capability.
func ParseRequest(raw []byte) (Request, error) {
	var request Request
	if strictDecode(raw, &request) != nil || request.Validate() != nil {
		return Request{}, ErrInvalidRequest
	}
	return request, nil
}

// ParseFacts supports the existing durable Foundation facts record while
// applying the same duplicate-key and unknown-field rejection as Request.
func ParseFacts(raw []byte) (connectionfoundation.Facts, error) {
	var facts connectionfoundation.Facts
	if strictDecode(raw, &facts) != nil || facts.Validate() != nil {
		return connectionfoundation.Facts{}, ErrInvalidRequest
	}
	return facts, nil
}

// Plan persists only the prior facts and values discovered during the
// creation read-back that are not part of Facts (the NAT Elastic IP and two
// explicit route-table association IDs).  Execute re-reads them before every
// mutation, so altering a persisted plan cannot redirect deletion.
type Plan struct {
	Schema                string                      `json:"schema"`
	Facts                 connectionfoundation.Facts `json:"facts"`
	NATEIPAllocationID    string                      `json:"nat_eip_allocation_id"`
	PublicAssociationID   string                      `json:"public_association_id"`
	PrivateAssociationID  string                      `json:"private_association_id"`
	KMSDeletionWindowDays int32                       `json:"kms_deletion_window_days"`
}

func (plan Plan) Validate() error {
	if plan.Schema != PlanSchema || plan.Facts.Validate() != nil ||
		!eipAllocationPattern.MatchString(plan.NATEIPAllocationID) ||
		!associationPattern.MatchString(plan.PublicAssociationID) ||
		!associationPattern.MatchString(plan.PrivateAssociationID) ||
		plan.PublicAssociationID == plan.PrivateAssociationID ||
		plan.KMSDeletionWindowDays != KMSDeletionWindowDays {
		return ErrInvalidPlan
	}
	return nil
}

func ParsePlan(raw []byte) (Plan, error) {
	var plan Plan
	if strictDecode(raw, &plan) != nil || plan.Validate() != nil {
		return Plan{}, ErrInvalidPlan
	}
	return plan, nil
}

func planDigest(plan Plan) string {
	raw, _ := json.Marshal(plan)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// State is a durable user-visible cleanup fact.  pending_key_deletion is
// deliberately distinct from verified_destroyed because KMS retains the key
// for its mandatory waiting window.
type State string

const (
	StatePlanned            State = "planned"
	StateDestroying         State = "destroying"
	StateVerifiedDestroyed  State = "verified_destroyed"
	StatePendingKeyDeletion State = "pending_key_deletion"
	StateBlocked            State = "blocked"
)

type ResourceKind string

const (
	ResourceArtifactBucket          ResourceKind = "s3_bucket"
	ResourceArtifactKMSKey          ResourceKind = "kms_key"
	ResourceWorkerSecurityGroup     ResourceKind = "ec2_security_group"
	ResourcePublicRouteAssociation  ResourceKind = "ec2_route_association"
	ResourcePrivateRouteAssociation ResourceKind = "ec2_route_association"
	ResourcePublicRoute             ResourceKind = "ec2_route"
	ResourcePrivateRoute            ResourceKind = "ec2_route"
	ResourceNATGateway              ResourceKind = "ec2_nat_gateway"
	ResourceNATEIP                  ResourceKind = "ec2_elastic_ip"
	ResourcePublicRouteTable        ResourceKind = "ec2_route_table"
	ResourcePrivateRouteTable       ResourceKind = "ec2_route_table"
	ResourceInternetGateway         ResourceKind = "ec2_internet_gateway"
	ResourcePublicSubnet            ResourceKind = "ec2_subnet"
	ResourcePrivateSubnet           ResourceKind = "ec2_subnet"
	ResourceVPC                     ResourceKind = "ec2_vpc"
)

type ItemReport struct {
	LogicalID  string       `json:"logical_id"`
	Kind       ResourceKind `json:"kind"`
	Identifier string       `json:"identifier"`
	State      State        `json:"state"`
}

type Report struct {
	PlanDigest string       `json:"plan_digest"`
	State      State        `json:"state"`
	Items      []ItemReport `json:"items"`
}

// ArtifactObject is one version or delete marker in the Foundation bucket.
// The provider never accepts a bucket/key from callers: it emits these only
// from ListObjectVersions for the Foundation bucket in Plan.
type ArtifactObject struct {
	Key          string
	VersionID    string
	DeleteMarker bool
}

// Observation contains only independently read cloud facts.  Empty IDs with
// Present=false represent normal asynchronous deletion; every Present=true
// record must match the reviewed tags and topology before it is actionable.
type Observation struct {
	AccountID string
	Region    string

	VPC                 VPCObservation
	PublicSubnet        SubnetObservation
	PrivateSubnet       SubnetObservation
	InternetGateway     InternetGatewayObservation
	NATGateway          NATGatewayObservation
	NATEIP              EIPObservation
	PublicRoute         RouteTableObservation
	PrivateRoute        RouteTableObservation
	WorkerSecurityGroup SecurityGroupObservation
	ArtifactBucket      BucketObservation
	ArtifactKMSKey      KMSKeyObservation
	ArtifactObjects     []ArtifactObject
}

type VPCObservation struct {
	Present bool
	ID      string
	OwnerID string
	Tags    map[string]string
}

type SubnetObservation struct {
	Present bool
	ID      string
	OwnerID string
	VPCID   string
	Tags    map[string]string
}

type InternetGatewayObservation struct {
	Present bool
	ID      string
	OwnerID string
	VPCID   string
	Tags    map[string]string
}

type NATGatewayObservation struct {
	Present         bool
	ID              string
	SubnetID        string
	EIPAllocationID string
	State           string
	Tags            map[string]string
}

type EIPObservation struct {
	Present      bool
	AllocationID string
	Tags         map[string]string
}

type RouteTableObservation struct {
	Present         bool
	ID              string
	OwnerID         string
	VPCID           string
	AssociationID   string
	AssociationTo   string
	AssociationMain bool
	DefaultTarget   string
	Tags            map[string]string
}

type SecurityGroupObservation struct {
	Present bool
	ID      string
	OwnerID string
	VPCID   string
	Tags    map[string]string
}

type BucketObservation struct {
	Present bool
	Name    string
	Tags    map[string]string
}

type KMSKeyObservation struct {
	Present bool
	ARN     string
	State   string
	Tags    map[string]string
}

type cleanupStep uint8

const (
	stepEmptyArtifactObjects cleanupStep = iota + 1
	stepDeleteArtifactBucket
	stepScheduleKMSKeyDeletion
	stepDeleteWorkerSecurityGroup
	stepDisassociatePublicRoute
	stepDisassociatePrivateRoute
	stepDeletePublicDefaultRoute
	stepDeletePrivateDefaultRoute
	stepDeleteNATGateway
	stepReleaseNATEIP
	stepDeletePublicRouteTable
	stepDeletePrivateRouteTable
	stepDetachInternetGateway
	stepDeleteInternetGateway
	stepDeletePublicSubnet
	stepDeletePrivateSubnet
	stepDeleteVPC
)

type teardownProvider interface {
	Observe(ctx context.Context, facts connectionfoundation.Facts) (Observation, error)
	Apply(ctx context.Context, plan Plan, step cleanupStep) error
}

func strictDecode(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > 128<<10 || rejectDuplicateKeys(raw) != nil {
		return ErrInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
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
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalidRequest
			}
			if _, found := seen[key]; found {
				return ErrInvalidRequest
			}
			seen[key] = struct{}{}
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalidRequest
		}
	case '[':
		for decoder.More() {
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidRequest
		}
	default:
		return fmt.Errorf("%w: unexpected JSON delimiter", ErrInvalidRequest)
	}
	return nil
}

func expectedNames(facts connectionfoundation.Facts) map[string]string {
	hash := sha256.Sum256([]byte("dirextalk.connection-foundation/v1\x00" + facts.ConnectionID + "\x00" + facts.AccountID + "\x00" + facts.Region))
	prefix := "dirextalk-cf-" + hex.EncodeToString(hash[:8])
	return map[string]string{
		"vpc":              prefix + "-vpc",
		"public_subnet":    prefix + "-public",
		"private_subnet":   prefix + "-private",
		"internet_gateway": prefix + "-igw",
		"nat_gateway":      prefix + "-nat",
		"nat_eip":          prefix + "-nat-eip",
		"public_route":     prefix + "-public-rt",
		"private_route":    prefix + "-private-rt",
		"worker_group":     prefix + "-worker",
		"artifact_bucket":  prefix + "-artifacts",
		"artifact_kms":     prefix + "-artifacts-kms",
	}
}

func expectedTags(facts connectionfoundation.Facts, name string) map[string]string {
	return map[string]string{
		"Name":                         name,
		"dirextalk:managed":           "true",
		"dirextalk:component":         "connection-foundation",
		"dirextalk:connection-id":     facts.ConnectionID,
		"dirextalk:foundation-schema": connectionfoundation.FactsSchema,
	}
}

func requiredTagsMatch(actual, expected map[string]string) bool {
	for key, expectedValue := range expected {
		if actual == nil || actual[key] != expectedValue {
			return false
		}
	}
	return true
}

func controlledArtifact(object ArtifactObject) bool {
	if object.Key == "" || object.VersionID == "" || object.VersionID == "null" || len(object.VersionID) > 1024 {
		return false
	}
	parts := strings.Split(object.Key, "/")
	if len(parts) != 4 || parts[0] != "releases" || !releaseVersionRE.MatchString(parts[2]) {
		return false
	}
	version := parts[2]
	lower := strings.ToLower(version)
	if version == "v1.0.3" || version == "1.0.3" || strings.Contains(lower, "latest") {
		return false
	}
	var prefix, extension string
	switch parts[1] {
	case "broker":
		prefix, extension = "broker", ".zip"
	case "worker":
		prefix, extension = "worker", ".tar"
	case "connection-stack":
		prefix, extension = "connection-stack", ".yaml"
	default:
		return false
	}
	wantPrefix := prefix + "-" + version + "-"
	if !strings.HasPrefix(parts[3], wantPrefix) || !strings.HasSuffix(parts[3], extension) {
		return false
	}
	digest := strings.TrimSuffix(strings.TrimPrefix(parts[3], wantPrefix), extension)
	return digestHexRE.MatchString(digest)
}

func sortedArtifacts(values []ArtifactObject) []ArtifactObject {
	result := append([]ArtifactObject(nil), values...)
	sort.Slice(result, func(left, right int) bool {
		if result[left].Key != result[right].Key {
			return result[left].Key < result[right].Key
		}
		if result[left].VersionID != result[right].VersionID {
			return result[left].VersionID < result[right].VersionID
		}
		return !result[left].DeleteMarker && result[right].DeleteMarker
	})
	return result
}
