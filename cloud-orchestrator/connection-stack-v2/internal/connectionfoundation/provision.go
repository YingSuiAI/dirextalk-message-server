package connectionfoundation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
)

const (
	ProvisionRequestSchema = "dirextalk.connection-foundation-request/v1"
	FactsSchema            = "dirextalk.connection-foundation-facts/v1"

	foundationComponent = "connection-foundation"
	foundationRevision  = "v1"
)

var (
	ErrInvalidProvisionRequest = errors.New("connection foundation provision request is invalid")
	ErrFoundationProvisioning  = errors.New("connection foundation provisioning failed")
	ErrFoundationReadback      = errors.New("connection foundation readback did not match the reviewed foundation")

	connectionIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{7,63}$`)
	accountIDPattern    = regexp.MustCompile(`^[0-9]{12}$`)
	resourceIDPattern   = regexp.MustCompile(`^(?:vpc|subnet|igw|nat|rtb|sg)-[0-9a-z]+$`)
	kmsKeyARNPattern    = regexp.MustCompile(`^arn:aws(?:-[a-z]+)*:kms:[a-z0-9-]+:[0-9]{12}:key/[0-9a-f-]{36}$`)
)

// ProvisionRequest deliberately has no network CIDR, ingress, IAM, bucket,
// KMS, route, or generic AWS fields. Those values are deterministically
// derived by this package so a root-key bootstrap cannot become an arbitrary
// cloud-resource passthrough.
type ProvisionRequest struct {
	SchemaVersion    string `json:"schema_version"`
	ConnectionID     string `json:"connection_id"`
	AccountID        string `json:"account_id"`
	Region           string `json:"region"`
	AvailabilityZone string `json:"availability_zone"`
}

// ParseProvisionRequest accepts only the closed, typed foundation request.
func ParseProvisionRequest(raw []byte) (ProvisionRequest, error) {
	if len(raw) == 0 || len(raw) > 4<<10 {
		return ProvisionRequest{}, ErrInvalidProvisionRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request ProvisionRequest
	if err := decoder.Decode(&request); err != nil {
		return ProvisionRequest{}, ErrInvalidProvisionRequest
	}
	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		return ProvisionRequest{}, ErrInvalidProvisionRequest
	}
	if err := request.Validate(); err != nil {
		return ProvisionRequest{}, err
	}
	return request, nil
}

func (request ProvisionRequest) Validate() error {
	if request.SchemaVersion != ProvisionRequestSchema ||
		!connectionIDPattern.MatchString(request.ConnectionID) ||
		!accountIDPattern.MatchString(request.AccountID) ||
		!regionPattern.MatchString(request.Region) ||
		!availabilityZonePattern.MatchString(request.AvailabilityZone) ||
		!strings.HasPrefix(request.AvailabilityZone, request.Region) {
		return ErrInvalidProvisionRequest
	}
	return nil
}

// Names are deterministic per connection and never carry caller supplied
// resource names. The S3 bucket is intentionally a stable, globally unique
// hash suffix, rather than a raw connection identifier.
type Names struct {
	Prefix            string
	VPC               string
	PublicSubnet      string
	PrivateSubnet     string
	InternetGateway   string
	NATGateway        string
	PublicRouteTable  string
	PrivateRouteTable string
	WorkerSecurityGrp string
	ArtifactBucket    string
	ArtifactKMSKey    string
}

// NetworkLayout contains the only network topology this MVP permits: a
// public NAT subnet plus a private Worker subnet, with the Worker receiving
// DNS-to-VPC-resolver and HTTPS-only egress and no inbound rules.
type NetworkLayout struct {
	VPCCIDR           string
	PublicSubnetCIDR  string
	PrivateSubnetCIDR string
	DNSResolverCIDR   string
}

// EgressRule is an observed or desired SG rule. CIDR is deliberately IPv4
// only; the Foundation does not create IPv6 routes or public IPv6 addressing.
type EgressRule struct {
	Protocol string
	FromPort int32
	ToPort   int32
	CIDR     string
}

// Spec is derived only from a validated ProvisionRequest. Provider adapters
// receive this concrete capability rather than a generic CloudFormation or
// AWS API request.
type Spec struct {
	Request    ProvisionRequest
	Names      Names
	Network    NetworkLayout
	Tags       map[string]string
	Egress     []EgressRule
	ClientTags map[string]string
}

// ClientToken returns deterministic idempotency tokens for AWS calls that
// support them. Calls which do not have client tokens must instead find their
// exact deterministic tags/names before attempting creation.
func (spec Spec) ClientToken(operation string) string {
	sum := sha256.Sum256([]byte("dirextalk.connection-foundation/v1\x00" + spec.Request.ConnectionID + "\x00" + spec.Request.AccountID + "\x00" + spec.Request.Region + "\x00" + operation))
	return "dtcf-" + hex.EncodeToString(sum[:16])
}

func deriveSpec(request ProvisionRequest) (Spec, error) {
	if err := request.Validate(); err != nil {
		return Spec{}, err
	}
	hash := sha256.Sum256([]byte("dirextalk.connection-foundation/v1\x00" + request.ConnectionID + "\x00" + request.AccountID + "\x00" + request.Region))
	short := hex.EncodeToString(hash[:8])

	// A /22 leaves two fixed /24 subnets and is derived inside 10/8. Existing
	// account topology is read before creation by the AWS adapter; a collision
	// never silently falls back to a caller-selected or broader CIDR.
	second := 64 + int(hash[8])%128
	third := int(hash[9]%64) * 4
	vpc := fmt.Sprintf("10.%d.%d.0/22", second, third)
	publicSubnet := fmt.Sprintf("10.%d.%d.0/24", second, third)
	privateSubnet := fmt.Sprintf("10.%d.%d.0/24", second, third+1)
	dns := fmt.Sprintf("10.%d.%d.2/32", second, third)
	if !layoutValid(NetworkLayout{VPCCIDR: vpc, PublicSubnetCIDR: publicSubnet, PrivateSubnetCIDR: privateSubnet, DNSResolverCIDR: dns}) {
		return Spec{}, ErrInvalidProvisionRequest
	}

	prefix := "dirextalk-cf-" + short
	names := Names{
		Prefix:            prefix,
		VPC:               prefix + "-vpc",
		PublicSubnet:      prefix + "-public",
		PrivateSubnet:     prefix + "-private",
		InternetGateway:   prefix + "-igw",
		NATGateway:        prefix + "-nat",
		PublicRouteTable:  prefix + "-public-rt",
		PrivateRouteTable: prefix + "-private-rt",
		WorkerSecurityGrp: prefix + "-worker",
		ArtifactBucket:    prefix + "-artifacts",
		ArtifactKMSKey:    prefix + "-artifacts-kms",
	}
	tags := map[string]string{
		"dirextalk:managed":           "true",
		"dirextalk:component":         foundationComponent,
		"dirextalk:connection-id":     request.ConnectionID,
		"dirextalk:foundation-schema": FactsSchema,
	}
	egress := []EgressRule{
		{Protocol: "tcp", FromPort: 443, ToPort: 443, CIDR: "0.0.0.0/0"},
		{Protocol: "tcp", FromPort: 53, ToPort: 53, CIDR: dns},
		{Protocol: "udp", FromPort: 53, ToPort: 53, CIDR: dns},
	}
	return Spec{Request: request, Names: names, Network: NetworkLayout{VPCCIDR: vpc, PublicSubnetCIDR: publicSubnet, PrivateSubnetCIDR: privateSubnet, DNSResolverCIDR: dns}, Tags: tags, Egress: egress, ClientTags: map[string]string{"dirextalk:client-token-family": specClientTokenFamily(request)}}, nil
}

func specClientTokenFamily(request ProvisionRequest) string {
	sum := sha256.Sum256([]byte("dirextalk.connection-foundation/v1\x00" + request.ConnectionID + "\x00" + request.AccountID + "\x00" + request.Region))
	return "dtcf-" + hex.EncodeToString(sum[:8])
}

func layoutValid(layout NetworkLayout) bool {
	vpc, err := netip.ParsePrefix(layout.VPCCIDR)
	if err != nil || vpc.Bits() != 22 || !vpc.Addr().Is4() {
		return false
	}
	publicSubnet, err := netip.ParsePrefix(layout.PublicSubnetCIDR)
	if err != nil || publicSubnet.Bits() != 24 || !vpc.Contains(publicSubnet.Addr()) {
		return false
	}
	privateSubnet, err := netip.ParsePrefix(layout.PrivateSubnetCIDR)
	if err != nil || privateSubnet.Bits() != 24 || !vpc.Contains(privateSubnet.Addr()) || publicSubnet.Overlaps(privateSubnet) {
		return false
	}
	dns, err := netip.ParsePrefix(layout.DNSResolverCIDR)
	return err == nil && dns.Bits() == 32 && publicSubnet.Contains(dns.Addr())
}

// Observation is the independent AWS readback shape. It contains the
// material facts needed to prove the actual account resources match Spec.
// In particular, Artifact.KMSKeyARN must be the canonical customer-key ARN,
// never an alias. That exact ARN is used by immutable artifact verification.
type Observation struct {
	VPC                 VPCObservation
	PublicSubnet        SubnetObservation
	PrivateSubnet       SubnetObservation
	InternetGateway     GatewayObservation
	NATGateway          NATGatewayObservation
	PublicRoute         RouteObservation
	PrivateRoute        RouteObservation
	WorkerSecurityGroup SecurityGroupObservation
	ArtifactBucket      BucketObservation
	ArtifactKMSKey      KMSKeyObservation
}

type VPCObservation struct {
	ID           string
	CIDR         string
	DNSSupport   bool
	DNSHostnames bool
	Tags         map[string]string
}

type SubnetObservation struct {
	ID                  string
	VPCID               string
	AvailabilityZone    string
	CIDR                string
	MapPublicIPOnLaunch bool
	Tags                map[string]string
}

type GatewayObservation struct {
	ID    string
	VPCID string
	Tags  map[string]string
}

type NATGatewayObservation struct {
	ID       string
	SubnetID string
	State    string
	Tags     map[string]string
}

type RouteObservation struct {
	RouteTableID       string
	AssociatedSubnetID string
	DestinationCIDR    string
	TargetID           string
	Tags               map[string]string
}

type SecurityGroupObservation struct {
	ID      string
	VPCID   string
	Ingress []EgressRule
	Egress  []EgressRule
	Tags    map[string]string
}

type BucketObservation struct {
	Name                   string
	VersioningEnabled      bool
	AllPublicAccessBlocked bool
	BucketOwnerEnforced    bool
	KMSKeyARN              string
	Tags                   map[string]string
}

type KMSKeyObservation struct {
	ARN         string
	Enabled     bool
	KeySpec     string
	KeyUsage    string
	MultiRegion bool
	Tags        map[string]string
}

// FoundationProvider is the narrow AWS adapter seam. Its only mutation is a
// deterministic Foundation Spec; there is no method that accepts an arbitrary
// AWS action, security group rule, route, policy, or bucket URL.
type FoundationProvider interface {
	Observe(context.Context, Spec) (Observation, bool, error)
	Ensure(context.Context, Spec) error
}

// Provisioner creates at most the reviewed foundation represented by Spec,
// then independently reads it back. A missing, stale, or broadened resource
// never produces usable facts.
type Provisioner struct {
	provider FoundationProvider
}

func NewProvisioner(provider FoundationProvider) (*Provisioner, error) {
	if provider == nil {
		return nil, ErrInvalidProvisionRequest
	}
	return &Provisioner{provider: provider}, nil
}

func (provisioner *Provisioner) Provision(ctx context.Context, request ProvisionRequest) (Facts, error) {
	if provisioner == nil || provisioner.provider == nil {
		return Facts{}, ErrInvalidProvisionRequest
	}
	spec, err := deriveSpec(request)
	if err != nil {
		return Facts{}, err
	}
	observation, found, err := provisioner.provider.Observe(ctx, spec)
	if err != nil {
		return Facts{}, fmt.Errorf("%w: observe foundation: %v", ErrFoundationProvisioning, err)
	}
	if found {
		return factsFromObservation(spec, observation)
	}
	if err := provisioner.provider.Ensure(ctx, spec); err != nil {
		return Facts{}, fmt.Errorf("%w: ensure foundation: %v", ErrFoundationProvisioning, err)
	}
	observation, found, err = provisioner.provider.Observe(ctx, spec)
	if err != nil {
		return Facts{}, fmt.Errorf("%w: read back foundation: %v", ErrFoundationProvisioning, err)
	}
	if !found {
		return Facts{}, ErrFoundationReadback
	}
	return factsFromObservation(spec, observation)
}

// Facts are durable, non-secret outputs used to bind a Worker plan and to
// configure immutable artifact upload/readback. It intentionally contains no
// access key, session token, raw object URL, or worker service credential.
type Facts struct {
	SchemaVersion         string `json:"schema_version"`
	ConnectionID          string `json:"connection_id"`
	AccountID             string `json:"account_id"`
	Region                string `json:"region"`
	AvailabilityZone      string `json:"availability_zone"`
	VPCID                 string `json:"vpc_id"`
	PublicSubnetID        string `json:"public_subnet_id"`
	PrivateSubnetID       string `json:"private_subnet_id"`
	InternetGatewayID     string `json:"internet_gateway_id"`
	NATGatewayID          string `json:"nat_gateway_id"`
	PublicRouteTableID    string `json:"public_route_table_id"`
	PrivateRouteTableID   string `json:"private_route_table_id"`
	WorkerSecurityGroupID string `json:"worker_security_group_id"`
	ArtifactBucket        string `json:"artifact_bucket"`
	ArtifactKMSKeyARN     string `json:"artifact_kms_key_arn"`
	EgressProfile         string `json:"egress_profile"`
	PublicIngressMode     string `json:"public_ingress_mode"`
}

func (facts Facts) Validate() error {
	if facts.SchemaVersion != FactsSchema || !connectionIDPattern.MatchString(facts.ConnectionID) || !accountIDPattern.MatchString(facts.AccountID) ||
		!regionPattern.MatchString(facts.Region) || !availabilityZonePattern.MatchString(facts.AvailabilityZone) || !strings.HasPrefix(facts.AvailabilityZone, facts.Region) ||
		!validResourceID(facts.VPCID, "vpc-") || !validResourceID(facts.PublicSubnetID, "subnet-") || !validResourceID(facts.PrivateSubnetID, "subnet-") ||
		!validResourceID(facts.InternetGatewayID, "igw-") || !validResourceID(facts.NATGatewayID, "nat-") || !validResourceID(facts.PublicRouteTableID, "rtb-") || !validResourceID(facts.PrivateRouteTableID, "rtb-") || !validResourceID(facts.WorkerSecurityGroupID, "sg-") ||
		!bucketPattern.MatchString(facts.ArtifactBucket) || !kmsKeyARNPattern.MatchString(facts.ArtifactKMSKeyARN) ||
		!strings.Contains(facts.ArtifactKMSKeyARN, ":kms:"+facts.Region+":"+facts.AccountID+":") ||
		facts.EgressProfile != PrivateNATHTTPSDNSEgress || facts.PublicIngressMode != NoPublicIngress {
		return ErrFoundationReadback
	}
	return nil
}

// BindWorker converts independently read-back Foundation facts into the
// Worker portion of a Connection Stack Plan. The worker archive must live in
// this Foundation's versioned, KMS-encrypted bucket; cross-bucket references
// and mutable raw URLs are rejected before Stack creation.
func (facts Facts) BindWorker(worker Worker) (Plan, error) {
	if err := facts.Validate(); err != nil || worker.Artifact.Bucket != facts.ArtifactBucket {
		return Plan{}, ErrFoundationReadback
	}
	plan := Plan{
		SchemaVersion: PlanSchema,
		Region:        facts.Region,
		Network: Network{
			VPCID:                 facts.VPCID,
			SubnetID:              facts.PrivateSubnetID,
			WorkerSecurityGroupID: facts.WorkerSecurityGroupID,
			AvailabilityZone:      facts.AvailabilityZone,
			PrivateSubnet:         true,
			EgressProfile:         facts.EgressProfile,
			PublicIngressMode:     facts.PublicIngressMode,
		},
		Worker: worker,
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, ErrFoundationReadback
	}
	return plan, nil
}

// ArtifactPolicy returns the only immutable-artifact target that can be used
// with this Foundation. KMSKeyID is deliberately the canonical customer key
// ARN obtained from S3/KMS readback, never an alias or caller-provided value.
func (facts Facts) ArtifactPolicy() (artifactpublish.Policy, error) {
	if err := facts.Validate(); err != nil {
		return artifactpublish.Policy{}, ErrFoundationReadback
	}
	policy := artifactpublish.Policy{Bucket: facts.ArtifactBucket, KMSKeyID: facts.ArtifactKMSKeyARN}
	if err := policy.Validate(); err != nil {
		return artifactpublish.Policy{}, ErrFoundationReadback
	}
	return policy, nil
}

func factsFromObservation(spec Spec, observation Observation) (Facts, error) {
	if err := validateObservation(spec, observation); err != nil {
		return Facts{}, ErrFoundationReadback
	}
	facts := Facts{
		SchemaVersion:         FactsSchema,
		ConnectionID:          spec.Request.ConnectionID,
		AccountID:             spec.Request.AccountID,
		Region:                spec.Request.Region,
		AvailabilityZone:      spec.Request.AvailabilityZone,
		VPCID:                 observation.VPC.ID,
		PublicSubnetID:        observation.PublicSubnet.ID,
		PrivateSubnetID:       observation.PrivateSubnet.ID,
		InternetGatewayID:     observation.InternetGateway.ID,
		NATGatewayID:          observation.NATGateway.ID,
		PublicRouteTableID:    observation.PublicRoute.RouteTableID,
		PrivateRouteTableID:   observation.PrivateRoute.RouteTableID,
		WorkerSecurityGroupID: observation.WorkerSecurityGroup.ID,
		ArtifactBucket:        observation.ArtifactBucket.Name,
		ArtifactKMSKeyARN:     observation.ArtifactKMSKey.ARN,
		EgressProfile:         PrivateNATHTTPSDNSEgress,
		PublicIngressMode:     NoPublicIngress,
	}
	if err := facts.Validate(); err != nil {
		return Facts{}, ErrFoundationReadback
	}
	return facts, nil
}

func validateObservation(spec Spec, observed Observation) error {
	if !layoutValid(spec.Network) || observed.VPC.ID == "" || observed.VPC.CIDR != spec.Network.VPCCIDR || !observed.VPC.DNSSupport || !observed.VPC.DNSHostnames ||
		!sameRequiredTags(observed.VPC.Tags, tagsFor(spec, spec.Names.VPC)) ||
		// The NAT subnet has an IGW route, but automatic public-address mapping
		// remains disabled. The NAT gateway alone receives its explicit EIP.
		!subnetMatches(observed.PublicSubnet, observed.VPC.ID, spec.Request.AvailabilityZone, spec.Network.PublicSubnetCIDR, false, tagsFor(spec, spec.Names.PublicSubnet)) ||
		!subnetMatches(observed.PrivateSubnet, observed.VPC.ID, spec.Request.AvailabilityZone, spec.Network.PrivateSubnetCIDR, false, tagsFor(spec, spec.Names.PrivateSubnet)) ||
		observed.InternetGateway.VPCID != observed.VPC.ID || !sameRequiredTags(observed.InternetGateway.Tags, tagsFor(spec, spec.Names.InternetGateway)) ||
		observed.NATGateway.SubnetID != observed.PublicSubnet.ID || observed.NATGateway.State != "available" || !sameRequiredTags(observed.NATGateway.Tags, tagsFor(spec, spec.Names.NATGateway)) ||
		!routeMatches(observed.PublicRoute, observed.PublicSubnet.ID, observed.InternetGateway.ID, tagsFor(spec, spec.Names.PublicRouteTable)) ||
		!routeMatches(observed.PrivateRoute, observed.PrivateSubnet.ID, observed.NATGateway.ID, tagsFor(spec, spec.Names.PrivateRouteTable)) ||
		observed.WorkerSecurityGroup.VPCID != observed.VPC.ID || len(observed.WorkerSecurityGroup.Ingress) != 0 || !sameRules(observed.WorkerSecurityGroup.Egress, spec.Egress) || !sameRequiredTags(observed.WorkerSecurityGroup.Tags, tagsFor(spec, spec.Names.WorkerSecurityGrp)) ||
		observed.ArtifactBucket.Name != spec.Names.ArtifactBucket || !observed.ArtifactBucket.VersioningEnabled || !observed.ArtifactBucket.AllPublicAccessBlocked || !observed.ArtifactBucket.BucketOwnerEnforced || !sameRequiredTags(observed.ArtifactBucket.Tags, tagsFor(spec, spec.Names.ArtifactBucket)) ||
		observed.ArtifactBucket.KMSKeyARN != observed.ArtifactKMSKey.ARN || !kmsKeyARNPattern.MatchString(observed.ArtifactKMSKey.ARN) || !strings.Contains(observed.ArtifactKMSKey.ARN, ":kms:"+spec.Request.Region+":"+spec.Request.AccountID+":") || !observed.ArtifactKMSKey.Enabled || observed.ArtifactKMSKey.KeySpec != "SYMMETRIC_DEFAULT" || observed.ArtifactKMSKey.KeyUsage != "ENCRYPT_DECRYPT" || observed.ArtifactKMSKey.MultiRegion || !sameRequiredTags(observed.ArtifactKMSKey.Tags, tagsFor(spec, spec.Names.ArtifactKMSKey)) {
		return ErrFoundationReadback
	}
	return nil
}

func validResourceID(id, prefix string) bool {
	return strings.HasPrefix(id, prefix) && resourceIDPattern.MatchString(id)
}

func subnetMatches(observed SubnetObservation, vpcID, zone, cidr string, publicIP bool, tags map[string]string) bool {
	return observed.ID != "" && observed.VPCID == vpcID && observed.AvailabilityZone == zone && observed.CIDR == cidr && observed.MapPublicIPOnLaunch == publicIP && sameRequiredTags(observed.Tags, tags)
}

func routeMatches(observed RouteObservation, subnetID, targetID string, tags map[string]string) bool {
	return validResourceID(observed.RouteTableID, "rtb-") && observed.AssociatedSubnetID == subnetID && observed.DestinationCIDR == "0.0.0.0/0" && observed.TargetID == targetID && sameRequiredTags(observed.Tags, tags)
}

func tagsFor(spec Spec, name string) map[string]string {
	result := make(map[string]string, len(spec.Tags)+1)
	for key, value := range spec.Tags {
		result[key] = value
	}
	result["Name"] = name
	return result
}

func sameRequiredTags(actual, required map[string]string) bool {
	for key, value := range required {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func sameRules(actual, required []EgressRule) bool {
	if len(actual) != len(required) {
		return false
	}
	canonical := func(rules []EgressRule) []string {
		values := make([]string, 0, len(rules))
		for _, rule := range rules {
			if rule.Protocol != "tcp" && rule.Protocol != "udp" || rule.FromPort < 0 || rule.FromPort != rule.ToPort || rule.CIDR == "" {
				return nil
			}
			values = append(values, fmt.Sprintf("%s:%d:%d:%s", rule.Protocol, rule.FromPort, rule.ToPort, rule.CIDR))
		}
		sort.Strings(values)
		return values
	}
	left, right := canonical(actual), canonical(required)
	if left == nil || right == nil || len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
