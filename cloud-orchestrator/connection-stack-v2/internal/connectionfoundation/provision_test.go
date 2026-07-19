package connectionfoundation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestProvisionerDerivesOnlyPrivateNoIngressFoundation(t *testing.T) {
	request := testProvisionRequest()
	provider := &fakeFoundationProvider{}
	provisioner, err := NewProvisioner(provider)
	if err != nil {
		t.Fatal(err)
	}

	// The fake records the exact typed Spec the provider would receive. It
	// cannot be supplied by the caller as a generic AWS/network request.
	provider.onEnsure = func(spec Spec) {
		provider.observation = validObservation(spec)
		provider.found = true
	}
	facts, err := provisioner.Provision(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if provider.ensures != 1 {
		t.Fatalf("Ensure calls=%d, want 1", provider.ensures)
	}
	if provider.spec.Request != request || provider.spec.Network.VPCCIDR == "" || provider.spec.Network.PrivateSubnetCIDR == "" || provider.spec.Names.ArtifactBucket == "" {
		t.Fatalf("unexpected derived spec: %#v", provider.spec)
	}
	if strings.Contains(provider.spec.Names.ArtifactBucket, request.ConnectionID) {
		t.Fatalf("bucket name leaked raw connection id: %q", provider.spec.Names.ArtifactBucket)
	}
	wantEgress := map[string]struct{}{
		"tcp:443:443:0.0.0.0/0":                              {},
		"tcp:53:53:" + provider.spec.Network.DNSResolverCIDR: {},
		"udp:53:53:" + provider.spec.Network.DNSResolverCIDR: {},
	}
	if len(provider.spec.Egress) != len(wantEgress) {
		t.Fatalf("egress=%#v", provider.spec.Egress)
	}
	for _, rule := range provider.spec.Egress {
		key := rule.Protocol + ":" + itoa(rule.FromPort) + ":" + itoa(rule.ToPort) + ":" + rule.CIDR
		if _, found := wantEgress[key]; !found {
			t.Fatalf("unexpected egress rule %q", key)
		}
	}
	if facts.PublicIngressMode != NoPublicIngress || facts.EgressProfile != PrivateNATHTTPSDNSEgress || facts.ArtifactKMSKeyARN != provider.observation.ArtifactKMSKey.ARN {
		t.Fatalf("unexpected facts: %#v", facts)
	}
	policy, err := facts.ArtifactPolicy()
	if err != nil || policy.Bucket != facts.ArtifactBucket || policy.KMSKeyID != facts.ArtifactKMSKeyARN {
		t.Fatalf("unexpected immutable artifact policy=%#v err=%v", policy, err)
	}

	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	withNetworkOverride := append(append([]byte{}, raw[:len(raw)-1]...), []byte(`,"network_cidr":"0.0.0.0/0"}`)...)
	if _, err := ParseProvisionRequest(withNetworkOverride); !errors.Is(err, ErrInvalidProvisionRequest) {
		t.Fatalf("network override error=%v", err)
	}
}

func TestProvisionerRecoversAfterCreateResponseLossWithoutSecondCreate(t *testing.T) {
	request := testProvisionRequest()
	provider := &fakeFoundationProvider{ensureErr: errors.New("response lost")}
	provider.onEnsure = func(spec Spec) {
		provider.observation = validObservation(spec)
		provider.found = true
	}
	provisioner, err := NewProvisioner(provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provisioner.Provision(context.Background(), request); !errors.Is(err, ErrFoundationProvisioning) {
		t.Fatalf("first Provision() error=%v", err)
	}
	facts, err := provisioner.Provision(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if provider.ensures != 1 {
		t.Fatalf("Ensure calls=%d, want recovery without second create", provider.ensures)
	}
	if facts.ConnectionID != request.ConnectionID || facts.ArtifactKMSKeyARN == "" {
		t.Fatalf("unexpected recovered facts: %#v", facts)
	}
}

func TestProvisionerFailsClosedWhenReadbackBroadensWorkerEgress(t *testing.T) {
	request := testProvisionRequest()
	provider := &fakeFoundationProvider{}
	provider.onEnsure = func(spec Spec) {
		provider.observation = validObservation(spec)
		provider.observation.WorkerSecurityGroup.Egress = append(provider.observation.WorkerSecurityGroup.Egress, EgressRule{Protocol: "tcp", FromPort: 22, ToPort: 22, CIDR: "0.0.0.0/0"})
		provider.found = true
	}
	provisioner, err := NewProvisioner(provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provisioner.Provision(context.Background(), request); !errors.Is(err, ErrFoundationReadback) {
		t.Fatalf("Provision() error=%v", err)
	}
}

func TestFactsBindWorkerRequiresFoundationArtifactBucket(t *testing.T) {
	request := testProvisionRequest()
	spec, err := deriveSpec(request)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := factsFromObservation(spec, validObservation(spec))
	if err != nil {
		t.Fatal(err)
	}
	worker := validPlan(t).Worker
	worker.Artifact.Bucket = facts.ArtifactBucket
	plan, err := facts.BindWorker(worker)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Network.SubnetID != facts.PrivateSubnetID || plan.Worker.Artifact.Bucket != facts.ArtifactBucket {
		t.Fatalf("unexpected bound plan: %#v", plan)
	}
	worker.Artifact.Bucket = "different-artifact-bucket"
	if _, err := facts.BindWorker(worker); !errors.Is(err, ErrFoundationReadback) {
		t.Fatalf("cross bucket BindWorker error=%v", err)
	}
}

type fakeFoundationProvider struct {
	found       bool
	observation Observation
	spec        Spec
	ensures     int
	ensureErr   error
	onEnsure    func(Spec)
}

func (provider *fakeFoundationProvider) Observe(_ context.Context, spec Spec) (Observation, bool, error) {
	provider.spec = spec
	return provider.observation, provider.found, nil
}

func (provider *fakeFoundationProvider) Ensure(_ context.Context, spec Spec) error {
	provider.spec = spec
	provider.ensures++
	if provider.onEnsure != nil {
		provider.onEnsure(spec)
	}
	return provider.ensureErr
}

func testProvisionRequest() ProvisionRequest {
	return ProvisionRequest{
		SchemaVersion:    ProvisionRequestSchema,
		ConnectionID:     "connection-01234567",
		AccountID:        "123456789012",
		Region:           "us-east-1",
		AvailabilityZone: "us-east-1a",
	}
}

func validObservation(spec Spec) Observation {
	keyARN := "arn:aws:kms:" + spec.Request.Region + ":123456789012:key/01234567-89ab-cdef-0123-456789abcdef"
	return Observation{
		VPC:                 VPCObservation{ID: "vpc-0123456789abcdef0", CIDR: spec.Network.VPCCIDR, DNSSupport: true, DNSHostnames: true, Tags: tagsFor(spec, spec.Names.VPC)},
		PublicSubnet:        SubnetObservation{ID: "subnet-0123456789abcdef0", VPCID: "vpc-0123456789abcdef0", AvailabilityZone: spec.Request.AvailabilityZone, CIDR: spec.Network.PublicSubnetCIDR, MapPublicIPOnLaunch: false, Tags: tagsFor(spec, spec.Names.PublicSubnet)},
		PrivateSubnet:       SubnetObservation{ID: "subnet-0123456789abcdee0", VPCID: "vpc-0123456789abcdef0", AvailabilityZone: spec.Request.AvailabilityZone, CIDR: spec.Network.PrivateSubnetCIDR, MapPublicIPOnLaunch: false, Tags: tagsFor(spec, spec.Names.PrivateSubnet)},
		InternetGateway:     GatewayObservation{ID: "igw-0123456789abcdef0", VPCID: "vpc-0123456789abcdef0", Tags: tagsFor(spec, spec.Names.InternetGateway)},
		NATGateway:          NATGatewayObservation{ID: "nat-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", State: "available", Tags: tagsFor(spec, spec.Names.NATGateway)},
		PublicRoute:         RouteObservation{RouteTableID: "rtb-0123456789abcdef0", AssociatedSubnetID: "subnet-0123456789abcdef0", DestinationCIDR: "0.0.0.0/0", TargetID: "igw-0123456789abcdef0", Tags: tagsFor(spec, spec.Names.PublicRouteTable)},
		PrivateRoute:        RouteObservation{RouteTableID: "rtb-0123456789abcdee0", AssociatedSubnetID: "subnet-0123456789abcdee0", DestinationCIDR: "0.0.0.0/0", TargetID: "nat-0123456789abcdef0", Tags: tagsFor(spec, spec.Names.PrivateRouteTable)},
		WorkerSecurityGroup: SecurityGroupObservation{ID: "sg-0123456789abcdef0", VPCID: "vpc-0123456789abcdef0", Egress: append([]EgressRule(nil), spec.Egress...), Tags: tagsFor(spec, spec.Names.WorkerSecurityGrp)},
		ArtifactBucket:      BucketObservation{Name: spec.Names.ArtifactBucket, VersioningEnabled: true, AllPublicAccessBlocked: true, BucketOwnerEnforced: true, KMSKeyARN: keyARN, Tags: tagsFor(spec, spec.Names.ArtifactBucket)},
		ArtifactKMSKey:      KMSKeyObservation{ARN: keyARN, Enabled: true, KeySpec: "SYMMETRIC_DEFAULT", KeyUsage: "ENCRYPT_DECRYPT", Tags: tagsFor(spec, spec.Names.ArtifactKMSKey)},
	}
}

func itoa(value int32) string {
	if value == 0 {
		return "0"
	}
	result := ""
	for value > 0 {
		result = string(rune('0'+value%10)) + result
		value /= 10
	}
	return result
}
