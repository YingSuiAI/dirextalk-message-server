//go:build rootbootstrap_wip
// +build rootbootstrap_wip

// This WIP contract test is deliberately retained with the staged Resolver
// implementation. It becomes part of the normal package test suite when that
// implementation lands; the default build must stay green while the user has
// explicitly paused this stage.
package rootbootstrap

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionbootstrap"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionfoundation"
)

func TestResolverPublishesPinnedArtifactsAndBindsFoundation(t *testing.T) {
	config, source, request := resolverFixture(t)
	provisioner := &fakeFoundationProvisioner{facts: fixtureFacts(request)}
	foundation := &fakeFoundationFactory{provisioner: provisioner}
	publisher := &fakeArtifactPublisher{}
	publishers := &fakeArtifactPublisherFactory{publisher: publisher}
	resolver, err := NewResolver(config,
		WithArtifactSource(source),
		WithFoundationFactory(foundation),
		WithArtifactPublisherFactory(publishers),
	)
	if err != nil {
		t.Fatal(err)
	}
	accessKey := []byte("access-key-not-returned")
	secretKey := []byte("secret-key-not-returned")
	resolution, err := resolver.ResolveFoundation(context.Background(), request, connectionbootstrap.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey})
	if err != nil {
		t.Fatalf("ResolveFoundation() error = %v", err)
	}
	if resolution.FoundationPlan.Validate() != nil || resolution.FoundationArtifact != publishers.policy ||
		resolution.ConnectionTemplate.ValidateFor(publishers.policy) != nil || resolution.BrokerArtifact.ValidateFor(publishers.policy) != nil {
		t.Fatalf("unexpected resolution: %#v", resolution)
	}
	if resolution.FoundationPlan.Worker.AMIID != config.Worker.AMIID ||
		resolution.FoundationPlan.Worker.Artifact.Version != config.Worker.Archive.Version ||
		resolution.FoundationPlan.Worker.Artifact.ArchiveSHA256 != config.Worker.Archive.SHA256 ||
		resolution.FoundationPlan.Worker.Artifact.ImageManifestSHA256 != config.Worker.ImageManifestSHA256 ||
		resolution.FoundationPlan.Worker.Artifact.WorkerResourceManifestDigest != config.Worker.WorkerResourceManifestDigest ||
		resolution.FoundationPlan.Worker.IIDVerifier != config.Worker.IIDVerifier {
		t.Fatalf("worker did not retain fixed release facts: %#v", resolution.FoundationPlan.Worker)
	}
	if provisioner.calls != 1 || provisioner.request.Region != request.Region || provisioner.request.AccountID != request.AccountID || provisioner.request.AvailabilityZone != config.Worker.AvailabilityZone {
		t.Fatalf("unexpected foundation provision request=%#v calls=%d", provisioner.request, provisioner.calls)
	}
	if got, want := len(publisher.published), 3; got != want || publisher.verifyCalls != want {
		t.Fatalf("published=%d verifies=%d, want three exact artifacts", got, publisher.verifyCalls)
	}
	if !allZero(accessKey) || !allZero(secretKey) {
		t.Fatal("resolver retained caller credential bytes")
	}
	raw, err := json.Marshal(resolution)
	if err != nil || bytes.Contains(raw, []byte("not-returned")) {
		t.Fatalf("resolution leaked credential material: %q / %v", raw, err)
	}
}

func TestResolverRejectsLocalDescriptorMismatchBeforeFoundation(t *testing.T) {
	config, source, request := resolverFixture(t)
	source.data[config.BrokerZIP.Path] = []byte("tampered broker payload")
	foundation := &fakeFoundationFactory{provisioner: &fakeFoundationProvisioner{facts: fixtureFacts(request)}}
	resolver, err := NewResolver(config,
		WithArtifactSource(source),
		WithFoundationFactory(foundation),
		WithArtifactPublisherFactory(&fakeArtifactPublisherFactory{publisher: &fakeArtifactPublisher{}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveFoundation(context.Background(), request, testCredentials()); !errors.Is(err, ErrArtifactContentMismatch) {
		t.Fatalf("ResolveFoundation() error=%v, want descriptor mismatch", err)
	}
	if foundation.calls != 0 {
		t.Fatalf("foundation was called after local artifact mismatch: %d", foundation.calls)
	}
}

func TestResolverRejectsCrossAccountAndRegionFactsBeforePublish(t *testing.T) {
	for _, mutation := range []struct {
		name string
		apply func(*connectionfoundation.Facts)
	}{
		{name: "account", apply: func(facts *connectionfoundation.Facts) { facts.AccountID = "210987654321"; facts.ArtifactKMSKeyARN = "arn:aws:kms:us-east-1:210987654321:key/01234567-89ab-cdef-0123-456789abcdef" }},
		{name: "region", apply: func(facts *connectionfoundation.Facts) { facts.Region, facts.AvailabilityZone, facts.ArtifactKMSKeyARN = "us-west-2", "us-west-2a", "arn:aws:kms:us-west-2:123456789012:key/01234567-89ab-cdef-0123-456789abcdef" }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			config, source, request := resolverFixture(t)
			facts := fixtureFacts(request)
			mutation.apply(&facts)
			foundation := &fakeFoundationFactory{provisioner: &fakeFoundationProvisioner{facts: facts}}
			publisher := &fakeArtifactPublisher{}
			publishers := &fakeArtifactPublisherFactory{publisher: publisher}
			resolver, err := NewResolver(config,
				WithArtifactSource(source), WithFoundationFactory(foundation), WithArtifactPublisherFactory(publishers),
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := resolver.ResolveFoundation(context.Background(), request, testCredentials()); !errors.Is(err, ErrFoundationResolution) {
				t.Fatalf("ResolveFoundation() error=%v, want cross-%s rejection", err, mutation.name)
			}
			if publishers.calls != 0 || len(publisher.published) != 0 {
				t.Fatalf("publisher was reached for cross-%s facts: factory=%d published=%d", mutation.name, publishers.calls, len(publisher.published))
			}
		})
	}
}

func TestResolverReturnsPublishFailureWithoutAResult(t *testing.T) {
	config, source, request := resolverFixture(t)
	publisher := &fakeArtifactPublisher{failKind: artifactpublish.KindBrokerZIP}
	resolver, err := NewResolver(config,
		WithArtifactSource(source),
		WithFoundationFactory(&fakeFoundationFactory{provisioner: &fakeFoundationProvisioner{facts: fixtureFacts(request)}}),
		WithArtifactPublisherFactory(&fakeArtifactPublisherFactory{publisher: publisher}),
	)
	if err != nil {
		t.Fatal(err)
	}
	resolution, err := resolver.ResolveFoundation(context.Background(), request, testCredentials())
	if !errors.Is(err, ErrArtifactPublication) || resolution != (connectionbootstrap.RootBootstrapResolution{}) {
		t.Fatalf("ResolveFoundation() resolution=%#v error=%v", resolution, err)
	}
	if got, want := len(publisher.published), 1; got != want {
		t.Fatalf("published=%d, want template only before broker failure", got)
	}
}

func TestResolverRejectsMissingCredentialsBeforeOpeningArtifacts(t *testing.T) {
	config, source, request := resolverFixture(t)
	foundation := &fakeFoundationFactory{provisioner: &fakeFoundationProvisioner{facts: fixtureFacts(request)}}
	resolver, err := NewResolver(config,
		WithArtifactSource(source),
		WithFoundationFactory(foundation),
		WithArtifactPublisherFactory(&fakeArtifactPublisherFactory{publisher: &fakeArtifactPublisher{}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveFoundation(context.Background(), request, connectionbootstrap.Credentials{}); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("ResolveFoundation() error=%v, want missing credentials", err)
	}
	if source.opens != 0 || foundation.calls != 0 {
		t.Fatalf("resolver used artifacts/AWS with missing credentials: opens=%d foundations=%d", source.opens, foundation.calls)
	}
}

func TestParseReleaseConfigRejectsUnknownAndDuplicateFields(t *testing.T) {
	config, _, _ := resolverFixture(t)
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseReleaseConfig(raw); err != nil || got != config {
		t.Fatalf("ParseReleaseConfig() got=%#v err=%v", got, err)
	}
	unknown := append(append([]byte{}, raw[:len(raw)-1]...), []byte(",\"unexpected\":true}")...)
	if _, err := ParseReleaseConfig(unknown); !errors.Is(err, ErrInvalidReleaseConfig) {
		t.Fatalf("unknown field error=%v", err)
	}
	duplicate := []byte(strings.Replace(string(raw), "\"path\":", "\"path\":\"ignored\",\"path\":", 1))
	if _, err := ParseReleaseConfig(duplicate); !errors.Is(err, ErrInvalidReleaseConfig) {
		t.Fatalf("duplicate field error=%v", err)
	}
}

func TestReleaseConfigRejectsMutableOrFormalVersions(t *testing.T) {
	for _, version := range []string{"latest", "v1.0.3", "1.0.3", "v1.2.3"} {
		t.Run(version, func(t *testing.T) {
			config, _, _ := resolverFixture(t)
			config.ConnectionTemplate.Version = version
			config.BrokerZIP.Version = version
			config.Worker.Archive.Version = version
			if err := config.Validate(); !errors.Is(err, ErrInvalidReleaseConfig) {
				t.Fatalf("Validate() error=%v for %q", err, version)
			}
		})
	}
}

func TestFileSourceRejectsNonRegularInput(t *testing.T) {
	descriptor := artifactpublish.ArtifactDescriptor{Kind: artifactpublish.KindBrokerZIP, Version: "v1.2.3-cloud-mvp.20260716.1", SHA256: "sha256:" + strings.Repeat("a", 64), SizeBytes: 1}
	if _, err := (FileSource{}).Open(context.Background(), t.TempDir(), descriptor); !errors.Is(err, ErrInvalidArtifactSource) {
		t.Fatalf("FileSource.Open() error=%v", err)
	}
}

func resolverFixture(t *testing.T) (ReleaseConfig, *fakeArtifactSource, connectionbootstrap.FoundationResolveRequest) {
	t.Helper()
	template := []byte("AWSTemplateFormatVersion: '2010-09-09'\n")
	broker := []byte("fixed broker zip fixture")
	worker := []byte("fixed worker tar fixture")
	dir := t.TempDir()
	config := ReleaseConfig{
		SchemaVersion: ReleaseConfigSchema,
		Region:        "us-east-1",
		ConnectionTemplate: LocalArtifact{Path: filepath.Join(dir, "connection-stack.yaml"), Version: "v1.2.3-cloud-mvp.20260716.1", SHA256: namedDigest(template), SizeBytes: int64(len(template))},
		BrokerZIP:          LocalArtifact{Path: filepath.Join(dir, "broker.zip"), Version: "v1.2.3-cloud-mvp.20260716.1", SHA256: namedDigest(broker), SizeBytes: int64(len(broker))},
		Worker: WorkerRelease{
			Archive:                      LocalArtifact{Path: filepath.Join(dir, "worker.tar"), Version: "v1.2.3-cloud-mvp.20260716.1", SHA256: namedDigest(worker), SizeBytes: int64(len(worker))},
			AMIID:                        "ami-0123456789abcdef0",
			ImageManifestSHA256:          "sha256:" + strings.Repeat("a", 64),
			WorkerResourceManifestDigest: "sha256:" + strings.Repeat("b", 64),
			AvailabilityZone:             "us-east-1a",
			IIDVerifier:                  fixtureIIDVerifier(t),
		},
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("fixture config invalid: %v", err)
	}
	request := connectionbootstrap.FoundationResolveRequest{BootstrapID: "bootstrap-root-0001", ConnectionID: "connection-root-bootstrap-0001", Region: "us-east-1", AccountID: "123456789012"}
	return config, &fakeArtifactSource{data: map[string][]byte{config.ConnectionTemplate.Path: template, config.BrokerZIP.Path: broker, config.Worker.Archive.Path: worker}}, request
}

func fixtureFacts(request connectionbootstrap.FoundationResolveRequest) connectionfoundation.Facts {
	return connectionfoundation.Facts{
		SchemaVersion: connectionfoundation.FactsSchema, ConnectionID: request.ConnectionID, AccountID: request.AccountID, Region: request.Region, AvailabilityZone: request.Region + "a",
		VPCID: "vpc-0123456789abcdef0", PublicSubnetID: "subnet-0123456789abcdef0", PrivateSubnetID: "subnet-0123456789abcdef1", InternetGatewayID: "igw-0123456789abcdef0",
		NATGatewayID: "nat-0123456789abcdef0", PublicRouteTableID: "rtb-0123456789abcdef0", PrivateRouteTableID: "rtb-0123456789abcdef1", WorkerSecurityGroupID: "sg-0123456789abcdef0",
		ArtifactBucket: "dirextalk-cf-01234567-artifacts", ArtifactKMSKeyARN: "arn:aws:kms:" + request.Region + ":" + request.AccountID + ":key/01234567-89ab-cdef-0123-456789abcdef",
		EgressProfile: connectionfoundation.PrivateNATHTTPSDNSEgress, PublicIngressMode: connectionfoundation.NoPublicIngress,
	}
}

func testCredentials() connectionbootstrap.Credentials {
	return connectionbootstrap.Credentials{AccessKeyID: []byte("AKIAEXAMPLE"), SecretAccessKey: []byte("secret-for-unit-test")}
}

type fakeArtifactSource struct {
	data  map[string][]byte
	opens int
}

func (source *fakeArtifactSource) Open(_ context.Context, path string, _ artifactpublish.ArtifactDescriptor) (ReadSeekCloser, error) {
	source.opens++
	raw, ok := source.data[path]
	if !ok {
		return nil, ErrInvalidArtifactSource
	}
	return memoryReadSeekCloser{Reader: bytes.NewReader(raw)}, nil
}

type memoryReadSeekCloser struct{ *bytes.Reader }

func (memoryReadSeekCloser) Close() error { return nil }

type fakeFoundationFactory struct {
	provisioner *fakeFoundationProvisioner
	err         error
	calls       int
}

func (factory *fakeFoundationFactory) New(_ aws.Config) (FoundationProvisioner, error) {
	factory.calls++
	if factory.err != nil {
		return nil, factory.err
	}
	return factory.provisioner, nil
}

type fakeFoundationProvisioner struct {
	facts   connectionfoundation.Facts
	err     error
	calls   int
	request connectionfoundation.ProvisionRequest
}

func (provisioner *fakeFoundationProvisioner) Provision(_ context.Context, request connectionfoundation.ProvisionRequest) (connectionfoundation.Facts, error) {
	provisioner.calls++
	provisioner.request = request
	return provisioner.facts, provisioner.err
}

type fakeArtifactPublisherFactory struct {
	publisher *fakeArtifactPublisher
	policy    artifactpublish.Policy
	err       error
	calls     int
}

func (factory *fakeArtifactPublisherFactory) New(_ aws.Config, policy artifactpublish.Policy) (ArtifactPublisher, error) {
	factory.calls++
	factory.policy = policy
	if factory.err != nil {
		return nil, factory.err
	}
	factory.publisher.policy = policy
	return factory.publisher, nil
}

type fakeArtifactPublisher struct {
	policy      artifactpublish.Policy
	published   []artifactpublish.Binding
	bindings    map[artifactpublish.ArtifactKind]artifactpublish.Binding
	verifyCalls int
	failKind    artifactpublish.ArtifactKind
}

func (publisher *fakeArtifactPublisher) Publish(_ context.Context, descriptor artifactpublish.ArtifactDescriptor, body io.Reader) (artifactpublish.Binding, error) {
	raw, err := io.ReadAll(body)
	if err != nil || int64(len(raw)) != descriptor.SizeBytes || namedDigest(raw) != descriptor.SHA256 {
		return artifactpublish.Binding{}, errors.New("bad source")
	}
	if descriptor.Kind == publisher.failKind {
		return artifactpublish.Binding{}, errors.New("publish failed")
	}
	if publisher.bindings == nil {
		publisher.bindings = make(map[artifactpublish.ArtifactKind]artifactpublish.Binding)
	}
	binding, err := artifactpublish.NewBinding(publisher.policy, descriptor, fmt.Sprintf("version-%d", len(publisher.published)+1))
	if err != nil {
		return artifactpublish.Binding{}, err
	}
	publisher.bindings[descriptor.Kind] = binding
	publisher.published = append(publisher.published, binding)
	return binding, nil
}

func (publisher *fakeArtifactPublisher) Verify(_ context.Context, binding artifactpublish.Binding) (artifactpublish.Binding, error) {
	publisher.verifyCalls++
	if publisher.bindings[binding.Kind] != binding {
		return artifactpublish.Binding{}, errors.New("binding was not read back")
	}
	return binding, nil
}

var fixtureKey struct {
	once sync.Once
	pem  string
	err  error
}

func fixtureIIDVerifier(t *testing.T) connectionfoundation.IIDVerifier {
	t.Helper()
	fixtureKey.once.Do(func() {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			fixtureKey.err = err
			return
		}
		raw, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		if err != nil {
			fixtureKey.err = err
			return
		}
		fixtureKey.pem = string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: raw}))
	})
	if fixtureKey.err != nil {
		t.Fatal(fixtureKey.err)
	}
	return connectionfoundation.IIDVerifier{Algorithm: connectionfoundation.EC2IIDRSASHA256Verifier, RSAPublicKeyPEM: fixtureKey.pem}
}

func namedDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
