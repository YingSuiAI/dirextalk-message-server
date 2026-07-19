package artifactpublish

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var testPolicy = Policy{Bucket: "dirextalk-artifacts-test", KMSKeyID: "alias/dirextalk-artifacts"}

func TestPublisherCreatesAndVerifiesPinnedBrokerBinding(t *testing.T) {
	descriptor := testDescriptor(KindBrokerZIP, "broker archive")
	store := &fakeStore{versionID: "3/L4kqtJlcpXroDTDmJ+3dcjk="}
	publisher, err := NewPublisher(testPolicy, store)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := publisher.Publish(context.Background(), descriptor, strings.NewReader("broker archive"))
	if err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	if binding.Kind != KindBrokerZIP || binding.VersionID != store.versionID || binding.Bucket != testPolicy.Bucket || binding.KMSKeyID != testPolicy.KMSKeyID ||
		binding.Key != "releases/broker/v1.1.0-cloud-mvp.20260716.1/broker-v1.1.0-cloud-mvp.20260716.1-"+strings.TrimPrefix(descriptor.SHA256, "sha256:")+".zip" {
		t.Fatalf("unexpected immutable binding: %#v", binding)
	}
	if store.put == nil || store.head == nil || *store.put != descriptor || store.head.Bucket != binding.Bucket ||
		store.head.Key != binding.Key || store.head.VersionID != binding.VersionID {
		t.Fatalf("publisher forwarded a mutable request: put=%#v head=%#v", store.put, store.head)
	}

	reference, err := NewBrokerArtifactReference(testPolicy, descriptor.Version, descriptor.SHA256, descriptor.SizeBytes, binding.VersionID)
	if err != nil || reference.ValidateFor(testPolicy) != nil {
		t.Fatalf("NewBrokerArtifactReference() = %#v, %v", reference, err)
	}
	raw, err := json.Marshal(reference)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseBrokerArtifactReference(raw, testPolicy)
	if err != nil || parsed.Binding != binding {
		t.Fatalf("ParseBrokerArtifactReference() = %#v, %v", parsed, err)
	}
}

func TestBindingRejectsMutableOrUnapprovedReleaseInputs(t *testing.T) {
	valid := testDescriptor(KindWorkerArchive, "worker archive")
	for _, version := range []string{"latest", "v1.0.3", "1.0.3", "v1.2.3-latest", "v1.2.3"} {
		descriptor := valid
		descriptor.Version = version
		if descriptor.Validate() == nil {
			t.Fatalf("accepted mutable or forbidden version %q", version)
		}
	}
	binding, err := NewBinding(testPolicy, valid, "version-0001")
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Binding){
		"bucket":       func(value *Binding) { value.Bucket = "other-bucket" },
		"key":          func(value *Binding) { value.Key = "releases/worker/latest/worker.tar" },
		"content_type": func(value *Binding) { value.ContentType = "application/octet-stream" },
		"kms":          func(value *Binding) { value.KMSKeyID = "alias/other" },
		"version_id":   func(value *Binding) { value.VersionID = "null" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := binding
			mutate(&changed)
			if changed.ValidateFor(testPolicy) == nil {
				t.Fatal("accepted mutable binding field")
			}
		})
	}
}

func TestParseBindingRejectsUnknownAndDuplicateFields(t *testing.T) {
	binding, err := NewBinding(testPolicy, testDescriptor(KindBrokerZIP, "broker archive"), "version-0001")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	unknown := append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"url":"https://mutable.invalid/broker.zip"}`)...)
	if _, err := ParseBinding(unknown, testPolicy); !errors.Is(err, ErrInvalidBinding) {
		t.Fatalf("unknown mutable field err=%v", err)
	}
	duplicate := append(append([]byte(nil), raw[:len(raw)-1]...), []byte(`,"version":"latest"}`)...)
	if _, err := ParseBinding(duplicate, testPolicy); !errors.Is(err, ErrInvalidBinding) {
		t.Fatalf("duplicate field err=%v", err)
	}
}

func TestPublisherRejectsObjectObservationMismatch(t *testing.T) {
	descriptor := testDescriptor(KindWorkerArchive, "worker archive")
	binding, err := NewBinding(testPolicy, descriptor, "version-0001")
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{metadata: ObjectMetadata{
		VersionID: binding.VersionID, SHA256: checksumBase64(binding.SHA256), SizeBytes: binding.SizeBytes,
		ContentType: binding.ContentType, KMSKeyID: "alias/substituted-key",
	}}
	publisher, err := NewPublisher(testPolicy, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Verify(context.Background(), binding); !errors.Is(err, ErrVerification) {
		t.Fatalf("Verify() error = %v, want mismatch rejection", err)
	}
}

func TestConnectionTemplateReferencePinsCloudFormationToExactS3Version(t *testing.T) {
	descriptor := testDescriptor(KindConnectionTemplate, "AWSTemplateFormatVersion: '2010-09-09'\n")
	versionID := "3/L4kqtJlcpXroDTDmJ+3dcjk="
	reference, err := NewConnectionTemplateReference(testPolicy, descriptor.Version, descriptor.SHA256, descriptor.SizeBytes, versionID)
	if err != nil || reference.ValidateFor(testPolicy) != nil {
		t.Fatalf("NewConnectionTemplateReference() = %#v, %v", reference, err)
	}
	value, err := reference.CloudFormationURL("us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "s3.us-east-1.amazonaws.com" || parsed.Query().Get("versionId") != versionID || parsed.Path != "/"+testPolicy.Bucket+"/"+reference.Key {
		t.Fatalf("CloudFormationURL()=%q parsed=%#v err=%v", value, parsed, err)
	}
	if _, err := (ConnectionTemplateReference{Binding: Binding{Kind: KindWorkerArchive}}).CloudFormationURL("us-east-1"); !errors.Is(err, ErrInvalidBinding) {
		t.Fatalf("mutable/foreign template reference error=%v", err)
	}
}

func TestAWSStoreMapsOnlyTypedImmutableRequest(t *testing.T) {
	descriptor := testDescriptor(KindBrokerZIP, "broker archive")
	checksum := checksumBase64(descriptor.SHA256)
	fake := &fakeS3API{putOutput: &s3.PutObjectOutput{VersionId: aws.String("version-0001")}, headOutput: &s3.HeadObjectOutput{
		VersionId: aws.String("version-0001"), ChecksumSHA256: aws.String(checksum), ContentLength: aws.Int64(descriptor.SizeBytes),
		ContentType: aws.String(BrokerZIPContentType), ServerSideEncryption: s3types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String(testPolicy.KMSKeyID),
	}}
	store, err := newAWSStore(fake, testPolicy)
	if err != nil {
		t.Fatal(err)
	}
	versionID, err := store.PutImmutable(context.Background(), descriptor, strings.NewReader("broker archive"))
	if err != nil || versionID != "version-0001" || fake.put == nil || aws.ToString(fake.put.Bucket) != testPolicy.Bucket || aws.ToString(fake.put.Key) != objectKey(descriptor) ||
		aws.ToString(fake.put.ChecksumSHA256) != checksum || fake.put.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms || aws.ToString(fake.put.SSEKMSKeyId) != testPolicy.KMSKeyID {
		t.Fatalf("PutImmutable() version=%q err=%v input=%#v", versionID, err, fake.put)
	}
	binding, err := NewBinding(testPolicy, descriptor, versionID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.HeadImmutable(context.Background(), binding)
	if err != nil || metadata.VersionID != versionID || fake.head == nil || aws.ToString(fake.head.VersionId) != versionID || fake.head.ChecksumMode != s3types.ChecksumModeEnabled {
		t.Fatalf("HeadImmutable() metadata=%#v err=%v input=%#v", metadata, err, fake.head)
	}
}

func testDescriptor(kind ArtifactKind, contents string) ArtifactDescriptor {
	sum := sha256.Sum256([]byte(contents))
	return ArtifactDescriptor{Kind: kind, Version: "v1.1.0-cloud-mvp.20260716.1", SHA256: "sha256:" + hex.EncodeToString(sum[:]), SizeBytes: int64(len(contents))}
}

type fakeStore struct {
	versionID string
	put       *ArtifactDescriptor
	head      *Binding
	metadata  ObjectMetadata
}

func (store *fakeStore) PutImmutable(_ context.Context, descriptor ArtifactDescriptor, _ io.Reader) (string, error) {
	store.put = &descriptor
	return store.versionID, nil
}

func (store *fakeStore) HeadImmutable(_ context.Context, binding Binding) (ObjectMetadata, error) {
	store.head = &binding
	if store.metadata.VersionID != "" {
		return store.metadata, nil
	}
	if store.put == nil {
		return ObjectMetadata{}, ErrProvider
	}
	return ObjectMetadata{VersionID: binding.VersionID, SHA256: checksumBase64(binding.SHA256), SizeBytes: binding.SizeBytes, ContentType: binding.ContentType, KMSKeyID: binding.KMSKeyID}, nil
}

type fakeS3API struct {
	putOutput  *s3.PutObjectOutput
	headOutput *s3.HeadObjectOutput
	put        *s3.PutObjectInput
	head       *s3.HeadObjectInput
}

func (fake *fakeS3API) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	fake.put = input
	_, _ = io.Copy(io.Discard, input.Body)
	return fake.putOutput, nil
}

func (fake *fakeS3API) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	fake.head = input
	return fake.headOutput, nil
}

func TestChecksumBase64RoundTrip(t *testing.T) {
	descriptor := testDescriptor(KindWorkerArchive, "worker archive")
	decoded, err := base64.StdEncoding.DecodeString(checksumBase64(descriptor.SHA256))
	if err != nil || hex.EncodeToString(decoded) != strings.TrimPrefix(descriptor.SHA256, "sha256:") {
		t.Fatalf("checksum round trip decoded=%x err=%v", decoded, err)
	}
}
