package provider

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type fakeIdentityEC2 struct {
	output *ec2.DescribeInstancesOutput
	err    error
	calls  int
}

func (f *fakeIdentityEC2) DescribeInstances(_ context.Context, input *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	f.calls++
	if len(input.InstanceIds) != 1 || input.InstanceIds[0] != "i-0123456789abcdef0" {
		return nil, errors.New("unexpected instance id")
	}
	return f.output, f.err
}

func TestAWSWorkerIdentityVerifierAcceptsSignedBoundInstance(t *testing.T) {
	privateKey := mustRSAKey(t)
	session := validIdentitySessionFixture()
	client := &fakeIdentityEC2{output: validIdentityReadBackFixture(session)}
	verifier := mustIdentityVerifier(t, client, privateKey)

	instanceID, err := verifier.Verify(context.Background(), signIdentityDocument(t, privateKey, validIdentityDocument()), session)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if instanceID != session.ExpectedInstanceID || client.calls != 1 {
		t.Fatalf("Verify() = %q, calls = %d", instanceID, client.calls)
	}
}

func TestAWSWorkerIdentityVerifierRejectsUntrustedProofBeforeReadBack(t *testing.T) {
	privateKey := mustRSAKey(t)
	session := validIdentitySessionFixture()
	tests := map[string]WorkerIdentityProof{
		"invalid signature": func() WorkerIdentityProof {
			proof := signIdentityDocument(t, privateKey, validIdentityDocument())
			proof.SignatureB64 = base64.StdEncoding.EncodeToString(make([]byte, privateKey.Size()))
			return proof
		}(),
		"noncanonical base64": func() WorkerIdentityProof {
			proof := signIdentityDocument(t, privateKey, validIdentityDocument())
			proof.DocumentB64 += "\n"
			return proof
		}(),
		"duplicate identity field": signRawIdentityDocument(t, privateKey, []byte(`{"accountId":"123456789012","accountId":"123456789012","architecture":"x86_64","availabilityZone":"us-east-1a","imageId":"ami-0123456789abcdef0","instanceId":"i-0123456789abcdef0","instanceType":"m7i.xlarge","region":"us-east-1"}`)),
		"wrong account":            signIdentityDocument(t, privateKey, identityDocumentFixture("accountId", "210987654321")),
		"wrong region":             signIdentityDocument(t, privateKey, identityDocumentFixture("region", "us-west-2")),
		"wrong instance":           signIdentityDocument(t, privateKey, identityDocumentFixture("instanceId", "i-0fedcba9876543210")),
		"wrong image":              signIdentityDocument(t, privateKey, identityDocumentFixture("imageId", "ami-0fedcba9876543210")),
		"wrong instance type":      signIdentityDocument(t, privateKey, identityDocumentFixture("instanceType", "m7i.2xlarge")),
		"wrong architecture":       signIdentityDocument(t, privateKey, identityDocumentFixture("architecture", "arm64")),
		"wrong availability zone":  signIdentityDocument(t, privateKey, identityDocumentFixture("availabilityZone", "us-east-1b")),
	}
	for name, proof := range tests {
		t.Run(name, func(t *testing.T) {
			client := &fakeIdentityEC2{output: validIdentityReadBackFixture(session)}
			verifier := mustIdentityVerifier(t, client, privateKey)
			if _, err := verifier.Verify(context.Background(), proof, session); providerErrorCode(err) != "worker_identity_invalid" {
				t.Fatalf("Verify() error = %v", err)
			}
			if client.calls != 0 {
				t.Fatalf("DescribeInstances calls = %d, want 0", client.calls)
			}
		})
	}
}

func TestAWSWorkerIdentityVerifierRejectsReadBackBoundaryMismatch(t *testing.T) {
	privateKey := mustRSAKey(t)
	session := validIdentitySessionFixture()
	proof := signIdentityDocument(t, privateKey, validIdentityDocument())
	tests := map[string]func(*ec2types.Instance){
		"instance id":  func(i *ec2types.Instance) { i.InstanceId = aws.String("i-0fedcba9876543210") },
		"image":        func(i *ec2types.Instance) { i.ImageId = aws.String("ami-0fedcba9876543210") },
		"type":         func(i *ec2types.Instance) { i.InstanceType = ec2types.InstanceTypeM7i2xlarge },
		"architecture": func(i *ec2types.Instance) { i.Architecture = ec2types.ArchitectureValuesArm64 },
		"vpc":          func(i *ec2types.Instance) { i.VpcId = aws.String("vpc-0fedcba9876543210") },
		"subnet":       func(i *ec2types.Instance) { i.SubnetId = aws.String("subnet-0fedcba9876543210") },
		"az":           func(i *ec2types.Instance) { i.Placement.AvailabilityZone = aws.String("us-east-1b") },
		"sg":           func(i *ec2types.Instance) { i.SecurityGroups[0].GroupId = aws.String("sg-0fedcba9876543210") },
		"missing tag":  func(i *ec2types.Instance) { i.Tags = i.Tags[:2] },
		"duplicate tag": func(i *ec2types.Instance) {
			i.Tags = append(i.Tags, i.Tags[0])
		},
		"public ip": func(i *ec2types.Instance) { i.PublicIpAddress = aws.String("203.0.113.2") },
		"eni public ip": func(i *ec2types.Instance) {
			i.NetworkInterfaces[0].Association = &ec2types.InstanceNetworkInterfaceAssociation{PublicIp: aws.String("203.0.113.3")}
		},
		"iam profile": func(i *ec2types.Instance) {
			i.IamInstanceProfile = &ec2types.IamInstanceProfile{Arn: aws.String("arn:aws:iam::123456789012:instance-profile/nope")}
		},
		"stopped": func(i *ec2types.Instance) { i.State.Name = ec2types.InstanceStateNameStopped },
		"imds disabled": func(i *ec2types.Instance) {
			i.MetadataOptions.HttpEndpoint = ec2types.InstanceMetadataEndpointStateDisabled
		},
		"imds optional": func(i *ec2types.Instance) {
			i.MetadataOptions.HttpTokens = ec2types.HttpTokensStateOptional
		},
		"imds hop": func(i *ec2types.Instance) {
			i.MetadataOptions.HttpPutResponseHopLimit = aws.Int32(2)
		},
		"metadata tags": func(i *ec2types.Instance) {
			i.MetadataOptions.InstanceMetadataTags = ec2types.InstanceMetadataTagsStateEnabled
		},
		"metadata pending": func(i *ec2types.Instance) {
			i.MetadataOptions.State = ec2types.InstanceMetadataOptionsStatePending
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			output := validIdentityReadBackFixture(session)
			mutate(&output.Reservations[0].Instances[0])
			client := &fakeIdentityEC2{output: output}
			verifier := mustIdentityVerifier(t, client, privateKey)
			if _, err := verifier.Verify(context.Background(), proof, session); providerErrorCode(err) != "worker_identity_readback_invalid" {
				t.Fatalf("Verify() error = %v", err)
			}
		})
	}
}

func TestAWSWorkerIdentityVerifierReportsReadBackFailureWithoutLeakingProviderError(t *testing.T) {
	privateKey := mustRSAKey(t)
	session := validIdentitySessionFixture()
	client := &fakeIdentityEC2{err: errors.New("provider secret")}
	verifier := mustIdentityVerifier(t, client, privateKey)
	_, err := verifier.Verify(context.Background(), signIdentityDocument(t, privateKey, validIdentityDocument()), session)
	if providerErrorCode(err) != "worker_identity_readback_unavailable" || strings.Contains(err.Error(), "provider secret") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func providerErrorCode(err error) string {
	var providerError *api.Error
	if !errors.As(err, &providerError) {
		return ""
	}
	return providerError.Code
}

func validIdentitySessionFixture() commandstore.WorkerSession {
	return commandstore.WorkerSession{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", ExpectedInstanceID: "i-0123456789abcdef0", ExpectedAMIID: "ami-0123456789abcdef0", ExpectedInstanceType: "m7i.xlarge", ExpectedArchitecture: "x86_64", ExpectedVPCID: "vpc-0123456789abcdef0", ExpectedSubnetID: "subnet-0123456789abcdef0", ExpectedAvailabilityZone: "us-east-1a", ExpectedSecurityGroupID: "sg-0123456789abcdef0"}
}

func validIdentityDocument() map[string]any {
	return map[string]any{"accountId": "123456789012", "architecture": "x86_64", "availabilityZone": "us-east-1a", "billingProducts": nil, "imageId": "ami-0123456789abcdef0", "instanceId": "i-0123456789abcdef0", "instanceType": "m7i.xlarge", "pendingTime": "2026-07-14T12:01:00Z", "privateIp": "10.0.1.2", "region": "us-east-1", "version": "2017-09-30"}
}

func identityDocumentFixture(key string, value any) map[string]any {
	document := validIdentityDocument()
	document[key] = value
	return document
}

func validIdentityReadBackFixture(session commandstore.WorkerSession) *ec2.DescribeInstancesOutput {
	return &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{
		InstanceId: aws.String(session.ExpectedInstanceID), ImageId: aws.String(session.ExpectedAMIID), InstanceType: ec2types.InstanceType(session.ExpectedInstanceType), Architecture: ec2types.ArchitectureValues(session.ExpectedArchitecture), VpcId: aws.String(session.ExpectedVPCID), SubnetId: aws.String(session.ExpectedSubnetID), Placement: &ec2types.Placement{AvailabilityZone: aws.String(session.ExpectedAvailabilityZone)}, State: &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning}, SecurityGroups: []ec2types.GroupIdentifier{{GroupId: aws.String(session.ExpectedSecurityGroupID)}}, NetworkInterfaces: []ec2types.InstanceNetworkInterface{{VpcId: aws.String(session.ExpectedVPCID), SubnetId: aws.String(session.ExpectedSubnetID), Groups: []ec2types.GroupIdentifier{{GroupId: aws.String(session.ExpectedSecurityGroupID)}}, PrivateIpAddresses: []ec2types.InstancePrivateIpAddress{{PrivateIpAddress: aws.String("10.0.1.2")}}}}, Tags: []ec2types.Tag{{Key: aws.String("dirextalk:managed"), Value: aws.String("true")}, {Key: aws.String("dirextalk:connection-id"), Value: aws.String(session.ConnectionID)}, {Key: aws.String("dirextalk:deployment-id"), Value: aws.String(session.DeploymentID)}}, MetadataOptions: &ec2types.InstanceMetadataOptionsResponse{State: ec2types.InstanceMetadataOptionsStateApplied, HttpEndpoint: ec2types.InstanceMetadataEndpointStateEnabled, HttpTokens: ec2types.HttpTokensStateRequired, HttpPutResponseHopLimit: aws.Int32(1), InstanceMetadataTags: ec2types.InstanceMetadataTagsStateDisabled},
	}}}}}
}

func mustRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustIdentityVerifier(t *testing.T, client EC2InstanceIdentityAPI, privateKey *rsa.PrivateKey) *AWSWorkerIdentityVerifier {
	t.Helper()
	encoded, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewAWSWorkerIdentityVerifier(client, "123456789012", "us-east-1", pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}))
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

func signIdentityDocument(t *testing.T, privateKey *rsa.PrivateKey, document map[string]any) WorkerIdentityProof {
	t.Helper()
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return signRawIdentityDocument(t, privateKey, raw)
}

func signRawIdentityDocument(t *testing.T, privateKey *rsa.PrivateKey, raw []byte) WorkerIdentityProof {
	t.Helper()
	digest := sha256.Sum256(raw)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return WorkerIdentityProof{DocumentB64: base64.StdEncoding.EncodeToString(raw), SignatureB64: base64.StdEncoding.EncodeToString(signature)}
}
