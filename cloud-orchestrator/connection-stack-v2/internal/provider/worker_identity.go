package provider

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

const (
	maxIdentityDocumentBytes  = 64 * 1024
	maxIdentitySignatureBytes = 32 * 1024
)

var (
	awsAccountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)
	awsRegionPattern    = regexp.MustCompile(`^[a-z]{2}(?:-[a-z0-9]+)+-[0-9]+$`)
)

// WorkerIdentityProof is the opaque AWS identity material carried by a Worker
// claim. The verifier never stores or returns either value.
type WorkerIdentityProof struct {
	DocumentB64  string
	SignatureB64 string
}

type EC2InstanceIdentityAPI interface {
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

type AWSWorkerIdentityVerifier struct {
	client    EC2InstanceIdentityAPI
	accountID string
	region    string
	publicKey *rsa.PublicKey
}

func NewAWSWorkerIdentityVerifier(client EC2InstanceIdentityAPI, accountID, region string, publicKeyPEM []byte) (*AWSWorkerIdentityVerifier, error) {
	publicKey, err := parseRSAPublicKey(publicKeyPEM)
	if client == nil || !awsAccountIDPattern.MatchString(accountID) || !awsRegionPattern.MatchString(region) || err != nil {
		return nil, api.NewError("worker_identity_verifier_invalid", 500)
	}
	return &AWSWorkerIdentityVerifier{client: client, accountID: accountID, region: region, publicKey: publicKey}, nil
}

// Verify authenticates the exact raw IID document and then independently
// reads EC2 state back. Success proves both the signed VM identity and the
// immutable deployment boundary recorded before instance creation.
func (v *AWSWorkerIdentityVerifier) Verify(ctx context.Context, proof WorkerIdentityProof, session commandstore.WorkerSession) (string, error) {
	if v == nil || v.client == nil || v.publicKey == nil || !validIdentitySession(session) {
		return "", api.NewError("worker_identity_invalid", 403)
	}
	document, ok := decodeCanonicalBase64(proof.DocumentB64, maxIdentityDocumentBytes)
	if !ok {
		return "", api.NewError("worker_identity_invalid", 403)
	}
	signature, ok := decodeCanonicalBase64(proof.SignatureB64, maxIdentitySignatureBytes)
	if !ok {
		return "", api.NewError("worker_identity_invalid", 403)
	}
	digest := sha256.Sum256(document)
	if rsa.VerifyPKCS1v15(v.publicKey, crypto.SHA256, digest[:], signature) != nil {
		return "", api.NewError("worker_identity_invalid", 403)
	}
	identity, err := parseIdentityDocument(document)
	if err != nil || identity.AccountID != v.accountID || identity.Region != v.region || identity.InstanceID != session.ExpectedInstanceID || identity.ImageID != session.ExpectedAMIID || identity.InstanceType != session.ExpectedInstanceType || identity.Architecture != session.ExpectedArchitecture || identity.AvailabilityZone != session.ExpectedAvailabilityZone {
		return "", api.NewError("worker_identity_invalid", 403)
	}

	output, err := v.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{session.ExpectedInstanceID}})
	if err != nil {
		return "", api.NewError("worker_identity_readback_unavailable", 503)
	}
	if !validIdentityReadBack(output, session) {
		return "", api.NewError("worker_identity_readback_invalid", 403)
	}
	return session.ExpectedInstanceID, nil
}

func (v *AWSWorkerIdentityVerifier) VerifyWorkerIdentity(ctx context.Context, request contract.WorkerSessionClaimRequest, session commandstore.WorkerSession) error {
	instanceID, err := v.Verify(ctx, WorkerIdentityProof{DocumentB64: request.InstanceIdentityDocumentB64, SignatureB64: request.InstanceIdentitySignatureB64}, session)
	if err != nil {
		return err
	}
	if instanceID != session.ExpectedInstanceID {
		return api.NewError("worker_identity_invalid", 403)
	}
	return nil
}

type identityDocument struct {
	AccountID        string
	Region           string
	InstanceID       string
	ImageID          string
	InstanceType     string
	Architecture     string
	AvailabilityZone string
}

func parseIdentityDocument(raw []byte) (identityDocument, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return identityDocument{}, api.NewError("worker_identity_invalid", 403)
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, keyErr := decoder.Token()
		key, keyOK := keyToken.(string)
		if keyErr != nil || !keyOK {
			return identityDocument{}, api.NewError("worker_identity_invalid", 403)
		}
		if _, duplicate := fields[key]; duplicate {
			return identityDocument{}, api.NewError("worker_identity_invalid", 403)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return identityDocument{}, api.NewError("worker_identity_invalid", 403)
		}
		fields[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || decoder.Decode(&struct{}{}) == nil {
		return identityDocument{}, api.NewError("worker_identity_invalid", 403)
	}
	value := identityDocument{}
	bindings := map[string]*string{
		"accountId": &value.AccountID, "region": &value.Region, "instanceId": &value.InstanceID,
		"imageId": &value.ImageID, "instanceType": &value.InstanceType, "architecture": &value.Architecture,
		"availabilityZone": &value.AvailabilityZone,
	}
	for key, target := range bindings {
		rawValue, found := fields[key]
		if !found || json.Unmarshal(rawValue, target) != nil || *target == "" {
			return identityDocument{}, api.NewError("worker_identity_invalid", 403)
		}
	}
	return value, nil
}

func validIdentityReadBack(output *ec2.DescribeInstancesOutput, session commandstore.WorkerSession) bool {
	if output == nil || len(output.Reservations) != 1 || len(output.Reservations[0].Instances) != 1 {
		return false
	}
	instance := output.Reservations[0].Instances[0]
	if aws.ToString(instance.InstanceId) != session.ExpectedInstanceID || aws.ToString(instance.ImageId) != session.ExpectedAMIID || string(instance.InstanceType) != session.ExpectedInstanceType || string(instance.Architecture) != session.ExpectedArchitecture || aws.ToString(instance.VpcId) != session.ExpectedVPCID || aws.ToString(instance.SubnetId) != session.ExpectedSubnetID || instance.Placement == nil || aws.ToString(instance.Placement.AvailabilityZone) != session.ExpectedAvailabilityZone || instance.State == nil || (instance.State.Name != ec2types.InstanceStateNamePending && instance.State.Name != ec2types.InstanceStateNameRunning) || aws.ToString(instance.PublicIpAddress) != "" || instance.IamInstanceProfile != nil {
		return false
	}
	if len(instance.SecurityGroups) != 1 || aws.ToString(instance.SecurityGroups[0].GroupId) != session.ExpectedSecurityGroupID || !hasWorkerIdentityTags(instance.Tags, session) {
		return false
	}
	if len(instance.NetworkInterfaces) != 1 {
		return false
	}
	networkInterface := instance.NetworkInterfaces[0]
	if networkInterface.Association != nil || aws.ToString(networkInterface.VpcId) != session.ExpectedVPCID || aws.ToString(networkInterface.SubnetId) != session.ExpectedSubnetID || len(networkInterface.Groups) != 1 || aws.ToString(networkInterface.Groups[0].GroupId) != session.ExpectedSecurityGroupID {
		return false
	}
	for _, address := range networkInterface.PrivateIpAddresses {
		if address.Association != nil {
			return false
		}
	}
	metadata := instance.MetadataOptions
	return metadata != nil && metadata.State == ec2types.InstanceMetadataOptionsStateApplied && metadata.HttpEndpoint == ec2types.InstanceMetadataEndpointStateEnabled && metadata.HttpTokens == ec2types.HttpTokensStateRequired && aws.ToInt32(metadata.HttpPutResponseHopLimit) == 1 && metadata.InstanceMetadataTags == ec2types.InstanceMetadataTagsStateDisabled
}

func hasWorkerIdentityTags(tags []ec2types.Tag, session commandstore.WorkerSession) bool {
	expected := map[string]string{
		"dirextalk:managed":       "true",
		"dirextalk:connection-id": session.ConnectionID,
		"dirextalk:deployment-id": session.DeploymentID,
	}
	seen := make(map[string]int, len(expected))
	for _, tag := range tags {
		key := aws.ToString(tag.Key)
		want, required := expected[key]
		if required {
			if aws.ToString(tag.Value) != want {
				return false
			}
			seen[key]++
		}
	}
	for key := range expected {
		if seen[key] != 1 {
			return false
		}
	}
	return true
}

func validIdentitySession(session commandstore.WorkerSession) bool {
	return session.ExpectedInstanceID != "" && session.ExpectedAMIID != "" && session.ExpectedInstanceType != "" && (session.ExpectedArchitecture == "x86_64" || session.ExpectedArchitecture == "arm64") && session.ExpectedVPCID != "" && session.ExpectedSubnetID != "" && session.ExpectedAvailabilityZone != "" && session.ExpectedSecurityGroupID != "" && session.ConnectionID != "" && session.DeploymentID != ""
}

func decodeCanonicalBase64(value string, maximum int) ([]byte, bool) {
	if value == "" || len(value) > base64.StdEncoding.EncodedLen(maximum) {
		return nil, false
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	return decoded, err == nil && len(decoded) > 0 && len(decoded) <= maximum && base64.StdEncoding.EncodeToString(decoded) == value
}

func parseRSAPublicKey(value []byte) (*rsa.PublicKey, error) {
	block, rest := pem.Decode(value)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return nil, api.NewError("worker_identity_verifier_invalid", 500)
	}
	if parsed, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		if publicKey, ok := parsed.(*rsa.PublicKey); ok && publicKey.N.BitLen() >= 2048 {
			return publicKey, nil
		}
	}
	if publicKey, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil && publicKey.N.BitLen() >= 2048 {
		return publicKey, nil
	}
	return nil, api.NewError("worker_identity_verifier_invalid", 500)
}
