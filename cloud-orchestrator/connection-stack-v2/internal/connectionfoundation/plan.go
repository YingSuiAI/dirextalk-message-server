// Package connectionfoundation defines the fail-closed, non-secret inputs
// that bind one Connection Stack to its reviewed Worker foundation. It does
// not create AWS resources and deliberately has no generic AWS parameter map.
package connectionfoundation

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"regexp"
	"strings"
)

const (
	PlanSchema                    = "dirextalk.connection-foundation-plan/v1"
	PrivateNATHTTPSDNSEgress      = "private_nat_https_dns"
	NoPublicIngress               = "no_public_ingress"
	EC2IIDRSASHA256Verifier       = "aws_ec2_iid_rsa_sha256"
	minimumIIDRSAModulusBitLength = 2048
	maxIIDRSAPublicKeyPEMBytes    = 2048
)

var (
	ErrInvalidPlan = errors.New("connection foundation plan is invalid")

	regionPattern           = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`)
	availabilityZonePattern = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9][a-z]$`)
	awsIDPattern            = regexp.MustCompile(`^[0-9a-f]{8,17}$`)
	namedDigestPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	artifactVersionPattern  = regexp.MustCompile(`^v?(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-[0-9A-Za-z][0-9A-Za-z.-]{0,127}$`)
	bucketPattern           = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$`)
	objectKeyPattern        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,511}$`)
	objectVersionPattern    = regexp.MustCompile(`^[A-Za-z0-9._~+/=-]+$`)
)

// Plan is reviewed configuration for an isolated Worker foundation. The
// declared network profile is later independently read back by the provider;
// this model intentionally does not accept arbitrary security-group, route,
// IAM, or CloudFormation settings.
type Plan struct {
	SchemaVersion string  `json:"schema_version"`
	Region        string  `json:"region"`
	Network       Network `json:"network"`
	Worker        Worker  `json:"worker"`
}

type Network struct {
	VPCID                 string `json:"vpc_id"`
	SubnetID              string `json:"subnet_id"`
	WorkerSecurityGroupID string `json:"worker_security_group_id"`
	AvailabilityZone      string `json:"availability_zone"`
	PrivateSubnet         bool   `json:"private_subnet"`
	EgressProfile         string `json:"egress_profile"`
	PublicIngressMode     string `json:"public_ingress_mode"`
}

type Worker struct {
	AMIID       string            `json:"ami_id"`
	Artifact    VersionedArtifact `json:"artifact"`
	IIDVerifier IIDVerifier       `json:"iid_verifier"`
}

// VersionedArtifact records the immutable Worker build inputs that produced
// the deployed AMI. "latest", formal v1.0.3, unversioned S3 objects, and
// mutable digest-less references are never valid.
type VersionedArtifact struct {
	Version                      string `json:"version"`
	Bucket                       string `json:"bucket"`
	ObjectKey                    string `json:"object_key"`
	ObjectVersionID              string `json:"object_version_id"`
	ArchiveSHA256                string `json:"archive_sha256"`
	ImageManifestSHA256          string `json:"image_manifest_sha256"`
	WorkerResourceManifestDigest string `json:"worker_resource_manifest_digest"`
}

type IIDVerifier struct {
	Algorithm       string `json:"algorithm"`
	RSAPublicKeyPEM string `json:"rsa_public_key_pem"`
}

// Parse accepts only the documented, typed plan shape. Unknown fields are
// rejected so a future generic pass-through cannot accidentally become live.
func Parse(raw []byte) (Plan, error) {
	if len(raw) == 0 || len(raw) > 32<<10 {
		return Plan{}, ErrInvalidPlan
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var plan Plan
	if err := decoder.Decode(&plan); err != nil {
		return Plan{}, ErrInvalidPlan
	}
	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		return Plan{}, ErrInvalidPlan
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (plan Plan) Validate() error {
	if plan.SchemaVersion != PlanSchema || !regionPattern.MatchString(plan.Region) ||
		!awsIDPattern.MatchString(strings.TrimPrefix(plan.Network.VPCID, "vpc-")) || !strings.HasPrefix(plan.Network.VPCID, "vpc-") ||
		!awsIDPattern.MatchString(strings.TrimPrefix(plan.Network.SubnetID, "subnet-")) || !strings.HasPrefix(plan.Network.SubnetID, "subnet-") ||
		!awsIDPattern.MatchString(strings.TrimPrefix(plan.Network.WorkerSecurityGroupID, "sg-")) || !strings.HasPrefix(plan.Network.WorkerSecurityGroupID, "sg-") ||
		!availabilityZonePattern.MatchString(plan.Network.AvailabilityZone) || !strings.HasPrefix(plan.Network.AvailabilityZone, plan.Region) ||
		!plan.Network.PrivateSubnet || plan.Network.EgressProfile != PrivateNATHTTPSDNSEgress || plan.Network.PublicIngressMode != NoPublicIngress ||
		!strings.HasPrefix(plan.Worker.AMIID, "ami-") || !awsIDPattern.MatchString(strings.TrimPrefix(plan.Worker.AMIID, "ami-")) {
		return ErrInvalidPlan
	}
	if err := plan.Worker.Artifact.validate(); err != nil {
		return err
	}
	if err := plan.Worker.IIDVerifier.validate(); err != nil {
		return err
	}
	return nil
}

func (artifact VersionedArtifact) validate() error {
	if !artifactVersionPattern.MatchString(artifact.Version) || artifact.Version == "v1.0.3" || artifact.Version == "1.0.3" ||
		!bucketPattern.MatchString(artifact.Bucket) || !objectKeyPattern.MatchString(artifact.ObjectKey) || strings.Contains(artifact.ObjectKey, "//") || strings.Contains(artifact.ObjectKey, "..") ||
		len(artifact.ObjectVersionID) > 1024 || !objectVersionPattern.MatchString(artifact.ObjectVersionID) || artifact.ObjectVersionID == "null" ||
		!namedDigestPattern.MatchString(artifact.ArchiveSHA256) || !namedDigestPattern.MatchString(artifact.ImageManifestSHA256) || !namedDigestPattern.MatchString(artifact.WorkerResourceManifestDigest) {
		return ErrInvalidPlan
	}
	return nil
}

func (verifier IIDVerifier) validate() error {
	if verifier.Algorithm != EC2IIDRSASHA256Verifier || len(verifier.RSAPublicKeyPEM) > maxIIDRSAPublicKeyPEMBytes {
		return ErrInvalidPlan
	}
	block, rest := pem.Decode([]byte(verifier.RSAPublicKeyPEM))
	if block == nil || strings.TrimSpace(string(rest)) != "" {
		return ErrInvalidPlan
	}
	var publicKey *rsa.PublicKey
	switch block.Type {
	case "PUBLIC KEY":
		parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return ErrInvalidPlan
		}
		var ok bool
		publicKey, ok = parsed.(*rsa.PublicKey)
		if !ok {
			return ErrInvalidPlan
		}
	case "RSA PUBLIC KEY":
		parsed, err := x509.ParsePKCS1PublicKey(block.Bytes)
		if err != nil {
			return ErrInvalidPlan
		}
		publicKey = parsed
	default:
		return ErrInvalidPlan
	}
	if publicKey == nil || publicKey.N.BitLen() < minimumIIDRSAModulusBitLength || publicKey.E < 3 || publicKey.E%2 == 0 {
		return ErrInvalidPlan
	}
	return nil
}

// TemplateParameters returns the complete fixed Worker subset consumed by the
// existing Connection Stack template. It never includes a caller-controlled
// generic parameter map or ingress/route/IAM override.
func (plan Plan) TemplateParameters() (map[string]string, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		"WorkerBaseAmiId":               plan.Worker.AMIID,
		"WorkerResourceManifestDigest":  plan.Worker.Artifact.WorkerResourceManifestDigest,
		"WorkerVpcId":                   plan.Network.VPCID,
		"WorkerSubnetId":                plan.Network.SubnetID,
		"WorkerSecurityGroupId":         plan.Network.WorkerSecurityGroupID,
		"WorkerAvailabilityZone":        plan.Network.AvailabilityZone,
		"WorkerIdentityRsaPublicKeyPem": plan.Worker.IIDVerifier.RSAPublicKeyPEM,
	}, nil
}
