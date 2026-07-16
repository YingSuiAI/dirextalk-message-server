package connectionfoundation

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"testing"
)

func TestPlanAcceptsOnlyFixedPrivateWorkerFoundation(t *testing.T) {
	plan := validPlan(t)
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parameters, err := parsed.TemplateParameters()
	if err != nil {
		t.Fatal(err)
	}
	if len(parameters) != 7 || parameters["WorkerBaseAmiId"] != parsed.Worker.AMIID || parameters["WorkerVpcId"] != parsed.Network.VPCID || parameters["WorkerSecurityGroupId"] != parsed.Network.WorkerSecurityGroupID || parameters["WorkerIdentityRsaPublicKeyPem"] != parsed.Worker.IIDVerifier.RSAPublicKeyPEM {
		t.Fatalf("unexpected fixed parameters: %#v", parameters)
	}
	for _, forbidden := range []string{"SecurityGroupIngress", "RouteTable", "IamInstanceProfile", "Parameters", "WorkerArtifactBucket", "WorkerArtifactKey"} {
		if _, found := parameters[forbidden]; found {
			t.Fatalf("closed foundation unexpectedly emitted %q", forbidden)
		}
	}
}

func TestParseRejectsUnknownOrMutableFoundationInputs(t *testing.T) {
	plan := validPlan(t)
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	withUnknown := append(append([]byte{}, raw[:len(raw)-1]...), []byte(`,"cloudformation_parameters":{"Anything":"goes"}}`)...)
	if _, err := Parse(withUnknown); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("unknown generic input error=%v", err)
	}

	for name, mutate := range map[string]func(*Plan){
		"public ingress":       func(value *Plan) { value.Network.PublicIngressMode = "allow_public_https" },
		"non private subnet":   func(value *Plan) { value.Network.PrivateSubnet = false },
		"non NAT egress":       func(value *Plan) { value.Network.EgressProfile = "open_egress" },
		"region zone mismatch": func(value *Plan) { value.Network.AvailabilityZone = "us-west-2a" },
		"missing worker group": func(value *Plan) { value.Network.WorkerSecurityGroupID = "" },
		"latest artifact":      func(value *Plan) { value.Worker.Artifact.Version = "latest" },
		"formal artifact":      func(value *Plan) { value.Worker.Artifact.Version = "v1.0.3" },
		"mutable object":       func(value *Plan) { value.Worker.Artifact.ObjectVersionID = "null" },
		"private key":          func(value *Plan) { value.Worker.IIDVerifier.RSAPublicKeyPEM = privateKeyPEM(t, 2048) },
		"IID key above template limit": func(value *Plan) {
			value.Worker.IIDVerifier.RSAPublicKeyPEM += strings.Repeat(" ", maxIIDRSAPublicKeyPEMBytes)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := validPlan(t)
			mutate(&candidate)
			if err := candidate.Validate(); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("Validate() error=%v", err)
			}
			if _, err := candidate.TemplateParameters(); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("TemplateParameters() error=%v", err)
			}
		})
	}
}

func TestPlanRejectsWeakIIDVerifierKey(t *testing.T) {
	plan := validPlan(t)
	plan.Worker.IIDVerifier.RSAPublicKeyPEM = publicKeyPEM(t, 1024)
	if err := plan.Validate(); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("weak RSA key error=%v", err)
	}
}

func validPlan(t *testing.T) Plan {
	t.Helper()
	return Plan{
		SchemaVersion: PlanSchema,
		Region:        "us-east-1",
		Network: Network{
			VPCID:                 "vpc-0123456789abcdef0",
			SubnetID:              "subnet-0123456789abcdef0",
			WorkerSecurityGroupID: "sg-0123456789abcdef0",
			AvailabilityZone:      "us-east-1a",
			PrivateSubnet:         true,
			EgressProfile:         PrivateNATHTTPSDNSEgress,
			PublicIngressMode:     NoPublicIngress,
		},
		Worker: Worker{
			AMIID: "ami-0123456789abcdef0",
			Artifact: VersionedArtifact{
				Version:                      "v1.1.0-cloud-mvp.20260715.1",
				Bucket:                       "dirextalk-worker-artifacts",
				ObjectKey:                    "worker/v1.1.0-cloud-mvp.20260715.1/artifact.tar",
				ObjectVersionID:              "3Lg5kqtJlcpXroDTDmJ+.yKk6aYxEtR2",
				ArchiveSHA256:                "sha256:" + strings.Repeat("a", 64),
				ImageManifestSHA256:          "sha256:" + strings.Repeat("b", 64),
				WorkerResourceManifestDigest: "sha256:" + strings.Repeat("c", 64),
			},
			IIDVerifier: IIDVerifier{Algorithm: EC2IIDRSASHA256Verifier, RSAPublicKeyPEM: publicKeyPEM(t, 2048)},
		},
	}
}

func publicKeyPEM(t *testing.T, bits int) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: raw}))
}

func privateKeyPEM(t *testing.T, bits int) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
}
