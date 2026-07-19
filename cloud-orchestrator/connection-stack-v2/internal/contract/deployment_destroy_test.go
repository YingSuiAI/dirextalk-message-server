package contract

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestServiceDestroyApprovalPayloadMatchesProductCoreGolden(t *testing.T) {
	now := time.Date(2026, time.July, 15, 6, 0, 0, 0, time.UTC)
	proof := ServiceDestroyApprovalProof{
		SchemaVersion: approvalSchemaVersion, Intent: serviceDestroyApprovalIntent,
		ApprovalID: "destroy-approval-0001", ChallengeID: "destroy-challenge-0001", SignerKeyID: "owner-device-0001",
		ServiceID: "service-destroy-0001", ServiceRevision: 3, DeploymentID: "deployment-destroy-0001", DeploymentRevision: 8,
		CloudConnectionID: "connection-destroy-0001", RecipeID: "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0bbbbbbbbbbbbbbbb", "eni-0aaaaaaaaaaaaaaaa"},
		IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64)),
	}
	digest, err := proof.PayloadSHA256()
	if err != nil {
		t.Fatal(err)
	}
	if digest != "740a7d50231910bcd74e55a4ea66b8118076946b0f168cd5fcc9d8eef3dca091" {
		t.Fatalf("ProductCore/Stack destroy approval payload drift: %s", digest)
	}
}

func TestDeploymentDestroyBindingRejectsResourceWidening(t *testing.T) {
	now := time.Date(2026, time.July, 15, 6, 0, 0, 0, time.UTC)
	proof := ServiceDestroyApprovalProof{SchemaVersion: approvalSchemaVersion, Intent: serviceDestroyApprovalIntent, ApprovalID: "destroy-approval-0001", ChallengeID: "destroy-challenge-0001", SignerKeyID: "owner-device-0001", ServiceID: "service-destroy-0001", ServiceRevision: 3, DeploymentID: "deployment-destroy-0001", DeploymentRevision: 8, CloudConnectionID: "connection-destroy-0001", RecipeID: "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64))}
	request := DeploymentDestroyRequest{Schema: DeploymentDestroySchema, ServiceID: "service-destroy-0001", DeploymentID: "deployment-destroy-0001", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}}
	request.VolumeIDs = append(request.VolumeIDs, "vol-0bbbbbbbbbbbbbbbb")
	payload, _ := json.Marshal(request)
	proofJSON, _ := json.Marshal(proof)
	command := Command{Action: ActionDeploymentDestroy, ConnectionID: proof.CloudConnectionID, PayloadB64: base64.StdEncoding.EncodeToString(payload), ApprovalProof: proofJSON}
	if err := command.ValidateDeploymentDestroyBinding(); Code(err) != "approval_scope_mismatch" {
		t.Fatalf("widened destroy request error=%v code=%s", err, Code(err))
	}
}

func TestDeploymentDestroySecretRefsAreCanonicalAndTamperBound(t *testing.T) {
	now := time.Date(2026, time.July, 15, 6, 0, 0, 0, time.UTC)
	proof := ServiceDestroyApprovalProof{SchemaVersion: approvalSchemaVersion, Intent: serviceDestroyApprovalIntent, ApprovalID: "destroy-approval-0001", ChallengeID: "destroy-challenge-0001", SignerKeyID: "owner-device-0001", ServiceID: "service-destroy-0001", ServiceRevision: 3, DeploymentID: "deployment-destroy-0001", DeploymentRevision: 8, CloudConnectionID: "connection-destroy-0001", RecipeID: "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}, SecretRefs: []string{"secret_ref:z-token", "secret_ref:a/token"}, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, 64))}
	digest, err := proof.PayloadSHA256()
	if err != nil {
		t.Fatal(err)
	}
	if digest != "e87d8e9060b48a01eeb79a6de86d6ee58ffb07940152b4736a30c2f2b33b76ad" {
		t.Fatalf("secret-scoped destroy approval payload drift: %s", digest)
	}
	request := DeploymentDestroyRequest{Schema: DeploymentDestroySchema, ServiceID: proof.ServiceID, DeploymentID: proof.DeploymentID, InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, NetworkInterfaceIDs: proof.NetworkInterfaceIDs, SecretRefs: proof.SecretRefs}
	payload, _ := json.Marshal(request)
	requestDigest := sha256.Sum256(payload)
	if got := fmt.Sprintf("%x", requestDigest); got != "bd53b65111390a1db09bb7c7645140bc92ee1440252394f71fa4f22196b92f73" {
		t.Fatalf("secret-scoped destroy request drift: %s", got)
	}
	proofJSON, _ := json.Marshal(proof)
	command := Command{Action: ActionDeploymentDestroy, ConnectionID: proof.CloudConnectionID, PayloadB64: base64.StdEncoding.EncodeToString(payload), ApprovalProof: proofJSON}
	if err = command.ValidateDeploymentDestroyBinding(); err != nil {
		t.Fatal(err)
	}
	request.SecretRefs[0] = "secret_ref:other"
	payload, _ = json.Marshal(request)
	command.PayloadB64 = base64.StdEncoding.EncodeToString(payload)
	if err = command.ValidateDeploymentDestroyBinding(); Code(err) != "approval_scope_mismatch" {
		t.Fatalf("tampered secret ref error=%v code=%s", err, Code(err))
	}
}
