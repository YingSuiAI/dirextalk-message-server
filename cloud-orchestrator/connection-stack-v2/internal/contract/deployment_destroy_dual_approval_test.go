package contract

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

func TestDeploymentDestroyApprovalMatchesProductCoreAndBindsServiceFreeRequest(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	proof := DeploymentDestroyApprovalProof{
		SchemaVersion: approvalSchemaVersion, Intent: "deployment_destroy",
		ApprovalID: "deployment-destroy-approval-golden-0001", ChallengeID: "deployment-destroy-challenge-golden-0001", SignerKeyID: "owner-device-golden-0001",
		DeploymentID: "deployment-destroy-golden-0001", DeploymentRevision: 12,
		PlanID: "plan-destroy-golden-0001", CloudConnectionID: "connection-destroy-golden-0001", ResourceStatus: "orphaned",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"},
		NetworkInterfaceIDs: []string{"eni-0bbbbbbbbbbbbbbbb", "eni-0aaaaaaaaaaaaaaaa"}, SecretRefs: []string{"secret_ref:plan/registry", "secret_ref:plan/model"},
		IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	digest, err := proof.PayloadSHA256()
	if err != nil {
		t.Fatal(err)
	}
	if digest != "b4e372c59fb8ea526fa0cf4b91fca57e0476192d85f33cd4dc9b8971eae5aa45" {
		t.Fatalf("ProductCore/Stack deployment destroy approval drift: %s", digest)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x4f}, ed25519.SeedSize))
	payload, _ := proof.SigningPayload()
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	if err := proof.Verify(privateKey.Public().(ed25519.PublicKey), now.Add(time.Minute)); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	request := DeploymentDestroyRequest{Schema: DeploymentDestroySchema, DeploymentID: proof.DeploymentID, InstanceID: proof.InstanceID, VolumeIDs: proof.VolumeIDs, NetworkInterfaceIDs: proof.NetworkInterfaceIDs, SecretRefs: proof.SecretRefs}
	requestJSON, _ := json.Marshal(request)
	proofJSON, _ := json.Marshal(proof)
	command := Command{Action: ActionDeploymentDestroy, ConnectionID: proof.CloudConnectionID, PayloadB64: base64.StdEncoding.EncodeToString(requestJSON), ApprovalProof: proofJSON}
	if err := command.ValidateDeploymentDestroyBinding(); err != nil {
		t.Fatalf("service-free deployment destroy binding: %v", err)
	}
	metadata, err := command.DestroyApprovalMetadata()
	if err != nil || metadata.Intent != "deployment_destroy" || metadata.ApprovalID != proof.ApprovalID || metadata.SignerKeyID != proof.SignerKeyID {
		t.Fatalf("DestroyApprovalMetadata() = %#v, %v", metadata, err)
	}

	tampered := proof
	tampered.ResourceStatus = "blocked"
	if err := tampered.Verify(privateKey.Public().(ed25519.PublicKey), now.Add(time.Minute)); err == nil {
		t.Fatal("resource status tamper verified")
	}
	request.ServiceID = "service-forged-0001"
	requestJSON, _ = json.Marshal(request)
	command.PayloadB64 = base64.StdEncoding.EncodeToString(requestJSON)
	if err := command.ValidateDeploymentDestroyBinding(); Code(err) != "approval_scope_mismatch" {
		t.Fatalf("deployment proof accepted service-bound request: %v", err)
	}
}

func TestDeploymentDestroyDualApprovalParsingRemainsStrict(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	proof := DeploymentDestroyApprovalProof{SchemaVersion: approvalSchemaVersion, Intent: "deployment_destroy", ApprovalID: "approval-destroy-strict-0001", ChallengeID: "challenge-destroy-strict-0001", SignerKeyID: "device-destroy-strict-0001", DeploymentID: "deployment-destroy-strict-0001", DeploymentRevision: 2, PlanID: "plan-destroy-strict-0001", CloudConnectionID: "connection-destroy-strict-0001", ResourceStatus: "retained_tracked", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}, IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
	raw, _ := json.Marshal(proof)
	raw = append(raw[:len(raw)-1], []byte(`,"unexpected":true}`)...)
	if _, err := ParseDeploymentDestroyApprovalProof(raw); Code(err) != "invalid_approval_proof" {
		t.Fatalf("unknown approval field error = %v", err)
	}
	request := []byte(`{"schema":"dirextalk.aws.deployment-destroy/v1","deployment_id":"deployment-destroy-strict-0001","instance_id":"i-0123456789abcdef0","volume_ids":["vol-0aaaaaaaaaaaaaaaa"],"network_interface_ids":["eni-0aaaaaaaaaaaaaaaa"],"unexpected":true}`)
	command := Command{Action: ActionDeploymentDestroy, PayloadB64: base64.StdEncoding.EncodeToString(request)}
	if _, err := command.DeploymentDestroyRequest(); Code(err) != "invalid_payload" {
		t.Fatalf("unknown request field error = %v", err)
	}
}
