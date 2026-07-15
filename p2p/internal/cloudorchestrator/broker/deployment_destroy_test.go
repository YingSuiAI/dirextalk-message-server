package broker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestDeploymentDestroyCommandBindsApprovalAndVerifiedReceipt(t *testing.T) {
	now := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	target := cloudcontracts.ServiceDestroyTargetV1{ServiceID: "service-destroy-0001", ServiceRevision: 3, DeploymentID: "deployment-destroy-0001", DeploymentRevision: 8, CloudConnectionID: "connection-destroy-0001", RecipeID: "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}, SecretRefs: []string{"secret_ref:plan/model"}}
	approval, err := cloudcontracts.NewServiceDestroyApprovalV1(target, "approval-destroy-0001", "challenge-destroy-0001", "device-destroy-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, devicePrivate, _ := ed25519.GenerateKey(nil)
	approval, err = approval.Sign(devicePrivate, now)
	if err != nil {
		t.Fatal(err)
	}
	nodePublic, nodePrivate, _ := ed25519.GenerateKey(nil)
	command, err := NewDeploymentDestroyCommand(DeploymentDestroyCommandInput{ConnectionID: target.CloudConnectionID, CommandID: "command-destroy-0001", NodeKeyID: "node-destroy-0001", ExpectedGeneration: 2, NodeCounter: 12, IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute), Request: DeploymentDestroyRequest{Schema: DeploymentDestroySchema, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs, SecretRefs: target.SecretRefs}, ApprovalProof: approval, PrivateKey: nodePrivate})
	if err != nil || command.VerifySignature(nodePublic) != nil {
		t.Fatalf("destroy command=%#v err=%v", command, err)
	}
	raw, _ := json.Marshal(command)
	parsed, err := ParseDeploymentDestroyCommand(raw)
	if err != nil || parsed.RequestSHA256() != command.RequestSHA256() {
		t.Fatalf("parse destroy command=%#v err=%v", parsed, err)
	}
	tampered := command
	tampered.ApprovalProof.InstanceID = "i-0ffffffffffffffff"
	if tampered.VerifySignature(nodePublic) == nil {
		t.Fatal("node signature did not bind the device-approved instance")
	}
	result := DeploymentDestroyResult{Schema: DeploymentDestroyResultSchema, Status: "verified_destroyed", Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: command.RequestSHA256(), Action: DeploymentDestroyAction}, Deployment: DeploymentDestroyEvidence{DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs, SecretRefs: target.SecretRefs}}
	if err := ValidateDeploymentDestroyResult(command, result); err != nil {
		t.Fatal(err)
	}
	result.Deployment.VolumeIDs = []string{"vol-0bbbbbbbbbbbbbbbb"}
	if err := ValidateDeploymentDestroyResult(command, result); err == nil {
		t.Fatal("verified destroy receipt widened the resource set")
	}
	result.Deployment.VolumeIDs = target.VolumeIDs
	result.Deployment.SecretRefs = []string{"secret_ref:plan/forged"}
	if err := ValidateDeploymentDestroyResult(command, result); err == nil {
		t.Fatal("verified destroy receipt changed the secret set")
	}
	if bytes.Contains(raw, []byte("private")) {
		t.Fatal("destroy envelope leaked private key material")
	}
}

func TestDeploymentDestroyRequestRejectsThirtyThreeSecretRefs(t *testing.T) {
	secretRefs := make([]string, 33)
	for index := range secretRefs {
		secretRefs[index] = fmt.Sprintf("secret_ref:plan/slot-%02d", index)
	}
	request := DeploymentDestroyRequest{Schema: DeploymentDestroySchema, ServiceID: "service-destroy-0001", DeploymentID: "deployment-destroy-0001", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}, SecretRefs: secretRefs}
	if err := validateDestroyRequest(request); err == nil {
		t.Fatal("destroy request accepted 33 secret refs")
	}
}
