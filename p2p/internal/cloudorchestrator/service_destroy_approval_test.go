package cloudorchestrator_test

import (
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestServiceDestroyApprovalBindsExactTrackedResourcesAndRevision(t *testing.T) {
	now := time.Date(2026, time.July, 15, 6, 0, 0, 0, time.UTC)
	target := cloudorchestrator.ServiceDestroyTargetV1{
		ServiceID: "service-destroy-0001", ServiceRevision: 3,
		DeploymentID: "deployment-destroy-0001", DeploymentRevision: 8,
		CloudConnectionID: "connection-destroy-0001",
		RecipeID:          "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID:          "i-0123456789abcdef0",
		VolumeIDs:           []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"},
		NetworkInterfaceIDs: []string{"eni-0bbbbbbbbbbbbbbbb", "eni-0aaaaaaaaaaaaaaaa"},
		SecretRefs:          []string{"secret_ref:plan/registry", "secret_ref:plan/model"},
	}
	approval, err := cloudorchestrator.NewServiceDestroyApprovalV1(target, "destroy-approval-0001", "destroy-challenge-0001", "owner-device-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if approval.VolumeIDs[0] != "vol-0aaaaaaaaaaaaaaaa" || approval.NetworkInterfaceIDs[0] != "eni-0aaaaaaaaaaaaaaaa" || approval.SecretRefs[0] != "secret_ref:plan/model" {
		t.Fatalf("resource identifiers were not canonicalized: %#v", approval)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := approval.Sign(privateKey, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := signed.Verify(publicKey, now.Add(time.Minute)); err != nil {
		t.Fatalf("verify signed destroy approval: %v", err)
	}
	if err := signed.ValidateAgainst(target, now.Add(time.Minute)); err != nil {
		t.Fatalf("validate exact destroy target: %v", err)
	}
	tampered := signed
	tampered.InstanceID = "i-0ffffffffffffffff"
	if err := tampered.Verify(publicKey, now.Add(time.Minute)); err == nil {
		t.Fatal("destroy approval signature did not bind the instance")
	}
	tampered = signed
	tampered.SecretRefs = []string{"secret_ref:plan/forged"}
	if err := tampered.Verify(publicKey, now.Add(time.Minute)); err == nil {
		t.Fatal("destroy approval signature did not bind secret refs")
	}
	changed := target
	changed.DeploymentRevision++
	if err := signed.ValidateAgainst(changed, now.Add(time.Minute)); err == nil {
		t.Fatal("destroy approval accepted a newer deployment revision")
	}
}

func TestServiceDestroyApprovalRejectsUnsafeOrExpiredTargets(t *testing.T) {
	now := time.Date(2026, time.July, 15, 6, 0, 0, 0, time.UTC)
	valid := cloudorchestrator.ServiceDestroyTargetV1{
		ServiceID: "service-destroy-0001", ServiceRevision: 3,
		DeploymentID: "deployment-destroy-0001", DeploymentRevision: 8,
		CloudConnectionID: "connection-destroy-0001",
		RecipeID:          "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"},
	}
	for name, mutate := range map[string]func(*cloudorchestrator.ServiceDestroyTargetV1){
		"missing volume": func(value *cloudorchestrator.ServiceDestroyTargetV1) { value.VolumeIDs = nil },
		"duplicate volume": func(value *cloudorchestrator.ServiceDestroyTargetV1) {
			value.VolumeIDs = []string{value.VolumeIDs[0], value.VolumeIDs[0]}
		},
		"credential shaped id": func(value *cloudorchestrator.ServiceDestroyTargetV1) { value.InstanceID = "AKIAABCDEFGHIJKLMNOP" },
		"duplicate secret ref": func(value *cloudorchestrator.ServiceDestroyTargetV1) {
			value.SecretRefs = []string{"secret_ref:plan/model", "secret_ref:plan/model"}
		},
		"secret value": func(value *cloudorchestrator.ServiceDestroyTargetV1) {
			value.SecretRefs = []string{"sk-secret-material"}
		},
		"33 secret refs": func(value *cloudorchestrator.ServiceDestroyTargetV1) {
			value.SecretRefs = make([]string, 33)
			for index := range value.SecretRefs {
				value.SecretRefs[index] = fmt.Sprintf("secret_ref:plan/slot-%02d", index)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			target := valid
			target.VolumeIDs = append([]string(nil), valid.VolumeIDs...)
			target.NetworkInterfaceIDs = append([]string(nil), valid.NetworkInterfaceIDs...)
			mutate(&target)
			if _, err := cloudorchestrator.NewServiceDestroyApprovalV1(target, "destroy-approval-0001", "destroy-challenge-0001", "owner-device-0001", now, now.Add(5*time.Minute)); err == nil {
				t.Fatal("unsafe destroy target was accepted")
			}
		})
	}
	if _, err := cloudorchestrator.NewServiceDestroyApprovalV1(valid, "destroy-approval-0001", "destroy-challenge-0001", "owner-device-0001", now, now.Add(5*time.Minute+time.Millisecond)); err == nil {
		t.Fatal("destroy approval lifetime exceeded five minutes")
	}
}
