package cloudorchestrator_test

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestDeploymentDestroyApprovalBindsExactRetainedResources(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	target := cloudorchestrator.DeploymentDestroyTargetV1{
		DeploymentID:        "deployment-destroy-retained-0001",
		DeploymentRevision:  12,
		PlanID:              "plan-destroy-retained-0001",
		CloudConnectionID:   "connection-destroy-retained-0001",
		ResourceStatus:      "orphaned",
		InstanceID:          "i-0123456789abcdef0",
		VolumeIDs:           []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"},
		NetworkInterfaceIDs: []string{"eni-0bbbbbbbbbbbbbbbb", "eni-0aaaaaaaaaaaaaaaa"},
		SecretRefs:          []string{"secret_ref:plan/registry", "secret_ref:plan/model"},
	}
	approval, err := cloudorchestrator.NewDeploymentDestroyApprovalV1(target, "deployment-destroy-approval-0001", "deployment-destroy-challenge-0001", "owner-device-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if approval.Intent != "deployment_destroy" || approval.VolumeIDs[0] != "vol-0aaaaaaaaaaaaaaaa" || approval.NetworkInterfaceIDs[0] != "eni-0aaaaaaaaaaaaaaaa" || approval.SecretRefs[0] != "secret_ref:plan/model" {
		t.Fatalf("approval was not canonically bound: %#v", approval)
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
		t.Fatalf("Verify() error = %v", err)
	}
	if err := signed.ValidateAgainst(target, now.Add(time.Minute)); err != nil {
		t.Fatalf("ValidateAgainst() error = %v", err)
	}

	tamperCases := map[string]func(*cloudorchestrator.DeploymentDestroyApprovalV1){
		"deployment revision": func(value *cloudorchestrator.DeploymentDestroyApprovalV1) { value.DeploymentRevision++ },
		"plan":                func(value *cloudorchestrator.DeploymentDestroyApprovalV1) { value.PlanID = "plan-destroy-forged-0001" },
		"connection": func(value *cloudorchestrator.DeploymentDestroyApprovalV1) {
			value.CloudConnectionID = "connection-destroy-forged-0001"
		},
		"resource status": func(value *cloudorchestrator.DeploymentDestroyApprovalV1) { value.ResourceStatus = "blocked" },
		"instance":        func(value *cloudorchestrator.DeploymentDestroyApprovalV1) { value.InstanceID = "i-0ffffffffffffffff" },
		"volumes": func(value *cloudorchestrator.DeploymentDestroyApprovalV1) {
			value.VolumeIDs = []string{"vol-0cccccccccccccccc"}
		},
		"interfaces": func(value *cloudorchestrator.DeploymentDestroyApprovalV1) {
			value.NetworkInterfaceIDs = []string{"eni-0cccccccccccccccc"}
		},
		"secret refs": func(value *cloudorchestrator.DeploymentDestroyApprovalV1) {
			value.SecretRefs = []string{"secret_ref:plan/forged"}
		},
	}
	for name, mutate := range tamperCases {
		t.Run(name, func(t *testing.T) {
			tampered := signed
			mutate(&tampered)
			if err := tampered.Verify(publicKey, now.Add(time.Minute)); err == nil {
				t.Fatal("tampered approval verified")
			}
		})
	}
}

func TestDeploymentDestroyApprovalRejectsUnsafeTargets(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	valid := cloudorchestrator.DeploymentDestroyTargetV1{
		DeploymentID: "deployment-destroy-retained-0001", DeploymentRevision: 12,
		PlanID: "plan-destroy-retained-0001", CloudConnectionID: "connection-destroy-retained-0001", ResourceStatus: "retained_tracked",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"},
	}
	for name, mutate := range map[string]func(*cloudorchestrator.DeploymentDestroyTargetV1){
		"untracked status":  func(value *cloudorchestrator.DeploymentDestroyTargetV1) { value.ResourceStatus = "none" },
		"destroying status": func(value *cloudorchestrator.DeploymentDestroyTargetV1) { value.ResourceStatus = "destroying" },
		"missing volume":    func(value *cloudorchestrator.DeploymentDestroyTargetV1) { value.VolumeIDs = nil },
		"duplicate volume": func(value *cloudorchestrator.DeploymentDestroyTargetV1) {
			value.VolumeIDs = append(value.VolumeIDs, value.VolumeIDs[0])
		},
		"missing interface": func(value *cloudorchestrator.DeploymentDestroyTargetV1) {
			value.NetworkInterfaceIDs = nil
		},
		"credential shaped instance": func(value *cloudorchestrator.DeploymentDestroyTargetV1) { value.InstanceID = "AKIAABCDEFGHIJKLMNOP" },
		"duplicate secret ref": func(value *cloudorchestrator.DeploymentDestroyTargetV1) {
			value.SecretRefs = []string{"secret_ref:plan/model", "secret_ref:plan/model"}
		},
		"secret material": func(value *cloudorchestrator.DeploymentDestroyTargetV1) {
			value.SecretRefs = []string{"sk-secret-material"}
		},
		"too many secret refs": func(value *cloudorchestrator.DeploymentDestroyTargetV1) {
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
			if _, err := cloudorchestrator.NewDeploymentDestroyApprovalV1(target, "deployment-destroy-approval-0001", "deployment-destroy-challenge-0001", "owner-device-0001", now, now.Add(5*time.Minute)); err == nil {
				t.Fatal("unsafe deployment destroy target was accepted")
			}
		})
	}
	if _, err := cloudorchestrator.NewDeploymentDestroyApprovalV1(valid, "deployment-destroy-approval-0001", "deployment-destroy-challenge-0001", "owner-device-0001", now, now.Add(5*time.Minute+time.Millisecond)); err == nil {
		t.Fatal("approval lifetime exceeded five minutes")
	}
}

func TestDeploymentDestroyApprovalJSONFields(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	approval, err := cloudorchestrator.NewDeploymentDestroyApprovalV1(cloudorchestrator.DeploymentDestroyTargetV1{
		DeploymentID: "deployment-json-0001", DeploymentRevision: 4, PlanID: "plan-json-0001", CloudConnectionID: "connection-json-0001", ResourceStatus: "active",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}, SecretRefs: []string{"secret_ref:plan/model"},
	}, "approval-json-0001", "challenge-json-0001", "device-json-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(object))
	for key := range object {
		got = append(got, key)
	}
	want := []string{"approval_id", "challenge_id", "cloud_connection_id", "deployment_id", "deployment_revision", "expires_at", "instance_id", "intent", "issued_at", "network_interface_ids", "plan_id", "resource_status", "schema_version", "secret_refs", "signer_key_id", "volume_ids"}
	// Signature is omitted before signing; all other contract fields are stable.
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON fields = %v, want %v", got, want)
	}
}
