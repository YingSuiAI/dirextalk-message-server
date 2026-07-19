package brokertransport

import (
	"crypto/ed25519"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestBuildDeploymentDestroyCommandBindsResidualResourcesWithoutService(t *testing.T) {
	now := time.Date(2026, time.July, 16, 11, 0, 0, 0, time.UTC)
	target := cloudcontracts.DeploymentDestroyTargetV1{DeploymentID: "deployment-retained-transport-1", DeploymentRevision: 5, PlanID: "plan-retained-transport-1", CloudConnectionID: "connection-retained-transport-1", ResourceStatus: "orphaned", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}}
	approval, err := cloudcontracts.NewDeploymentDestroyApprovalV1(target, "approval-retained-transport-1", "challenge-retained-transport-1", "device-retained-transport-1", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, deviceKey, _ := ed25519.GenerateKey(nil)
	approval, err = approval.Sign(deviceKey, now)
	if err != nil {
		t.Fatal(err)
	}
	request := broker.DeploymentDestroyRequest{Schema: broker.DeploymentDestroySchema, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs}
	digest, err := runtime.ServiceDestroyRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	command := runtime.ServiceDestroyCommand{CommandID: "command-retained-transport-1", DeploymentID: request.DeploymentID, ConnectionID: approval.CloudConnectionID, NodeKeyID: "node-retained-transport-1", ExpectedGeneration: 1, NodeCounter: 8, Attempt: 1, RequestDigest: digest}
	_, nodeKey, _ := ed25519.GenerateKey(nil)
	transport, err := New(nodeKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildDeploymentDestroyCommand(command, request, approval)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := broker.ParseDeploymentDestroyCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if err = actual.ValidateBinding(broker.DeploymentDestroyCommandBinding{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: request, DeploymentApprovalProof: approval}); err != nil {
		t.Fatal(err)
	}
	drifted := request
	drifted.NetworkInterfaceIDs = []string{"eni-0bbbbbbbbbbbbbbbb"}
	if _, err = transport.BuildDeploymentDestroyCommand(command, drifted, approval); err == nil {
		t.Fatal("drifted deployment resource set must not be signed")
	}
}
