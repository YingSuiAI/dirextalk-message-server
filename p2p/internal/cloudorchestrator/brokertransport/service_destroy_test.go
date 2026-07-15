package brokertransport

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestBuildServiceDestroyCommandBindsExactApprovalAndResources(t *testing.T) {
	now := time.Date(2026, time.July, 15, 11, 0, 0, 0, time.UTC)
	request, approval := destroyTransportFixture(t, now)
	digest, err := runtime.ServiceDestroyRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	command := runtime.ServiceDestroyCommand{CommandID: "command-destroy-transport-1", ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, ConnectionID: approval.CloudConnectionID, NodeKeyID: "node-destroy-transport-1", ExpectedGeneration: 1, NodeCounter: 7, Attempt: 1, RequestDigest: digest}
	_, nodeKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := New(nodeKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildServiceDestroyCommand(command, request, approval)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := broker.ParseDeploymentDestroyCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if err = actual.ValidateBinding(broker.DeploymentDestroyCommandBinding{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: request, ApprovalProof: approval}); err != nil {
		t.Fatal(err)
	}
	drifted := request
	drifted.VolumeIDs = []string{"vol-0bbbbbbbbbbbbbbbb"}
	if _, err = transport.BuildServiceDestroyCommand(command, drifted, approval); err == nil {
		t.Fatal("drifted resource set must not be signed")
	}
}

func destroyTransportFixture(t *testing.T, now time.Time) (broker.DeploymentDestroyRequest, cloudcontracts.ServiceDestroyApprovalV1) {
	t.Helper()
	target := cloudcontracts.ServiceDestroyTargetV1{ServiceID: "service-destroy-transport-1", ServiceRevision: 2, DeploymentID: "deployment-destroy-transport-1", DeploymentRevision: 5, CloudConnectionID: "connection-destroy-transport-1", RecipeID: "recipe-destroy-transport-1", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}}
	approval, err := cloudcontracts.NewServiceDestroyApprovalV1(target, "approval-destroy-transport-1", "challenge-destroy-transport-1", "device-destroy-transport-1", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	approval, err = approval.Sign(key, now)
	if err != nil {
		t.Fatal(err)
	}
	return broker.DeploymentDestroyRequest{Schema: broker.DeploymentDestroySchema, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs}, approval
}
