package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestDynamoDeploymentDestroyConsumesApprovalBeforeMutationAndFinalizesReceipt(t *testing.T) {
	client := &fakeDynamo{}
	repository := mustDynamoRepository(t, client)
	request := contract.DeploymentDestroyRequest{Schema: contract.DeploymentDestroySchema, ServiceID: "service-destroy-0001", DeploymentID: "deployment-destroy-0001", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}}
	requestJSON, _ := json.Marshal(request)
	reservation := DeploymentDestroyReservation{ConnectionID: "connection-destroy-0001", DeploymentID: request.DeploymentID, ServiceID: request.ServiceID, CommandID: "command-destroy-0001", RequestSHA256: strings.Repeat("a", 64), ExpectedGeneration: 2, NodeCounter: 11, ApprovalID: "approval-destroy-0001", ChallengeID: "challenge-destroy-0001", SignerKeyID: "device-destroy-0001", RequestJSON: requestJSON, State: "reserved"}
	stored, created, err := repository.ReserveDeploymentDestroy(t.Context(), reservation)
	if err != nil || !created || !stored.SameIdentity(reservation) {
		t.Fatalf("ReserveDeploymentDestroy()=(%#v,%t,%v)", stored, created, err)
	}
	if client.transactInput == nil || len(client.transactInput.TransactItems) != 4 {
		t.Fatalf("reserve transaction = %#v", client.transactInput)
	}
	if client.transactInput.TransactItems[2].Put == nil || client.transactInput.TransactItems[3].Put == nil {
		t.Fatal("destroy approval and challenge were not consumed atomically")
	}

	result := contract.DeploymentDestroyResult{Schema: contract.DeploymentDestroyResultSchema, Status: "verified_destroyed", Receipt: contract.DeploymentCommandReceipt{Schema: contract.ReceiptSchema, Disposition: "committed", ConnectionID: reservation.ConnectionID, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, Action: contract.ActionDeploymentDestroy}, Deployment: contract.DeploymentDestroyEvidence{DeploymentID: reservation.DeploymentID, InstanceID: request.InstanceID, VolumeIDs: request.VolumeIDs, NetworkInterfaceIDs: request.NetworkInterfaceIDs}}
	resultJSON, _ := json.Marshal(result)
	receipt := Record{ConnectionID: reservation.ConnectionID, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, Action: contract.ActionDeploymentDestroy, ResultJSON: resultJSON}
	storedReceipt, finalized, err := repository.FinalizeDeploymentDestroy(t.Context(), reservation, receipt)
	if err != nil || !finalized || !storedReceipt.SameIdentity(receipt) {
		t.Fatalf("FinalizeDeploymentDestroy()=(%#v,%t,%v)", storedReceipt, finalized, err)
	}
	if client.transactInput == nil || len(client.transactInput.TransactItems) != 2 {
		t.Fatalf("finalize transaction = %#v", client.transactInput)
	}
}

func TestDynamoDeploymentDestroyPersistsServiceFreeReservation(t *testing.T) {
	client := &fakeDynamo{}
	repository := mustDynamoRepository(t, client)
	request := contract.DeploymentDestroyRequest{Schema: contract.DeploymentDestroySchema, DeploymentID: "deployment-destroy-retained-0001", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0aaaaaaaaaaaaaaaa"}}
	requestJSON, _ := json.Marshal(request)
	reservation := DeploymentDestroyReservation{ConnectionID: "connection-destroy-retained-0001", DeploymentID: request.DeploymentID, CommandID: "command-destroy-retained-0001", RequestSHA256: strings.Repeat("b", 64), ExpectedGeneration: 2, NodeCounter: 12, ApprovalID: "approval-destroy-retained-0001", ChallengeID: "challenge-destroy-retained-0001", SignerKeyID: "device-destroy-retained-0001", RequestJSON: requestJSON, State: "reserved"}
	stored, created, err := repository.ReserveDeploymentDestroy(t.Context(), reservation)
	if err != nil || !created || !stored.SameIdentity(reservation) {
		t.Fatalf("ReserveDeploymentDestroy()=(%#v,%t,%v)", stored, created, err)
	}
	item := client.transactInput.TransactItems[1].Put.Item
	if _, present := item["service_id"]; present {
		t.Fatal("service-free deployment reservation persisted a synthetic service_id")
	}
	decoded, err := deploymentDestroyFromItem(item)
	if err != nil || !decoded.SameIdentity(reservation) {
		t.Fatalf("deploymentDestroyFromItem()=(%#v,%v)", decoded, err)
	}
}
