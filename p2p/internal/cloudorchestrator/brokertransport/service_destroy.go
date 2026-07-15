package brokertransport

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func (t *Transport) BuildServiceDestroyCommand(command runtime.ServiceDestroyCommand, request broker.DeploymentDestroyRequest, approval cloudcontracts.ServiceDestroyApprovalV1) (runtime.SignedServiceDestroyCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedServiceDestroyCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	digest, err := runtime.ServiceDestroyRequestDigest(request)
	if err != nil || command.RequestDigest != digest || command.CommandID == "" || command.ServiceID != request.ServiceID || command.DeploymentID != request.DeploymentID || command.ConnectionID != approval.CloudConnectionID || command.NodeCounter <= 0 || command.ExpectedGeneration <= 0 {
		return runtime.SignedServiceDestroyCommand{}, errors.New("service destroy command does not bind request")
	}
	issued := t.now().UTC().Truncate(time.Millisecond)
	expires := issued.Add(commandLifetime)
	if approval.ExpiresAt.Before(expires) {
		expires = approval.ExpiresAt.UTC().Truncate(time.Millisecond)
	}
	if !expires.After(issued) {
		return runtime.SignedServiceDestroyCommand{}, errors.New("service destroy approval expired")
	}
	actual, err := broker.NewDeploymentDestroyCommand(broker.DeploymentDestroyCommandInput{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issued, ExpiresAt: expires, Request: request, ApprovalProof: approval, PrivateKey: t.privateKey})
	if err != nil {
		return runtime.SignedServiceDestroyCommand{}, errors.New("sign service destroy command failed")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil {
		return runtime.SignedServiceDestroyCommand{}, errors.New("signed service destroy payload is invalid")
	}
	envelope, err := json.Marshal(actual)
	if err != nil {
		return runtime.SignedServiceDestroyCommand{}, errors.New("signed service destroy envelope is invalid")
	}
	return runtime.SignedServiceDestroyCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: actual.PayloadSHA256, RequestSHA256: actual.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}

func (t *Transport) RequestServiceDestroy(ctx context.Context, endpoint string, command runtime.ServiceDestroyCommand, signed runtime.SignedServiceDestroyCommand, request broker.DeploymentDestroyRequest, approval cloudcontracts.ServiceDestroyApprovalV1) (runtime.ServiceDestroyResult, error) {
	actual, err := broker.ParseDeploymentDestroyCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.ServiceDestroyResult{}, errors.New("persisted service destroy envelope is invalid")
	}
	if err = actual.ValidateBinding(broker.DeploymentDestroyCommandBinding{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: request, ApprovalProof: approval}); err != nil || actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 {
		return runtime.ServiceDestroyResult{}, errors.New("persisted service destroy envelope does not bind command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return runtime.ServiceDestroyResult{}, errors.New("persisted service destroy payload is invalid")
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.ServiceDestroyResult{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitDeploymentDestroy(ctx, actual)
	if err != nil {
		return runtime.ServiceDestroyResult{}, classifyServiceDestroyBrokerError(err)
	}
	receipt, err := json.Marshal(result.Receipt)
	if err != nil {
		return runtime.ServiceDestroyResult{}, errors.New("service destroy receipt cannot be encoded")
	}
	return runtime.ServiceDestroyResult{Status: result.Status, DeploymentID: result.Deployment.DeploymentID, InstanceID: result.Deployment.InstanceID, VolumeIDs: append([]string(nil), result.Deployment.VolumeIDs...), NetworkInterfaceIDs: append([]string(nil), result.Deployment.NetworkInterfaceIDs...), CommandID: result.Receipt.CommandID, RequestSHA256: result.Receipt.RequestSHA256, ReceiptJSON: string(receipt)}, nil
}

func classifyServiceDestroyBrokerError(err error) error {
	var typed *broker.Error
	if !errors.As(err, &typed) {
		return runtime.ServiceDestroyRetryable("broker_unavailable", err)
	}
	if typed.Code == "deployment_destroy_in_progress" {
		return runtime.ServiceDestroyRetryable(typed.Code, err)
	}
	if typed.StatusCode == 429 || typed.StatusCode >= 500 {
		return runtime.ServiceDestroyRetryable("broker_unavailable", err)
	}
	switch typed.Code {
	case "broker_timeout":
		return runtime.ServiceDestroyRetryable("broker_timeout", err)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.ServiceDestroyRetryable("broker_unavailable", err)
	}
	return err
}
