package brokertransport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func (t *Transport) BuildDeploymentDestroyCommand(command runtime.ServiceDestroyCommand, request broker.DeploymentDestroyRequest, approval cloudcontracts.DeploymentDestroyApprovalV1) (runtime.SignedServiceDestroyCommand, error) {
	if t == nil || len(t.privateKey) == 0 {
		return runtime.SignedServiceDestroyCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	digest, err := runtime.ServiceDestroyRequestDigest(request)
	if err != nil || digest != command.RequestDigest {
		return runtime.SignedServiceDestroyCommand{}, errors.New("deployment destroy command does not bind request")
	}
	issued := t.now().UTC()
	expires := issued.Add(5 * time.Minute)
	if approval.ExpiresAt.Before(expires) {
		expires = approval.ExpiresAt
	}
	if !expires.After(issued) {
		return runtime.SignedServiceDestroyCommand{}, errors.New("deployment destroy approval expired")
	}
	actual, err := broker.NewDeploymentDestroyCommand(broker.DeploymentDestroyCommandInput{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issued, ExpiresAt: expires,
		Request: request, DeploymentApprovalProof: approval, PrivateKey: t.privateKey,
	})
	if err != nil {
		return runtime.SignedServiceDestroyCommand{}, errors.New("sign deployment destroy command failed")
	}
	envelope, err := json.Marshal(actual)
	if err != nil {
		return runtime.SignedServiceDestroyCommand{}, errors.New("signed deployment destroy payload is invalid")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return runtime.SignedServiceDestroyCommand{}, errors.New("signed deployment destroy envelope is invalid")
	}
	return runtime.SignedServiceDestroyCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: actual.PayloadSHA256, RequestSHA256: actual.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}

func (t *Transport) RequestDeploymentDestroy(ctx context.Context, endpoint string, command runtime.ServiceDestroyCommand, signed runtime.SignedServiceDestroyCommand, request broker.DeploymentDestroyRequest, approval cloudcontracts.DeploymentDestroyApprovalV1) (runtime.ServiceDestroyResult, error) {
	actual, err := broker.ParseDeploymentDestroyCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.ServiceDestroyResult{}, errors.New("persisted deployment destroy envelope is invalid")
	}
	if err = actual.ValidateBinding(broker.DeploymentDestroyCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt,
		Request: request, DeploymentApprovalProof: approval,
	}); err != nil || actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 {
		return runtime.ServiceDestroyResult{}, errors.New("persisted deployment destroy envelope does not bind command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return runtime.ServiceDestroyResult{}, errors.New("persisted deployment destroy payload is invalid")
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
		return runtime.ServiceDestroyResult{}, errors.New("deployment destroy receipt cannot be encoded")
	}
	return runtime.ServiceDestroyResult{Status: result.Status, DeploymentID: result.Deployment.DeploymentID, InstanceID: result.Deployment.InstanceID, VolumeIDs: append([]string(nil), result.Deployment.VolumeIDs...), NetworkInterfaceIDs: append([]string(nil), result.Deployment.NetworkInterfaceIDs...), SecretRefs: append([]string(nil), result.Deployment.SecretRefs...), CommandID: result.Receipt.CommandID, RequestSHA256: result.Receipt.RequestSHA256, ReceiptJSON: string(receipt)}, nil
}
