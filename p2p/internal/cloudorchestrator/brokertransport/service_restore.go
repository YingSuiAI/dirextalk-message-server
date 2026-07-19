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

func (transport *Transport) BuildServiceRestoreCommand(command runtime.ServiceRestoreCommand, request broker.ServiceRestoreRequest, approval cloudcontracts.ServiceRestoreApprovalV1) (runtime.SignedServiceRestoreCommand, error) {
	if transport == nil || len(transport.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedServiceRestoreCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	digest, err := runtime.ServiceRestoreRequestDigest(request)
	if err != nil || command.RequestDigest != digest || command.CommandID == "" || command.RestoreID != request.RestoreID || command.ServiceID != request.ServiceID || command.DeploymentID != request.DeploymentID || command.ConnectionID != approval.CloudConnectionID || command.NodeCounter <= 0 || command.ExpectedGeneration <= 0 {
		return runtime.SignedServiceRestoreCommand{}, errors.New("service restore command does not bind request")
	}
	issued := transport.now().UTC().Truncate(time.Millisecond)
	expires := issued.Add(commandLifetime)
	if approval.ExpiresAt.Before(expires) {
		expires = approval.ExpiresAt.UTC().Truncate(time.Millisecond)
	}
	if !expires.After(issued) {
		return runtime.SignedServiceRestoreCommand{}, errors.New("service restore approval expired")
	}
	actual, err := broker.NewServiceRestoreCommand(broker.ServiceRestoreCommandInput{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issued, ExpiresAt: expires, Request: request, ApprovalProof: approval, PrivateKey: transport.privateKey})
	if err != nil {
		return runtime.SignedServiceRestoreCommand{}, errors.New("sign service restore command failed")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil {
		return runtime.SignedServiceRestoreCommand{}, errors.New("signed service restore payload is invalid")
	}
	envelope, err := json.Marshal(actual)
	if err != nil {
		return runtime.SignedServiceRestoreCommand{}, errors.New("signed service restore envelope is invalid")
	}
	return runtime.SignedServiceRestoreCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: actual.PayloadSHA256, RequestSHA256: actual.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}

func (transport *Transport) RequestServiceRestore(ctx context.Context, endpoint string, command runtime.ServiceRestoreCommand, signed runtime.SignedServiceRestoreCommand, request broker.ServiceRestoreRequest, approval cloudcontracts.ServiceRestoreApprovalV1) (runtime.ServiceRestoreResult, error) {
	actual, err := broker.ParseServiceRestoreCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.ServiceRestoreResult{}, errors.New("persisted service restore envelope is invalid")
	}
	if actual.ValidateBinding(broker.ServiceRestoreCommandBinding{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: request, ApprovalProof: approval}) != nil || actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 {
		return runtime.ServiceRestoreResult{}, errors.New("persisted service restore envelope does not bind command")
	}
	payload, _ := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if string(payload) != signed.PayloadJSON {
		return runtime.ServiceRestoreResult{}, errors.New("persisted service restore payload is invalid")
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.ServiceRestoreResult{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitServiceRestore(ctx, actual)
	if err != nil {
		return runtime.ServiceRestoreResult{}, classifyServiceRestoreBrokerError(err)
	}
	receipt, err := json.Marshal(result.Receipt)
	if err != nil {
		return runtime.ServiceRestoreResult{}, errors.New("service restore receipt cannot be encoded")
	}
	return runtime.ServiceRestoreResult{Status: result.Status, Evidence: result.Restore, CommandID: result.Receipt.CommandID, RequestSHA256: result.Receipt.RequestSHA256, ReceiptJSON: string(receipt)}, nil
}

func classifyServiceRestoreBrokerError(err error) error {
	var typed *broker.Error
	if !errors.As(err, &typed) {
		return runtime.ServiceRestoreRetryable("broker_unavailable", err)
	}
	if typed.Code == "service_restore_in_progress" {
		return runtime.ServiceRestoreRetryable(typed.Code, err)
	}
	if typed.StatusCode == 429 || typed.StatusCode >= 500 {
		return runtime.ServiceRestoreRetryable("broker_unavailable", err)
	}
	switch typed.Code {
	case "broker_timeout":
		return runtime.ServiceRestoreRetryable("broker_timeout", err)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.ServiceRestoreRetryable("broker_unavailable", err)
	default:
		return err
	}
}
