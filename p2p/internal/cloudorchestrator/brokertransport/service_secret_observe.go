package brokertransport

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.ServiceSecretObserveTransport = (*Transport)(nil)

func (t *Transport) BuildServiceSecretObserveCommand(command runtime.ServiceSecretObserveCommand, request runtime.ServiceSecretObserveRequest, now time.Time) (runtime.SignedServiceSecretObserveCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedServiceSecretObserveCommand{}, errors.New("node signing key unavailable")
	}
	issued := now.UTC().Truncate(time.Millisecond)
	expires := issued.Add(commandLifetime)
	c, err := broker.NewServiceSecretObserveCommand(broker.ServiceSecretObserveCommandInput{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issued, ExpiresAt: expires, Request: broker.ServiceSecretObserveRequest{SessionID: request.SessionID, DeploymentID: request.DeploymentID, TaskID: request.TaskID, ExecutionID: request.ExecutionID, ManifestDigest: request.ManifestDigest, SecretRef: request.SecretRef, ContextDigest: request.ContextDigest}, PrivateKey: t.privateKey})
	if err != nil {
		return runtime.SignedServiceSecretObserveCommand{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(c.PayloadB64)
	envelope, _ := json.Marshal(c)
	return runtime.SignedServiceSecretObserveCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: c.PayloadSHA256, RequestSHA256: c.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}

func (t *Transport) RequestServiceSecretObserve(ctx context.Context, endpoint string, command runtime.ServiceSecretObserveCommand, signed runtime.SignedServiceSecretObserveCommand, request runtime.ServiceSecretObserveRequest) (runtime.ServiceSecretObservation, error) {
	c, err := broker.ParseServiceSecretObserveCommand([]byte(signed.EnvelopeJSON))
	payload, payloadErr := base64.StdEncoding.DecodeString(c.PayloadB64)
	issuedAt, issuedErr := time.Parse("2006-01-02T15:04:05.000Z", c.IssuedAt)
	expiresAt, expiresErr := time.Parse("2006-01-02T15:04:05.000Z", c.ExpiresAt)
	if err != nil || payloadErr != nil || issuedErr != nil || expiresErr != nil || !issuedAt.Equal(signed.IssuedAt) || !expiresAt.Equal(signed.ExpiresAt) || string(payload) != signed.PayloadJSON || c.CommandID != command.CommandID || c.ConnectionID != command.ConnectionID || c.NodeKeyID != command.NodeKeyID || c.ExpectedGeneration != command.ExpectedGeneration || c.NodeCounter != command.NodeCounter || c.Action != runtime.ServiceSecretObserveAction || c.PayloadSHA256 != signed.PayloadSHA256 || c.RequestSHA256() != signed.RequestSHA256 {
		return runtime.ServiceSecretObservation{}, errors.New("persisted service secret observe envelope invalid")
	}
	bound, err := c.Request()
	if err != nil || bound != (broker.ServiceSecretObserveRequest{SessionID: request.SessionID, DeploymentID: request.DeploymentID, TaskID: request.TaskID, ExecutionID: request.ExecutionID, ManifestDigest: request.ManifestDigest, SecretRef: request.SecretRef, ContextDigest: request.ContextDigest}) {
		return runtime.ServiceSecretObservation{}, errors.New("persisted service secret observe request invalid")
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint), RootCAs: t.rootCAs})
	if err != nil {
		return runtime.ServiceSecretObservation{}, err
	}
	result, err := client.SubmitServiceSecretObserve(ctx, c)
	if err != nil {
		var brokerErr *broker.Error
		if errors.As(err, &brokerErr) {
			if brokerErr.StatusCode == http.StatusNotFound {
				return runtime.ServiceSecretObservation{}, runtime.ServiceSecretObserveUnavailable(err)
			}
			switch brokerErr.Code {
			case "expired_command":
				return runtime.ServiceSecretObservation{}, runtime.ServiceSecretObserveCommandExpired(err)
			case "service_secret_observe_unavailable":
				return runtime.ServiceSecretObservation{}, runtime.ServiceSecretObserveUnavailable(err)
			}
		}
		return runtime.ServiceSecretObservation{}, runtime.ExecutionProbeRetryable("service_secret_observe_broker_unavailable", err)
	}
	return runtime.ServiceSecretObservation{SessionID: result.SessionID, Status: result.Status, ProviderVersion: result.ProviderVersion, BindingDigest: result.BindingDigest, UpdatedMarker: result.UpdatedMarker}, nil
}
