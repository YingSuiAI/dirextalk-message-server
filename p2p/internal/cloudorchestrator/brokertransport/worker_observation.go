package brokertransport

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.WorkerBootstrapObservationTransport = (*Transport)(nil)

// BuildWorkerBootstrapObservationCommand creates the one read-only command
// permitted after a deployment create receipt exists. The command's own
// request digest and durable node counter prevent it from being redirected to
// another deployment or silently re-signed after a lost response.
func (t *Transport) BuildWorkerBootstrapObservationCommand(command runtime.WorkerBootstrapObservationCommand, request runtime.WorkerBootstrapObservationRequest, now time.Time) (runtime.SignedWorkerBootstrapObservationCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedWorkerBootstrapObservationCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	if err := request.Validate(); err != nil {
		return runtime.SignedWorkerBootstrapObservationCommand{}, errors.New("invalid worker bootstrap observation request")
	}
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest || command.CommandID == "" || command.DeploymentID != request.DeploymentID ||
		command.ConnectionID == "" || command.NodeKeyID == "" || command.ExpectedGeneration <= 0 || command.NodeCounter <= 0 {
		return runtime.SignedWorkerBootstrapObservationCommand{}, errors.New("worker bootstrap observation command does not bind the request")
	}
	issuedAt := now.UTC().Truncate(time.Millisecond)
	if issuedAt.IsZero() {
		return runtime.SignedWorkerBootstrapObservationCommand{}, errors.New("worker bootstrap observation clock is invalid")
	}
	brokerCommand, err := broker.NewDeploymentObserveCommand(broker.DeploymentObserveCommandInput{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(commandLifetime),
		Request: broker.DeploymentObserveRequest{DeploymentID: request.DeploymentID}, PrivateKey: t.privateKey,
	})
	if err != nil {
		return runtime.SignedWorkerBootstrapObservationCommand{}, errors.New("sign worker bootstrap observation command failed")
	}
	payload, err := base64.StdEncoding.DecodeString(brokerCommand.PayloadB64)
	if err != nil {
		return runtime.SignedWorkerBootstrapObservationCommand{}, errors.New("signed worker bootstrap observation payload is invalid")
	}
	envelope, err := json.Marshal(brokerCommand)
	if err != nil {
		return runtime.SignedWorkerBootstrapObservationCommand{}, errors.New("signed worker bootstrap observation envelope is invalid")
	}
	return runtime.SignedWorkerBootstrapObservationCommand{
		EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: brokerCommand.PayloadSHA256,
		RequestSHA256: brokerCommand.RequestSHA256(), IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(commandLifetime),
	}, nil
}

// RequestWorkerBootstrapObservation re-parses and re-binds the exact durable
// envelope before it makes one Broker call. It returns no raw Stack session
// state or receipt; only the closed, de-secreted observation is mapped into
// the runtime type.
func (t *Transport) RequestWorkerBootstrapObservation(ctx context.Context, endpoint string, command runtime.WorkerBootstrapObservationCommand, signed runtime.SignedWorkerBootstrapObservationCommand, request runtime.WorkerBootstrapObservationRequest) (runtime.WorkerBootstrapObservation, error) {
	brokerCommand, err := broker.ParseDeploymentObserveCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.WorkerBootstrapObservation{}, errors.New("persisted worker bootstrap observation envelope is invalid")
	}
	if err := workerBootstrapObservationCommandMatches(command, signed, brokerCommand, request); err != nil {
		return runtime.WorkerBootstrapObservation{}, err
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.WorkerBootstrapObservation{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitDeploymentObserve(ctx, brokerCommand)
	if err != nil {
		return runtime.WorkerBootstrapObservation{}, classifyWorkerBootstrapObservationBrokerError(err)
	}
	return runtimeWorkerBootstrapObservation(result)
}

func workerBootstrapObservationCommandMatches(command runtime.WorkerBootstrapObservationCommand, signed runtime.SignedWorkerBootstrapObservationCommand, actual broker.DeploymentObserveCommand, request runtime.WorkerBootstrapObservationRequest) error {
	digest, err := request.Digest()
	if err != nil || command.RequestDigest != digest || command.DeploymentID != request.DeploymentID {
		return errors.New("persisted worker bootstrap observation envelope does not bind the request")
	}
	if err := actual.ValidateBinding(broker.DeploymentObserveCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt,
		Request: broker.DeploymentObserveRequest{DeploymentID: request.DeploymentID},
	}); err != nil {
		return errors.New("persisted worker bootstrap observation envelope does not bind the command")
	}
	if actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 ||
		(command.RequestSHA256 != "" && actual.RequestSHA256() != command.RequestSHA256) {
		return errors.New("persisted worker bootstrap observation envelope does not bind the command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("persisted worker bootstrap observation payload is invalid")
	}
	return nil
}

func runtimeWorkerBootstrapObservation(result broker.DeploymentObserveResult) (runtime.WorkerBootstrapObservation, error) {
	leaseExpiresAt, err := optionalObservationTime(result.Observation.Worker.LeaseExpiresAt)
	if err != nil {
		return runtime.WorkerBootstrapObservation{}, errors.New("broker worker lease timestamp is invalid")
	}
	lastEventAt, err := optionalObservationTime(result.Observation.Worker.LastEventAt)
	if err != nil {
		return runtime.WorkerBootstrapObservation{}, errors.New("broker worker event timestamp is invalid")
	}
	observedAt, err := time.Parse("2006-01-02T15:04:05.000Z", result.Observation.ObservedAt)
	if err != nil {
		return runtime.WorkerBootstrapObservation{}, errors.New("broker worker observation timestamp is invalid")
	}
	return runtime.WorkerBootstrapObservation{
		Schema: result.Observation.Schema, DeploymentID: result.Observation.DeploymentID,
		ResourceStatus: result.Observation.Resource.Status, InstanceID: result.Observation.Resource.InstanceID,
		WorkerSessionState: result.Observation.Worker.BootstrapSessionState, LeaseEpoch: result.Observation.Worker.LeaseEpoch,
		LeaseExpiresAt: leaseExpiresAt, LastSequence: result.Observation.Worker.LastSequence, LastEventAt: lastEventAt,
		ObservedAt: observedAt.UTC(),
	}, nil
}

func optionalObservationTime(value *string) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000Z", *value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func classifyWorkerBootstrapObservationBrokerError(err error) error {
	var brokerError *broker.Error
	if !errors.As(err, &brokerError) {
		return runtime.WorkerBootstrapObservationRetryable("broker_unavailable", err)
	}
	if brokerError.Code == "expired_command" {
		return runtime.WorkerBootstrapObservationCommandExpired(err)
	}
	if brokerError.StatusCode == 429 || brokerError.StatusCode >= 500 {
		return runtime.WorkerBootstrapObservationRetryable("broker_unavailable", err)
	}
	switch brokerError.Code {
	case "broker_timeout":
		return runtime.WorkerBootstrapObservationRetryable("broker_timeout", err)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.WorkerBootstrapObservationRetryable("broker_unavailable", err)
	default:
		return err
	}
}
