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

var _ runtime.ExecutionProbeTransport = (*Transport)(nil)

// BuildExecutionProbeIssueCommand signs the sole digest-only Worker task that
// the runtime may issue before a Recipe executor exists. The returned exact
// envelope must be persisted before RequestExecutionProbeIssue can send it.
func (t *Transport) BuildExecutionProbeIssueCommand(command runtime.ExecutionProbeCommand, request runtime.ExecutionProbeIssueRequest, now time.Time) (runtime.SignedExecutionProbeCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedExecutionProbeCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	if err := request.Validate(); err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("invalid execution probe issue request")
	}
	digest, err := request.Digest()
	if err != nil || !executionProbeCommandBindsRequest(command, request.DeploymentID, request.TaskID, runtime.ExecutionProbeIssueAction, digest) {
		return runtime.SignedExecutionProbeCommand{}, errors.New("execution probe issue command does not bind the request")
	}
	issuedAt, expiresAt, err := executionProbeCommandTimes(now)
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, err
	}
	brokerCommand, err := broker.NewWorkerTaskIssueCommand(broker.WorkerTaskIssueCommandInput{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: issuedAt, ExpiresAt: expiresAt, Request: brokerWorkerTaskIssueRequest(request), PrivateKey: t.privateKey,
	})
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("sign execution probe issue command failed")
	}
	return signedExecutionProbeIssueCommand(brokerCommand, issuedAt, expiresAt)
}

// RequestExecutionProbeIssue re-parses the exact persisted envelope and proves
// it remains bound to the leased command before the one typed Stack request.
// Only the closed, de-secreted task summary crosses back into the runtime.
func (t *Transport) RequestExecutionProbeIssue(ctx context.Context, endpoint string, command runtime.ExecutionProbeCommand, signed runtime.SignedExecutionProbeCommand, request runtime.ExecutionProbeIssueRequest) (runtime.ExecutionProbeTaskResult, error) {
	brokerCommand, err := broker.ParseWorkerTaskIssueCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.ExecutionProbeTaskResult{}, errors.New("persisted execution probe issue envelope is invalid")
	}
	if err := executionProbeIssueCommandMatches(command, signed, brokerCommand, request); err != nil {
		return runtime.ExecutionProbeTaskResult{}, err
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.ExecutionProbeTaskResult{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitWorkerTaskIssue(ctx, brokerCommand)
	if err != nil {
		return runtime.ExecutionProbeTaskResult{}, classifyExecutionProbeBrokerError(err)
	}
	return runtimeExecutionProbeTaskResult(result.Task)
}

// BuildExecutionProbeObserveCommand signs the read-only task observation that
// follows an already-issued execution probe. It carries no execution material.
func (t *Transport) BuildExecutionProbeObserveCommand(command runtime.ExecutionProbeCommand, request runtime.ExecutionProbeObserveRequest, now time.Time) (runtime.SignedExecutionProbeCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedExecutionProbeCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	if err := request.Validate(); err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("invalid execution probe observe request")
	}
	digest, err := request.Digest()
	if err != nil || !executionProbeCommandBindsRequest(command, request.DeploymentID, request.TaskID, runtime.ExecutionProbeObserveAction, digest) {
		return runtime.SignedExecutionProbeCommand{}, errors.New("execution probe observe command does not bind the request")
	}
	issuedAt, expiresAt, err := executionProbeCommandTimes(now)
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, err
	}
	brokerCommand, err := broker.NewWorkerTaskObserveCommand(broker.WorkerTaskObserveCommandInput{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: issuedAt, ExpiresAt: expiresAt, Request: brokerWorkerTaskObserveRequest(request), PrivateKey: t.privateKey,
	})
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("sign execution probe observe command failed")
	}
	return signedExecutionProbeObserveCommand(brokerCommand, issuedAt, expiresAt)
}

// RequestExecutionProbeObserve re-parses and re-binds the private persisted
// envelope before the typed Stack observation request. It never returns a
// receipt, task document, log, URL, bearer or Worker identity.
func (t *Transport) RequestExecutionProbeObserve(ctx context.Context, endpoint string, command runtime.ExecutionProbeCommand, signed runtime.SignedExecutionProbeCommand, request runtime.ExecutionProbeObserveRequest) (runtime.ExecutionProbeTaskResult, error) {
	brokerCommand, err := broker.ParseWorkerTaskObserveCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return runtime.ExecutionProbeTaskResult{}, errors.New("persisted execution probe observe envelope is invalid")
	}
	if err := executionProbeObserveCommandMatches(command, signed, brokerCommand, request); err != nil {
		return runtime.ExecutionProbeTaskResult{}, err
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.ExecutionProbeTaskResult{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitWorkerTaskObserve(ctx, brokerCommand)
	if err != nil {
		return runtime.ExecutionProbeTaskResult{}, classifyExecutionProbeBrokerError(err)
	}
	return runtimeExecutionProbeTaskResult(result.Task)
}

func executionProbeCommandTimes(now time.Time) (time.Time, time.Time, error) {
	issuedAt := now.UTC().Truncate(time.Millisecond)
	if issuedAt.IsZero() {
		return time.Time{}, time.Time{}, errors.New("execution probe command clock is invalid")
	}
	return issuedAt, issuedAt.Add(commandLifetime), nil
}

func executionProbeCommandBindsRequest(command runtime.ExecutionProbeCommand, deploymentID, taskID, action, requestDigest string) bool {
	return command.RequestDigest == requestDigest && command.CommandID != "" && command.DeploymentID == deploymentID && command.TaskID == taskID &&
		command.ConnectionID != "" && command.NodeKeyID != "" && command.ExpectedGeneration > 0 && command.NodeCounter > 0 && command.Attempt > 0 && command.Action == action
}

func brokerWorkerTaskIssueRequest(request runtime.ExecutionProbeIssueRequest) broker.WorkerTaskIssueRequest {
	return broker.WorkerTaskIssueRequest{
		Schema: request.Schema, DeploymentID: request.DeploymentID, TaskID: request.TaskID, TaskKind: request.TaskKind,
		ExecutionManifestDigest: request.ExecutionManifestDigest, InputDigest: request.InputDigest,
	}
}

func brokerWorkerTaskObserveRequest(request runtime.ExecutionProbeObserveRequest) broker.WorkerTaskObserveRequest {
	return broker.WorkerTaskObserveRequest{DeploymentID: request.DeploymentID, TaskID: request.TaskID}
}

func signedExecutionProbeIssueCommand(command broker.WorkerTaskIssueCommand, issuedAt, expiresAt time.Time) (runtime.SignedExecutionProbeCommand, error) {
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("signed execution probe issue payload is invalid")
	}
	envelope, err := json.Marshal(command)
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("signed execution probe issue envelope is invalid")
	}
	return runtime.SignedExecutionProbeCommand{
		EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: command.PayloadSHA256,
		RequestSHA256: command.RequestSHA256(), IssuedAt: issuedAt, ExpiresAt: expiresAt,
	}, nil
}

func signedExecutionProbeObserveCommand(command broker.WorkerTaskObserveCommand, issuedAt, expiresAt time.Time) (runtime.SignedExecutionProbeCommand, error) {
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("signed execution probe observe payload is invalid")
	}
	envelope, err := json.Marshal(command)
	if err != nil {
		return runtime.SignedExecutionProbeCommand{}, errors.New("signed execution probe observe envelope is invalid")
	}
	return runtime.SignedExecutionProbeCommand{
		EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: command.PayloadSHA256,
		RequestSHA256: command.RequestSHA256(), IssuedAt: issuedAt, ExpiresAt: expiresAt,
	}, nil
}

func executionProbeIssueCommandMatches(command runtime.ExecutionProbeCommand, signed runtime.SignedExecutionProbeCommand, actual broker.WorkerTaskIssueCommand, request runtime.ExecutionProbeIssueRequest) error {
	digest, err := request.Digest()
	if err != nil || !executionProbeCommandBindsRequest(command, request.DeploymentID, request.TaskID, runtime.ExecutionProbeIssueAction, digest) {
		return errors.New("persisted execution probe issue envelope does not bind the request")
	}
	if err := actual.ValidateBinding(broker.WorkerTaskIssueCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: brokerWorkerTaskIssueRequest(request),
	}); err != nil {
		return errors.New("persisted execution probe issue envelope does not bind the command")
	}
	if actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 ||
		(command.RequestSHA256 != "" && actual.RequestSHA256() != command.RequestSHA256) {
		return errors.New("persisted execution probe issue envelope does not bind the command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("persisted execution probe issue envelope payload is invalid")
	}
	return nil
}

func executionProbeObserveCommandMatches(command runtime.ExecutionProbeCommand, signed runtime.SignedExecutionProbeCommand, actual broker.WorkerTaskObserveCommand, request runtime.ExecutionProbeObserveRequest) error {
	digest, err := request.Digest()
	if err != nil || !executionProbeCommandBindsRequest(command, request.DeploymentID, request.TaskID, runtime.ExecutionProbeObserveAction, digest) {
		return errors.New("persisted execution probe observe envelope does not bind the request")
	}
	if err := actual.ValidateBinding(broker.WorkerTaskObserveCommandBinding{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, Request: brokerWorkerTaskObserveRequest(request),
	}); err != nil {
		return errors.New("persisted execution probe observe envelope does not bind the command")
	}
	if actual.PayloadSHA256 != signed.PayloadSHA256 || actual.RequestSHA256() != signed.RequestSHA256 ||
		(command.RequestSHA256 != "" && actual.RequestSHA256() != command.RequestSHA256) {
		return errors.New("persisted execution probe observe envelope does not bind the command")
	}
	payload, err := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("persisted execution probe observe envelope payload is invalid")
	}
	return nil
}

func runtimeExecutionProbeTaskResult(summary broker.WorkerTaskSummary) (runtime.ExecutionProbeTaskResult, error) {
	updatedAt, err := time.Parse("2006-01-02T15:04:05.000Z", summary.UpdatedAt)
	if err != nil {
		return runtime.ExecutionProbeTaskResult{}, errors.New("broker execution probe task timestamp is invalid")
	}
	return runtime.ExecutionProbeTaskResult{
		TaskID: summary.TaskID, DeploymentID: summary.DeploymentID, Status: summary.Status, Attempt: summary.Attempt,
		LastSequence: summary.LastSequence, Checkpoint: copyExecutionProbeString(summary.Checkpoint),
		ErrorCode: copyExecutionProbeString(summary.ErrorCode), EvidenceDigest: copyExecutionProbeString(summary.EvidenceDigest),
		UpdatedAt: updatedAt.UTC(),
	}, nil
}

func copyExecutionProbeString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func classifyExecutionProbeBrokerError(err error) error {
	var brokerError *broker.Error
	if !errors.As(err, &brokerError) {
		return runtime.ExecutionProbeRetryable("broker_unavailable", err)
	}
	if brokerError.Code == "expired_command" {
		return runtime.ExecutionProbeCommandExpired(err)
	}
	if brokerError.StatusCode == 429 || brokerError.StatusCode >= 500 {
		return runtime.ExecutionProbeRetryable("broker_unavailable", err)
	}
	switch brokerError.Code {
	case "broker_timeout":
		return runtime.ExecutionProbeRetryable("broker_timeout", err)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.ExecutionProbeRetryable("broker_unavailable", err)
	default:
		return err
	}
}
