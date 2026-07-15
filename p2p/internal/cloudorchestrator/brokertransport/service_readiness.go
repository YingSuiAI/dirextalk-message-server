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

var _ runtime.ServiceReadinessTransport = (*Transport)(nil)

func (t *Transport) BuildServiceReadinessIssueCommand(command runtime.ServiceReadinessCommand, request runtime.ServiceReadinessIssueRequest, now time.Time) (runtime.SignedServiceReadinessCommand, error) {
	return t.buildServiceReadiness(command, request, runtime.ServiceReadinessIssueAction, now)
}
func (t *Transport) BuildServiceReadinessObserveCommand(command runtime.ServiceReadinessCommand, request runtime.ServiceReadinessObserveRequest, now time.Time) (runtime.SignedServiceReadinessCommand, error) {
	return t.buildServiceReadiness(command, request, runtime.ServiceReadinessObserveAction, now)
}
func (t *Transport) buildServiceReadiness(command runtime.ServiceReadinessCommand, request any, action string, now time.Time) (runtime.SignedServiceReadinessCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedServiceReadinessCommand{}, errors.New("node signing key unavailable")
	}
	issued := now.UTC().Truncate(time.Millisecond)
	expires := issued.Add(commandLifetime)
	input := broker.ServiceReadinessCommandInput{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issued, ExpiresAt: expires, Action: action, PrivateKey: t.privateKey}
	switch value := request.(type) {
	case runtime.ServiceReadinessIssueRequest:
		input.Issue = broker.ServiceReadinessIssueRequest{Schema: value.Schema, ExecutionID: value.ExecutionID, DeploymentID: value.DeploymentID, ServiceID: value.ServiceID, TaskID: value.TaskID, ProbeKind: value.ProbeKind, RecipeExecutionManifestDigest: value.RecipeExecutionManifestDigest, InstallEvidenceDigest: value.InstallEvidenceDigest, SemanticExpectationDigest: value.SemanticExpectationDigest}
	case runtime.ServiceReadinessObserveRequest:
		input.Observe = broker.ServiceReadinessObserveRequest{DeploymentID: value.DeploymentID, ServiceID: value.ServiceID, TaskID: value.TaskID}
	default:
		return runtime.SignedServiceReadinessCommand{}, errors.New("service readiness request invalid")
	}
	c, err := broker.NewServiceReadinessCommand(input)
	if err != nil {
		return runtime.SignedServiceReadinessCommand{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(c.PayloadB64)
	envelope, _ := json.Marshal(c)
	return runtime.SignedServiceReadinessCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: c.PayloadSHA256, RequestSHA256: c.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}
func (t *Transport) RequestServiceReadinessIssue(ctx context.Context, endpoint string, command runtime.ServiceReadinessCommand, signed runtime.SignedServiceReadinessCommand, _ runtime.ServiceReadinessIssueRequest) (runtime.ServiceReadinessResult, error) {
	return t.requestServiceReadiness(ctx, endpoint, command, signed)
}
func (t *Transport) RequestServiceReadinessObserve(ctx context.Context, endpoint string, command runtime.ServiceReadinessCommand, signed runtime.SignedServiceReadinessCommand, _ runtime.ServiceReadinessObserveRequest) (runtime.ServiceReadinessResult, error) {
	return t.requestServiceReadiness(ctx, endpoint, command, signed)
}
func (t *Transport) requestServiceReadiness(ctx context.Context, endpoint string, command runtime.ServiceReadinessCommand, signed runtime.SignedServiceReadinessCommand) (runtime.ServiceReadinessResult, error) {
	c, err := broker.ParseServiceReadinessCommand([]byte(signed.EnvelopeJSON))
	if err != nil || c.CommandID != command.CommandID || c.ConnectionID != command.ConnectionID || c.NodeCounter != command.NodeCounter || c.Action != command.Action || c.PayloadSHA256 != signed.PayloadSHA256 || c.RequestSHA256() != signed.RequestSHA256 {
		return runtime.ServiceReadinessResult{}, errors.New("persisted service readiness envelope invalid")
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.ServiceReadinessResult{}, err
	}
	result, err := client.SubmitServiceReadiness(ctx, c)
	if err != nil {
		return runtime.ServiceReadinessResult{}, classifyServiceReadinessBrokerError(err)
	}
	updated, err := time.Parse("2006-01-02T15:04:05.000Z", result.Task.UpdatedAt)
	if err != nil {
		return runtime.ServiceReadinessResult{}, errors.New("service readiness timestamp invalid")
	}
	return runtime.ServiceReadinessResult{ExecutionID: result.Task.ExecutionID, DeploymentID: result.Task.DeploymentID, ServiceID: result.Task.ServiceID, TaskID: result.Task.TaskID, Status: result.Task.Status, Checkpoint: result.Task.Checkpoint, Attempt: result.Task.Attempt, LastSequence: result.Task.LastSequence, ChallengeDigest: result.Task.ChallengeDigest, SemanticEvidenceDigest: result.Task.SemanticEvidenceDigest, StackObservationDigest: result.Task.StackObservationDigest, ErrorCode: result.Task.ErrorCode, UpdatedAt: updated.UTC()}, nil
}
func classifyServiceReadinessBrokerError(err error) error {
	var brokerError *broker.Error
	if errors.As(err, &brokerError) && brokerError.Code == "expired_command" {
		return runtime.ServiceReadinessCommandExpired(err)
	}
	return runtime.ExecutionProbeRetryable("service_readiness_broker_unavailable", err)
}
