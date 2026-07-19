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

var _ runtime.RecipeInstallTransport = (*Transport)(nil)

func (t *Transport) BuildRecipeInstallIssueCommand(command runtime.RecipeInstallCommand, request runtime.RecipeInstallIssueRequest, now time.Time) (runtime.SignedRecipeInstallCommand, error) {
	return t.buildRecipeInstall(command, request, runtime.RecipeInstallIssueAction, now)
}
func (t *Transport) BuildRecipeInstallObserveCommand(command runtime.RecipeInstallCommand, request runtime.RecipeInstallObserveRequest, now time.Time) (runtime.SignedRecipeInstallCommand, error) {
	return t.buildRecipeInstall(command, request, runtime.RecipeInstallObserveAction, now)
}
func (t *Transport) buildRecipeInstall(command runtime.RecipeInstallCommand, request any, action string, now time.Time) (runtime.SignedRecipeInstallCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedRecipeInstallCommand{}, errors.New("node signing key unavailable")
	}
	issued := now.UTC().Truncate(time.Millisecond)
	expires := issued.Add(commandLifetime)
	input := broker.RecipeTaskCommandInput{ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issued, ExpiresAt: expires, Action: action, PrivateKey: t.privateKey}
	switch value := request.(type) {
	case runtime.RecipeInstallIssueRequest:
		input.Issue = broker.RecipeTaskIssueRequest{Schema: value.Schema, ExecutionID: value.ExecutionID, DeploymentID: value.DeploymentID, TaskID: value.TaskID, TaskKind: value.TaskKind, ManifestDigest: value.RecipeExecutionManifestDigest, InputDigest: value.InputDigest, CheckpointSequence: append([]string(nil), value.CheckpointSequence...), Manifest: value.Manifest}
	case runtime.RecipeInstallObserveRequest:
		input.Observe = broker.RecipeTaskObserveRequest{DeploymentID: value.DeploymentID, TaskID: value.TaskID}
	default:
		return runtime.SignedRecipeInstallCommand{}, errors.New("recipe install request invalid")
	}
	c, err := broker.NewRecipeTaskCommand(input)
	if err != nil {
		return runtime.SignedRecipeInstallCommand{}, err
	}
	payload, _ := base64.StdEncoding.DecodeString(c.PayloadB64)
	envelope, _ := json.Marshal(c)
	return runtime.SignedRecipeInstallCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: c.PayloadSHA256, RequestSHA256: c.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}
func (t *Transport) RequestRecipeInstallIssue(ctx context.Context, endpoint string, command runtime.RecipeInstallCommand, signed runtime.SignedRecipeInstallCommand, request runtime.RecipeInstallIssueRequest) (runtime.RecipeInstallResult, error) {
	return t.requestRecipeInstall(ctx, endpoint, command, signed)
}
func (t *Transport) RequestRecipeInstallObserve(ctx context.Context, endpoint string, command runtime.RecipeInstallCommand, signed runtime.SignedRecipeInstallCommand, request runtime.RecipeInstallObserveRequest) (runtime.RecipeInstallResult, error) {
	return t.requestRecipeInstall(ctx, endpoint, command, signed)
}
func (t *Transport) requestRecipeInstall(ctx context.Context, endpoint string, command runtime.RecipeInstallCommand, signed runtime.SignedRecipeInstallCommand) (runtime.RecipeInstallResult, error) {
	c, err := broker.ParseRecipeTaskCommand([]byte(signed.EnvelopeJSON))
	if err != nil || c.CommandID != command.CommandID || c.ConnectionID != command.ConnectionID || c.NodeCounter != command.NodeCounter || c.Action != command.Action || c.PayloadSHA256 != signed.PayloadSHA256 || c.RequestSHA256() != signed.RequestSHA256 {
		return runtime.RecipeInstallResult{}, errors.New("persisted recipe install envelope invalid")
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if err != nil {
		return runtime.RecipeInstallResult{}, err
	}
	result, err := client.SubmitRecipeTask(ctx, c)
	if err != nil {
		return runtime.RecipeInstallResult{}, classifyRecipeInstallBrokerError(err)
	}
	updated, err := time.Parse("2006-01-02T15:04:05.000Z", result.Task.UpdatedAt)
	if err != nil {
		return runtime.RecipeInstallResult{}, errors.New("recipe task timestamp invalid")
	}
	return runtime.RecipeInstallResult{ExecutionID: result.Task.ExecutionID, DeploymentID: result.Task.DeploymentID, TaskID: result.Task.TaskID, Status: result.Task.Status, Attempt: result.Task.Attempt, LastSequence: result.Task.LastSequence, LastCheckpoint: result.Task.LastCheckpoint, ErrorCode: result.Task.ErrorCode, UpdatedAt: updated.UTC()}, nil
}

func classifyRecipeInstallBrokerError(err error) error {
	var brokerError *broker.Error
	if errors.As(err, &brokerError) && brokerError.Code == "expired_command" {
		return runtime.RecipeInstallCommandExpired(err)
	}
	return runtime.ExecutionProbeRetryable("recipe_install_broker_unavailable", err)
}
