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

var _ runtime.RecipeArtifactTransferTransport = (*Transport)(nil)

func (transport *Transport) BuildRecipeArtifactPrepareCommand(command runtime.RecipeArtifactTransferCommand, request runtime.RecipeArtifactPutPrepareRequest, now time.Time) (runtime.SignedRecipeArtifactTransferCommand, error) {
	prepare := broker.ArtifactPutPrepareRequest{Schema: request.Schema, ArtifactPutBinding: brokerArtifactBinding(request.RecipeArtifactTransferBinding)}
	return transport.buildRecipeArtifactCommand(command, &prepare, nil, request, now)
}

func (transport *Transport) BuildRecipeArtifactCompleteCommand(command runtime.RecipeArtifactTransferCommand, request runtime.RecipeArtifactPutCompleteRequest, now time.Time) (runtime.SignedRecipeArtifactTransferCommand, error) {
	complete := broker.ArtifactPutCompleteRequest{Schema: request.Schema, ArtifactPutBinding: brokerArtifactBinding(request.RecipeArtifactTransferBinding), VersionID: request.VersionID}
	return transport.buildRecipeArtifactCommand(command, nil, &complete, request, now)
}

func (transport *Transport) buildRecipeArtifactCommand(command runtime.RecipeArtifactTransferCommand, prepare *broker.ArtifactPutPrepareRequest, complete *broker.ArtifactPutCompleteRequest, request interface{ Digest() (string, error) }, now time.Time) (runtime.SignedRecipeArtifactTransferCommand, error) {
	if transport == nil || len(transport.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedRecipeArtifactTransferCommand{}, errors.New("node signing key unavailable")
	}
	digest, err := request.Digest()
	if err != nil || digest != command.RequestDigest || command.Action != runtime.RecipeArtifactPutAction || command.NodeCounter < 1 {
		return runtime.SignedRecipeArtifactTransferCommand{}, errors.New("recipe artifact command does not bind request")
	}
	issuedAt := now.UTC().Truncate(time.Millisecond)
	expiresAt := issuedAt.Add(commandLifetime)
	value, err := broker.NewArtifactPutCommand(broker.ArtifactPutCommandInput{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, NodeKeyID: command.NodeKeyID,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, IssuedAt: issuedAt, ExpiresAt: expiresAt,
		Prepare: prepare, Complete: complete, PrivateKey: transport.privateKey,
	})
	if err != nil {
		return runtime.SignedRecipeArtifactTransferCommand{}, err
	}
	payload, err := base64.StdEncoding.DecodeString(value.PayloadB64)
	if err != nil {
		return runtime.SignedRecipeArtifactTransferCommand{}, errors.New("recipe artifact payload is invalid")
	}
	envelope, err := json.Marshal(value)
	if err != nil {
		return runtime.SignedRecipeArtifactTransferCommand{}, errors.New("recipe artifact envelope is invalid")
	}
	return runtime.SignedRecipeArtifactTransferCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: value.PayloadSHA256, RequestSHA256: value.RequestSHA256(), IssuedAt: issuedAt, ExpiresAt: expiresAt}, nil
}

func (transport *Transport) RequestRecipeArtifactPrepare(ctx context.Context, endpoint string, command runtime.RecipeArtifactTransferCommand, signed runtime.SignedRecipeArtifactTransferCommand, request runtime.RecipeArtifactPutPrepareRequest) (runtime.RecipeArtifactUploadGrant, error) {
	value, err := persistedRecipeArtifactCommand(command, signed)
	if err != nil {
		return runtime.RecipeArtifactUploadGrant{}, err
	}
	parsed, err := value.PrepareRequest()
	if err != nil || parsed != (broker.ArtifactPutPrepareRequest{Schema: request.Schema, ArtifactPutBinding: brokerArtifactBinding(request.RecipeArtifactTransferBinding)}) {
		return runtime.RecipeArtifactUploadGrant{}, errors.New("persisted recipe artifact prepare payload is invalid")
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint), RootCAs: transport.rootCAs})
	if err != nil {
		return runtime.RecipeArtifactUploadGrant{}, errors.New("cloud broker endpoint is invalid")
	}
	result, err := client.SubmitArtifactPutPrepare(ctx, value)
	if err != nil {
		return runtime.RecipeArtifactUploadGrant{}, classifyRecipeArtifactBrokerError(err)
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, result.Upload.ExpiresAt)
	if err != nil {
		return runtime.RecipeArtifactUploadGrant{}, errors.New("artifact upload expiry is invalid")
	}
	headers := make(map[string]string, len(result.Upload.Headers))
	for key, headerValue := range result.Upload.Headers {
		headers[key] = headerValue
	}
	return runtime.RecipeArtifactUploadGrant{Method: result.Upload.Method, URL: result.Upload.URL, ExpiresAt: expiresAt, Headers: headers}, nil
}

func (transport *Transport) RequestRecipeArtifactComplete(ctx context.Context, endpoint string, command runtime.RecipeArtifactTransferCommand, signed runtime.SignedRecipeArtifactTransferCommand, request runtime.RecipeArtifactPutCompleteRequest) error {
	value, err := persistedRecipeArtifactCommand(command, signed)
	if err != nil {
		return err
	}
	parsed, err := value.CompleteRequest()
	if err != nil || parsed != (broker.ArtifactPutCompleteRequest{Schema: request.Schema, ArtifactPutBinding: brokerArtifactBinding(request.RecipeArtifactTransferBinding), VersionID: request.VersionID}) {
		return errors.New("persisted recipe artifact complete payload is invalid")
	}
	client, err := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint), RootCAs: transport.rootCAs})
	if err != nil {
		return errors.New("cloud broker endpoint is invalid")
	}
	_, err = client.SubmitArtifactPutComplete(ctx, value)
	if err != nil {
		return classifyRecipeArtifactBrokerError(err)
	}
	return nil
}

func persistedRecipeArtifactCommand(command runtime.RecipeArtifactTransferCommand, signed runtime.SignedRecipeArtifactTransferCommand) (broker.ArtifactPutCommand, error) {
	value, err := broker.ParseArtifactPutCommand([]byte(signed.EnvelopeJSON))
	if err != nil || value.CommandID != command.CommandID || value.ConnectionID != command.ConnectionID || value.NodeKeyID != command.NodeKeyID ||
		value.ExpectedGeneration != command.ExpectedGeneration || value.NodeCounter != command.NodeCounter || value.Action != command.Action ||
		value.PayloadSHA256 != signed.PayloadSHA256 || value.RequestSHA256() != signed.RequestSHA256 {
		return broker.ArtifactPutCommand{}, errors.New("persisted recipe artifact envelope is invalid")
	}
	return value, nil
}

func brokerArtifactBinding(binding runtime.RecipeArtifactTransferBinding) broker.ArtifactPutBinding {
	return broker.ArtifactPutBinding{
		DeploymentID: binding.DeploymentID, TaskID: binding.TaskID, ExecutionID: binding.ExecutionID,
		RecipeDigest: binding.RecipeDigest, ArtifactDigest: binding.ArtifactDigest, ManifestDigest: binding.ManifestDigest,
		ArchiveSHA256: binding.ArchiveSHA256, SizeBytes: binding.SizeBytes, MediaType: binding.MediaType,
	}
}

func classifyRecipeArtifactBrokerError(err error) error {
	var brokerError *broker.Error
	if errors.As(err, &brokerError) && brokerError.Code == "expired_command" {
		return runtime.RecipeArtifactCommandExpired(err)
	}
	return err
}
