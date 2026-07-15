// Package provider contains only bounded Connection Stack operations. The
// registration attestor is configuration-only and the quote provider is
// read-only; no type in this package exposes a generic AWS API passthrough.
package provider

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

var (
	accountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)
	apiIDPattern     = regexp.MustCompile(`^[a-z0-9]{10}$`)
)

type RegistrationConfig struct {
	ConnectionID                 string
	ConnectionGeneration         int64
	NodeKeyID                    string
	AccountID                    string
	Region                       string
	StackARN                     string
	URLSuffix                    string
	StageName                    string
	WorkerArtifact               contract.WorkerArtifactReference
	WorkerNetwork                contract.WorkerNetworkReference
	WorkerResourceManifestDigest string
}

type RegistrationAttestor struct{ config RegistrationConfig }

func NewRegistrationAttestor(config RegistrationConfig) (*RegistrationAttestor, error) {
	stackRegion, stackAccount, stackOK := contract.ParseStackARN(config.StackARN)
	if !contract.ValidConnectionID(config.ConnectionID) || config.ConnectionGeneration < 1 || config.ConnectionGeneration > 9007199254740991 || !contract.ValidNodeKeyID(config.NodeKeyID) || !accountIDPattern.MatchString(config.AccountID) || !stackOK || stackRegion != config.Region || stackAccount != config.AccountID || config.StageName != "prod" || (config.URLSuffix != "amazonaws.com" && config.URLSuffix != "amazonaws.com.cn") {
		return nil, errors.New("invalid registration attestor configuration")
	}
	probe := contract.Registration{
		Schema: contract.RegistrationSchema, BootstrapID: "bootstrap-probe", ConnectionID: config.ConnectionID,
		AccountID: config.AccountID, Region: config.Region, BrokerCommandURL: "https://abcdefghij.execute-api." + config.Region + "." + config.URLSuffix + "/" + config.StageName + "/v2/commands",
		NodeKeyID: config.NodeKeyID, ConnectionGeneration: config.ConnectionGeneration, WorkerArtifact: config.WorkerArtifact,
		WorkerNetwork: config.WorkerNetwork, WorkerResourceManifestDigest: config.WorkerResourceManifestDigest,
		StackARN: config.StackARN, CommandID: "command-probe", RequestSHA256: strings.Repeat("a", 64),
	}
	command := contract.Command{ConnectionID: config.ConnectionID, NodeKeyID: config.NodeKeyID, ExpectedGeneration: config.ConnectionGeneration, CommandID: "command-probe"}
	request := contract.RegistrationRequest{BootstrapID: "bootstrap-probe", RequestedRegion: config.Region, StackARN: config.StackARN}
	if !validRegistrationShape(command, request, probe) {
		return nil, errors.New("invalid registration attestor configuration")
	}
	return &RegistrationAttestor{config: config}, nil
}

func (a *RegistrationAttestor) Attest(_ context.Context, runtime api.GatewayRuntime, command contract.Command, request contract.RegistrationRequest) (contract.Registration, error) {
	if a == nil || command.ConnectionID != a.config.ConnectionID || command.NodeKeyID != a.config.NodeKeyID || command.ExpectedGeneration != a.config.ConnectionGeneration || request.RequestedRegion != a.config.Region || request.StackARN != a.config.StackARN || runtime.Stage != a.config.StageName {
		return contract.Registration{}, api.NewError("registration_config_invalid", 500)
	}
	suffix := ".execute-api." + a.config.Region + "." + a.config.URLSuffix
	if !strings.HasSuffix(runtime.DomainName, suffix) {
		return contract.Registration{}, api.NewError("registration_config_invalid", 500)
	}
	apiID := strings.TrimSuffix(runtime.DomainName, suffix)
	if !apiIDPattern.MatchString(apiID) {
		return contract.Registration{}, api.NewError("registration_config_invalid", 500)
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return contract.Registration{}, api.NewError("registration_config_invalid", 500)
	}
	return contract.Registration{
		Schema:                       contract.RegistrationSchema,
		BootstrapID:                  request.BootstrapID,
		ConnectionID:                 command.ConnectionID,
		AccountID:                    a.config.AccountID,
		Region:                       a.config.Region,
		BrokerCommandURL:             "https://" + runtime.DomainName + "/" + runtime.Stage + "/v2/commands",
		NodeKeyID:                    command.NodeKeyID,
		ConnectionGeneration:         command.ExpectedGeneration,
		WorkerArtifact:               a.config.WorkerArtifact,
		WorkerNetwork:                a.config.WorkerNetwork,
		WorkerResourceManifestDigest: a.config.WorkerResourceManifestDigest,
		StackARN:                     a.config.StackARN,
		CommandID:                    command.CommandID,
		RequestSHA256:                requestSHA,
	}, nil
}

func validRegistrationShape(command contract.Command, request contract.RegistrationRequest, registration contract.Registration) bool {
	return registration.Schema == contract.RegistrationSchema && registration.BootstrapID == request.BootstrapID && registration.ConnectionID == command.ConnectionID && registration.NodeKeyID == command.NodeKeyID && registration.ConnectionGeneration == command.ExpectedGeneration && registration.StackARN == request.StackARN && registration.Region == request.RequestedRegion && contract.ValidWorkerBindings(registration.Region, registration.WorkerArtifact, registration.WorkerNetwork, registration.WorkerResourceManifestDigest)
}
