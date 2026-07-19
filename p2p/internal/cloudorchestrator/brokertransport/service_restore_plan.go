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

func (t *Transport) BuildServiceRestorePlanCommand(c runtime.ServiceRestorePlanCommand, r broker.ServiceRestorePlanRequest) (runtime.SignedServiceRestorePlanCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedServiceRestorePlanCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	d, e := runtime.ServiceRestorePlanRequestDigest(r)
	if e != nil || c.RequestDigest != d || c.CommandID == "" || c.RestorePlanID != r.RestorePlanID || c.ServiceID != r.ServiceID || c.DeploymentID != r.DeploymentID || c.BackupID != r.BackupID || c.ConnectionID == "" || c.NodeKeyID == "" || c.ExpectedGeneration <= 0 || c.NodeCounter <= 0 {
		return runtime.SignedServiceRestorePlanCommand{}, errors.New("service restore plan command does not bind request")
	}
	issued := t.now().UTC().Truncate(time.Millisecond)
	expires := issued.Add(commandLifetime)
	actual, e := broker.NewServiceRestorePlanCommand(broker.ServiceRestorePlanCommandInput{ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, IssuedAt: issued, ExpiresAt: expires, Request: r, PrivateKey: t.privateKey})
	if e != nil {
		return runtime.SignedServiceRestorePlanCommand{}, errors.New("sign service restore plan command failed")
	}
	payload, e := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if e != nil {
		return runtime.SignedServiceRestorePlanCommand{}, errors.New("signed service restore plan payload is invalid")
	}
	envelope, e := json.Marshal(actual)
	if e != nil {
		return runtime.SignedServiceRestorePlanCommand{}, errors.New("signed service restore plan envelope is invalid")
	}
	return runtime.SignedServiceRestorePlanCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: actual.PayloadSHA256, RequestSHA256: actual.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}

func (t *Transport) RequestServiceRestorePlan(ctx context.Context, endpoint string, c runtime.ServiceRestorePlanCommand, s runtime.SignedServiceRestorePlanCommand, r broker.ServiceRestorePlanRequest) (runtime.ServiceRestorePlanResult, error) {
	actual, e := broker.ParseServiceRestorePlanCommand([]byte(s.EnvelopeJSON))
	if e != nil {
		return runtime.ServiceRestorePlanResult{}, errors.New("persisted service restore plan envelope is invalid")
	}
	if actual.ValidateBinding(broker.ServiceRestorePlanCommandBinding{ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, IssuedAt: s.IssuedAt, ExpiresAt: s.ExpiresAt, Request: r}) != nil || actual.PayloadSHA256 != s.PayloadSHA256 || actual.RequestSHA256() != s.RequestSHA256 {
		return runtime.ServiceRestorePlanResult{}, errors.New("persisted service restore plan envelope does not bind command")
	}
	payload, _ := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if string(payload) != s.PayloadJSON {
		return runtime.ServiceRestorePlanResult{}, errors.New("persisted service restore plan payload is invalid")
	}
	client, e := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if e != nil {
		return runtime.ServiceRestorePlanResult{}, errors.New("cloud broker endpoint is invalid")
	}
	result, e := client.SubmitServiceRestorePlan(ctx, actual)
	if e != nil {
		return runtime.ServiceRestorePlanResult{}, classifyServiceRestorePlanBrokerError(e)
	}
	receipt, e := json.Marshal(result.Receipt)
	if e != nil {
		return runtime.ServiceRestorePlanResult{}, errors.New("service restore plan receipt cannot be encoded")
	}
	return runtime.ServiceRestorePlanResult{Status: result.Status, Plan: result.Plan, CommandID: result.Receipt.CommandID, RequestSHA256: result.Receipt.RequestSHA256, ReceiptJSON: string(receipt)}, nil
}

func classifyServiceRestorePlanBrokerError(e error) error {
	var typed *broker.Error
	if !errors.As(e, &typed) {
		return runtime.QuoteRetryable("broker_unavailable", e)
	}
	if typed.Code == "expired_command" {
		return runtime.QuoteCommandExpired(e)
	}
	if typed.StatusCode == 429 || typed.StatusCode >= 500 {
		return runtime.QuoteRetryable("broker_unavailable", e)
	}
	switch typed.Code {
	case "broker_timeout":
		return runtime.QuoteRetryable("broker_timeout", e)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.QuoteRetryable("broker_unavailable", e)
	}
	return e
}
