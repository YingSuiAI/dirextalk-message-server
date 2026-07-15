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

func (t *Transport) BuildServiceBackupCommand(c runtime.ServiceBackupCommand, r broker.ServiceBackupRequest, a cloudcontracts.ServiceBackupApprovalV1) (runtime.SignedServiceBackupCommand, error) {
	if t == nil || len(t.privateKey) != ed25519.PrivateKeySize {
		return runtime.SignedServiceBackupCommand{}, errors.New("cloud broker node signing key is unavailable")
	}
	d, e := runtime.ServiceBackupRequestDigest(r)
	if e != nil || c.RequestDigest != d || c.CommandID == "" || c.BackupID != r.BackupID || c.ServiceID != r.ServiceID || c.DeploymentID != r.DeploymentID || c.ConnectionID != a.CloudConnectionID || c.NodeCounter <= 0 || c.ExpectedGeneration <= 0 {
		return runtime.SignedServiceBackupCommand{}, errors.New("service backup command does not bind request")
	}
	issued := t.now().UTC().Truncate(time.Millisecond)
	expires := issued.Add(commandLifetime)
	if a.ExpiresAt.Before(expires) {
		expires = a.ExpiresAt.UTC().Truncate(time.Millisecond)
	}
	if !expires.After(issued) {
		return runtime.SignedServiceBackupCommand{}, errors.New("service backup approval expired")
	}
	actual, e := broker.NewServiceBackupCommand(broker.ServiceBackupCommandInput{ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, IssuedAt: issued, ExpiresAt: expires, Request: r, ApprovalProof: a, PrivateKey: t.privateKey})
	if e != nil {
		return runtime.SignedServiceBackupCommand{}, errors.New("sign service backup command failed")
	}
	payload, e := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if e != nil {
		return runtime.SignedServiceBackupCommand{}, errors.New("signed service backup payload is invalid")
	}
	envelope, e := json.Marshal(actual)
	if e != nil {
		return runtime.SignedServiceBackupCommand{}, errors.New("signed service backup envelope is invalid")
	}
	return runtime.SignedServiceBackupCommand{EnvelopeJSON: string(envelope), PayloadJSON: string(payload), PayloadSHA256: actual.PayloadSHA256, RequestSHA256: actual.RequestSHA256(), IssuedAt: issued, ExpiresAt: expires}, nil
}
func (t *Transport) RequestServiceBackup(ctx context.Context, endpoint string, c runtime.ServiceBackupCommand, s runtime.SignedServiceBackupCommand, r broker.ServiceBackupRequest, a cloudcontracts.ServiceBackupApprovalV1) (runtime.ServiceBackupResult, error) {
	actual, e := broker.ParseServiceBackupCommand([]byte(s.EnvelopeJSON))
	if e != nil {
		return runtime.ServiceBackupResult{}, errors.New("persisted service backup envelope is invalid")
	}
	if actual.ValidateBinding(broker.ServiceBackupCommandBinding{ConnectionID: c.ConnectionID, CommandID: c.CommandID, NodeKeyID: c.NodeKeyID, ExpectedGeneration: c.ExpectedGeneration, NodeCounter: c.NodeCounter, IssuedAt: s.IssuedAt, ExpiresAt: s.ExpiresAt, Request: r, ApprovalProof: a}) != nil || actual.PayloadSHA256 != s.PayloadSHA256 || actual.RequestSHA256() != s.RequestSHA256 {
		return runtime.ServiceBackupResult{}, errors.New("persisted service backup envelope does not bind command")
	}
	payload, _ := base64.StdEncoding.DecodeString(actual.PayloadB64)
	if string(payload) != s.PayloadJSON {
		return runtime.ServiceBackupResult{}, errors.New("persisted service backup payload is invalid")
	}
	client, e := broker.NewClient(broker.ClientOptions{Endpoint: strings.TrimSpace(endpoint)})
	if e != nil {
		return runtime.ServiceBackupResult{}, errors.New("cloud broker endpoint is invalid")
	}
	result, e := client.SubmitServiceBackup(ctx, actual)
	if e != nil {
		return runtime.ServiceBackupResult{}, classifyServiceBackupBrokerError(e)
	}
	receipt, e := json.Marshal(result.Receipt)
	if e != nil {
		return runtime.ServiceBackupResult{}, errors.New("service backup receipt cannot be encoded")
	}
	return runtime.ServiceBackupResult{Status: result.Status, BackupID: result.Backup.BackupID, ServiceID: result.Backup.ServiceID, DeploymentID: result.Backup.DeploymentID, InstanceID: result.Backup.InstanceID, ImageID: result.Backup.ImageID, Snapshots: append([]broker.ServiceBackupSnapshot(nil), result.Backup.Snapshots...), CommandID: result.Receipt.CommandID, RequestSHA256: result.Receipt.RequestSHA256, ReceiptJSON: string(receipt)}, nil
}
func classifyServiceBackupBrokerError(e error) error {
	var typed *broker.Error
	if !errors.As(e, &typed) {
		return runtime.ServiceBackupRetryable("broker_unavailable", e)
	}
	if typed.Code == "service_backup_in_progress" {
		return runtime.ServiceBackupRetryable(typed.Code, e)
	}
	if typed.StatusCode == 429 || typed.StatusCode >= 500 {
		return runtime.ServiceBackupRetryable("broker_unavailable", e)
	}
	switch typed.Code {
	case "broker_timeout":
		return runtime.ServiceBackupRetryable("broker_timeout", e)
	case "broker_unavailable", "broker_request_unavailable", "broker_response_unavailable":
		return runtime.ServiceBackupRetryable("broker_unavailable", e)
	}
	return e
}
