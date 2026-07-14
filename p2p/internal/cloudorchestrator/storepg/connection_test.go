package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreConnectionRegistrationRetryReplaysCommandAndExpiryAllocatesNewCounter(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now, store, claim := prepareConnectionRegistrationClaim(t, ctx, database)
	signed := signedConnectionRegistrationCommand(t, claim, now)
	if err := store.MarkConnectionRegistrationStarted(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistConnectionRegistrationCommand(ctx, claim, signed); err != nil {
		t.Fatal(err)
	}
	if err := store.DeferConnectionRegistration(ctx, claim, "broker_unavailable", now); err != nil {
		t.Fatal(err)
	}
	retry, found, err := store.ClaimConnectionRegistration(ctx, "orchestrator-b", time.Minute)
	if err != nil || !found {
		t.Fatalf("retry claim found=%v err=%v", found, err)
	}
	if retry.Command.CommandID != claim.Command.CommandID || retry.Command.NodeCounter != claim.Command.NodeCounter || retry.Command.Attempt != claim.Command.Attempt ||
		retry.Command.SignedEnvelope != signed.EnvelopeJSON || retry.Command.RequestSHA256 != signed.RequestSHA256 {
		t.Fatalf("indeterminate retry must replay the original command: first=%#v retry=%#v", claim.Command, retry.Command)
	}
	if err := store.ExpireConnectionRegistrationCommand(ctx, retry); err != nil {
		t.Fatal(err)
	}
	next, found, err := store.ClaimConnectionRegistration(ctx, "orchestrator-c", time.Minute)
	if err != nil || !found {
		t.Fatalf("post-expiry claim found=%v err=%v", found, err)
	}
	if next.Command.CommandID == claim.Command.CommandID || next.Command.NodeCounter != claim.Command.NodeCounter+1 || next.Command.Attempt != claim.Command.Attempt+1 ||
		next.Command.SignedEnvelope != "" || next.Command.RequestSHA256 != "" {
		t.Fatalf("only an expired command may allocate a new counter: first=%#v next=%#v", claim.Command, next.Command)
	}
	connections, err := database.ListCloudConnections(ctx)
	if err != nil || len(connections) != 0 {
		t.Fatalf("retry/expiry must not activate a connection: %#v err=%v", connections, err)
	}
}

func TestStoreCommitConnectionRegistrationMakesOnlySafeConnectionMaterialVisible(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now, store, claim := prepareConnectionRegistrationClaim(t, ctx, database)
	signed := signedConnectionRegistrationCommand(t, claim, now)
	if err := store.MarkConnectionRegistrationStarted(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistConnectionRegistrationCommand(ctx, claim, signed); err != nil {
		t.Fatal(err)
	}
	registration := validBrokerRegistration(claim, signed)
	if err := store.CommitConnectionRegistration(ctx, claim, registration); err != nil {
		t.Fatal(err)
	}

	connections, err := database.ListCloudConnections(ctx)
	if err != nil || len(connections) != 1 {
		t.Fatalf("active connections = %#v err=%v", connections, err)
	}
	connection := connections[0]
	if connection.ConnectionID != claim.ConnectionID || connection.Provider != "aws" || connection.AccountID != "123456789012" ||
		connection.Region != claim.RequestedRegion || connection.Mode != "connection_stack_v2" || connection.Status != "active" || connection.Revision != 1 {
		t.Fatalf("connection projection = %#v", connection)
	}
	encodedConnection, err := json.Marshal(connection)
	if err != nil {
		t.Fatal(err)
	}
	if containsAny(string(encodedConnection), []string{"execute-api", "stack/", "private", "signature", "worker_", "ami-", "vpc-", "subnet-"}) {
		t.Fatalf("public connection leaked private Stack facts: %s", encodedConnection)
	}

	var commandState, storedReceipt, bootstrapStatus string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT state, receipt_json FROM p2p_cloud_connection_registration_commands WHERE command_id = $1`, claim.Command.CommandID,
	).Scan(&commandState, &storedReceipt); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(ctx, `
		SELECT status FROM p2p_cloud_connection_bootstraps WHERE bootstrap_id = $1`, claim.BootstrapID,
	).Scan(&bootstrapStatus); err != nil {
		t.Fatal(err)
	}
	if commandState != "accepted" || bootstrapStatus != cloudmodule.ConnectionBootstrapActive ||
		containsAny(storedReceipt, []string{"execute-api", "stack/", "raw-private-receipt", "signature"}) {
		t.Fatalf("registration audit state=%q bootstrap=%q receipt=%s", commandState, bootstrapStatus, storedReceipt)
	}
	var projection string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT payload_json FROM p2p_cloud_projection_outbox
		WHERE type = 'cloud.connection.changed' ORDER BY projection_id DESC LIMIT 1`,
	).Scan(&projection); err != nil {
		t.Fatal(err)
	}
	if containsAny(projection, []string{"execute-api", "stack/", "raw-private-receipt", "node_key", "worker_", "ami-", "vpc-", "subnet-"}) {
		t.Fatalf("connection projection leaked private material: %s", projection)
	}
	var artifactKind, amiID, vpcID, subnetID, availabilityZone, manifestDigest string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT worker_artifact_kind, worker_ami_id, worker_vpc_id, worker_subnet_id,
			worker_availability_zone, worker_resource_manifest_digest
		FROM p2p_cloud_connection_brokers WHERE cloud_connection_id = $1`, claim.ConnectionID,
	).Scan(&artifactKind, &amiID, &vpcID, &subnetID, &availabilityZone, &manifestDigest); err != nil {
		t.Fatal(err)
	}
	if artifactKind != "fixed_ami" || amiID != "ami-0123456789abcdef0" || vpcID != "vpc-0123456789abcdef0" ||
		subnetID != "subnet-0123456789abcdef0" || availabilityZone != claim.RequestedRegion+"a" ||
		manifestDigest != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("private worker placement binding = artifact:%q ami:%q vpc:%q subnet:%q az:%q digest:%q", artifactKind, amiID, vpcID, subnetID, availabilityZone, manifestDigest)
	}
	var execution, outcome, checkpoint string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT execution_status, outcome_status, checkpoint FROM p2p_cloud_jobs WHERE job_id = $1`, claim.JobID,
	).Scan(&execution, &outcome, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if execution != "finished" || outcome != "succeeded" || checkpoint != "connection_verified" {
		t.Fatalf("registration job = execution:%q outcome:%q checkpoint:%q", execution, outcome, checkpoint)
	}
}

func TestStoreRejectsMismatchedRegistrationBeforeActivation(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now, store, claim := prepareConnectionRegistrationClaim(t, ctx, database)
	signed := signedConnectionRegistrationCommand(t, claim, now)
	if err := store.MarkConnectionRegistrationStarted(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistConnectionRegistrationCommand(ctx, claim, signed); err != nil {
		t.Fatal(err)
	}
	registration := validBrokerRegistration(claim, signed)
	registration.BrokerCommandURL = "https://f1e2d3c4b5.execute-api.ap-northeast-1.amazonaws.com/prod/v2/commands"
	if err := store.CommitConnectionRegistration(ctx, claim, registration); err == nil {
		t.Fatal("mismatched Stack Broker endpoint must not activate a connection")
	}
	connections, err := database.ListCloudConnections(ctx)
	if err != nil || len(connections) != 0 {
		t.Fatalf("invalid registration activated connections=%#v err=%v", connections, err)
	}
}

func prepareConnectionRegistrationClaim(t *testing.T, ctx context.Context, database *p2pstorage.DatabaseStore) (time.Time, *Store, runtime.ConnectionRegistrationClaim) {
	t.Helper()
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	seedConnectionRegistrationBootstrap(t, ctx, database, now)
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	claim, found, err := store.ClaimConnectionRegistration(ctx, "orchestrator-a", time.Minute)
	if err != nil || !found {
		t.Fatalf("connection registration claim found=%v err=%v", found, err)
	}
	return now, store, claim
}

func seedConnectionRegistrationBootstrap(t *testing.T, ctx context.Context, database *p2pstorage.DatabaseStore, now time.Time) {
	t.Helper()
	nodePublic := connectionRegistrationSPKI(t)
	devicePublic := connectionRegistrationSPKI(t)
	bootstrap := cloudmodule.ConnectionBootstrap{
		BootstrapID: "bootstrap-1", OwnerMXID: "@owner:example.com", ConnectionID: "connection-1", Provider: "aws",
		RequestedRegion: "ap-northeast-1", TemplateURL: "https://artifacts.example.invalid/connection-stack-v2/template.json",
		TemplateDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SourceTreeDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", StackName: "dirextalk-connection-1",
		NodeKeyID: "node-key-1", NodePublicKeySPKIBase64: nodePublic, DeviceApprovalKeyID: "device-key-1",
		DeviceApprovalPublicKeySPKIBase64: devicePublic, Status: cloudmodule.ConnectionBootstrapAwaitingStack,
		Revision: 1, IdempotencyHash: "bootstrap-idempotency", RequestDigest: "bootstrap-request", ExpiresAt: now.Add(15 * time.Minute).UnixMilli(),
		CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli(),
	}
	if _, err := database.CreateCloudConnectionBootstrap(ctx, cloudmodule.CreateConnectionBootstrapRequest{Bootstrap: bootstrap}); err != nil {
		t.Fatal(err)
	}
	job := cloudmodule.Job{
		JobID: "connection-registration-job-1", Kind: "connection_registration", Execution: "queued", Outcome: "pending",
		Checkpoint: "connection_verification_queued", Revision: 1, CreatedAt: now.UnixMilli(), UpdatedAt: now.UnixMilli(),
	}
	summary, err := json.Marshal(map[string]any{
		"job_id": job.JobID, "plan_id": "", "deployment_id": "", "kind": job.Kind,
		"execution_status": job.Execution, "outcome_status": job.Outcome, "checkpoint": job.Checkpoint,
		"error_code": "", "revision": int64(1), "created_at": now.UnixMilli(), "updated_at": now.UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.CompleteCloudConnectionBootstrap(ctx, cloudmodule.CompleteConnectionBootstrapRequest{
		OwnerMXID: bootstrap.OwnerMXID, BootstrapID: bootstrap.BootstrapID, ExpectedRevision: 1,
		IdempotencyHash: "completion-idempotency", RequestDigest: "completion-request", BrokerCommandURL: connectionRegistrationBrokerURL,
		StackARN: connectionRegistrationStackARN, Job: job,
		Event:  cloudmodule.Event{EventID: "connection-registration-event-1", Type: "cloud.job.changed", AggregateType: "job", AggregateID: job.JobID, Revision: 1, SummaryJSON: string(summary), CreatedAt: now.UnixMilli()},
		Outbox: cloudmodule.OutboxEntry{OutboxID: "connection-registration-outbox-1", Kind: cloudmodule.OutboxKindConnectionRegistrationRequested, AggregateType: "connection_bootstrap", AggregateID: bootstrap.BootstrapID, PayloadJSON: `{"bootstrap_id":"bootstrap-1"}`, CreatedAt: now.UnixMilli()},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func signedConnectionRegistrationCommand(t *testing.T, claim runtime.ConnectionRegistrationClaim, now time.Time) runtime.SignedConnectionRegistrationCommand {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildConnectionRegistrationCommand(claim.Command, claim.Request)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func validBrokerRegistration(claim runtime.ConnectionRegistrationClaim, signed runtime.SignedConnectionRegistrationCommand) runtime.BrokerRegistration {
	return runtime.BrokerRegistration{
		Schema: "dirextalk.aws.connection-registration/v1", BootstrapID: claim.BootstrapID, ConnectionID: claim.ConnectionID,
		AccountID: "123456789012", Region: claim.RequestedRegion, BrokerCommandURL: claim.BrokerEndpoint,
		NodeKeyID: claim.NodeKeyID, ConnectionGeneration: claim.ExpectedGeneration, StackARN: claim.StackARN,
		WorkerArtifact: runtime.WorkerArtifactReferenceV1{Kind: "fixed_ami", AMIID: "ami-0123456789abcdef0"},
		WorkerNetwork: runtime.DeploymentNetworkReference{
			VPCID: "vpc-0123456789abcdef0", SubnetID: "subnet-0123456789abcdef0", AvailabilityZone: claim.RequestedRegion + "a",
		},
		WorkerResourceManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CommandID:                    claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: `{"raw-private-receipt":"ignored-by-store"}`,
	}
}

func connectionRegistrationSPKI(t *testing.T) string {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(der)
}

const (
	connectionRegistrationBrokerURL = "https://a1b2c3d4e5.execute-api.ap-northeast-1.amazonaws.com/prod/v2/commands"
	connectionRegistrationStackARN  = "arn:aws:cloudformation:ap-northeast-1:123456789012:stack/dirextalk-test/12345678-1234-1234-1234-123456789012"
)
