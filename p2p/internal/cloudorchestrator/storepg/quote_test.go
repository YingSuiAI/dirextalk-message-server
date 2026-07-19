package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreQuoteRetryReplaysTheSameCommandAndExpiryAllocatesANewCounter(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now, store, claim := prepareQuotedClaim(t, ctx, database)
	signed := signedQuoteCommand(t, claim, now)
	if err := store.MarkQuoteStarted(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistQuoteCommand(ctx, claim, signed); err != nil {
		t.Fatal(err)
	}
	if err := store.DeferQuote(ctx, claim, "broker_unavailable", now); err != nil {
		t.Fatal(err)
	}
	retry, found, err := store.ClaimQuoteRequest(ctx, "orchestrator-b", time.Minute)
	if err != nil || !found {
		t.Fatalf("retry claim found=%v err=%v", found, err)
	}
	if retry.Command.CommandID != claim.Command.CommandID || retry.Command.NodeCounter != claim.Command.NodeCounter || retry.Command.Attempt != claim.Command.Attempt || retry.Command.SignedEnvelope != signed.EnvelopeJSON || retry.Command.RequestSHA256 != signed.RequestSHA256 {
		t.Fatalf("indeterminate retry must replay the original command: first=%#v retry=%#v", claim.Command, retry.Command)
	}
	if err := store.ExpireQuoteCommand(ctx, retry); err != nil {
		t.Fatal(err)
	}
	next, found, err := store.ClaimQuoteRequest(ctx, "orchestrator-c", time.Minute)
	if err != nil || !found {
		t.Fatalf("post-expiry claim found=%v err=%v", found, err)
	}
	if next.Command.CommandID == claim.Command.CommandID || next.Command.NodeCounter != claim.Command.NodeCounter+1 || next.Command.Attempt != claim.Command.Attempt+1 || next.Command.SignedEnvelope != "" || next.Command.RequestSHA256 != "" {
		t.Fatalf("only an expired command may allocate a new counter: first=%#v next=%#v", claim.Command, next.Command)
	}
}

func TestStoreRejectsSignedCommandThatDoesNotMatchItsEnvelope(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now, store, claim := prepareQuotedClaim(t, ctx, database)
	signed := signedQuoteCommand(t, claim, now)
	signed.PayloadJSON += " "
	if err := store.PersistQuoteCommand(ctx, claim, signed); err == nil {
		t.Fatal("store must reject a signed payload that differs from the durable envelope")
	}
}

func TestStoreCommitQuoteMakesOnlySafeQuoteMaterialVisible(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now, store, claim := prepareQuotedClaim(t, ctx, database)
	signed := signedQuoteCommand(t, claim, now)
	if err := store.MarkQuoteStarted(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistQuoteCommand(ctx, claim, signed); err != nil {
		t.Fatal(err)
	}
	result := validBrokerQuote(claim, signed)
	if err := store.CommitQuote(ctx, claim, result); err != nil {
		t.Fatal(err)
	}

	var quoteID, planHash string
	var revision int64
	if err := database.DB().QueryRowContext(ctx, `
		SELECT quote_id, plan_hash, revision FROM p2p_cloud_plans WHERE plan_id = $1`, claim.PlanID,
	).Scan(&quoteID, &planHash, &revision); err != nil {
		t.Fatal(err)
	}
	if quoteID != result.QuoteID || planHash != "" || revision != claim.PlanRevision+1 {
		t.Fatalf("quoted plan = quote_id:%q plan_hash:%q revision:%d", quoteID, planHash, revision)
	}
	view, found, err := database.GetCloudQuote(ctx, result.QuoteID)
	if err != nil || !found {
		t.Fatalf("safe quote view found=%v err=%v", found, err)
	}
	if view.QuoteID != result.QuoteID || view.ConnectionID != claim.ConnectionID || len(view.Candidates) != 1 || view.Candidates[0].HourlyMinor != 2000 || view.Candidates[0].Tier != string(cloudcontracts.QuoteTierRecommended) ||
		view.Candidates[0].Architecture != string(cloudcontracts.ArchitectureAMD64) || view.Candidates[0].VCPU != 4 || view.Candidates[0].MemoryMiB != 16384 || view.Candidates[0].GPUCount != 0 || view.Candidates[0].GPUMemoryMiB != 0 {
		t.Fatalf("safe quote view = %#v", view)
	}
	var commandState, requestSHA, receipt string
	if err := database.DB().QueryRowContext(ctx, `
		SELECT state, request_sha256, receipt_json FROM p2p_cloud_broker_commands WHERE command_id = $1`, claim.Command.CommandID,
	).Scan(&commandState, &requestSHA, &receipt); err != nil {
		t.Fatal(err)
	}
	if commandState != "accepted" || requestSHA != signed.RequestSHA256 || containsAny(receipt, []string{"broker.example", "signature", "payload_b64", "private"}) {
		t.Fatalf("persisted command audit record = state:%q request_sha:%q receipt:%s", commandState, requestSHA, receipt)
	}
	var count int
	if err := database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_plan_versions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("approval plan versions=%d err=%v, want none before confirmation", count, err)
	}
}

func prepareQuotedClaim(t *testing.T, ctx context.Context, database *p2pstorage.DatabaseStore) (time.Time, *Store, runtime.QuoteClaim) {
	t.Helper()
	seedResearchGoal(t, ctx, database)
	now := time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC)
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	researchClaim, found, err := store.ClaimResearchGoal(ctx, "orchestrator-a", time.Minute)
	if err != nil || !found {
		t.Fatalf("research claim found=%v err=%v", found, err)
	}
	if err := store.MarkResearchStarted(ctx, researchClaim); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitResearch(ctx, researchClaim, testResearchOutput(t, now)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_connections (
			cloud_connection_id, provider, account_id, region, mode, status, revision, created_at, updated_at
		) VALUES ('connection-1', 'aws', '123456789012', 'ap-south-1', 'role', 'active', 1, $1, $1)
	`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_connection_brokers (
			cloud_connection_id, broker_command_url, broker_region, connection_generation, node_key_id, next_node_counter, created_at, updated_at
		) VALUES ('connection-1', 'https://broker.example/v2/commands', 'ap-south-1', 1, 'node-key-1', 0, $1, $1)
	`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	claim, found := runtime.QuoteClaim{}, false
	claim, found, err = store.ClaimQuoteRequest(ctx, "orchestrator-a", time.Minute)
	if err != nil || !found {
		t.Fatalf("quote claim found=%v err=%v", found, err)
	}
	return now, store, claim
}

func signedQuoteCommand(t *testing.T, claim runtime.QuoteClaim, now time.Time) runtime.SignedQuoteCommand {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildQuoteCommand(claim.Command, claim.Request)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func validBrokerQuote(claim runtime.QuoteClaim, signed runtime.SignedQuoteCommand) runtime.BrokerQuote {
	request := claim.Request
	candidates := make([]cloudcontracts.QuoteCandidateV1, len(request.Candidates))
	for index, candidate := range request.Candidates {
		candidates[index] = cloudcontracts.QuoteCandidateV1{
			CandidateID: candidate.CandidateID, Tier: candidate.Tier, InstanceType: candidate.InstanceType,
			PurchaseOption: candidate.PurchaseOption, Architecture: cloudcontracts.ArchitectureAMD64, VCPU: 4, MemoryMiB: 16384,
			GPUCount: 0, GPUMemoryMiB: 0, HourlyMinor: 2000, ThirtyDayMinor: 1_440_000,
			StartupUpperMinor: 0, EstimatedDiskGiB: candidate.EstimatedDiskGiB, AvailabilityZones: []string{"ap-south-1a"},
		}
	}
	digest, _ := request.Digest()
	return runtime.BrokerQuote{
		Schema: "dirextalk.aws.quote/v1", QuoteID: "quote-" + signed.RequestSHA256[:32], ConnectionID: claim.ConnectionID,
		CommandID: claim.Command.CommandID, RequestSHA256: signed.RequestSHA256, QuoteRequestID: request.QuoteRequestID,
		PlanDigest: digest, Region: request.Region, Currency: "USD", QuotedAt: signed.IssuedAt,
		ValidUntil: signed.IssuedAt.Add(15 * time.Minute), Candidates: candidates,
		IncludedItems:   []string{"ec2_linux_ondemand"},
		UnincludedItems: []string{"cloudwatch_logs", "data_transfer", "ebs_gp3", "public_ipv4", "snapshots", "taxes"},
		ReceiptJSON:     `{"private":"ignored-by-store"}`,
	}
}
