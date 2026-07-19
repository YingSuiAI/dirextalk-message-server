package storepg

import (
	"bytes"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestStoreServiceSecretObservePersistsBeforeIOAndFencesReplay(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	seedServiceSecretObserve(t, database, now, "approval-observe-0001", "secret-session-0001", now.Add(10*time.Minute))
	leaseToken := "lease-secret-observe-1"
	store := New(database.DB(), Config{Now: func() time.Time { return now }, NewLeaseToken: func() string { return leaseToken }})
	claim, found, err := store.ClaimPendingServiceSecretObserve(ctx, "observer-1", time.Minute)
	if err != nil || !found || claim.Command.NodeCounter != 1 || claim.Command.SignedEnvelope != "" {
		t.Fatalf("claim=%#v found=%v err=%v", claim, found, err)
	}
	// Crash before signing: after lease expiry the same allocated command and
	// reserved counter are reclaimed rather than allocating a second counter.
	restartNow := now.Add(2 * time.Minute)
	restart := New(database.DB(), Config{Now: func() time.Time { return restartNow }, NewLeaseToken: func() string { return "lease-secret-observe-2" }})
	reclaimed, found, err := restart.ClaimPendingServiceSecretObserve(ctx, "observer-2", time.Minute)
	if err != nil || !found || reclaimed.Command.CommandID != claim.Command.CommandID || reclaimed.Command.NodeCounter != claim.Command.NodeCounter {
		t.Fatalf("reclaimed=%#v found=%v err=%v", reclaimed, found, err)
	}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x73}, ed25519.SeedSize))
	transport, err := brokertransport.New(key, func() time.Time { return restartNow })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildServiceSecretObserveCommand(reclaimed.Command, reclaimed.Request, restartNow)
	if err != nil {
		t.Fatal(err)
	}
	if err = restart.PersistServiceSecretObserveCommand(ctx, reclaimed, signed); err != nil {
		t.Fatal(err)
	}
	if err = restart.DeferServiceSecretObserve(ctx, reclaimed, "service_secret_observe_transport_failed", restartNow.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	thirdNow := restartNow.Add(time.Minute)
	third := New(database.DB(), Config{Now: func() time.Time { return thirdNow }, NewLeaseToken: func() string { return "lease-secret-observe-3" }})
	replayed, found, err := third.ClaimPendingServiceSecretObserve(ctx, "observer-3", time.Minute)
	if err != nil || !found || replayed.Command.SignedEnvelope != signed.EnvelopeJSON || replayed.Command.NodeCounter != 1 {
		t.Fatalf("signed replay=%#v found=%v err=%v", replayed, found, err)
	}
	observation := runtime.ServiceSecretObservation{SessionID: replayed.Request.SessionID, Status: "completed", ProviderVersion: "version-opaque-1", BindingDigest: replayed.Request.ContextDigest, UpdatedMarker: strings.Repeat("d", 64)}
	if err = third.CompleteServiceSecretObserve(ctx, replayed, observation); err != nil {
		t.Fatal(err)
	}
	if err = restart.CompleteServiceSecretObserve(ctx, reclaimed, observation); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old lease late complete err=%v", err)
	}
	var status, marker, lastError string
	var revision, commands, counter int64
	if err = database.DB().QueryRowContext(ctx, `SELECT status,updated_marker,last_error_code,revision FROM p2p_cloud_service_secret_bootstrap_approvals WHERE session_id=$1`, replayed.Request.SessionID).Scan(&status, &marker, &lastError, &revision); err != nil {
		t.Fatal(err)
	}
	if status != "ready" || marker != observation.UpdatedMarker || lastError != "" || revision < 4 {
		t.Fatalf("approval=%q %q %q rev=%d", status, marker, lastError, revision)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_service_secret_observe_commands WHERE session_id=$1`, replayed.Request.SessionID).Scan(&commands); err != nil || commands != 1 {
		t.Fatalf("commands=%d err=%v", commands, err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT next_node_counter FROM p2p_cloud_connection_brokers WHERE cloud_connection_id='connection-observe-0001'`).Scan(&counter); err != nil || counter != 1 {
		t.Fatalf("counter=%d err=%v", counter, err)
	}
	// Provider replay markers are globally single-use. A duplicate cannot turn a
	// second approval ready, and the failed transaction preserves its lease.
	seedServiceSecretObserve(t, database, thirdNow, "approval-observe-duplicate-0001", "secret-session-duplicate-0001", thirdNow.Add(10*time.Minute))
	duplicate, found, err := third.ClaimPendingServiceSecretObserve(ctx, "observer-3", time.Minute)
	if err != nil || !found {
		t.Fatalf("duplicate claim found=%v err=%v", found, err)
	}
	duplicateTransport, err := brokertransport.New(key, func() time.Time { return thirdNow })
	if err != nil {
		t.Fatal(err)
	}
	duplicateSigned, err := duplicateTransport.BuildServiceSecretObserveCommand(duplicate.Command, duplicate.Request, thirdNow)
	if err != nil {
		t.Fatal(err)
	}
	if err = third.PersistServiceSecretObserveCommand(ctx, duplicate, duplicateSigned); err != nil {
		t.Fatal(err)
	}
	duplicateObservation := runtime.ServiceSecretObservation{SessionID: duplicate.Request.SessionID, Status: "completed", ProviderVersion: "version-opaque-2", BindingDigest: duplicate.Request.ContextDigest, UpdatedMarker: observation.UpdatedMarker}
	if err = third.CompleteServiceSecretObserve(ctx, duplicate, duplicateObservation); err == nil {
		t.Fatal("duplicate updated marker was accepted")
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT status FROM p2p_cloud_service_secret_bootstrap_approvals WHERE session_id=$1`, duplicate.Request.SessionID).Scan(&status); err != nil || status != "observing" {
		t.Fatalf("duplicate status=%q err=%v", status, err)
	}
	if err = third.FailServiceSecretObserve(ctx, duplicate, "service_secret_observe_duplicate_marker"); err != nil {
		t.Fatal(err)
	}
	encoded := serviceSecretObserveJournal(t, database)
	for _, canary := range []string{"sk-canary-never-store", "version-opaque-1", "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/commands", "sealed_secret", "session_token"} {
		if strings.Contains(encoded, canary) {
			t.Fatalf("observer journal leaked %q: %s", canary, encoded)
		}
	}
}

func TestStoreServiceSecretObserveExpiresFailsAndRejectsDuplicateMarker(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now := time.Date(2026, 7, 15, 17, 0, 0, 0, time.UTC)
	seedServiceSecretObserve(t, database, now, "approval-expired-0001", "secret-session-expired-0001", now)
	store := New(database.DB(), Config{Now: func() time.Time { return now }, NewLeaseToken: func() string { return "lease-expire" }})
	late, found, err := store.ClaimPendingServiceSecretObserve(ctx, "observer", time.Minute)
	if err != nil || !found || !late.ApprovalExpiresAt.Equal(now) {
		t.Fatalf("late claim=%#v found=%v err=%v", late, found, err)
	}
	var status string
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x41}, ed25519.SeedSize))
	transport, err := brokertransport.New(key, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildServiceSecretObserveCommand(late.Command, late.Request, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PersistServiceSecretObserveCommand(ctx, late, signed); err != nil {
		t.Fatal(err)
	}
	lateCompleted := runtime.ServiceSecretObservation{SessionID: late.Request.SessionID, Status: "completed", ProviderVersion: "version-at-boundary", BindingDigest: late.Request.ContextDigest, UpdatedMarker: strings.Repeat("e", 64)}
	if err = store.CompleteServiceSecretObserve(ctx, late, lateCompleted); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRowContext(ctx, `SELECT status FROM p2p_cloud_service_secret_bootstrap_approvals WHERE session_id='secret-session-expired-0001'`).Scan(&status); err != nil || status != "ready" {
		t.Fatalf("late completed status=%q err=%v", status, err)
	}
	seedServiceSecretObserve(t, database, now, "approval-missing-expired-0001", "secret-session-missing-expired-0001", now)
	missingStore := New(database.DB(), Config{Now: func() time.Time { return now }, NewLeaseToken: func() string { return "lease-missing-expired" }})
	missing, found, err := missingStore.ClaimPendingServiceSecretObserve(ctx, "observer", time.Minute)
	if err != nil || !found {
		t.Fatalf("missing claim found=%v err=%v", found, err)
	}
	if err = missingStore.ExpireServiceSecretObserve(ctx, missing); err != nil {
		t.Fatal(err)
	}
	if err = database.DB().QueryRowContext(ctx, `SELECT status FROM p2p_cloud_service_secret_bootstrap_approvals WHERE session_id=$1`, missing.Request.SessionID).Scan(&status); err != nil || status != "expired" {
		t.Fatalf("missing status=%q err=%v", status, err)
	}
	seedServiceSecretObserve(t, database, now, "approval-fail-0001", "secret-session-fail-0001", now.Add(10*time.Minute))
	claim, _, err := missingStore.ClaimPendingServiceSecretObserve(ctx, "observer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.FailServiceSecretObserve(ctx, claim, "sk-canary-never-store"); err != nil {
		t.Fatal(err)
	}
	var code string
	if err = database.DB().QueryRowContext(ctx, `SELECT status,last_error_code FROM p2p_cloud_service_secret_bootstrap_approvals WHERE session_id=$1`, claim.Request.SessionID).Scan(&status, &code); err != nil || status != "failed" || code != "service_secret_observe_failed" {
		t.Fatalf("failed status=%q code=%q err=%v", status, code, err)
	}
}

func seedServiceSecretObserve(t *testing.T, database interface{ DB() *sql.DB }, now time.Time, approvalID, sessionID string, expires time.Time) {
	t.Helper()
	db := database.DB()
	ts := now.UnixMilli()
	deploymentID := "deployment-" + approvalID
	if _, err := db.Exec(`INSERT INTO p2p_cloud_connections(cloud_connection_id,provider,account_id,region,mode,status,revision,created_at,updated_at)VALUES('connection-observe-0001','aws','123456789012','us-east-1','role','active',1,$1,$1) ON CONFLICT DO NOTHING`, ts); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO p2p_cloud_connection_brokers(cloud_connection_id,broker_command_url,broker_region,connection_generation,node_key_id,next_node_counter,created_at,updated_at)VALUES('connection-observe-0001','https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/commands','us-east-1',2,'node-key-observe-1',0,$1,$1) ON CONFLICT DO NOTHING`, ts); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO p2p_cloud_service_secret_bootstrap_approvals(approval_id,challenge_id,session_id,owner_mxid,deployment_id,deployment_revision,plan_id,plan_revision,cloud_connection_id,task_id,execution_id,manifest_digest,recipe_digest,artifact_digest,slot_id,secret_ref,purpose,delivery,context_digest,signer_key_id,approval_json,signing_payload_cbor,status,prepare_idempotency_hash,prepare_request_digest,expires_at,created_at,updated_at)VALUES($1,$2,$3,'@owner:example.com',$4,1,'plan-observe-0001',1,'connection-observe-0001','recipe-task-observe-0001','execution-observe-0001',$5,$6,$7,'model_token','secret_ref:model-token','model access','environment',$8,'device-key-observe-1','{}',$9,'pending',$10,$11,$12,$13,$13)`, approvalID, "challenge-"+approvalID, sessionID, deploymentID, "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64), "sha256:"+strings.Repeat("c", 64), "sha256:"+strings.Repeat("d", 64), []byte{1}, "idem-"+approvalID, "request-"+approvalID, expires.UnixMilli(), ts); err != nil {
		t.Fatal(err)
	}
}

func serviceSecretObserveJournal(t *testing.T, database interface{ DB() *sql.DB }) string {
	t.Helper()
	var approvals, commands string
	if err := database.DB().QueryRow(`SELECT COALESCE(json_agg(a)::text,'[]') FROM p2p_cloud_service_secret_bootstrap_approvals a`).Scan(&approvals); err != nil {
		t.Fatal(err)
	}
	if err := database.DB().QueryRow(`SELECT COALESCE(json_agg(c)::text,'[]') FROM p2p_cloud_service_secret_observe_commands c`).Scan(&commands); err != nil {
		t.Fatal(err)
	}
	return approvals + commands
}
