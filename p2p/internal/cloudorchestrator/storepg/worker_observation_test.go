package storepg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
)

func TestStoreWorkerBootstrapObservationPersistsPrivateEvidenceAndAdvancesJobOnce(t *testing.T) {
	now, database, store, claim := prepareWorkerBootstrapObservationClaim(t)
	signed := signedWorkerBootstrapObservationCommand(t, claim, now)
	if err := store.MarkWorkerBootstrapObservationStarted(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistWorkerBootstrapObservationCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	observation := validWorkerBootstrapObservation(claim, now, 2)
	if err := store.CommitWorkerBootstrapObservation(context.Background(), claim, observation); err != nil {
		t.Fatal(err)
	}
	// A late response retry after its first durable settlement must not emit a
	// second Job event or advance the revision, even after the reported Worker
	// lease has naturally expired.
	store.cfg.Now = func() time.Time { return now.Add(10 * time.Minute) }
	if err := store.CommitWorkerBootstrapObservation(context.Background(), claim, observation); err != nil {
		t.Fatalf("duplicate verified observation = %v", err)
	}

	var deploymentExecution, deploymentOutcome, deploymentResource, checkpoint, jobExecution string
	var jobOutcome string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT deployment.execution_status, deployment.outcome_status, deployment.resource_status,
			job.execution_status, job.outcome_status, job.checkpoint
		FROM p2p_cloud_deployments AS deployment
		JOIN p2p_cloud_jobs AS job ON job.deployment_id = deployment.deployment_id AND job.kind = 'provision'
		WHERE deployment.deployment_id = $1
	`, claim.DeploymentID).Scan(&deploymentExecution, &deploymentOutcome, &deploymentResource, &jobExecution, &jobOutcome, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if deploymentExecution != "verifying" || deploymentOutcome != "pending" || deploymentResource != "active" ||
		jobExecution != "verifying" || jobOutcome != "pending" || checkpoint != "worker_bootstrap_verified" {
		t.Fatalf("verified Worker state deployment=%s/%s/%s job=%s/%s/%s", deploymentExecution, deploymentOutcome, deploymentResource, jobExecution, jobOutcome, checkpoint)
	}

	var connectionID, instanceID, sessionState string
	var leaseEpoch, lastSequence, leaseExpiresAt, lastEventAt, observedAt int64
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT cloud_connection_id, instance_id, worker_session_state, worker_lease_epoch,
			worker_last_sequence, worker_lease_expires_at, worker_last_event_at, observed_at
		FROM p2p_cloud_worker_bootstrap_observations WHERE deployment_id = $1
	`, claim.DeploymentID).Scan(&connectionID, &instanceID, &sessionState, &leaseEpoch, &lastSequence, &leaseExpiresAt, &lastEventAt, &observedAt); err != nil {
		t.Fatal(err)
	}
	if connectionID != claim.ConnectionID || instanceID != claim.InstanceID || sessionState != "active" || leaseEpoch != observation.LeaseEpoch ||
		lastSequence != observation.LastSequence || leaseExpiresAt != observation.LeaseExpiresAt.UnixMilli() || lastEventAt != 0 || observedAt != observation.ObservedAt.UnixMilli() {
		t.Fatalf("private evidence = connection:%q instance:%q state:%q epoch:%d sequence:%d lease:%d event:%d observed:%d", connectionID, instanceID, sessionState, leaseEpoch, lastSequence, leaseExpiresAt, lastEventAt, observedAt)
	}

	var verifiedEvents int
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM p2p_cloud_events
		WHERE aggregate_type = 'job' AND aggregate_id = $1 AND type = 'cloud.job.changed'
			AND summary_json LIKE '%worker_bootstrap_verified%'
	`, claim.JobID).Scan(&verifiedEvents); err != nil {
		t.Fatal(err)
	}
	if verifiedEvents != 1 {
		t.Fatalf("verified job events=%d, want 1", verifiedEvents)
	}
	var projected string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT payload_json FROM p2p_cloud_projection_outbox
		WHERE type = 'cloud.job.changed' AND payload_json LIKE '%worker_bootstrap_verified%'
		ORDER BY created_at DESC, projection_id DESC LIMIT 1
	`).Scan(&projected); err != nil {
		t.Fatal(err)
	}
	if containsAny(projected, []string{claim.InstanceID, "lease_epoch", "last_sequence", "bootstrap_session", "token", "iid"}) {
		t.Fatalf("worker evidence leaked into public projection: %s", projected)
	}
}

func TestStoreWorkerBootstrapObservationRejectsUnboundOrStaleResults(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*runtime.WorkerBootstrapObservationClaim, *runtime.WorkerBootstrapObservation)
	}{
		{name: "wrong_instance", mutate: func(_ *runtime.WorkerBootstrapObservationClaim, observation *runtime.WorkerBootstrapObservation) {
			observation.InstanceID = "i-0123456789abcdef1"
		}},
		{name: "inactive_session", mutate: func(_ *runtime.WorkerBootstrapObservationClaim, observation *runtime.WorkerBootstrapObservation) {
			observation.WorkerSessionState = "bound"
			observation.LeaseEpoch = 0
			observation.LeaseExpiresAt = time.Time{}
		}},
		{name: "wrong_connection", mutate: func(claim *runtime.WorkerBootstrapObservationClaim, _ *runtime.WorkerBootstrapObservation) {
			claim.ConnectionID = "connection-other-1"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			now, database, store, claim := prepareWorkerBootstrapObservationClaim(t)
			observation := validWorkerBootstrapObservation(claim, now, 2)
			test.mutate(&claim, &observation)
			if err := store.CommitWorkerBootstrapObservation(context.Background(), claim, observation); err == nil {
				t.Fatal("unbound Worker observation was accepted")
			}
			var checkpoint string
			if err := database.DB().QueryRowContext(context.Background(), `SELECT checkpoint FROM p2p_cloud_jobs WHERE job_id = $1`, "job-provision-1").Scan(&checkpoint); err != nil {
				t.Fatal(err)
			}
			if checkpoint != "worker_bootstrap_pending" {
				t.Fatalf("unbound observation changed checkpoint=%q", checkpoint)
			}
		})
	}

	now, _, store, claim := prepareWorkerBootstrapObservationClaim(t)
	signed := signedWorkerBootstrapObservationCommand(t, claim, now)
	if err := store.PersistWorkerBootstrapObservationCommand(context.Background(), claim, signed); err != nil {
		t.Fatal(err)
	}
	fresh := validWorkerBootstrapObservation(claim, now, 2)
	if err := store.CommitWorkerBootstrapObservation(context.Background(), claim, fresh); err != nil {
		t.Fatal(err)
	}
	stale := fresh
	stale.LeaseEpoch = 1
	stale.LeaseExpiresAt = now.Add(3 * time.Minute)
	if err := store.CommitWorkerBootstrapObservation(context.Background(), claim, stale); err == nil {
		t.Fatal("stale Worker epoch was accepted")
	}
}

func prepareWorkerBootstrapObservationClaim(t *testing.T) (time.Time, *p2pstorage.DatabaseStore, *Store, runtime.WorkerBootstrapObservationClaim) {
	t.Helper()
	now, database, store, provision := prepareProvisionClaim(t)
	signedProvision := signedDeploymentCreateCommand(t, provision, now)
	if err := store.MarkDeploymentProvisionStarted(context.Background(), provision); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistDeploymentCreateCommand(context.Background(), provision, signedProvision); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitDeploymentProvision(context.Background(), provision, validProvisionReceipt(t, provision, signedProvision)); err != nil {
		t.Fatal(err)
	}
	claim, found, err := store.ClaimWorkerBootstrapObservation(context.Background(), "orchestrator-worker-observe", time.Minute)
	if err != nil || !found {
		t.Fatalf("worker bootstrap observation claim found=%v err=%v", found, err)
	}
	return now, database, store, claim
}

func signedWorkerBootstrapObservationCommand(t *testing.T, claim runtime.WorkerBootstrapObservationClaim, now time.Time) runtime.SignedWorkerBootstrapObservationCommand {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := brokertransport.New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	signed, err := transport.BuildWorkerBootstrapObservationCommand(claim.Command, claim.Request, now)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func validWorkerBootstrapObservation(claim runtime.WorkerBootstrapObservationClaim, now time.Time, epoch int64) runtime.WorkerBootstrapObservation {
	return runtime.WorkerBootstrapObservation{
		Schema: runtime.WorkerBootstrapObservationSchema, DeploymentID: claim.DeploymentID, ResourceStatus: "provisioning", InstanceID: claim.InstanceID,
		WorkerSessionState: "active", LeaseEpoch: epoch, LeaseExpiresAt: now.Add(4 * time.Minute), LastSequence: 0, ObservedAt: now,
	}
}
