package storepg

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.DeploymentProvisionStore = (*Store)(nil)

// ClaimDeploymentProvision leases one device-approved, closed deployment.create
// request. The claim joins the immutable approved PlanV1 to the Connection
// Stack's private fixed Worker binding; it deliberately has no caller-provided
// AMI, subnet, user-data, credential, ingress, or generic AWS action field.
func (s *Store) ClaimDeploymentProvision(ctx context.Context, workerID string, lease time.Duration) (runtime.DeploymentProvisionClaim, bool, error) {
	if s == nil || s.db == nil {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("cloud orchestrator database is unavailable")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" || len(workerID) > 128 || strings.ContainsAny(workerID, "\r\n\t") {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("cloud orchestrator worker id is invalid")
	}
	if lease <= 0 || lease > 5*time.Minute {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("cloud deployment provision lease must be between 1ns and 5m")
	}
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("cloud orchestrator lease token is invalid")
	}
	row := s.db.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT outbox.outbox_id
			FROM p2p_cloud_outbox AS outbox
			JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = outbox.aggregate_id
			JOIN p2p_cloud_plans AS plan ON plan.plan_id = deployment.plan_id
			JOIN p2p_cloud_jobs AS job ON job.deployment_id = deployment.deployment_id AND job.kind = 'provision'
			JOIN p2p_cloud_plan_approvals AS approval ON approval.deployment_id = deployment.deployment_id
			JOIN p2p_cloud_plan_versions AS version ON version.plan_id = plan.plan_id AND version.revision = approval.plan_revision
			JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
			JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
			WHERE outbox.kind = $1
				AND outbox.aggregate_type = 'deployment'
				AND outbox.completed_at = 0
				AND outbox.available_at <= $2
				AND outbox.lease_until <= $2
				AND plan.status = 'approved'
				AND deployment.execution_status IN ('queued', 'provisioning')
				AND deployment.outcome_status = 'pending'
				AND deployment.resource_status IN ('none', 'orphaned')
				AND job.execution_status IN ('queued', 'provisioning')
				AND job.outcome_status = 'pending'
				AND approval.status = 'approved'
				AND approval.signature <> ''
				AND connection.status = 'active'
				AND connection.region = broker.broker_region
				AND broker.worker_artifact_kind <> ''
				AND broker.worker_ami_id <> ''
				AND broker.worker_vpc_id <> ''
				AND broker.worker_subnet_id <> ''
				AND broker.worker_availability_zone <> ''
				AND broker.worker_resource_manifest_digest <> ''
			ORDER BY outbox.created_at ASC, outbox.outbox_id ASC
			FOR UPDATE OF outbox SKIP LOCKED
			LIMIT 1
		), claimed AS (
			UPDATE p2p_cloud_outbox AS outbox
			SET lease_owner = $3,
				lease_token = $4,
				lease_until = $5,
				attempts = outbox.attempts + 1,
				last_error_code = ''
			FROM selected
			WHERE outbox.outbox_id = selected.outbox_id
			RETURNING outbox.outbox_id, outbox.kind, outbox.aggregate_type, outbox.aggregate_id,
				outbox.payload_json, outbox.lease_token, outbox.attempts
		)
		SELECT claimed.outbox_id, claimed.kind, claimed.aggregate_type, claimed.aggregate_id,
			deployment.deployment_id, deployment.plan_id, deployment.cloud_connection_id,
			approval.plan_revision, version.quote_valid_until,
			claimed.payload_json, claimed.lease_token, claimed.attempts,
			broker.broker_region, broker.broker_command_url, broker.connection_generation, broker.node_key_id,
			broker.worker_artifact_kind, broker.worker_ami_id, broker.worker_vpc_id, broker.worker_subnet_id,
			broker.worker_availability_zone, broker.worker_resource_manifest_digest,
			approval.approval_id, approval.approval_json, approval.signature,
			version.display_json, version.plan_hash, version.quote_id, version.quote_digest
		FROM claimed
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = claimed.aggregate_id
		JOIN p2p_cloud_plan_approvals AS approval ON approval.deployment_id = deployment.deployment_id
		JOIN p2p_cloud_plan_versions AS version ON version.plan_id = deployment.plan_id AND version.revision = approval.plan_revision
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
	`, runtime.DeploymentProvisionRequested, now, workerID, token, now+lease.Milliseconds())

	var claim runtime.DeploymentProvisionClaim
	var payloadJSON, approvalID, approvalJSON, approvalSignature, planJSON, planHash, quoteID, quoteDigest string
	var quoteValidUntil int64
	var artifactKind, amiID, vpcID, subnetID, availabilityZone, resourceManifestDigest string
	if err := row.Scan(
		&claim.OutboxID, &claim.Kind, &claim.AggregateType, &claim.AggregateID,
		&claim.DeploymentID, &claim.PlanID, &claim.ConnectionID,
		&claim.PlanRevision, &quoteValidUntil,
		&payloadJSON, &claim.LeaseToken, &claim.Attempt,
		&claim.Region, &claim.BrokerEndpoint, &claim.ExpectedGeneration, &claim.NodeKeyID,
		&artifactKind, &amiID, &vpcID, &subnetID, &availabilityZone, &resourceManifestDigest,
		&approvalID, &approvalJSON, &approvalSignature,
		&planJSON, &planHash, &quoteID, &quoteDigest,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.DeploymentProvisionClaim{}, false, nil
		}
		return runtime.DeploymentProvisionClaim{}, false, fmt.Errorf("claim cloud deployment provision outbox: %w", err)
	}
	deploymentID, err := decodeDeploymentProvisionOutbox(payloadJSON)
	if err != nil || deploymentID != claim.DeploymentID {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("deployment provision outbox payload does not bind the claimed deployment")
	}
	plan, err := decodeProvisionPlan(planJSON)
	if err != nil || plan.PlanID != claim.PlanID || int64(plan.Revision) != claim.PlanRevision || planHash == "" {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("approved plan version does not bind the claimed deployment")
	}
	computedPlanHash, err := plan.Hash()
	if err != nil || computedPlanHash != planHash || plan.Quote.QuoteID != quoteID || plan.Quote.Digest != quoteDigest || plan.Quote.ValidUntil.UnixMilli() != quoteValidUntil {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("approved plan hash does not bind the claimed deployment")
	}
	approval, proofJSON, err := materializeApprovedProvisionProof(approvalJSON, approvalSignature)
	if err != nil || approval.ApprovalID != approvalID || approval.ValidateAgainstPlan(plan, time.Time{}) != nil {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("approved device proof does not bind the claimed deployment")
	}
	quote, err := loadProvisionQuote(ctx, s.db, quoteID)
	if err != nil || quote.CloudConnectionID != claim.ConnectionID || quote.ValidUntil.UnixMilli() != quoteValidUntil || quoteDigestForProvision(quote) != quoteDigest || !provisionCandidateMatchesPlan(quote, plan, availabilityZone) {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("approved quote does not bind the claimed deployment")
	}
	if plan.NetworkScope.PublicIngress || len(plan.NetworkScope.Ingress) != 0 {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("first deployment provision does not permit public ingress")
	}
	claim.QuoteValidUntil = time.UnixMilli(quoteValidUntil).UTC()
	claim.ApprovalValidUntil = approval.ExpiresAt.UTC()
	claim.ApprovalProofJSON = proofJSON
	claim.JobID, err = provisionJobID(ctx, s.db, claim.DeploymentID)
	if err != nil {
		return runtime.DeploymentProvisionClaim{}, false, err
	}
	claim.Request = runtime.DeploymentCreateRequest{
		Schema:                 runtime.DeploymentCreateSchema,
		DeploymentID:           claim.DeploymentID,
		ConnectionGeneration:   claim.ExpectedGeneration,
		PlanHash:               planHash,
		PlanRevision:           uint64(claim.PlanRevision),
		QuoteID:                quoteID,
		QuoteDigest:            quoteDigest,
		CandidateID:            plan.Quote.CandidateID,
		ResourceManifestDigest: resourceManifestDigest,
		WorkerArtifact:         runtime.WorkerArtifactReferenceV1{Kind: artifactKind, AMIID: amiID},
		Network: runtime.DeploymentNetworkReference{
			VPCID: vpcID, SubnetID: subnetID, AvailabilityZone: availabilityZone,
		},
	}
	if err := claim.Request.Validate(); err != nil {
		return runtime.DeploymentProvisionClaim{}, false, errors.New("deployment Worker placement is not attested")
	}
	command, err := s.prepareDeploymentCreateCommand(ctx, claim, approvalID)
	if err != nil {
		return runtime.DeploymentProvisionClaim{}, false, err
	}
	claim.Command = command
	return claim, true, nil
}

func (s *Store) prepareDeploymentCreateCommand(ctx context.Context, claim runtime.DeploymentProvisionClaim, approvalID string) (runtime.DeploymentCreateCommand, error) {
	digest, err := claim.Request.Digest()
	if err != nil {
		return runtime.DeploymentCreateCommand{}, fmt.Errorf("deployment create request digest: %w", err)
	}
	var command runtime.DeploymentCreateCommand
	err = s.withDeploymentProvisionClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		latest, found, err := selectLatestDeploymentCreateCommand(ctx, tx, claim, digest)
		if err != nil {
			return err
		}
		if found && latest.Command.State != "expired" {
			if latest.Command.State == "failed" {
				return errors.New("deployment create command is terminal while its outbox remains pending")
			}
			command = latest.Command
			return nil
		}
		attempt := 1
		if found {
			attempt = latest.Command.Attempt + 1
		}
		var counter int64
		if err := tx.QueryRowContext(ctx, `
			UPDATE p2p_cloud_connection_brokers
			SET next_node_counter = next_node_counter + 1, updated_at = $1
			WHERE cloud_connection_id = $2
			RETURNING next_node_counter
		`, now, claim.ConnectionID).Scan(&counter); err != nil {
			return err
		}
		command = runtime.DeploymentCreateCommand{
			CommandID:          stableID("cloud_broker_deployment_", claim.ConnectionID, claim.DeploymentID, fmt.Sprint(claim.PlanRevision), digest, fmt.Sprint(attempt)),
			DeploymentID:       claim.DeploymentID,
			ConnectionID:       claim.ConnectionID,
			NodeKeyID:          claim.NodeKeyID,
			ExpectedGeneration: claim.ExpectedGeneration,
			NodeCounter:        counter,
			Attempt:            attempt,
			RequestDigest:      digest,
			State:              "allocated",
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_deployment_commands (
				command_id, deployment_id, cloud_connection_id, plan_id, plan_revision, approval_id, request_digest,
				command_attempt, action, node_key_id, expected_generation, node_counter, state, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'deployment.create', $9, $10, $11, 'allocated', $12, $12)
		`, command.CommandID, command.DeploymentID, command.ConnectionID, claim.PlanID, claim.PlanRevision, approvalID,
			digest, command.Attempt, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
		return err
	})
	return command, err
}

func (s *Store) PersistDeploymentCreateCommand(ctx context.Context, claim runtime.DeploymentProvisionClaim, signed runtime.SignedDeploymentCreateCommand) error {
	if err := validPersistedDeploymentCreateCommand(claim, signed); err != nil {
		return err
	}
	return s.withDeploymentProvisionClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		existing, err := selectDeploymentCreateCommandByID(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if existing.Command.DeploymentID != claim.DeploymentID || existing.Command.ConnectionID != claim.ConnectionID || existing.PlanID != claim.PlanID ||
			existing.PlanRevision != claim.PlanRevision || existing.Command.NodeKeyID != claim.NodeKeyID || existing.Command.ExpectedGeneration != claim.ExpectedGeneration ||
			existing.Command.NodeCounter != claim.Command.NodeCounter || existing.Command.Attempt != claim.Command.Attempt || existing.Command.RequestDigest != claim.Command.RequestDigest {
			return errors.New("persisted deployment create command does not match the claim")
		}
		if existing.Command.State == "signed" || existing.Command.State == "indeterminate" || existing.Command.State == "accepted" {
			if existing.Command.PayloadJSON == signed.PayloadJSON && existing.Command.PayloadSHA256 == signed.PayloadSHA256 && existing.Command.RequestSHA256 == signed.RequestSHA256 &&
				existing.Command.SignedEnvelope == signed.EnvelopeJSON && existing.Command.IssuedAt.UTC().Equal(signed.IssuedAt.UTC()) && existing.Command.ExpiresAt.UTC().Equal(signed.ExpiresAt.UTC()) {
				return nil
			}
			return errors.New("deployment create command already has a different signed envelope")
		}
		if existing.Command.State != "allocated" {
			return errors.New("deployment create command cannot be signed in its current state")
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_commands
			SET canonical_payload_json = $1, payload_sha256 = $2, request_sha256 = $3, signed_envelope_json = $4,
				issued_at = $5, expires_at = $6, state = 'signed', updated_at = $7
			WHERE command_id = $8 AND state = 'allocated'
		`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON,
			signed.IssuedAt.UTC().UnixMilli(), signed.ExpiresAt.UTC().UnixMilli(), now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func (s *Store) MarkDeploymentProvisionStarted(ctx context.Context, claim runtime.DeploymentProvisionClaim) error {
	return s.withDeploymentProvisionClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionProvisionDeployment(ctx, tx, claim, now, "provisioning", "pending", ""); err != nil {
			return err
		}
		_, err := transitionProvisionJob(ctx, tx, claim, now, researchJobTransition{
			execution: "provisioning", outcome: "pending", checkpoint: "broker_create_pending", errorCode: "",
			stepStatus: "running", stepSummary: "The approved dedicated Worker is being created through the signed AWS Connection Stack.",
		})
		return err
	})
}

// DeferDeploymentProvision retains an indeterminate create command for an
// exact replay. A network result may have been lost after EC2 creation, so the
// public resource axis becomes orphaned rather than falsely reporting none.
func (s *Store) DeferDeploymentProvision(ctx context.Context, claim runtime.DeploymentProvisionClaim, code string, availableAt time.Time) error {
	code = durableErrorCode(code, "deployment_provision_retryable")
	return s.withDeploymentProvisionClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionProvisionDeployment(ctx, tx, claim, now, "provisioning", "pending", "orphaned"); err != nil {
			return err
		}
		if _, err := transitionProvisionJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "broker_create_retry_scheduled", errorCode: code,
			stepStatus: "queued", stepSummary: "The Worker create command is awaiting an exact signed retry; resource state remains uncertain until Stack read-back.",
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_commands
			SET state = CASE WHEN state = 'accepted' THEN 'accepted' ELSE 'indeterminate' END,
				attempts = attempts + 1, last_error_code = $1, updated_at = $2
			WHERE command_id = $3
		`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		available := availableAt.UTC().UnixMilli()
		if available < now {
			available = now
		}
		return releaseDeploymentProvisionOutbox(ctx, tx, claim, available, code)
	})
}

func (s *Store) ExpireDeploymentCreateCommand(ctx context.Context, claim runtime.DeploymentProvisionClaim) error {
	return s.withDeploymentProvisionClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		if _, err := transitionProvisionDeployment(ctx, tx, claim, now, "queued", "pending", "none"); err != nil {
			return err
		}
		if _, err := transitionProvisionJob(ctx, tx, claim, now, researchJobTransition{
			execution: "queued", outcome: "pending", checkpoint: "broker_create_command_expired", errorCode: "deployment_create_command_expired",
			stepStatus: "queued", stepSummary: "The signed Worker create command expired before a receipt was recorded; a new command will be prepared.",
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_commands
			SET state = 'expired', attempts = attempts + 1, last_error_code = 'deployment_create_command_expired', updated_at = $1
			WHERE command_id = $2 AND state IN ('allocated', 'signed', 'indeterminate')
		`, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		return releaseDeploymentProvisionOutbox(ctx, tx, claim, now, "deployment_create_command_expired")
	})
}

func (s *Store) FailDeploymentProvision(ctx context.Context, claim runtime.DeploymentProvisionClaim, code string) error {
	code = durableErrorCode(code, "deployment_provision_transport_failed")
	knownNoCreate := code == runtime.DeploymentProvisionQuoteExpired || code == runtime.DeploymentProvisionApprovalExpired || code == "invalid_deployment_provision_claim"
	return s.withDeploymentProvisionClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		resource := "orphaned"
		checkpoint := "provision_uncertain"
		stepSummary := "The Worker create result could not be verified; any retained cloud resource remains tracked as uncertain."
		if knownNoCreate {
			resource = "none"
			checkpoint = "provision_failed_before_create"
			stepSummary = "The Worker create request failed before a cloud mutation was sent."
		}
		if _, err := transitionProvisionDeployment(ctx, tx, claim, now, "finished", "failed", resource); err != nil {
			return err
		}
		if _, err := transitionProvisionJob(ctx, tx, claim, now, researchJobTransition{
			execution: "finished", outcome: "failed", checkpoint: checkpoint, errorCode: code,
			stepStatus: "failed", stepSummary: stepSummary,
		}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_commands
			SET state = 'failed', attempts = attempts + 1, last_error_code = $1, updated_at = $2
			WHERE command_id = $3 AND state IN ('allocated', 'signed', 'indeterminate')
		`, code, now, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(result); err != nil {
			return err
		}
		if code == runtime.DeploymentProvisionQuoteExpired || code == runtime.DeploymentProvisionApprovalExpired {
			if err := expireApprovedProvisionPlan(ctx, tx, claim.PlanID, now, code); err != nil {
				return err
			}
		}
		return completeDeploymentProvisionOutbox(ctx, tx, claim, now)
	})
}

func (s *Store) CommitDeploymentProvision(ctx context.Context, claim runtime.DeploymentProvisionClaim, deployment runtime.BrokerDeployment) error {
	return s.withDeploymentProvisionClaimTransaction(ctx, claim, func(tx *sql.Tx, now int64) error {
		stored, err := selectDeploymentCreateCommandByID(ctx, tx, claim.Command.CommandID)
		if err != nil {
			return err
		}
		if stored.Command.State != "signed" && stored.Command.State != "indeterminate" && stored.Command.State != "accepted" {
			return errors.New("deployment create command is not eligible for a receipt")
		}
		validatedClaim := claim
		validatedClaim.Command = stored.Command
		signed := runtime.SignedDeploymentCreateCommand{
			EnvelopeJSON: stored.Command.SignedEnvelope, PayloadJSON: stored.Command.PayloadJSON,
			PayloadSHA256: stored.Command.PayloadSHA256, RequestSHA256: stored.Command.RequestSHA256,
			IssuedAt: stored.Command.IssuedAt, ExpiresAt: stored.Command.ExpiresAt,
		}
		if err := validPersistedDeploymentCreateCommand(validatedClaim, signed); err != nil {
			return err
		}
		if err := runtime.ValidateBrokerDeployment(validatedClaim, signed, deployment); err != nil {
			return err
		}
		safeReceipt, err := deploymentReceiptJSON(deployment)
		if err != nil {
			return err
		}
		volumesJSON, err := json.Marshal(deployment.VolumeIDs)
		if err != nil {
			return err
		}
		interfacesJSON, err := json.Marshal(deployment.NetworkInterfaceIDs)
		if err != nil {
			return err
		}
		commandUpdate, err := tx.ExecContext(ctx, `
			UPDATE p2p_cloud_deployment_commands
			SET state = 'accepted', attempts = attempts + 1, last_error_code = '', receipt_json = $1, updated_at = $2
			WHERE command_id = $3
		`, safeReceipt, now, stored.Command.CommandID)
		if err != nil {
			return err
		}
		if err := requireOneAffected(commandUpdate); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_deployment_resources (
				deployment_id, cloud_connection_id, request_sha256, resource_status, instance_id,
				volume_ids_json, network_interface_ids_json, broker_receipt_json, created_at, updated_at
			) VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, $8, $8)
			ON CONFLICT (deployment_id) DO UPDATE
			SET cloud_connection_id = EXCLUDED.cloud_connection_id, request_sha256 = EXCLUDED.request_sha256,
				resource_status = EXCLUDED.resource_status, instance_id = EXCLUDED.instance_id,
				volume_ids_json = EXCLUDED.volume_ids_json, network_interface_ids_json = EXCLUDED.network_interface_ids_json,
				broker_receipt_json = EXCLUDED.broker_receipt_json, updated_at = EXCLUDED.updated_at
		`, claim.DeploymentID, claim.ConnectionID, deployment.RequestSHA256, deployment.InstanceID,
			string(volumesJSON), string(interfacesJSON), safeReceipt, now); err != nil {
			return err
		}
		if _, err := transitionProvisionDeployment(ctx, tx, claim, now, "provisioning", "pending", "active"); err != nil {
			return err
		}
		if _, err := transitionProvisionJob(ctx, tx, claim, now, researchJobTransition{
			execution: "provisioning", outcome: "pending", checkpoint: "worker_bootstrap_pending", errorCode: "",
			stepStatus: "running", stepSummary: "The dedicated Worker was created and is awaiting its outbound bootstrap claim.",
		}); err != nil {
			return err
		}
		return completeDeploymentProvisionOutbox(ctx, tx, claim, now)
	})
}

type storedDeploymentCreateCommand struct {
	Command      runtime.DeploymentCreateCommand
	PlanID       string
	PlanRevision int64
	ApprovalID   string
}

func selectLatestDeploymentCreateCommand(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentProvisionClaim, digest string) (storedDeploymentCreateCommand, bool, error) {
	stored, err := scanDeploymentCreateCommand(tx.QueryRowContext(ctx, `
		SELECT command_id, deployment_id, cloud_connection_id, plan_id, plan_revision, approval_id,
			request_digest, command_attempt, node_key_id, expected_generation, node_counter,
			canonical_payload_json, payload_sha256, request_sha256, signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_deployment_commands
		WHERE deployment_id = $1 AND request_digest = $2
		ORDER BY command_attempt DESC LIMIT 1 FOR UPDATE
	`, claim.DeploymentID, digest))
	if errors.Is(err, sql.ErrNoRows) {
		return storedDeploymentCreateCommand{}, false, nil
	}
	if err != nil {
		return storedDeploymentCreateCommand{}, false, err
	}
	return stored, true, nil
}

func selectDeploymentCreateCommandByID(ctx context.Context, tx *sql.Tx, commandID string) (storedDeploymentCreateCommand, error) {
	return scanDeploymentCreateCommand(tx.QueryRowContext(ctx, `
		SELECT command_id, deployment_id, cloud_connection_id, plan_id, plan_revision, approval_id,
			request_digest, command_attempt, node_key_id, expected_generation, node_counter,
			canonical_payload_json, payload_sha256, request_sha256, signed_envelope_json, issued_at, expires_at, state
		FROM p2p_cloud_deployment_commands WHERE command_id = $1 FOR UPDATE
	`, commandID))
}

func scanDeploymentCreateCommand(row interface{ Scan(...any) error }) (storedDeploymentCreateCommand, error) {
	var stored storedDeploymentCreateCommand
	var issuedAt, expiresAt int64
	err := row.Scan(
		&stored.Command.CommandID, &stored.Command.DeploymentID, &stored.Command.ConnectionID, &stored.PlanID, &stored.PlanRevision, &stored.ApprovalID,
		&stored.Command.RequestDigest, &stored.Command.Attempt, &stored.Command.NodeKeyID, &stored.Command.ExpectedGeneration, &stored.Command.NodeCounter,
		&stored.Command.PayloadJSON, &stored.Command.PayloadSHA256, &stored.Command.RequestSHA256, &stored.Command.SignedEnvelope,
		&issuedAt, &expiresAt, &stored.Command.State,
	)
	if err != nil {
		return storedDeploymentCreateCommand{}, err
	}
	stored.Command.IssuedAt = time.UnixMilli(issuedAt).UTC()
	stored.Command.ExpiresAt = time.UnixMilli(expiresAt).UTC()
	return stored, nil
}

func validPersistedDeploymentCreateCommand(claim runtime.DeploymentProvisionClaim, signed runtime.SignedDeploymentCreateCommand) error {
	if claim.Command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" || len(signed.EnvelopeJSON) > 256*1024 ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" || len(signed.PayloadJSON) > 16*1024 ||
		signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed deployment create command is invalid")
	}
	command, err := broker.ParseDeploymentCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		return errors.New("signed deployment create command is invalid")
	}
	binding, err := deploymentCommandBinding(claim, signed)
	if err != nil || command.ValidateBinding(binding) != nil || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("signed deployment create command is invalid")
	}
	payload, err := base64.StdEncoding.DecodeString(command.PayloadB64)
	if err != nil || string(payload) != signed.PayloadJSON {
		return errors.New("signed deployment create command is invalid")
	}
	return nil
}

func deploymentCommandBinding(claim runtime.DeploymentProvisionClaim, signed runtime.SignedDeploymentCreateCommand) (broker.DeploymentCommandBinding, error) {
	proof, err := decodeProvisionApprovalProof(claim.ApprovalProofJSON)
	if err != nil || claim.PlanID == "" || claim.ConnectionID == "" || claim.DeploymentID == "" || claim.PlanRevision <= 0 ||
		claim.Command.DeploymentID != claim.DeploymentID || claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID == "" ||
		claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 {
		return broker.DeploymentCommandBinding{}, errors.New("deployment create command does not bind the claim")
	}
	digest, err := claim.Request.Digest()
	if err != nil || claim.Command.RequestDigest != digest {
		return broker.DeploymentCommandBinding{}, errors.New("deployment create command does not bind the request")
	}
	return broker.DeploymentCommandBinding{
		ConnectionID: claim.ConnectionID, CommandID: claim.Command.CommandID, NodeKeyID: claim.Command.NodeKeyID,
		ExpectedGeneration: claim.Command.ExpectedGeneration, NodeCounter: claim.Command.NodeCounter,
		IssuedAt: signed.IssuedAt, ExpiresAt: signed.ExpiresAt, ApprovalProof: proof,
		Request: broker.DeploymentRequest{
			Schema: claim.Request.Schema, DeploymentID: claim.Request.DeploymentID, ConnectionGeneration: claim.Request.ConnectionGeneration,
			PlanHash: claim.Request.PlanHash, PlanRevision: claim.Request.PlanRevision, QuoteID: claim.Request.QuoteID,
			QuoteDigest: claim.Request.QuoteDigest, CandidateID: claim.Request.CandidateID, ResourceManifestDigest: claim.Request.ResourceManifestDigest,
			WorkerArtifact: broker.DeploymentWorkerArtifact{Kind: claim.Request.WorkerArtifact.Kind, AMIID: claim.Request.WorkerArtifact.AMIID},
			Network:        broker.DeploymentNetwork{VPCID: claim.Request.Network.VPCID, SubnetID: claim.Request.Network.SubnetID, AvailabilityZone: claim.Request.Network.AvailabilityZone},
		},
	}, nil
}

func (s *Store) withDeploymentProvisionClaimTransaction(ctx context.Context, claim runtime.DeploymentProvisionClaim, run func(*sql.Tx, int64) error) (err error) {
	if s == nil || s.db == nil {
		return errors.New("cloud orchestrator database is unavailable")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	if err = verifyDeploymentProvisionClaimFence(ctx, tx, claim, now); err != nil {
		return err
	}
	if err = run(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func verifyDeploymentProvisionClaimFence(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentProvisionClaim, now int64) error {
	var leaseToken, aggregateType, aggregateID, deploymentID, planID, connectionID, planStatus string
	var deploymentExecution, deploymentOutcome, deploymentResource, jobID, jobKind, jobOutcome string
	var jobExecution, endpoint, brokerRegion, nodeKeyID, approvalID, approvalStatus string
	var workerKind, workerAMI, workerVPC, workerSubnet, workerAZ, workerManifest string
	var approvalJSON, approvalSignature, planHash, quoteID, quoteDigest string
	var leaseUntil, completedAt, generation, approvalPlanRevision, quoteValidUntil int64
	var jobPlanID string
	err := tx.QueryRowContext(ctx, `
		SELECT outbox.lease_token, outbox.lease_until, outbox.completed_at, outbox.aggregate_type, outbox.aggregate_id,
			deployment.deployment_id, deployment.plan_id, deployment.cloud_connection_id,
			deployment.execution_status, deployment.outcome_status, deployment.resource_status,
			plan.status,
			job.job_id, job.plan_id, job.execution_status, job.outcome_status, job.kind,
			broker.broker_command_url, broker.broker_region, broker.connection_generation, broker.node_key_id,
			broker.worker_artifact_kind, broker.worker_ami_id, broker.worker_vpc_id, broker.worker_subnet_id,
			broker.worker_availability_zone, broker.worker_resource_manifest_digest,
			approval.approval_id, approval.plan_revision, approval.status, approval.approval_json, approval.signature,
			version.plan_hash, version.quote_id, version.quote_digest, version.quote_valid_until
		FROM p2p_cloud_outbox AS outbox
		JOIN p2p_cloud_deployments AS deployment ON deployment.deployment_id = outbox.aggregate_id
		JOIN p2p_cloud_plans AS plan ON plan.plan_id = deployment.plan_id
		JOIN p2p_cloud_jobs AS job ON job.deployment_id = deployment.deployment_id AND job.kind = 'provision'
		JOIN p2p_cloud_connections AS connection ON connection.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers AS broker ON broker.cloud_connection_id = deployment.cloud_connection_id
		JOIN p2p_cloud_plan_approvals AS approval ON approval.deployment_id = deployment.deployment_id
		JOIN p2p_cloud_plan_versions AS version ON version.plan_id = plan.plan_id AND version.revision = approval.plan_revision
		WHERE outbox.outbox_id = $1
		FOR UPDATE OF outbox, deployment, plan, job, connection, broker, approval, version
	`, claim.OutboxID).Scan(
		&leaseToken, &leaseUntil, &completedAt, &aggregateType, &aggregateID,
		&deploymentID, &planID, &connectionID, &deploymentExecution, &deploymentOutcome, &deploymentResource,
		&planStatus,
		&jobID, &jobPlanID, &jobExecution, &jobOutcome, &jobKind,
		&endpoint, &brokerRegion, &generation, &nodeKeyID,
		&workerKind, &workerAMI, &workerVPC, &workerSubnet, &workerAZ, &workerManifest,
		&approvalID, &approvalPlanRevision, &approvalStatus, &approvalJSON, &approvalSignature,
		&planHash, &quoteID, &quoteDigest, &quoteValidUntil,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	proof, proofJSON, proofErr := materializeApprovedProvisionProof(approvalJSON, approvalSignature)
	if proofErr != nil {
		return ErrLeaseLost
	}
	if claim.Kind != runtime.DeploymentProvisionRequested || claim.LeaseToken == "" || leaseToken != claim.LeaseToken || leaseUntil <= now || completedAt != 0 ||
		aggregateType != "deployment" || aggregateID != claim.AggregateID || deploymentID != claim.DeploymentID || planID != claim.PlanID || connectionID != claim.ConnectionID ||
		planStatus != "approved" || deploymentOutcome != "pending" || (deploymentExecution != "queued" && deploymentExecution != "provisioning") ||
		(deploymentResource != "none" && deploymentResource != "orphaned") || jobID != claim.JobID || jobPlanID != claim.PlanID || jobKind != "provision" || jobOutcome != "pending" ||
		(jobExecution != "queued" && jobExecution != "provisioning") || endpoint != claim.BrokerEndpoint || brokerRegion != claim.Region || generation != claim.ExpectedGeneration || nodeKeyID != claim.NodeKeyID ||
		workerKind != claim.Request.WorkerArtifact.Kind || workerAMI != claim.Request.WorkerArtifact.AMIID || workerVPC != claim.Request.Network.VPCID || workerSubnet != claim.Request.Network.SubnetID ||
		workerAZ != claim.Request.Network.AvailabilityZone || workerManifest != claim.Request.ResourceManifestDigest || approvalID != proof.ApprovalID || approvalPlanRevision != claim.PlanRevision || approvalStatus != "approved" ||
		proofJSON != claim.ApprovalProofJSON || proof.PlanHash != planHash || claim.Request.PlanHash != planHash || claim.Request.QuoteID != quoteID || claim.Request.QuoteDigest != quoteDigest ||
		claim.QuoteValidUntil.UnixMilli() != quoteValidUntil || claim.QuoteValidUntil.UTC().UnixMilli() != proof.QuoteValidUntil.UTC().UnixMilli() || claim.ApprovalValidUntil.UTC().UnixMilli() != proof.ExpiresAt.UTC().UnixMilli() || claim.Request.ConnectionGeneration != generation {
		return ErrLeaseLost
	}
	return nil
}

func transitionProvisionDeployment(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentProvisionClaim, now int64, execution, outcome, resource string) (cloudmodule.Deployment, error) {
	return transitionDeployment(ctx, tx, claim.DeploymentID, claim.PlanID, claim.ConnectionID, now, execution, outcome, resource)
}

// transitionDeployment is the one durable deployment state/event writer used
// by both the provision command and the later Worker bootstrap observation.
// It keeps private resource evidence out of the ProductCore projection while
// preserving monotonic deployment revisions across the two execution stages.
func transitionDeployment(ctx context.Context, tx *sql.Tx, deploymentID, planID, connectionID string, now int64, execution, outcome, resource string) (cloudmodule.Deployment, error) {
	var deployment cloudmodule.Deployment
	err := tx.QueryRowContext(ctx, `
		SELECT deployment_id, plan_id, cloud_connection_id, execution_status, outcome_status, resource_status, revision, created_at, updated_at
		FROM p2p_cloud_deployments WHERE deployment_id = $1 FOR UPDATE
	`, deploymentID).Scan(
		&deployment.DeploymentID, &deployment.PlanID, &deployment.ConnectionID, &deployment.Execution, &deployment.Outcome, &deployment.Resource,
		&deployment.Revision, &deployment.CreatedAt, &deployment.UpdatedAt,
	)
	if err != nil {
		return cloudmodule.Deployment{}, err
	}
	if deployment.PlanID != planID || deployment.ConnectionID != connectionID || deployment.Revision <= 0 {
		return cloudmodule.Deployment{}, ErrLeaseLost
	}
	if resource == "" {
		resource = deployment.Resource
	}
	if deployment.Outcome != "pending" && outcome != deployment.Outcome {
		return cloudmodule.Deployment{}, ErrLeaseLost
	}
	if deployment.Execution == execution && deployment.Outcome == outcome && deployment.Resource == resource {
		return deployment, nil
	}
	previousRevision := deployment.Revision
	previousOutcome := deployment.Outcome
	deployment.Execution, deployment.Outcome, deployment.Resource = execution, outcome, resource
	deployment.Revision++
	deployment.UpdatedAt = now
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_deployments
		SET execution_status = $1, outcome_status = $2, resource_status = $3, revision = $4, updated_at = $5
		WHERE deployment_id = $6 AND revision = $7 AND outcome_status = $8
	`, deployment.Execution, deployment.Outcome, deployment.Resource, deployment.Revision, now, deployment.DeploymentID, previousRevision, previousOutcome)
	if err != nil {
		return cloudmodule.Deployment{}, err
	}
	if err := requireOneAffected(result); err != nil {
		return cloudmodule.Deployment{}, err
	}
	if err := writeEventAndProjection(ctx, tx,
		stableID("cloud_event_", deployment.DeploymentID, fmt.Sprint(deployment.Revision), deployment.Execution, deployment.Outcome, deployment.Resource),
		"cloud.deployment.changed", "deployment", deployment.DeploymentID, deployment.Revision, deploymentSummary(deployment), now); err != nil {
		return cloudmodule.Deployment{}, err
	}
	return deployment, nil
}

func transitionProvisionJob(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentProvisionClaim, now int64, transition researchJobTransition) (cloudmodule.Job, error) {
	return transitionCloudJob(ctx, tx, claim.JobID, claim.PlanID, claim.DeploymentID, "provision", "provision", now, transition)
}

func deploymentSummary(deployment cloudmodule.Deployment) map[string]any {
	return map[string]any{
		"deployment_id": deployment.DeploymentID, "plan_id": deployment.PlanID, "cloud_connection_id": deployment.ConnectionID,
		"execution_status": deployment.Execution, "outcome_status": deployment.Outcome, "resource_status": deployment.Resource,
		"revision": deployment.Revision, "created_at": deployment.CreatedAt, "updated_at": deployment.UpdatedAt,
	}
}

func releaseDeploymentProvisionOutbox(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentProvisionClaim, available int64, code string) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_outbox
		SET lease_owner = '', lease_token = '', lease_until = 0, available_at = $1, last_error_code = $2
		WHERE outbox_id = $3 AND lease_token = $4 AND completed_at = 0
	`, available, code, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func completeDeploymentProvisionOutbox(ctx context.Context, tx *sql.Tx, claim runtime.DeploymentProvisionClaim, now int64) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_outbox
		SET lease_owner = '', lease_token = '', lease_until = 0, completed_at = $1,
			delivered_at = $1, available_at = $1, last_error_code = ''
		WHERE outbox_id = $2 AND lease_token = $3 AND completed_at = 0
	`, now, claim.OutboxID, claim.LeaseToken)
	if err != nil {
		return err
	}
	return requireOneAffected(result)
}

func expireApprovedProvisionPlan(ctx context.Context, tx *sql.Tx, planID string, now int64, reason string) error {
	var goalID, status, title, summary, connectionID, recipeDigest, quoteID, planHash string
	var revision, createdAt int64
	if err := tx.QueryRowContext(ctx, `
		SELECT goal_id, status, title, summary, cloud_connection_id, recipe_digest, quote_id, plan_hash, revision, created_at
		FROM p2p_cloud_plans WHERE plan_id = $1 FOR UPDATE
	`, planID).Scan(&goalID, &status, &title, &summary, &connectionID, &recipeDigest, &quoteID, &planHash, &revision, &createdAt); err != nil {
		return err
	}
	if status == "expired" {
		return nil
	}
	if status != "approved" || revision <= 0 {
		return ErrLeaseLost
	}
	nextRevision := revision + 1
	result, err := tx.ExecContext(ctx, `
		UPDATE p2p_cloud_plans SET status = 'expired', revision = $1, updated_at = $2
		WHERE plan_id = $3 AND revision = $4 AND status = 'approved'
	`, nextRevision, now, planID, revision)
	if err != nil {
		return err
	}
	if err := requireOneAffected(result); err != nil {
		return err
	}
	return writeEventAndProjection(ctx, tx,
		stableID("cloud_event_", planID, fmt.Sprint(nextRevision), reason),
		"cloud.plan.changed", "plan", planID, nextRevision, map[string]any{
			"plan_id": planID, "goal_id": goalID, "cloud_connection_id": connectionID, "status": "expired",
			"title": title, "summary": summary, "recipe_digest": recipeDigest, "quote_id": quoteID, "plan_hash": planHash,
			"revision": nextRevision, "created_at": createdAt, "updated_at": now,
		}, now)
}

func deploymentReceiptJSON(deployment runtime.BrokerDeployment) (string, error) {
	value := struct {
		Schema        string `json:"schema"`
		ConnectionID  string `json:"connection_id"`
		DeploymentID  string `json:"deployment_id"`
		CommandID     string `json:"command_id"`
		RequestSHA256 string `json:"request_sha256"`
		ResourceState string `json:"resource_status"`
	}{
		Schema: deployment.Schema, ConnectionID: deployment.ConnectionID, DeploymentID: deployment.DeploymentID,
		CommandID: deployment.CommandID, RequestSHA256: deployment.RequestSHA256, ResourceState: deployment.ResourceStatus,
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeDeploymentProvisionOutbox(payload string) (string, error) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var value struct {
		DeploymentID string `json:"deployment_id"`
	}
	if err := decoder.Decode(&value); err != nil {
		return "", errors.New("deployment provision outbox payload is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", errors.New("deployment provision outbox payload contains trailing JSON")
	}
	if strings.TrimSpace(value.DeploymentID) != value.DeploymentID || value.DeploymentID == "" {
		return "", errors.New("deployment provision outbox payload is invalid")
	}
	return value.DeploymentID, nil
}

func decodeProvisionPlan(raw string) (cloudcontracts.PlanV1, error) {
	var plan cloudcontracts.PlanV1
	if err := decodeProvisionJSON(raw, &plan); err != nil || plan.Validate() != nil {
		return cloudcontracts.PlanV1{}, errors.New("approved plan version is invalid")
	}
	return plan, nil
}

func decodeProvisionApprovalProof(raw string) (cloudcontracts.ApprovalV1, error) {
	var approval cloudcontracts.ApprovalV1
	if err := decodeProvisionJSON(raw, &approval); err != nil || approval.Validate() != nil || approval.Signature == "" {
		return cloudcontracts.ApprovalV1{}, errors.New("approved device proof is invalid")
	}
	return approval, nil
}

func materializeApprovedProvisionProof(raw, signature string) (cloudcontracts.ApprovalV1, string, error) {
	var approval cloudcontracts.ApprovalV1
	if err := decodeProvisionJSON(raw, &approval); err != nil {
		return cloudcontracts.ApprovalV1{}, "", err
	}
	approval.Signature = signature
	if err := approval.Validate(); err != nil || approval.Signature == "" {
		return cloudcontracts.ApprovalV1{}, "", errors.New("approved device proof is invalid")
	}
	encoded, err := json.Marshal(approval)
	if err != nil {
		return cloudcontracts.ApprovalV1{}, "", err
	}
	return approval, string(encoded), nil
}

func decodeProvisionJSON(raw string, target any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("cloud provision JSON contains trailing data")
	}
	return nil
}

func loadProvisionQuote(ctx context.Context, db *sql.DB, quoteID string) (cloudcontracts.QuoteV1, error) {
	var displayJSON string
	if err := db.QueryRowContext(ctx, `SELECT display_json FROM p2p_cloud_quotes WHERE quote_id = $1`, quoteID).Scan(&displayJSON); err != nil {
		return cloudcontracts.QuoteV1{}, err
	}
	var quote cloudcontracts.QuoteV1
	if err := decodeProvisionJSON(displayJSON, &quote); err != nil || quote.Validate() != nil || quote.QuoteID != quoteID {
		return cloudcontracts.QuoteV1{}, errors.New("approved quote is invalid")
	}
	return quote, nil
}

func quoteDigestForProvision(quote cloudcontracts.QuoteV1) string {
	digest, err := quote.Digest()
	if err != nil {
		return ""
	}
	return digest
}

func provisionCandidateMatchesPlan(quote cloudcontracts.QuoteV1, plan cloudcontracts.PlanV1, workerAZ string) bool {
	if quote.QuoteID != plan.Quote.QuoteID || !quote.ValidUntil.Equal(plan.Quote.ValidUntil) {
		return false
	}
	for _, candidate := range quote.Candidates {
		if candidate.CandidateID != plan.Quote.CandidateID {
			continue
		}
		resource := plan.ResourceScope
		if candidate.InstanceType != resource.InstanceType || candidate.Architecture != resource.Architecture || candidate.VCPU != resource.VCPU ||
			candidate.MemoryMiB != resource.MemoryMiB || candidate.GPUCount != resource.GPUCount || candidate.GPUMemoryMiB != resource.GPUMemoryMiB ||
			candidate.EstimatedDiskGiB != resource.DiskGiB || candidate.PurchaseOption != resource.PurchaseOption {
			return false
		}
		return containsProvisionAZ(candidate.AvailabilityZones, workerAZ) && containsProvisionAZ(resource.AvailabilityZones, workerAZ)
	}
	return false
}

func containsProvisionAZ(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func provisionJobID(ctx context.Context, db *sql.DB, deploymentID string) (string, error) {
	var jobID string
	err := db.QueryRowContext(ctx, `SELECT job_id FROM p2p_cloud_jobs WHERE deployment_id = $1 AND kind = 'provision'`, deploymentID).Scan(&jobID)
	if err != nil {
		return "", err
	}
	return jobID, nil
}
