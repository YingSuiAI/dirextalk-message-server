package storepg

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.ServiceSecretObserveStore = (*Store)(nil)

var serviceSecretOpaqueVersion = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)

func (s *Store) ClaimPendingServiceSecretObserve(ctx context.Context, workerID string, lease time.Duration) (claim runtime.ServiceSecretObserveClaim, found bool, err error) {
	if s == nil || s.db == nil || strings.TrimSpace(workerID) == "" || lease <= 0 || lease > 5*time.Minute {
		return claim, false, errors.New("service secret observe claim configuration is invalid")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return claim, false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	now := s.now().UnixMilli()
	token := strings.TrimSpace(s.cfg.NewLeaseToken())
	if token == "" || len(token) > 128 {
		return claim, false, errors.New("service secret observe lease token is invalid")
	}
	var approvalID string
	var approvalExpiresAt int64
	err = tx.QueryRowContext(ctx, `
		SELECT approval.approval_id,approval.session_id,approval.deployment_id,approval.task_id,approval.execution_id,approval.manifest_digest,approval.expires_at,
			approval.secret_ref,approval.context_digest,approval.cloud_connection_id,connection.region,broker.broker_command_url,broker.node_key_id,broker.connection_generation
		FROM p2p_cloud_service_secret_bootstrap_approvals approval
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=approval.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=approval.cloud_connection_id
		WHERE approval.status IN('pending','observing') AND approval.available_at<=$1 AND approval.lease_until<=$1
			AND connection.status='active' AND connection.region=broker.broker_region
		ORDER BY approval.available_at,approval.created_at,approval.approval_id FOR UPDATE OF approval SKIP LOCKED LIMIT 1
	`, now).Scan(&approvalID, &claim.Request.SessionID, &claim.Request.DeploymentID, &claim.Request.TaskID, &claim.Request.ExecutionID, &claim.Request.ManifestDigest, &approvalExpiresAt,
		&claim.Request.SecretRef, &claim.Request.ContextDigest, &claim.Command.ConnectionID, &claim.Region, &claim.BrokerEndpoint, &claim.Command.NodeKeyID, &claim.Command.ExpectedGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		if err = tx.Commit(); err != nil {
			return runtime.ServiceSecretObserveClaim{}, false, err
		}
		return runtime.ServiceSecretObserveClaim{}, false, nil
	}
	if err != nil {
		return claim, false, err
	}
	claim.ApprovalExpiresAt = time.UnixMilli(approvalExpiresAt).UTC()
	claim.LeaseToken = token
	result, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_bootstrap_approvals SET status='observing',lease_owner=$1,lease_token=$2,lease_until=$3,attempts=attempts+1,last_error_code='',revision=revision+1,updated_at=$4 WHERE approval_id=$5 AND status IN('pending','observing') AND lease_until<=$4`, strings.TrimSpace(workerID), token, now+lease.Milliseconds(), now, approvalID)
	if e != nil {
		return claim, false, e
	}
	if e = requireOneAffected(result); e != nil {
		return claim, false, ErrLeaseLost
	}
	claim.Command, err = prepareServiceSecretObserveCommand(ctx, tx, approvalID, claim, now)
	if err != nil {
		return claim, false, err
	}
	if err = tx.Commit(); err != nil {
		return claim, false, err
	}
	return claim, true, nil
}

func prepareServiceSecretObserveCommand(ctx context.Context, tx *sql.Tx, approvalID string, claim runtime.ServiceSecretObserveClaim, now int64) (runtime.ServiceSecretObserveCommand, error) {
	digest, err := serviceSecretObserveRequestDigest(claim.Request)
	if err != nil {
		return runtime.ServiceSecretObserveCommand{}, err
	}
	var c runtime.ServiceSecretObserveCommand
	var issued, expires int64
	var state string
	err = tx.QueryRowContext(ctx, `SELECT command_id,command_attempt,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state FROM p2p_cloud_service_secret_observe_commands WHERE approval_id=$1 AND request_digest=$2 AND state IN('allocated','signed','indeterminate') AND (state='allocated' OR expires_at>$3) ORDER BY command_attempt DESC LIMIT 1 FOR UPDATE`, approvalID, digest, now).Scan(&c.CommandID, &c.Attempt, &c.NodeCounter, &c.PayloadJSON, &c.PayloadSHA256, &c.RequestSHA256, &c.SignedEnvelope, &issued, &expires, &state)
	if err == nil {
		c.ConnectionID = claim.Command.ConnectionID
		c.NodeKeyID = claim.Command.NodeKeyID
		c.ExpectedGeneration = claim.Command.ExpectedGeneration
		c.Action = runtime.ServiceSecretObserveAction
		c.IssuedAt = time.UnixMilli(issued).UTC()
		c.ExpiresAt = time.UnixMilli(expires).UTC()
		return c, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return c, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_observe_commands SET state='expired',updated_at=$1 WHERE approval_id=$2 AND request_digest=$3 AND state IN('signed','indeterminate') AND expires_at<=$1`, now, approvalID, digest); err != nil {
		return c, err
	}
	var attempt int
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_attempt),0)+1 FROM p2p_cloud_service_secret_observe_commands WHERE approval_id=$1 AND request_digest=$2`, approvalID, digest).Scan(&attempt); err != nil {
		return c, err
	}
	var counter int64
	if err = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.Command.ConnectionID).Scan(&counter); err != nil {
		return c, err
	}
	c = runtime.ServiceSecretObserveCommand{CommandID: stableID("cloud_service_secret_observe_command_", approvalID, fmt.Sprint(attempt)), ConnectionID: claim.Command.ConnectionID, NodeKeyID: claim.Command.NodeKeyID, ExpectedGeneration: claim.Command.ExpectedGeneration, NodeCounter: counter, Attempt: attempt, Action: runtime.ServiceSecretObserveAction}
	_, err = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_service_secret_observe_commands(command_id,approval_id,session_id,deployment_id,task_id,execution_id,cloud_connection_id,manifest_digest,secret_ref,context_digest,request_digest,command_attempt,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'allocated',$17,$17)`, c.CommandID, approvalID, claim.Request.SessionID, claim.Request.DeploymentID, claim.Request.TaskID, claim.Request.ExecutionID, c.ConnectionID, claim.Request.ManifestDigest, claim.Request.SecretRef, claim.Request.ContextDigest, digest, c.Attempt, c.Action, c.NodeKeyID, c.ExpectedGeneration, c.NodeCounter, now)
	return c, err
}

func (s *Store) PersistServiceSecretObserveCommand(ctx context.Context, claim runtime.ServiceSecretObserveClaim, signed runtime.SignedServiceSecretObserveCommand) error {
	if signed.EnvelopeJSON == "" || signed.PayloadJSON == "" || signed.PayloadSHA256 == "" || signed.RequestSHA256 == "" || signed.IssuedAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) {
		return errors.New("signed service secret observe command is invalid")
	}
	command, parseErr := broker.ParseServiceSecretObserveCommand([]byte(signed.EnvelopeJSON))
	request, requestErr := command.Request()
	want := broker.ServiceSecretObserveRequest{SessionID: claim.Request.SessionID, DeploymentID: claim.Request.DeploymentID, TaskID: claim.Request.TaskID, ExecutionID: claim.Request.ExecutionID, ManifestDigest: claim.Request.ManifestDigest, SecretRef: claim.Request.SecretRef, ContextDigest: claim.Request.ContextDigest}
	if parseErr != nil || requestErr != nil || request != want || command.CommandID != claim.Command.CommandID || command.ConnectionID != claim.Command.ConnectionID || command.NodeKeyID != claim.Command.NodeKeyID || command.ExpectedGeneration != claim.Command.ExpectedGeneration || command.NodeCounter != claim.Command.NodeCounter || command.PayloadSHA256 != signed.PayloadSHA256 || command.RequestSHA256() != signed.RequestSHA256 {
		return errors.New("signed service secret observe command binding is invalid")
	}
	return s.withServiceSecretObserveClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_observe_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7 WHERE command_id=$8 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256, signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, claim.Command.CommandID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}

func (s *Store) CompleteServiceSecretObserve(ctx context.Context, claim runtime.ServiceSecretObserveClaim, observation runtime.ServiceSecretObservation) error {
	if observation.SessionID != claim.Request.SessionID || observation.Status != "completed" || observation.BindingDigest != claim.Request.ContextDigest || !serviceSecretOpaqueVersion.MatchString(observation.ProviderVersion) || !lowerHex64(observation.UpdatedMarker) {
		return errors.New("service secret observation is invalid")
	}
	return s.withServiceSecretObserveClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		var state string
		if e := tx.QueryRowContext(ctx, `SELECT state FROM p2p_cloud_service_secret_observe_commands WHERE command_id=$1 AND session_id=$2 AND deployment_id=$3 AND task_id=$4 AND execution_id=$5 AND manifest_digest=$6 AND secret_ref=$7 AND context_digest=$8 FOR UPDATE`, claim.Command.CommandID, claim.Request.SessionID, claim.Request.DeploymentID, claim.Request.TaskID, claim.Request.ExecutionID, claim.Request.ManifestDigest, claim.Request.SecretRef, claim.Request.ContextDigest).Scan(&state); e != nil {
			return e
		}
		if state != "signed" && state != "indeterminate" {
			return ErrLeaseLost
		}
		if r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_observe_commands SET state='accepted',last_error_code='',updated_at=$1 WHERE command_id=$2 AND state IN('signed','indeterminate')`, now, claim.Command.CommandID); e != nil {
			return e
		} else if e = requireOneAffected(r); e != nil {
			return e
		}
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_bootstrap_approvals SET status='ready',updated_marker=$1,revision=revision+1,lease_owner='',lease_token='',lease_until=0,last_error_code='',updated_at=$2 WHERE session_id=$3 AND status='observing'`, observation.UpdatedMarker, now, claim.Request.SessionID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}

func (s *Store) DeferServiceSecretObserve(ctx context.Context, claim runtime.ServiceSecretObserveClaim, code string, availableAt time.Time) error {
	return s.settleServiceSecretObserve(ctx, claim, "pending", "indeterminate", safeServiceSecretErrorCode(code, "service_secret_observe_retryable"), availableAt.UTC().UnixMilli())
}
func (s *Store) ExpireServiceSecretObserve(ctx context.Context, claim runtime.ServiceSecretObserveClaim) error {
	return s.settleServiceSecretObserve(ctx, claim, "expired", "expired", "service_secret_observe_expired", 0)
}
func (s *Store) FailServiceSecretObserve(ctx context.Context, claim runtime.ServiceSecretObserveClaim, code string) error {
	return s.settleServiceSecretObserve(ctx, claim, "failed", "failed", safeServiceSecretErrorCode(code, "service_secret_observe_failed"), 0)
}

func (s *Store) settleServiceSecretObserve(ctx context.Context, claim runtime.ServiceSecretObserveClaim, status, commandState, code string, available int64) error {
	return s.withServiceSecretObserveClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		if available < now && status == "pending" {
			available = now
		}
		r, e := tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_observe_commands SET state=$1,last_error_code=$2,updated_at=$3 WHERE command_id=$4 AND state IN('allocated','signed','indeterminate')`, commandState, code, now, claim.Command.CommandID)
		if e != nil {
			return e
		}
		if e = requireOneAffected(r); e != nil {
			return ErrLeaseLost
		}
		r, e = tx.ExecContext(ctx, `UPDATE p2p_cloud_service_secret_bootstrap_approvals SET status=$1,available_at=$2,lease_owner='',lease_token='',lease_until=0,last_error_code=$3,revision=revision+1,updated_at=$4 WHERE session_id=$5 AND status='observing'`, status, available, code, now, claim.Request.SessionID)
		if e != nil {
			return e
		}
		return requireOneAffected(r)
	})
}

func (s *Store) withServiceSecretObserveClaim(ctx context.Context, claim runtime.ServiceSecretObserveClaim, fn func(*sql.Tx, int64) error) (err error) {
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
	var status, token, session, deployment, task, execution, manifest, ref, digest string
	var leaseUntil, expiresAt int64
	err = tx.QueryRowContext(ctx, `SELECT status,lease_token,lease_until,expires_at,session_id,deployment_id,task_id,execution_id,manifest_digest,secret_ref,context_digest FROM p2p_cloud_service_secret_bootstrap_approvals WHERE session_id=$1 FOR UPDATE`, claim.Request.SessionID).Scan(&status, &token, &leaseUntil, &expiresAt, &session, &deployment, &task, &execution, &manifest, &ref, &digest)
	if err != nil {
		return err
	}
	if status != "observing" || token == "" || token != claim.LeaseToken || leaseUntil <= now || expiresAt != claim.ApprovalExpiresAt.UnixMilli() || session != claim.Request.SessionID || deployment != claim.Request.DeploymentID || task != claim.Request.TaskID || execution != claim.Request.ExecutionID || manifest != claim.Request.ManifestDigest || ref != claim.Request.SecretRef || digest != claim.Request.ContextDigest {
		return ErrLeaseLost
	}
	if err = fn(tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

func serviceSecretObserveRequestDigest(request runtime.ServiceSecretObserveRequest) (string, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
func lowerHex64(v string) bool {
	if len(v) != 64 {
		return false
	}
	decoded, e := hex.DecodeString(v)
	return e == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == v
}

func safeServiceSecretErrorCode(value, fallback string) string {
	value = durableErrorCode(value, fallback)
	if strings.HasPrefix(value, "service_secret_") || strings.HasPrefix(value, "invalid_service_secret_") {
		return value
	}
	return fallback
}
