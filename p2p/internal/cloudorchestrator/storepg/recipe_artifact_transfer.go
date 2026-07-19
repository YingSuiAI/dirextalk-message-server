package storepg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var _ runtime.RecipeArtifactTransferStore = (*Store)(nil)

func (store *Store) LoadOrCreateRecipeArtifactTransfer(ctx context.Context, claim runtime.RecipeInstallClaim, binding runtime.RecipeArtifactTransferBinding) (transfer runtime.RecipeArtifactTransfer, err error) {
	if claim.Phase != runtime.RecipeInstallPhaseIssue || binding.Validate() != nil || binding.ExecutionID != claim.ExecutionID || binding.DeploymentID != claim.DeploymentID || binding.TaskID != claim.TaskID ||
		binding.ManifestDigest != claim.ManifestDigest || binding.RecipeDigest != claim.Manifest.RecipeDigest || binding.ArtifactDigest != claim.Manifest.ArtifactDigest {
		return transfer, ErrLeaseLost
	}
	err = store.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		stored, found, loadErr := lockRecipeArtifactTransfer(ctx, tx, claim.ExecutionID)
		if loadErr != nil {
			return loadErr
		}
		if !found {
			_, loadErr = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_artifact_transfers(
				execution_id,deployment_id,task_id,cloud_connection_id,recipe_digest,artifact_digest,manifest_digest,archive_sha256,size_bytes,media_type,state,created_at,updated_at
			)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11,$11)`, binding.ExecutionID, binding.DeploymentID, binding.TaskID, claim.ConnectionID,
				binding.RecipeDigest, binding.ArtifactDigest, binding.ManifestDigest, binding.ArchiveSHA256, binding.SizeBytes, binding.MediaType, now)
			if loadErr != nil {
				return loadErr
			}
			stored = runtime.RecipeArtifactTransfer{Binding: binding, ConnectionID: claim.ConnectionID}
		} else if stored.Binding != binding || stored.ConnectionID != claim.ConnectionID {
			return errors.New("recipe artifact transfer binding conflict")
		}
		if stored.Phase == runtime.RecipeArtifactTransferPhaseVerified {
			transfer = stored
			return nil
		}
		phase := runtime.RecipeArtifactTransferPhasePrepare
		var requestDigest string
		request := runtime.RecipeArtifactPutPrepareRequest{Schema: runtime.RecipeArtifactPutPrepareSchema, RecipeArtifactTransferBinding: binding}
		requestDigest, loadErr = request.Digest()
		if stored.VersionID != "" {
			phase = runtime.RecipeArtifactTransferPhaseComplete
			complete := runtime.RecipeArtifactPutCompleteRequest{Schema: runtime.RecipeArtifactPutCompleteSchema, RecipeArtifactTransferBinding: binding, VersionID: stored.VersionID}
			requestDigest, loadErr = complete.Digest()
		}
		if loadErr != nil {
			return loadErr
		}
		command, found, loadErr := lockRecipeArtifactTransferCommand(ctx, tx, claim, phase)
		if loadErr != nil {
			return loadErr
		}
		if found {
			if command.RequestDigest != requestDigest || command.Action != runtime.RecipeArtifactPutAction {
				return errors.New("recipe artifact transfer command binding conflict")
			}
			if command.State == "expired" || command.State == "failed" {
				return runtime.RecipeArtifactCommandExpired(errors.New("recipe artifact transfer command is terminal"))
			}
			if command.State == "accepted" {
				return errors.New("recipe artifact transfer state conflicts with accepted command")
			}
		} else {
			var counter int64
			if loadErr = tx.QueryRowContext(ctx, `UPDATE p2p_cloud_connection_brokers SET next_node_counter=next_node_counter+1,updated_at=$1 WHERE cloud_connection_id=$2 RETURNING next_node_counter`, now, claim.ConnectionID).Scan(&counter); loadErr != nil {
				return loadErr
			}
			command = runtime.RecipeArtifactTransferCommand{
				CommandID: stableID("cloud_recipe_artifact_command_", claim.ExecutionID, phase), ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID,
				TaskID: claim.TaskID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration,
				NodeCounter: counter, Phase: phase, Action: runtime.RecipeArtifactPutAction, RequestDigest: requestDigest, State: "allocated",
			}
			_, loadErr = tx.ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_artifact_commands(
				command_id,execution_id,deployment_id,task_id,cloud_connection_id,phase,request_digest,action,node_key_id,expected_generation,node_counter,state,created_at,updated_at
			)VALUES($1,$2,$3,$4,$5,$6,$7,'artifact.put',$8,$9,$10,'allocated',$11,$11)`, command.CommandID, command.ExecutionID, command.DeploymentID,
				command.TaskID, command.ConnectionID, command.Phase, command.RequestDigest, command.NodeKeyID, command.ExpectedGeneration, command.NodeCounter, now)
			if loadErr != nil {
				return loadErr
			}
		}
		stored.Phase = phase
		stored.Command = command
		transfer = stored
		return nil
	})
	return transfer, err
}

func (store *Store) PersistRecipeArtifactTransferCommand(ctx context.Context, claim runtime.RecipeInstallClaim, transfer runtime.RecipeArtifactTransfer, signed runtime.SignedRecipeArtifactTransferCommand) error {
	return store.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_artifact_commands SET canonical_payload_json=$1,payload_sha256=$2,request_sha256=$3,signed_envelope_json=$4,issued_at=$5,expires_at=$6,state='signed',updated_at=$7
			WHERE command_id=$8 AND execution_id=$9 AND phase=$10 AND request_digest=$11 AND state='allocated'`, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256,
			signed.EnvelopeJSON, signed.IssuedAt.UnixMilli(), signed.ExpiresAt.UnixMilli(), now, transfer.Command.CommandID, claim.ExecutionID, transfer.Phase, transfer.Command.RequestDigest)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func (store *Store) RecordRecipeArtifactVersion(ctx context.Context, claim runtime.RecipeInstallClaim, transfer runtime.RecipeArtifactTransfer, versionID string) error {
	if transfer.Phase != runtime.RecipeArtifactTransferPhasePrepare || runtime.ValidateRecipeArtifactVersionID(versionID) != nil {
		return ErrLeaseLost
	}
	return store.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		stored, found, err := lockRecipeArtifactTransfer(ctx, tx, claim.ExecutionID)
		if err != nil || !found || stored.Binding != transfer.Binding || stored.ConnectionID != claim.ConnectionID {
			if err != nil {
				return err
			}
			return ErrLeaseLost
		}
		if stored.VersionID != "" {
			if stored.VersionID == versionID && (stored.Phase == runtime.RecipeArtifactTransferPhaseComplete || stored.Phase == runtime.RecipeArtifactTransferPhaseVerified) {
				return nil
			}
			return errors.New("recipe artifact version conflict")
		}
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_artifact_commands SET state='accepted',updated_at=$1 WHERE command_id=$2 AND execution_id=$3 AND phase='prepare' AND state IN('signed','indeterminate')`, now, transfer.Command.CommandID, claim.ExecutionID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(result); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_artifact_transfers SET version_id=$1,state='uploaded',updated_at=$2 WHERE execution_id=$3 AND state='pending' AND version_id=''`, versionID, now, claim.ExecutionID)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func (store *Store) CommitRecipeArtifactTransfer(ctx context.Context, claim runtime.RecipeInstallClaim, transfer runtime.RecipeArtifactTransfer) error {
	if transfer.Phase != runtime.RecipeArtifactTransferPhaseComplete || runtime.ValidateRecipeArtifactVersionID(transfer.VersionID) != nil {
		return ErrLeaseLost
	}
	return store.withRecipeInstallClaim(ctx, claim, func(tx *sql.Tx, now int64) error {
		stored, found, err := lockRecipeArtifactTransfer(ctx, tx, claim.ExecutionID)
		if err != nil || !found || stored.Binding != transfer.Binding || stored.ConnectionID != claim.ConnectionID || stored.VersionID != transfer.VersionID {
			if err != nil {
				return err
			}
			return ErrLeaseLost
		}
		if stored.Phase == runtime.RecipeArtifactTransferPhaseVerified {
			return nil
		}
		result, err := tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_artifact_commands SET state='accepted',updated_at=$1 WHERE command_id=$2 AND execution_id=$3 AND phase='complete' AND state IN('signed','indeterminate')`, now, transfer.Command.CommandID, claim.ExecutionID)
		if err != nil {
			return err
		}
		if err = requireOneAffected(result); err != nil {
			return err
		}
		result, err = tx.ExecContext(ctx, `UPDATE p2p_cloud_recipe_artifact_transfers SET state='verified',updated_at=$1 WHERE execution_id=$2 AND state='uploaded' AND version_id=$3`, now, claim.ExecutionID, transfer.VersionID)
		if err != nil {
			return err
		}
		return requireOneAffected(result)
	})
}

func lockRecipeArtifactTransfer(ctx context.Context, tx *sql.Tx, executionID string) (runtime.RecipeArtifactTransfer, bool, error) {
	var transfer runtime.RecipeArtifactTransfer
	var state string
	err := tx.QueryRowContext(ctx, `SELECT deployment_id,task_id,cloud_connection_id,recipe_digest,artifact_digest,manifest_digest,archive_sha256,size_bytes,media_type,version_id,state
		FROM p2p_cloud_recipe_artifact_transfers WHERE execution_id=$1 FOR UPDATE`, executionID).Scan(&transfer.Binding.DeploymentID, &transfer.Binding.TaskID, &transfer.ConnectionID,
		&transfer.Binding.RecipeDigest, &transfer.Binding.ArtifactDigest, &transfer.Binding.ManifestDigest, &transfer.Binding.ArchiveSHA256, &transfer.Binding.SizeBytes, &transfer.Binding.MediaType, &transfer.VersionID, &state)
	transfer.Binding.ExecutionID = executionID
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.RecipeArtifactTransfer{}, false, nil
	}
	if err != nil {
		return runtime.RecipeArtifactTransfer{}, false, err
	}
	if state == "verified" {
		transfer.Phase = runtime.RecipeArtifactTransferPhaseVerified
	} else if transfer.VersionID != "" {
		transfer.Phase = runtime.RecipeArtifactTransferPhaseComplete
	} else {
		transfer.Phase = runtime.RecipeArtifactTransferPhasePrepare
	}
	return transfer, true, nil
}

func lockRecipeArtifactTransferCommand(ctx context.Context, tx *sql.Tx, claim runtime.RecipeInstallClaim, phase string) (runtime.RecipeArtifactTransferCommand, bool, error) {
	var command runtime.RecipeArtifactTransferCommand
	var issuedAt, expiresAt int64
	err := tx.QueryRowContext(ctx, `SELECT command_id,request_digest,action,node_key_id,expected_generation,node_counter,canonical_payload_json,payload_sha256,request_sha256,signed_envelope_json,issued_at,expires_at,state
		FROM p2p_cloud_recipe_artifact_commands WHERE execution_id=$1 AND phase=$2 FOR UPDATE`, claim.ExecutionID, phase).Scan(&command.CommandID, &command.RequestDigest, &command.Action,
		&command.NodeKeyID, &command.ExpectedGeneration, &command.NodeCounter, &command.PayloadJSON, &command.PayloadSHA256, &command.RequestSHA256, &command.SignedEnvelope, &issuedAt, &expiresAt, &command.State)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.RecipeArtifactTransferCommand{}, false, nil
	}
	if err != nil {
		return runtime.RecipeArtifactTransferCommand{}, false, err
	}
	command.ExecutionID, command.DeploymentID, command.TaskID, command.ConnectionID, command.Phase = claim.ExecutionID, claim.DeploymentID, claim.TaskID, claim.ConnectionID, phase
	command.IssuedAt, command.ExpiresAt = time.UnixMilli(issuedAt).UTC(), time.UnixMilli(expiresAt).UTC()
	if command.NodeKeyID != claim.NodeKeyID || command.ExpectedGeneration != claim.ExpectedGeneration {
		return runtime.RecipeArtifactTransferCommand{}, false, fmt.Errorf("recipe artifact command connection binding conflict")
	}
	return command, true, nil
}
