package storepg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

var errRecipeExecutionBindingUnavailable = errors.New("trusted recipe execution binding is unavailable")

// RegisterNextTrustedRecipeExecutionManifest derives one sealed execution
// manifest exclusively from durable, already-approved control-plane facts.
// It accepts no caller-supplied execution scope. The Deployment row is the
// serialization lock, so multiple Orchestrator processes and crash replays
// cannot register two executions for one exclusive Worker.
func (s *Store) RegisterNextTrustedRecipeExecutionManifest(ctx context.Context) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("cloud orchestrator database is unavailable")
	}
	now := s.now().UTC().UnixMilli()
	if now <= 0 {
		return false, errRecipeExecutionBindingUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // commit below owns the successful path

	candidate, found, err := lockRecipeManifestRegistrationCandidate(ctx, tx, now)
	if err != nil || !found {
		return false, err
	}
	plan, approvedPlanHash, storedPlanHash, err := loadApprovedRecipeExecutionPlan(ctx, tx, candidate)
	if err != nil {
		return false, fmt.Errorf("load approved recipe execution plan: %w", err)
	}
	recipe, err := lockExactCurrentCloudRecipe(ctx, tx, candidate.RecipeID, candidate.RecipeRevision, candidate.RecipeDigest)
	if err != nil {
		return false, fmt.Errorf("load current recipe execution recipe: %w", err)
	}
	artifact, artifactDescriptorDigest, err := lockUniqueRecipeExecutionArtifact(ctx, tx, candidate, recipe)
	if err != nil {
		return false, fmt.Errorf("load unique recipe execution artifact: %w", err)
	}
	if err := validateAcceptedDeploymentManifestBinding(ctx, tx, candidate, storedPlanHash); err != nil {
		return false, fmt.Errorf("validate recipe execution deployment binding: %w", err)
	}
	manifest, err := deriveRecipeExecutionManifest(candidate, plan, recipe, approvedPlanHash, artifact, artifactDescriptorDigest)
	if err != nil {
		return false, fmt.Errorf("derive recipe execution manifest: %w", err)
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		return false, errRecipeExecutionBindingUnavailable
	}
	manifestCBOR, err := manifest.CanonicalRecipeExecutionManifestCBOR()
	if err != nil {
		return false, errRecipeExecutionBindingUnavailable
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_execution_manifests (
			execution_id,deployment_id,plan_id,plan_revision,plan_hash,cloud_connection_id,
			manifest_digest,manifest_cbor,manifest_json,status,revision,created_at,updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'registered',1,$10,$10)
	`, manifest.ExecutionID, manifest.DeploymentID, manifest.PlanID, manifest.PlanRevision, manifest.PlanHash,
		candidate.ConnectionID, manifestDigest, manifestCBOR, string(manifestJSON), now); err != nil {
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return false, cloudmodule.ErrRecipeExecutionManifestConflict
		}
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

type recipeManifestRegistrationCandidate struct {
	DeploymentID       string
	PlanID             string
	ConnectionID       string
	InstanceID         string
	RecipeID           string
	RecipeRevision     uint64
	RecipeDigest       string
	PlanRevision       uint64
	ApprovalID         string
	ApprovalRevision   uint64
	ApprovalPlanHash   string
	ApprovalJSON       string
	ApprovalSignature  string
	BrokerManifestHash string
}

func lockRecipeManifestRegistrationCandidate(ctx context.Context, tx *sql.Tx, now int64) (recipeManifestRegistrationCandidate, bool, error) {
	var value recipeManifestRegistrationCandidate
	err := tx.QueryRowContext(ctx, `
		SELECT deployment.deployment_id,deployment.plan_id,deployment.cloud_connection_id,resource.instance_id,
			recipe.recipe_id,recipe.revision,plan.recipe_digest,plan.revision,
			approval.approval_id,approval.plan_revision,approval.plan_hash,approval.approval_json,approval.signature,
			broker.worker_resource_manifest_digest
		FROM p2p_cloud_deployments deployment
		JOIN p2p_cloud_plans plan ON plan.plan_id=deployment.plan_id
		JOIN p2p_cloud_plan_approvals approval ON approval.deployment_id=deployment.deployment_id AND approval.plan_id=plan.plan_id
		JOIN p2p_cloud_deployment_resources resource ON resource.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_connections connection ON connection.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_connection_brokers broker ON broker.cloud_connection_id=deployment.cloud_connection_id
		JOIN p2p_cloud_worker_bootstrap_observations observation ON observation.deployment_id=deployment.deployment_id
		JOIN p2p_cloud_recipes recipe ON recipe.digest=plan.recipe_digest
		WHERE plan.status='approved' AND plan.revision=approval.plan_revision+1
			AND approval.status='approved' AND approval.signature<>'' AND approval.plan_hash=plan.plan_hash
			AND deployment.execution_status='verifying' AND deployment.outcome_status='pending' AND deployment.resource_status='active'
			AND resource.cloud_connection_id=deployment.cloud_connection_id AND resource.resource_status='active' AND resource.instance_id<>''
			AND connection.status='active' AND connection.region=broker.broker_region
			AND broker.worker_resource_manifest_digest<>''
			AND observation.cloud_connection_id=deployment.cloud_connection_id AND observation.instance_id=resource.instance_id
			AND observation.worker_session_state='active' AND observation.worker_lease_expires_at>$1
			AND NOT EXISTS (SELECT 1 FROM p2p_cloud_recipe_execution_manifests manifest WHERE manifest.deployment_id=deployment.deployment_id)
			AND (SELECT COUNT(*) FROM p2p_cloud_recipe_artifacts artifact
				WHERE artifact.recipe_id=recipe.recipe_id AND artifact.recipe_revision=recipe.revision
					AND artifact.recipe_digest=plan.recipe_digest AND artifact.worker_resource_manifest_digest=broker.worker_resource_manifest_digest
					AND artifact.status='verified')=1
			AND (SELECT COUNT(*) FROM p2p_cloud_deployment_commands command
				WHERE command.deployment_id=deployment.deployment_id AND command.cloud_connection_id=deployment.cloud_connection_id
					AND command.plan_id=plan.plan_id AND command.approval_id=approval.approval_id AND command.state='accepted')=1
		ORDER BY deployment.created_at,deployment.deployment_id
		FOR UPDATE OF deployment,plan,approval,resource,connection,broker,observation,recipe SKIP LOCKED
		LIMIT 1
	`, now).Scan(&value.DeploymentID, &value.PlanID, &value.ConnectionID, &value.InstanceID,
		&value.RecipeID, &value.RecipeRevision, &value.RecipeDigest, &value.PlanRevision,
		&value.ApprovalID, &value.ApprovalRevision, &value.ApprovalPlanHash, &value.ApprovalJSON, &value.ApprovalSignature,
		&value.BrokerManifestHash)
	if errors.Is(err, sql.ErrNoRows) {
		return recipeManifestRegistrationCandidate{}, false, nil
	}
	if err != nil {
		return recipeManifestRegistrationCandidate{}, false, err
	}
	return value, true, nil
}

func loadApprovedRecipeExecutionPlan(ctx context.Context, tx *sql.Tx, candidate recipeManifestRegistrationCandidate) (cloudcontracts.PlanV1, string, string, error) {
	var displayJSON, versionPlanHash string
	var canonical []byte
	var versionRevision uint64
	err := tx.QueryRowContext(ctx, `
		SELECT display_json,canonical_cbor,plan_hash,revision
		FROM p2p_cloud_plan_versions
		WHERE plan_id=$1 AND revision=$2
		FOR UPDATE
	`, candidate.PlanID, candidate.ApprovalRevision).Scan(&displayJSON, &canonical, &versionPlanHash, &versionRevision)
	if err != nil {
		return cloudcontracts.PlanV1{}, "", "", errRecipeExecutionBindingUnavailable
	}
	var stored cloudcontracts.PlanV1
	if decodeRecipeManifestJSON(displayJSON, &stored) != nil || stored.Validate() != nil || stored.Status != cloudcontracts.PlanReadyForConfirmation {
		return cloudcontracts.PlanV1{}, "", "", fmt.Errorf("plan contract: %w", errRecipeExecutionBindingUnavailable)
	}
	if stored.PlanID != candidate.PlanID || stored.CloudConnectionID != candidate.ConnectionID {
		return cloudcontracts.PlanV1{}, "", "", fmt.Errorf("plan deployment identity: %w", errRecipeExecutionBindingUnavailable)
	}
	if stored.Revision != candidate.ApprovalRevision || versionRevision != candidate.ApprovalRevision {
		return cloudcontracts.PlanV1{}, "", "", fmt.Errorf("plan revision identity: %w", errRecipeExecutionBindingUnavailable)
	}
	if stored.Recipe.RecipeID != candidate.RecipeID || stored.Recipe.Digest != candidate.RecipeDigest {
		return cloudcontracts.PlanV1{}, "", "", fmt.Errorf("plan recipe identity: %w", errRecipeExecutionBindingUnavailable)
	}
	computedCanonical, canonicalErr := stored.CanonicalPlanCBOR()
	storedHash, hashErr := stored.Hash()
	if canonicalErr != nil || hashErr != nil || !bytes.Equal(computedCanonical, canonical) {
		return cloudcontracts.PlanV1{}, "", "", fmt.Errorf("plan canonical form: %w", errRecipeExecutionBindingUnavailable)
	}
	if storedHash != versionPlanHash || storedHash != candidate.ApprovalPlanHash || candidate.PlanRevision != candidate.ApprovalRevision+1 {
		return cloudcontracts.PlanV1{}, "", "", fmt.Errorf("plan approval hash: %w", errRecipeExecutionBindingUnavailable)
	}
	stored.Status = cloudcontracts.PlanApproved
	stored.Revision = candidate.PlanRevision
	if stored.Validate() != nil {
		return cloudcontracts.PlanV1{}, "", "", errRecipeExecutionBindingUnavailable
	}
	approvedHash, err := stored.Hash()
	if err != nil {
		return cloudcontracts.PlanV1{}, "", "", errRecipeExecutionBindingUnavailable
	}
	return stored, approvedHash, storedHash, nil
}

func lockUniqueRecipeExecutionArtifact(ctx context.Context, tx *sql.Tx, candidate recipeManifestRegistrationCandidate, recipe cloudcontracts.RecipeV1) (cloudcontracts.CompiledRecipeArtifactV1, string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT artifact_digest,descriptor_digest,canonical_cbor,descriptor_json
		FROM p2p_cloud_recipe_artifacts
		WHERE recipe_id=$1 AND recipe_revision=$2 AND recipe_digest=$3
			AND worker_resource_manifest_digest=$4 AND status='verified'
		FOR UPDATE
	`, candidate.RecipeID, candidate.RecipeRevision, candidate.RecipeDigest, candidate.BrokerManifestHash)
	if err != nil {
		return cloudcontracts.CompiledRecipeArtifactV1{}, "", err
	}
	defer rows.Close()
	type storedArtifact struct {
		artifactDigest   string
		descriptorDigest string
		canonical        []byte
		descriptorJSON   string
	}
	values := make([]storedArtifact, 0, 2)
	for rows.Next() {
		var value storedArtifact
		if err := rows.Scan(&value.artifactDigest, &value.descriptorDigest, &value.canonical, &value.descriptorJSON); err != nil {
			return cloudcontracts.CompiledRecipeArtifactV1{}, "", err
		}
		values = append(values, value)
		if len(values) > 1 {
			return cloudcontracts.CompiledRecipeArtifactV1{}, "", errRecipeExecutionBindingUnavailable
		}
	}
	if err := rows.Err(); err != nil {
		return cloudcontracts.CompiledRecipeArtifactV1{}, "", err
	}
	if len(values) != 1 {
		return cloudcontracts.CompiledRecipeArtifactV1{}, "", errRecipeExecutionBindingUnavailable
	}
	artifact, err := cloudcontracts.ParseCompiledRecipeArtifactV1([]byte(values[0].descriptorJSON))
	if err != nil || validateCompiledArtifactAgainstRecipe(artifact, recipe) != nil {
		return cloudcontracts.CompiledRecipeArtifactV1{}, "", errRecipeExecutionBindingUnavailable
	}
	canonical, canonicalErr := artifact.CanonicalCompiledRecipeArtifactCBOR()
	descriptorDigest, digestErr := artifact.Digest()
	if canonicalErr != nil || digestErr != nil || artifact.ArtifactDigest != values[0].artifactDigest ||
		descriptorDigest != values[0].descriptorDigest || !bytes.Equal(canonical, values[0].canonical) ||
		artifact.RecipeID != candidate.RecipeID || artifact.RecipeRevision != candidate.RecipeRevision ||
		artifact.RecipeDigest != candidate.RecipeDigest || artifact.WorkerResourceManifestDigest != candidate.BrokerManifestHash {
		return cloudcontracts.CompiledRecipeArtifactV1{}, "", errRecipeExecutionBindingUnavailable
	}
	return artifact, descriptorDigest, nil
}

func validateAcceptedDeploymentManifestBinding(ctx context.Context, tx *sql.Tx, candidate recipeManifestRegistrationCandidate, storedPlanHash string) error {
	var envelope, requestDigest, approvalID, connectionID, planID string
	var planRevision uint64
	err := tx.QueryRowContext(ctx, `
		SELECT signed_envelope_json,request_digest,approval_id,cloud_connection_id,plan_id,plan_revision
		FROM p2p_cloud_deployment_commands
		WHERE deployment_id=$1 AND cloud_connection_id=$2 AND plan_id=$3 AND approval_id=$4 AND state='accepted'
		FOR UPDATE
	`, candidate.DeploymentID, candidate.ConnectionID, candidate.PlanID, candidate.ApprovalID).Scan(
		&envelope, &requestDigest, &approvalID, &connectionID, &planID, &planRevision,
	)
	if err != nil || approvalID != candidate.ApprovalID || connectionID != candidate.ConnectionID || planID != candidate.PlanID || planRevision != candidate.ApprovalRevision {
		return errRecipeExecutionBindingUnavailable
	}
	command, err := broker.ParseDeploymentCommand([]byte(envelope))
	if err != nil || command.ConnectionID != candidate.ConnectionID {
		return errRecipeExecutionBindingUnavailable
	}
	request, err := command.DeploymentRequest()
	if err != nil || request.DeploymentID != candidate.DeploymentID || request.PlanHash != storedPlanHash ||
		request.PlanRevision != candidate.ApprovalRevision || request.ResourceManifestDigest != candidate.BrokerManifestHash {
		return errRecipeExecutionBindingUnavailable
	}
	runtimeRequest := runtime.DeploymentCreateRequest{
		Schema: request.Schema, DeploymentID: request.DeploymentID, ConnectionGeneration: request.ConnectionGeneration,
		PlanHash: request.PlanHash, PlanRevision: request.PlanRevision, QuoteID: request.QuoteID, QuoteDigest: request.QuoteDigest,
		CandidateID: request.CandidateID, ResourceManifestDigest: request.ResourceManifestDigest,
		WorkerArtifact: runtime.WorkerArtifactReferenceV1{Kind: request.WorkerArtifact.Kind, AMIID: request.WorkerArtifact.AMIID},
		Network:        runtime.DeploymentNetworkReference{VPCID: request.Network.VPCID, SubnetID: request.Network.SubnetID, AvailabilityZone: request.Network.AvailabilityZone},
	}
	computedRequestDigest, err := runtimeRequest.Digest()
	if err != nil || computedRequestDigest != requestDigest {
		return errRecipeExecutionBindingUnavailable
	}
	var approval cloudcontracts.ApprovalV1
	if decodeRecipeManifestJSON(candidate.ApprovalJSON, &approval) != nil {
		return errRecipeExecutionBindingUnavailable
	}
	approval.Signature = candidate.ApprovalSignature
	if approval.Validate() != nil || !reflect.DeepEqual(approval, command.ApprovalProof) {
		return errRecipeExecutionBindingUnavailable
	}
	return nil
}

func deriveRecipeExecutionManifest(candidate recipeManifestRegistrationCandidate, plan cloudcontracts.PlanV1, recipe cloudcontracts.RecipeV1, approvedPlanHash string, artifact cloudcontracts.CompiledRecipeArtifactV1, artifactDescriptorDigest string) (cloudcontracts.RecipeExecutionManifestV1, error) {
	if !recipeSlotRequirementsEqual(recipe, artifact) {
		return cloudcontracts.RecipeExecutionManifestV1{}, errRecipeExecutionBindingUnavailable
	}
	volumeSlots, err := cloudcontracts.VolumeSlotsForRecipe(plan.PlanID, artifact.VolumeSlots)
	if err != nil {
		return cloudcontracts.RecipeExecutionManifestV1{}, errRecipeExecutionBindingUnavailable
	}
	dataSlots, err := cloudcontracts.DataSlotsForRecipe(plan.PlanID, artifact.DataSlots)
	if err != nil {
		return cloudcontracts.RecipeExecutionManifestV1{}, errRecipeExecutionBindingUnavailable
	}
	var install cloudcontracts.CompiledRecipeActionV1
	installCount := 0
	for _, action := range artifact.Actions {
		if action.Kind == cloudcontracts.CompiledRecipeActionInstall {
			install, installCount = action, installCount+1
		}
	}
	if installCount != 1 {
		return cloudcontracts.RecipeExecutionManifestV1{}, errRecipeExecutionBindingUnavailable
	}
	secretRequirements := append([]cloudcontracts.RecipeSecretSlotRequirementV1(nil), artifact.SecretSlots...)
	sort.Slice(secretRequirements, func(i, j int) bool { return secretRequirements[i].SlotID < secretRequirements[j].SlotID })
	secretSlots := make([]cloudcontracts.SecretSlotV1, 0, len(secretRequirements))
	for _, requirement := range secretRequirements {
		reference, err := cloudcontracts.SecretReferenceForRecipeSlot(plan.PlanID, requirement)
		if err != nil {
			return cloudcontracts.RecipeExecutionManifestV1{}, errRecipeExecutionBindingUnavailable
		}
		secretSlots = append(secretSlots, cloudcontracts.SecretSlotV1{SlotID: requirement.SlotID, SecretRef: reference.SecretRef})
	}
	manifest := cloudcontracts.RecipeExecutionManifestV1{
		SchemaVersion: cloudcontracts.RecipeExecutionManifestV1Schema,
		ExecutionID: stableID("cloud_recipe_execution_", candidate.DeploymentID, approvedPlanHash, artifactDescriptorDigest,
			artifact.ArtifactDigest, artifact.WorkerResourceManifestDigest, install.ActionID),
		DeploymentID: candidate.DeploymentID, PlanID: candidate.PlanID, PlanHash: approvedPlanHash, PlanRevision: candidate.PlanRevision,
		RecipeDigest: artifact.RecipeDigest, WorkerResourceManifestDigest: artifact.WorkerResourceManifestDigest,
		ArtifactDigest: artifact.ArtifactDigest, ActionID: install.ActionID, RootRequired: install.RootRequired,
		TimeoutSeconds: install.TimeoutSeconds, CheckpointSequence: append([]string(nil), install.CheckpointSequence...), SemanticReadiness: artifact.SemanticReadiness,
		VolumeSlots: volumeSlots, DataSlots: dataSlots, SecretSlots: secretSlots,
	}
	if manifest.ValidateForPlanAndRecipe(plan, recipe) != nil {
		return cloudcontracts.RecipeExecutionManifestV1{}, errRecipeExecutionBindingUnavailable
	}
	return manifest, nil
}

func decodeRecipeManifestJSON(raw string, target any) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("trusted recipe execution JSON has trailing data")
	}
	return nil
}
