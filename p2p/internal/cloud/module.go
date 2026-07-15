package cloud

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/google/uuid"
)

const (
	actionBootstrap                                     = "cloud.bootstrap"
	actionConnectionsList                               = "cloud.connections.list"
	actionConnectionsGet                                = "cloud.connections.get"
	actionPlansList                                     = "cloud.plans.list"
	actionPlansGet                                      = "cloud.plans.get"
	actionDeploymentsList                               = "cloud.deployments.list"
	actionDeploymentsGet                                = "cloud.deployments.get"
	actionServicesList                                  = "cloud.services.list"
	actionServicesGet                                   = "cloud.services.get"
	actionRecipesList                                   = "cloud.recipes.list"
	actionRecipesGet                                    = "cloud.recipes.get"
	actionEventsList                                    = "cloud.events.list"
	actionGoalsCreate                                   = "cloud.goals.create"
	actionConnectionsRolePlan                           = "cloud.connections.role_plan"
	actionConnectionsRegistrationComplete               = "cloud.connections.registration.complete"
	actionPlansConfirmationPrepare                      = "cloud.plans.confirmation.prepare"
	actionPlansApprove                                  = "cloud.plans.approve"
	actionDeploymentsRecipeExecutionConfirmationPrepare = "cloud.deployments.recipe_execution.confirmation.prepare"
	actionDeploymentsRecipeExecutionApprove             = "cloud.deployments.recipe_execution.approve"
	actionDeploymentsPairingResume                      = "cloud.deployments.pairing.resume"
	actionServicesOperationPlan                         = "cloud.services.operation.plan"
	actionServicesOperationApprove                      = "cloud.services.operation.approve"
	actionServicesDestroyPlan                           = "cloud.services.destroy.plan"
	actionServicesDestroyApprove                        = "cloud.services.destroy.approve"
	cloudUnavailableCode                                = "cloud_orchestrator_unavailable"
	cloudIdempotencyInvalidCode                         = "cloud_idempotency_key_invalid"
	cloudGoalInvalidCode                                = "cloud_goal_invalid"
	cloudConnectionIDInvalidCode                        = "cloud_connection_id_invalid"
	cloudConnectionRequiredCode                         = "cloud_connection_required"
	cloudInvalidParamsCode                              = "cloud_invalid_params"
	cloudIdempotencyConflictCode                        = "cloud_idempotency_conflict"
	cloudConnectionStackUnavailableCode                 = "cloud_connection_stack_unavailable"
	cloudConnectionBootstrapInvalidCode                 = "cloud_connection_bootstrap_invalid"
	cloudConnectionBootstrapExpiredCode                 = "cloud_connection_bootstrap_expired"
	cloudConnectionBootstrapConflictCode                = "cloud_connection_bootstrap_conflict"
	cloudPlanConfirmationInvalidCode                    = "cloud_plan_confirmation_invalid"
	cloudPlanConfirmationConflictCode                   = "cloud_plan_confirmation_conflict"
	cloudQuoteExpiredCode                               = "cloud_quote_expired"
	cloudPlanApprovalInvalidCode                        = "cloud_plan_approval_invalid"
	cloudPlanApprovalConflictCode                       = "cloud_plan_approval_conflict"
	cloudPlanApprovalExpiredCode                        = "cloud_plan_approval_expired"
	cloudPlanApprovalSignatureCode                      = "cloud_plan_approval_signature_invalid"
	cloudRecipeExecutionConfirmationInvalidCode         = "cloud_recipe_execution_confirmation_invalid"
	cloudRecipeExecutionConfirmationConflictCode        = "cloud_recipe_execution_confirmation_conflict"
	cloudRecipeExecutionApprovalExpiredCode             = "cloud_recipe_execution_approval_expired"
	cloudRecipeExecutionApprovalSignatureCode           = "cloud_recipe_execution_approval_signature_invalid"
	cloudServiceDestroyConfirmationInvalidCode          = "cloud_service_destroy_confirmation_invalid"
	cloudServiceDestroyConfirmationConflictCode         = "cloud_service_destroy_confirmation_conflict"
	cloudServiceDestroyApprovalExpiredCode              = "cloud_service_destroy_approval_expired"
	cloudServiceDestroyApprovalSignatureCode            = "cloud_service_destroy_approval_signature_invalid"
	cloudServiceOperationConfirmationInvalidCode        = "cloud_service_operation_confirmation_invalid"
	cloudServiceOperationConfirmationConflictCode       = "cloud_service_operation_confirmation_conflict"
	cloudServiceOperationApprovalExpiredCode            = "cloud_service_operation_approval_expired"
	cloudServiceOperationApprovalSignatureCode          = "cloud_service_operation_approval_signature_invalid"
)

type Config struct {
	OwnerMXID       func() string
	Now             func() time.Time
	NewID           func(kind string) string
	Publish         func(context.Context, string, string, map[string]any) error
	ConnectionStack ConnectionStackConfig
}

type Module struct {
	store Store
	cfg   Config
}

func New(store Store, cfg Config) *Module {
	return &Module{store: store, cfg: cfg}
}

func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionBootstrap:                       m.bootstrap,
		actionConnectionsList:                 m.connectionsList,
		actionConnectionsGet:                  m.connectionsGet,
		actionPlansList:                       m.plansList,
		actionPlansGet:                        m.plansGet,
		actionDeploymentsList:                 m.deploymentsList,
		actionDeploymentsGet:                  m.deploymentsGet,
		actionServicesList:                    m.servicesList,
		actionServicesGet:                     m.servicesGet,
		actionRecipesList:                     m.recipesList,
		actionRecipesGet:                      m.recipesGet,
		actionEventsList:                      m.eventsList,
		actionGoalsCreate:                     m.createGoal,
		actionConnectionsRolePlan:             m.createConnectionRolePlan,
		actionConnectionsRegistrationComplete: m.completeConnectionRegistration,
		actionPlansConfirmationPrepare:        m.preparePlanConfirmation,
		actionPlansApprove:                    m.approvePlan,
		actionDeploymentsRecipeExecutionConfirmationPrepare: m.prepareRecipeExecutionConfirmation,
		actionDeploymentsRecipeExecutionApprove:             m.approveRecipeExecution,
		actionDeploymentsPairingResume:                      m.unavailableWrite,
		actionServicesOperationPlan:                         m.prepareServiceOperation,
		actionServicesOperationApprove:                      m.approveServiceOperation,
		actionServicesDestroyPlan:                           m.prepareServiceDestroy,
		actionServicesDestroyApprove:                        m.approveServiceDestroy,
	}
}

// createConnectionRolePlan creates a short-lived, immutable CloudFormation
// handoff. It does not call AWS, receive AWS credentials, contact a Broker, or
// create a public Connection record. The device key is public-key material
// only; the Flutter private approval key never crosses this boundary.
func (m *Module) createConnectionRolePlan(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "provider", "region", "device_approval_key_id", "device_approval_public_key_spki_base64", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	stackConfig := m.cfg.ConnectionStack
	if err := ValidateConnectionStackConfig(stackConfig); err != nil {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudConnectionStackUnavailableCode, "cloud connection stack is not configured")
	}
	values := actionbase.Params(params)
	provider := values.String("provider")
	region := values.String("region")
	deviceKeyID := values.String("device_approval_key_id")
	devicePublicKey := values.String("device_approval_public_key_spki_base64")
	idempotencyKey := values.String("idempotency_key")
	if provider != "aws" || !cloudRegionPattern.MatchString(region) || !cloudKeyIDPattern.MatchString(deviceKeyID) ||
		ContainsSensitiveGoalMaterial(deviceKeyID) || validateEd25519SPKIBase64(devicePublicKey) != nil || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud connection role plan is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UnixMilli()
	bootstrapID := m.newID("connection_bootstrap")
	connectionID := m.newID("connection")
	bootstrap := ConnectionBootstrap{
		BootstrapID: bootstrapID, OwnerMXID: ownerMXID, ConnectionID: connectionID, Provider: provider,
		RequestedRegion: region, TemplateURL: stackConfig.TemplateURL, TemplateDigest: stackConfig.TemplateDigest, SourceTreeDigest: stackConfig.SourceTreeDigest,
		StackName: connectionStackName(connectionID), NodeKeyID: stackConfig.NodeKeyID,
		NodePublicKeySPKIBase64: stackConfig.NodePublicKeySPKIBase64, DeviceApprovalKeyID: deviceKeyID,
		DeviceApprovalPublicKeySPKIBase64: devicePublicKey, Status: ConnectionBootstrapAwaitingStack,
		Revision: 1, IdempotencyHash: digest(idempotencyKey),
		RequestDigest: connectionBootstrapRequestDigest(provider, region, deviceKeyID, devicePublicKey),
		ExpiresAt:     now + stackConfig.RolePlanTTL.Milliseconds(), CreatedAt: now, UpdatedAt: now,
	}
	if err := validateConnectionBootstrap(bootstrap); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud connection role plan is invalid")
	}
	created, err := m.store.CreateCloudConnectionBootstrap(ctx, CreateConnectionBootstrapRequest{Bootstrap: bootstrap})
	if err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			return nil, actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud connection role plan")
		}
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"role_plan": created.Bootstrap.RolePlan()}, nil
}

// completeConnectionRegistration records a user-returned Stack output as a
// pending verification request. It intentionally cannot activate a connection
// or directly request the candidate endpoint: the mounted-key Orchestrator
// must submit the fixed signed Broker attestation first.
func (m *Module) completeConnectionRegistration(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "bootstrap_id", "expected_revision", "idempotency_key", "broker_command_url", "stack_arn"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	bootstrapID := values.String("bootstrap_id")
	idempotencyKey := values.String("idempotency_key")
	brokerCommandURL := values.String("broker_command_url")
	stackARN := values.String("stack_arn")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(bootstrapID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) ||
		ContainsSensitiveGoalMaterial(brokerCommandURL) || ContainsSensitiveGoalMaterial(stackARN) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud connection registration is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	// The Store repeats these facts while holding the bootstrap row lock. This
	// initial validation keeps malformed client values out of durable state.
	// Region is deliberately checked by Store after it reads the immutable plan.
	if len(brokerCommandURL) == 0 || len(stackARN) == 0 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud connection registration is invalid")
	}
	now := m.now().UnixMilli()
	job := Job{
		JobID: m.newID("connection_registration"), Kind: "connection_registration", Execution: "queued", Outcome: "pending",
		Checkpoint: "connection_verification_queued", Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	event := Event{
		EventID: m.newID("event"), Type: "cloud.job.changed", AggregateType: "job", AggregateID: job.JobID,
		Revision: job.Revision, SummaryJSON: mustJSON(jobPayload(job)), CreatedAt: now,
	}
	payload, marshalErr := json.Marshal(map[string]string{"bootstrap_id": bootstrapID})
	if marshalErr != nil {
		return nil, actionbase.InternalError(marshalErr)
	}
	completed, err := m.store.CompleteCloudConnectionBootstrap(ctx, CompleteConnectionBootstrapRequest{
		OwnerMXID: ownerMXID, BootstrapID: bootstrapID, ExpectedRevision: expectedRevision,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: connectionBootstrapCompletionDigest(bootstrapID, brokerCommandURL, stackARN),
		BrokerCommandURL: brokerCommandURL, StackARN: stackARN, Job: job, Event: event,
		Outbox: OutboxEntry{OutboxID: m.newID("outbox"), Kind: OutboxKindConnectionRegistrationRequested, AggregateType: "connection_bootstrap", AggregateID: bootstrapID, PayloadJSON: string(payload), CreatedAt: now},
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrIdempotencyConflict):
			return nil, actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud connection registration")
		case errors.Is(err, ErrConnectionBootstrapExpired):
			return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapExpiredCode, "cloud connection role plan has expired")
		case errors.Is(err, ErrConnectionBootstrapConflict):
			return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection registration revision conflicts with the current role plan")
		case errors.Is(err, ErrConnectionBootstrapInputInvalid):
			return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud connection registration is invalid")
		case errors.Is(err, ErrConnectionBootstrapInvalid):
			return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapInvalidCode, "cloud connection registration is not in a completable state")
		default:
			return nil, actionbase.InternalError(err)
		}
	}
	if completed.Created {
		m.publish(ctx, event.Type, event.EventID, jobPayload(job))
	}
	return map[string]any{"registration": completed.Bootstrap.Registration()}, nil
}

// preparePlanConfirmation materializes one immutable PlanV1 from a verified
// quote tier and returns the short-lived device-signing challenge. The first
// deployment release deliberately fixes all other scopes to their safe
// defaults; public ingress, secret delivery, and integrations must be added by
// a later revisioned plan instead of being smuggled into a purchase approval.
func (m *Module) preparePlanConfirmation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "plan_id", "expected_revision", "quote_id", "candidate_tier", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(PlanConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	planID := values.String("plan_id")
	quoteID := values.String("quote_id")
	tier := values.String("candidate_tier")
	idempotencyKey := values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(planID) || !cloudIdentifierPattern.MatchString(quoteID) || expectedRevision <= 0 ||
		(tier != "economy" && tier != "recommended" && tier != "performance") || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudPlanConfirmationInvalidCode, "cloud plan confirmation is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	quote, found, err := m.store.GetCloudQuote(ctx, quoteID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !found {
		return nil, actionbase.CodedError(http.StatusNotFound, cloudPlanConfirmationInvalidCode, "cloud quote was not found")
	}
	now := m.now()
	if !quote.ValidUntil.After(now) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudQuoteExpiredCode, "cloud quote has expired")
	}
	expiresAt := now.Add(5 * time.Minute)
	if quote.ValidUntil.Before(expiresAt) {
		expiresAt = quote.ValidUntil
	}
	if !expiresAt.After(now) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudQuoteExpiredCode, "cloud quote has expired")
	}
	created, err := store.PrepareCloudPlanConfirmation(ctx, PreparePlanConfirmationRequest{
		OwnerMXID: ownerMXID, PlanID: planID, ExpectedRevision: expectedRevision, QuoteID: quoteID, CandidateTier: tier,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: digestFields(planID, fmt.Sprint(expectedRevision), quoteID, tier),
		ApprovalID: m.newID("approval"), ChallengeID: m.newID("approval_challenge"),
		ExpiresAt: expiresAt.UnixMilli(), CreatedAt: now.UnixMilli(),
	})
	if err != nil {
		return nil, planConfirmationError(err)
	}
	if created.Created {
		m.publish(ctx, "cloud.plan.changed", created.EventID, planPayload(created.Confirmation.Plan))
	}
	return map[string]any{"confirmation": created.Confirmation}, nil
}

// approvePlan consumes the exact Flutter device signature for a previously
// persisted confirmation challenge. Neither the native Agent nor MCP gets a
// path to this handler, and the store atomically records the private provision
// request before it exposes any queued Deployment projection.
func (m *Module) approvePlan(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "plan_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(PlanConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	planID := values.String("plan_id")
	idempotencyKey := values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(planID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudPlanApprovalInvalidCode, "cloud plan approval is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeApprovalV1(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudPlanApprovalInvalidCode, "cloud plan approval is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UnixMilli()
	deployment := Deployment{
		DeploymentID: m.newID("deployment"), PlanID: planID, Execution: "queued", Outcome: "pending", Resource: "none",
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	job := Job{
		JobID: m.newID("provision"), PlanID: planID, DeploymentID: deployment.DeploymentID, Kind: "provision",
		Execution: "queued", Outcome: "pending", Checkpoint: "provision_queued", Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	payload, marshalErr := json.Marshal(map[string]string{"deployment_id": deployment.DeploymentID})
	if marshalErr != nil {
		return nil, actionbase.InternalError(marshalErr)
	}
	requestPlanEventID := m.newID("event")
	requestDeploymentEventID := m.newID("event")
	requestJobEventID := m.newID("event")
	created, err := store.ApproveCloudPlan(ctx, ApproveCloudPlanRequest{
		OwnerMXID: ownerMXID, PlanID: planID, ExpectedRevision: expectedRevision, IdempotencyHash: digest(idempotencyKey), Approval: approval,
		Deployment: deployment, Job: job,
		Outbox:      OutboxEntry{OutboxID: m.newID("outbox"), Kind: OutboxKindDeploymentProvisionRequested, AggregateType: "deployment", AggregateID: deployment.DeploymentID, PayloadJSON: string(payload), CreatedAt: now},
		PlanEventID: requestPlanEventID, DeploymentEventID: requestDeploymentEventID, JobEventID: requestJobEventID, CreatedAt: now,
	})
	if err != nil {
		return nil, planApprovalError(err)
	}
	if created.Created {
		m.publish(ctx, "cloud.plan.changed", requestPlanEventID, planPayload(created.Plan))
		m.publish(ctx, "cloud.deployment.changed", requestDeploymentEventID, deploymentPayload(created.Deployment))
		m.publish(ctx, "cloud.job.changed", requestJobEventID, jobPayload(created.Job))
	}
	return map[string]any{"plan": created.Plan, "deployment": created.Deployment, "job": created.Job}, nil
}

// prepareRecipeExecutionConfirmation creates a fresh, short-lived challenge
// for the one trusted sealed manifest already registered to a Deployment. The
// public request can name only the Deployment revision; artifact, command,
// root, secret-slot, and network scope remain server-side trusted data.
func (m *Module) prepareRecipeExecutionConfirmation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "deployment_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(RecipeExecutionConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	deploymentID := values.String("deployment_id")
	idempotencyKey := values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(deploymentID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudRecipeExecutionConfirmationInvalidCode, "cloud recipe execution confirmation is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC()
	prepared, err := store.PrepareCloudRecipeExecutionConfirmation(ctx, PrepareRecipeExecutionConfirmationRequest{
		OwnerMXID: ownerMXID, DeploymentID: deploymentID, ExpectedRevision: expectedRevision,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: digestFields(deploymentID, fmt.Sprint(expectedRevision)),
		ApprovalID: m.newID("recipe_execution_approval"), ChallengeID: m.newID("recipe_execution_challenge"),
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, recipeExecutionConfirmationError(err)
	}
	return map[string]any{"confirmation": prepared.Confirmation}, nil
}

// approveRecipeExecution verifies the exact device signature for a trusted
// manifest and atomically creates an install Job plus a private digest-only
// outbox intent. It deliberately does not claim a Worker, issue a task, call
// the Broker, mutate AWS, or change Deployment/Service readiness.
func (m *Module) approveRecipeExecution(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "deployment_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(RecipeExecutionConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	deploymentID := values.String("deployment_id")
	idempotencyKey := values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(deploymentID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudRecipeExecutionConfirmationInvalidCode, "cloud recipe execution confirmation is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeRecipeExecutionApprovalV1(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudRecipeExecutionConfirmationInvalidCode, "cloud recipe execution confirmation is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UnixMilli()
	job := Job{
		JobID: m.newID("install"), PlanID: approval.PlanID, DeploymentID: deploymentID, Kind: "install",
		Execution: "queued", Outcome: "pending", Checkpoint: "install_queued", Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	jobEventID := m.newID("event")
	approved, err := store.ApproveCloudRecipeExecution(ctx, ApproveRecipeExecutionRequest{
		OwnerMXID: ownerMXID, DeploymentID: deploymentID, ExpectedRevision: expectedRevision,
		IdempotencyHash: digest(idempotencyKey), Approval: approval, Job: job,
		OutboxID: m.newID("outbox"), JobEventID: jobEventID, CreatedAt: now,
	})
	if err != nil {
		return nil, recipeExecutionApprovalError(err)
	}
	if approved.Created {
		m.publish(ctx, "cloud.job.changed", jobEventID, jobPayload(approved.Job))
	}
	return map[string]any{"execution": approved.Execution, "job": approved.Job}, nil
}

// prepareServiceOperation resolves the lifecycle capability from the exact
// installed managed Recipe. The client may select only start, stop or restart;
// it cannot select the Worker action, artifact, checkpoints, timeout or root
// scope included in the device signing payload.
func (m *Module) prepareServiceOperation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "operation", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(ServiceOperationConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	operation := cloudcontracts.ServiceOperation(values.String("operation"))
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 ||
		(operation != cloudcontracts.ServiceOperationStart && operation != cloudcontracts.ServiceOperationStop && operation != cloudcontracts.ServiceOperationRestart) ||
		ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceOperationConfirmationInvalidCode, "cloud service operation confirmation is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC()
	prepared, err := store.PrepareCloudServiceOperation(ctx, PrepareServiceOperationRequest{
		OwnerMXID: ownerMXID, ServiceID: serviceID, ExpectedRevision: expectedRevision, Operation: operation,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: digestFields(serviceID, fmt.Sprint(expectedRevision), string(operation)),
		ApprovalID: m.newID("service_operation_approval"), ChallengeID: m.newID("service_operation_challenge"),
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, serviceOperationConfirmationError(err)
	}
	return map[string]any{"confirmation": prepared.Confirmation}, nil
}

// approveServiceOperation verifies the exact device signature and atomically
// queues a sealed Worker task. ProductCore never executes a VM command.
func (m *Module) approveServiceOperation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(ServiceOperationConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceOperationConfirmationInvalidCode, "cloud service operation confirmation is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeServiceOperationApprovalV1(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceOperationConfirmationInvalidCode, "cloud service operation confirmation is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now, jobEventID := m.now().UnixMilli(), m.newID("event")
	approved, err := store.ApproveCloudServiceOperation(ctx, ApproveServiceOperationRequest{
		OwnerMXID: ownerMXID, ServiceID: serviceID, ExpectedRevision: expectedRevision,
		IdempotencyHash: digest(idempotencyKey), Approval: approval,
		OperationID: m.newID("service_operation"), JobID: m.newID("service_operation"), OutboxID: m.newID("outbox"),
		JobEventID: jobEventID, CreatedAt: now,
	})
	if err != nil {
		return nil, serviceOperationApprovalError(err)
	}
	if approved.Created {
		m.publish(ctx, "cloud.job.changed", jobEventID, jobPayload(approved.Job))
	}
	return map[string]any{"service": approved.Service, "operation": approved.Operation, "job": approved.Job}, nil
}

// prepareServiceDestroy resolves the exact private provider resource set from
// durable read-back facts. The client can select only the Service revision;
// it cannot add an instance, volume, network interface, Region, or AWS API.
func (m *Module) prepareServiceDestroy(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(ServiceDestroyConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID := values.String("service_id")
	idempotencyKey := values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceDestroyConfirmationInvalidCode, "cloud service destroy confirmation is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC()
	prepared, err := store.PrepareCloudServiceDestroy(ctx, PrepareServiceDestroyRequest{
		OwnerMXID: ownerMXID, ServiceID: serviceID, ExpectedRevision: expectedRevision,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: digestFields(serviceID, fmt.Sprint(expectedRevision)),
		ApprovalID: m.newID("service_destroy_approval"), ChallengeID: m.newID("service_destroy_challenge"),
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, serviceDestroyConfirmationError(err)
	}
	return map[string]any{"confirmation": prepared.Confirmation}, nil
}

// approveServiceDestroy consumes the exact device signature and atomically
// queues a private typed destroy intent. ProductCore never calls AWS and a
// failed later destroy remains visible as blocked rather than destroyed.
func (m *Module) approveServiceDestroy(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(ServiceDestroyConfirmationStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID := values.String("service_id")
	idempotencyKey := values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceDestroyConfirmationInvalidCode, "cloud service destroy confirmation is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeServiceDestroyApprovalV1(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceDestroyConfirmationInvalidCode, "cloud service destroy confirmation is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UnixMilli()
	serviceEventID, deploymentEventID, jobEventID := m.newID("event"), m.newID("event"), m.newID("event")
	approved, err := store.ApproveCloudServiceDestroy(ctx, ApproveServiceDestroyRequest{
		OwnerMXID: ownerMXID, ServiceID: serviceID, ExpectedRevision: expectedRevision,
		IdempotencyHash: digest(idempotencyKey), Approval: approval,
		JobID: m.newID("destroy"), OutboxID: m.newID("outbox"),
		ServiceEventID: serviceEventID, DeploymentEventID: deploymentEventID, JobEventID: jobEventID, CreatedAt: now,
	})
	if err != nil {
		return nil, serviceDestroyApprovalError(err)
	}
	if approved.Created {
		m.publish(ctx, "cloud.service.changed", serviceEventID, servicePayload(approved.Service))
		m.publish(ctx, "cloud.deployment.changed", deploymentEventID, deploymentPayload(approved.Deployment))
		m.publish(ctx, "cloud.job.changed", jobEventID, jobPayload(approved.Job))
	}
	return map[string]any{"service": approved.Service, "deployment": approved.Deployment, "job": approved.Job}, nil
}

func decodeApprovalV1(value any) (cloudcontracts.ApprovalV1, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.ApprovalV1{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var approval cloudcontracts.ApprovalV1
	if err := decoder.Decode(&approval); err != nil {
		return cloudcontracts.ApprovalV1{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudcontracts.ApprovalV1{}, errors.New("approval contains trailing JSON")
	}
	if err := approval.Validate(); err != nil {
		return cloudcontracts.ApprovalV1{}, err
	}
	return approval, nil
}

func decodeRecipeExecutionApprovalV1(value any) (cloudcontracts.RecipeExecutionApprovalV1, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.RecipeExecutionApprovalV1{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var approval cloudcontracts.RecipeExecutionApprovalV1
	if err := decoder.Decode(&approval); err != nil {
		return cloudcontracts.RecipeExecutionApprovalV1{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudcontracts.RecipeExecutionApprovalV1{}, errors.New("recipe execution approval contains trailing JSON")
	}
	if err := approval.Validate(); err != nil {
		return cloudcontracts.RecipeExecutionApprovalV1{}, err
	}
	return approval, nil
}

func decodeServiceDestroyApprovalV1(value any) (cloudcontracts.ServiceDestroyApprovalV1, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.ServiceDestroyApprovalV1{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var approval cloudcontracts.ServiceDestroyApprovalV1
	if err := decoder.Decode(&approval); err != nil {
		return cloudcontracts.ServiceDestroyApprovalV1{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudcontracts.ServiceDestroyApprovalV1{}, errors.New("service destroy approval contains trailing JSON")
	}
	if err := approval.Validate(); err != nil {
		return cloudcontracts.ServiceDestroyApprovalV1{}, err
	}
	return approval, nil
}

func decodeServiceOperationApprovalV1(value any) (cloudcontracts.ServiceOperationApprovalV1, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.ServiceOperationApprovalV1{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var approval cloudcontracts.ServiceOperationApprovalV1
	if err := decoder.Decode(&approval); err != nil {
		return cloudcontracts.ServiceOperationApprovalV1{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudcontracts.ServiceOperationApprovalV1{}, errors.New("service operation approval contains trailing JSON")
	}
	if err := approval.Validate(); err != nil {
		return cloudcontracts.ServiceOperationApprovalV1{}, err
	}
	return approval, nil
}

func planConfirmationError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud plan confirmation")
	case errors.Is(err, ErrPlanQuoteExpired):
		return actionbase.CodedError(http.StatusConflict, cloudQuoteExpiredCode, "cloud quote has expired")
	case errors.Is(err, ErrPlanConfirmationConflict):
		return actionbase.CodedError(http.StatusConflict, cloudPlanConfirmationConflictCode, "cloud plan confirmation revision conflicts with the current plan")
	case errors.Is(err, ErrPlanConfirmationInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudPlanConfirmationInvalidCode, "cloud plan is not ready for confirmation")
	default:
		return actionbase.InternalError(err)
	}
}

func planApprovalError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud plan approval")
	case errors.Is(err, ErrPlanQuoteExpired):
		return actionbase.CodedError(http.StatusConflict, cloudQuoteExpiredCode, "cloud quote has expired")
	case errors.Is(err, ErrPlanApprovalExpired):
		return actionbase.CodedError(http.StatusConflict, cloudPlanApprovalExpiredCode, "cloud plan approval has expired")
	case errors.Is(err, ErrPlanApprovalSignature):
		return actionbase.CodedError(http.StatusUnauthorized, cloudPlanApprovalSignatureCode, "cloud plan approval signature is invalid")
	case errors.Is(err, ErrPlanApprovalConflict):
		return actionbase.CodedError(http.StatusConflict, cloudPlanApprovalConflictCode, "cloud plan approval revision conflicts with the current plan")
	case errors.Is(err, ErrPlanApprovalInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudPlanApprovalInvalidCode, "cloud plan approval is invalid")
	default:
		return actionbase.InternalError(err)
	}
}

func recipeExecutionConfirmationError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud recipe execution confirmation")
	case errors.Is(err, ErrRecipeExecutionConfirmationConflict), errors.Is(err, ErrRecipeExecutionManifestConflict):
		return actionbase.CodedError(http.StatusConflict, cloudRecipeExecutionConfirmationConflictCode, "cloud recipe execution confirmation conflicts with the current deployment")
	case errors.Is(err, ErrRecipeExecutionConfirmationInvalid), errors.Is(err, ErrRecipeExecutionManifestInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudRecipeExecutionConfirmationInvalidCode, "cloud recipe execution is not ready for confirmation")
	default:
		return actionbase.InternalError(err)
	}
}

func recipeExecutionApprovalError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud recipe execution approval")
	case errors.Is(err, ErrRecipeExecutionApprovalExpired):
		return actionbase.CodedError(http.StatusConflict, cloudRecipeExecutionApprovalExpiredCode, "cloud recipe execution approval has expired")
	case errors.Is(err, ErrRecipeExecutionApprovalSignature):
		return actionbase.CodedError(http.StatusUnauthorized, cloudRecipeExecutionApprovalSignatureCode, "cloud recipe execution approval signature is invalid")
	case errors.Is(err, ErrRecipeExecutionConfirmationConflict), errors.Is(err, ErrRecipeExecutionManifestConflict):
		return actionbase.CodedError(http.StatusConflict, cloudRecipeExecutionConfirmationConflictCode, "cloud recipe execution approval conflicts with the current deployment")
	case errors.Is(err, ErrRecipeExecutionConfirmationInvalid), errors.Is(err, ErrRecipeExecutionManifestInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudRecipeExecutionConfirmationInvalidCode, "cloud recipe execution approval is invalid")
	default:
		return actionbase.InternalError(err)
	}
}

func serviceDestroyConfirmationError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud service destroy confirmation")
	case errors.Is(err, ErrServiceDestroyConfirmationConflict):
		return actionbase.CodedError(http.StatusConflict, cloudServiceDestroyConfirmationConflictCode, "cloud service destroy confirmation conflicts with the current service")
	case errors.Is(err, ErrServiceDestroyConfirmationInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudServiceDestroyConfirmationInvalidCode, "cloud service is not destroyable")
	default:
		return actionbase.InternalError(err)
	}
}

func serviceOperationConfirmationError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud service operation confirmation")
	case errors.Is(err, ErrServiceOperationConfirmationConflict):
		return actionbase.CodedError(http.StatusConflict, cloudServiceOperationConfirmationConflictCode, "cloud service operation confirmation conflicts with the current service")
	case errors.Is(err, ErrServiceOperationConfirmationInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudServiceOperationConfirmationInvalidCode, "cloud service does not expose this managed operation")
	default:
		return actionbase.InternalError(err)
	}
}

func serviceOperationApprovalError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud service operation approval")
	case errors.Is(err, ErrServiceOperationApprovalExpired):
		return actionbase.CodedError(http.StatusConflict, cloudServiceOperationApprovalExpiredCode, "cloud service operation approval has expired")
	case errors.Is(err, ErrServiceOperationApprovalSignature):
		return actionbase.CodedError(http.StatusUnauthorized, cloudServiceOperationApprovalSignatureCode, "cloud service operation approval signature is invalid")
	case errors.Is(err, ErrServiceOperationConfirmationConflict):
		return actionbase.CodedError(http.StatusConflict, cloudServiceOperationConfirmationConflictCode, "cloud service operation approval conflicts with the current service")
	case errors.Is(err, ErrServiceOperationConfirmationInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudServiceOperationConfirmationInvalidCode, "cloud service operation approval is invalid")
	default:
		return actionbase.InternalError(err)
	}
}

func serviceDestroyApprovalError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud service destroy approval")
	case errors.Is(err, ErrServiceDestroyApprovalExpired):
		return actionbase.CodedError(http.StatusConflict, cloudServiceDestroyApprovalExpiredCode, "cloud service destroy approval has expired")
	case errors.Is(err, ErrServiceDestroyApprovalSignature):
		return actionbase.CodedError(http.StatusUnauthorized, cloudServiceDestroyApprovalSignatureCode, "cloud service destroy approval signature is invalid")
	case errors.Is(err, ErrServiceDestroyConfirmationConflict):
		return actionbase.CodedError(http.StatusConflict, cloudServiceDestroyConfirmationConflictCode, "cloud service destroy approval conflicts with the current service")
	case errors.Is(err, ErrServiceDestroyConfirmationInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudServiceDestroyConfirmationInvalidCode, "cloud service destroy approval is invalid")
	default:
		return actionbase.InternalError(err)
	}
}

// CreateResearchGoal is the only Native Agent-facing entrypoint. It keeps the
// Eino runtime on the same validated, idempotent ProductCore path as an owner
// request, while deliberately exposing no approval or cloud mutation method.
func (m *Module) CreateResearchGoal(ctx context.Context, goal, connectionID, idempotencyKey string) (map[string]any, error) {
	result, actionErr := m.createGoal(ctx, map[string]any{
		"goal":                goal,
		"cloud_connection_id": connectionID,
		"idempotency_key":     idempotencyKey,
	})
	if actionErr != nil {
		return nil, fmt.Errorf("%s", actionErr.Error)
	}
	response, ok := result.(map[string]any)
	if !ok {
		return nil, errors.New("cloud research goal returned an invalid response")
	}
	return response, nil
}

// ReadCloudStatus is the narrow read-only Agent port. It deliberately uses a
// smaller model-facing DTO than cloud.bootstrap: provider account/region data,
// Connection identifiers, private goal prompts, recipe/plan digests, and alert
// messages are not needed for conversational progress reporting.
func (m *Module) ReadCloudStatus(ctx context.Context) (map[string]any, error) {
	snapshot, err := m.readCloudStatusSnapshot(ctx, false)
	if err != nil {
		return nil, err
	}
	return cloudDialogueStatusPayload(snapshot), nil
}

type cloudStatusSnapshot struct {
	goals       []Goal
	plans       []Plan
	jobs        []Job
	connections []Connection
	deployments []Deployment
	services    []Service
	recipes     []Recipe
	alerts      []Alert
}

func (m *Module) readCloudStatusSnapshot(ctx context.Context, includeRecipes bool) (cloudStatusSnapshot, error) {
	if m == nil || m.store == nil {
		return cloudStatusSnapshot{}, errors.New("cloud status is not configured")
	}
	goals, err := m.store.ListCloudGoals(ctx)
	if err != nil {
		return cloudStatusSnapshot{}, err
	}
	plans, err := m.store.ListCloudPlans(ctx)
	if err != nil {
		return cloudStatusSnapshot{}, err
	}
	jobs, err := m.store.ListCloudJobs(ctx)
	if err != nil {
		return cloudStatusSnapshot{}, err
	}
	connections, err := m.store.ListCloudConnections(ctx)
	if err != nil {
		return cloudStatusSnapshot{}, err
	}
	deployments, err := m.store.ListCloudDeployments(ctx)
	if err != nil {
		return cloudStatusSnapshot{}, err
	}
	services, err := m.store.ListCloudServices(ctx)
	if err != nil {
		return cloudStatusSnapshot{}, err
	}
	var recipes []Recipe
	if includeRecipes {
		recipes, err = m.store.ListCloudRecipes(ctx)
		if err != nil {
			return cloudStatusSnapshot{}, err
		}
	}
	alerts, err := m.store.ListCloudAlerts(ctx)
	if err != nil {
		return cloudStatusSnapshot{}, err
	}
	return cloudStatusSnapshot{
		goals: goals, plans: planSummaries(plans), jobs: jobs, connections: connections,
		deployments: deployments, services: services, recipes: recipes, alerts: alerts,
	}, nil
}

func cloudBootstrapStatusPayload(snapshot cloudStatusSnapshot) map[string]any {
	summaries := make([]GoalSummary, 0, len(snapshot.goals))
	for _, goal := range snapshot.goals {
		summaries = append(summaries, goal.Summary())
	}
	return map[string]any{
		"synced_at": time.Now().UTC().Format(time.RFC3339Nano), "goals": summaries, "plans": snapshot.plans, "jobs": snapshot.jobs,
		"connections": snapshot.connections, "deployments": snapshot.deployments, "services": snapshot.services, "recipes": snapshot.recipes, "alerts": snapshot.alerts,
	}
}

type cloudDialogueGoalStatus struct {
	GoalID    string `json:"goal_id"`
	PlanID    string `json:"plan_id"`
	Status    string `json:"status"`
	Revision  int64  `json:"revision"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type cloudDialoguePlanStatus struct {
	PlanID    string `json:"plan_id"`
	GoalID    string `json:"goal_id"`
	Status    string `json:"status"`
	Title     string `json:"title,omitempty"`
	Summary   string `json:"summary,omitempty"`
	Revision  int64  `json:"revision"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type cloudDialogueJobStatus struct {
	JobID        string `json:"job_id"`
	PlanID       string `json:"plan_id"`
	DeploymentID string `json:"deployment_id,omitempty"`
	Kind         string `json:"kind"`
	Execution    string `json:"execution_status"`
	Outcome      string `json:"outcome_status"`
	Checkpoint   string `json:"checkpoint"`
	ErrorCode    string `json:"error_code,omitempty"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type cloudDialogueConnectionStatus struct {
	Status    string `json:"status"`
	Revision  int64  `json:"revision"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type cloudDialogueDeploymentStatus struct {
	DeploymentID string `json:"deployment_id"`
	PlanID       string `json:"plan_id"`
	Execution    string `json:"execution_status"`
	Outcome      string `json:"outcome_status"`
	Resource     string `json:"resource_status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type cloudDialogueServiceStatus struct {
	ServiceID    string `json:"service_id"`
	DeploymentID string `json:"deployment_id"`
	Name         string `json:"name"`
	Status       string `json:"service_status"`
	Integration  string `json:"integration_status"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type cloudDialogueAlertStatus struct {
	AlertID      string `json:"alert_id"`
	DeploymentID string `json:"deployment_id,omitempty"`
	ServiceID    string `json:"service_id,omitempty"`
	Severity     string `json:"severity"`
	Code         string `json:"code"`
	Acknowledged bool   `json:"acknowledged"`
	Revision     int64  `json:"revision"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

func cloudDialogueStatusPayload(snapshot cloudStatusSnapshot) map[string]any {
	goals := make([]cloudDialogueGoalStatus, 0, len(snapshot.goals))
	for _, goal := range snapshot.goals {
		goals = append(goals, cloudDialogueGoalStatus{
			GoalID: goal.GoalID, PlanID: goal.PlanID, Status: goal.Status,
			Revision: goal.Revision, CreatedAt: goal.CreatedAt, UpdatedAt: goal.UpdatedAt,
		})
	}
	plans := make([]cloudDialoguePlanStatus, 0, len(snapshot.plans))
	for _, plan := range snapshot.plans {
		plans = append(plans, cloudDialoguePlanStatus{
			PlanID: plan.PlanID, GoalID: plan.GoalID, Status: plan.Status, Title: plan.Title, Summary: plan.Summary,
			Revision: plan.Revision, CreatedAt: plan.CreatedAt, UpdatedAt: plan.UpdatedAt,
		})
	}
	jobs := make([]cloudDialogueJobStatus, 0, len(snapshot.jobs))
	for _, job := range snapshot.jobs {
		jobs = append(jobs, cloudDialogueJobStatus{
			JobID: job.JobID, PlanID: job.PlanID, DeploymentID: job.DeploymentID, Kind: job.Kind,
			Execution: job.Execution, Outcome: job.Outcome, Checkpoint: job.Checkpoint, ErrorCode: job.ErrorCode,
			Revision: job.Revision, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt,
		})
	}
	connections := make([]cloudDialogueConnectionStatus, 0, len(snapshot.connections))
	for _, connection := range snapshot.connections {
		connections = append(connections, cloudDialogueConnectionStatus{
			Status: connection.Status, Revision: connection.Revision, CreatedAt: connection.CreatedAt, UpdatedAt: connection.UpdatedAt,
		})
	}
	deployments := make([]cloudDialogueDeploymentStatus, 0, len(snapshot.deployments))
	for _, deployment := range snapshot.deployments {
		deployments = append(deployments, cloudDialogueDeploymentStatus{
			DeploymentID: deployment.DeploymentID, PlanID: deployment.PlanID, Execution: deployment.Execution,
			Outcome: deployment.Outcome, Resource: deployment.Resource, Revision: deployment.Revision,
			CreatedAt: deployment.CreatedAt, UpdatedAt: deployment.UpdatedAt,
		})
	}
	services := make([]cloudDialogueServiceStatus, 0, len(snapshot.services))
	for _, service := range snapshot.services {
		services = append(services, cloudDialogueServiceStatus{
			ServiceID: service.ServiceID, DeploymentID: service.DeploymentID, Name: service.Name,
			Status: service.Status, Integration: service.Integration, Revision: service.Revision,
			CreatedAt: service.CreatedAt, UpdatedAt: service.UpdatedAt,
		})
	}
	alerts := make([]cloudDialogueAlertStatus, 0, len(snapshot.alerts))
	for _, alert := range snapshot.alerts {
		alerts = append(alerts, cloudDialogueAlertStatus{
			AlertID: alert.AlertID, DeploymentID: alert.DeploymentID, ServiceID: alert.ServiceID,
			Severity: alert.Severity, Code: alert.Code, Acknowledged: alert.Acknowledged,
			Revision: alert.Revision, CreatedAt: alert.CreatedAt, UpdatedAt: alert.UpdatedAt,
		})
	}
	return map[string]any{
		"synced_at": time.Now().UTC().Format(time.RFC3339Nano), "goals": goals, "plans": plans, "jobs": jobs,
		"connections": connections, "deployments": deployments, "services": services, "alerts": alerts,
	}
}

func (m *Module) createGoal(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "goal", "cloud_connection_id", "idempotency_key"); err != nil {
		return nil, err
	}
	values := actionbase.Params(params)
	goalText := values.String("goal")
	if count := utf8.RuneCountInString(goalText); count == 0 || count > 12000 || strings.IndexByte(goalText, 0) >= 0 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudGoalInvalidCode, "goal must contain 1 to 12000 characters")
	}
	if ContainsSensitiveGoalMaterial(goalText) {
		return nil, actionbase.CodedError(http.StatusBadRequest, "cloud_goal_secret_not_allowed", "cloud goal must use a secret_ref instead of secret material")
	}
	idempotencyKey := values.String("idempotency_key")
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	connectionID := values.String("cloud_connection_id")
	if len(connectionID) > 128 || strings.ContainsAny(connectionID, "\r\n\t") || strings.IndexByte(connectionID, 0) >= 0 {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionIDInvalidCode, "cloud_connection_id is invalid")
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	if connectionID == "" {
		// A reviewable QuoteV1 and PlanV1 bind one immutable Cloud Connection.
		// Do not create a private outbox row that no compliant Orchestrator can
		// ever claim. A future waiting_connection + attach flow must be a
		// separate, revisioned state transition rather than an implicit retry.
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionRequiredCode, "cloud_connection_id is required before research can start")
	}
	_, found, err := m.store.GetCloudConnection(ctx, connectionID)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !found {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_connection_not_found", "cloud connection was not found")
	}
	ownerMXID := ""
	if m.cfg.OwnerMXID != nil {
		ownerMXID = strings.TrimSpace(m.cfg.OwnerMXID())
	}
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UnixMilli()
	goalID := m.newID("goal")
	planID := m.newID("plan")
	outboxID := m.newID("outbox")
	jobID := ResearchJobID(outboxID)
	goal := Goal{
		GoalID: goalID, OwnerMXID: ownerMXID, Prompt: goalText, ConnectionID: connectionID,
		PlanID: planID, Status: GoalStatusResearching,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: digestFields(goalText, connectionID),
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	plan := Plan{
		PlanID: planID, GoalID: goalID, ConnectionID: connectionID, Status: PlanStatusResearching,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	job := Job{
		JobID: jobID, PlanID: planID, Kind: "research", Execution: "queued", Outcome: "pending",
		Checkpoint: "research_queued", Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	payload, err := json.Marshal(map[string]string{
		"goal_id": goalID, "plan_id": planID, "cloud_connection_id": connectionID, "goal": goalText,
	})
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	request := CreateGoalRequest{
		Goal: goal,
		Plan: plan,
		Job:  job,
		Events: []Event{
			{EventID: m.newID("event"), Type: "cloud.goal.changed", AggregateType: "goal", AggregateID: goalID, Revision: 1, SummaryJSON: mustJSON(goal.Summary()), CreatedAt: now},
			{EventID: m.newID("event"), Type: "cloud.plan.changed", AggregateType: "plan", AggregateID: planID, Revision: 1, SummaryJSON: mustJSON(plan), CreatedAt: now},
			{EventID: m.newID("event"), Type: "cloud.job.changed", AggregateType: "job", AggregateID: jobID, Revision: 1, SummaryJSON: mustJSON(job), CreatedAt: now},
		},
		Outbox: OutboxEntry{
			OutboxID: outboxID, Kind: OutboxKindResearchGoalRequested,
			AggregateType: "goal", AggregateID: goalID, PayloadJSON: string(payload), CreatedAt: now,
		},
	}
	created, err := m.store.CreateCloudGoal(ctx, request)
	if err != nil {
		if err == ErrIdempotencyConflict {
			return nil, actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different cloud goal")
		}
		return nil, actionbase.InternalError(err)
	}
	if created.Created {
		for _, event := range request.Events {
			switch event.Type {
			case "cloud.goal.changed":
				m.publish(ctx, event.Type, event.EventID, goalPayload(created.Goal.Summary()))
			case "cloud.plan.changed":
				m.publish(ctx, event.Type, event.EventID, planPayload(created.Plan))
			case "cloud.job.changed":
				m.publish(ctx, event.Type, event.EventID, jobPayload(job))
			}
		}
	}
	return map[string]any{"goal": created.Goal.Summary(), "plan": created.Plan}, nil
}

func (m *Module) bootstrap(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	snapshot, err := m.readCloudStatusSnapshot(ctx, true)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return cloudBootstrapStatusPayload(snapshot), nil
}

func (m *Module) connectionsList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudConnections(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"connections": items}, nil
}

func (m *Module) connectionsGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "cloud_connection_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("cloud_connection_id")
	if id == "" {
		return nil, actionbase.BadRequest("cloud_connection_id is required")
	}
	item, ok, err := m.store.GetCloudConnection(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_connection_not_found", "cloud connection was not found")
	}
	return item, nil
}

func (m *Module) plansList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudPlans(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"plans": planSummaries(items)}, nil
}

func (m *Module) plansGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "plan_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("plan_id")
	if id == "" {
		return nil, actionbase.BadRequest("plan_id is required")
	}
	item, ok, err := m.store.GetCloudPlan(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_plan_not_found", "cloud plan was not found")
	}
	// A store only persists the quote ID on plans. Clear any implementation
	// supplied detail first so this is the sole projection path that can attach
	// a quote, and only when the immutable binding exists.
	item.Quote = nil
	if item.QuoteID != "" {
		quote, found, err := m.store.GetCloudQuote(ctx, item.QuoteID)
		if err != nil {
			return nil, actionbase.InternalError(err)
		}
		if found {
			if quote.QuoteID != item.QuoteID || quote.ConnectionID != item.ConnectionID {
				return nil, actionbase.InternalError(errors.New("cloud quote does not match plan"))
			}
			item.Quote = &quote
		}
	}
	return item, nil
}

func (m *Module) deploymentsList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudDeployments(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"deployments": items}, nil
}

func (m *Module) deploymentsGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "deployment_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("deployment_id")
	if id == "" {
		return nil, actionbase.BadRequest("deployment_id is required")
	}
	item, ok, err := m.store.GetCloudDeployment(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_deployment_not_found", "cloud deployment was not found")
	}
	return item, nil
}

func (m *Module) servicesList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudServices(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"services": items}, nil
}

func (m *Module) servicesGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("service_id")
	if id == "" {
		return nil, actionbase.BadRequest("service_id is required")
	}
	item, ok, err := m.store.GetCloudService(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_service_not_found", "cloud service was not found")
	}
	return item, nil
}

func (m *Module) recipesList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params); err != nil {
		return nil, err
	}
	items, err := m.store.ListCloudRecipes(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"recipes": items}, nil
}

func (m *Module) recipesGet(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "recipe_id"); err != nil {
		return nil, err
	}
	id := actionbase.Params(params).String("recipe_id")
	if id == "" {
		return nil, actionbase.BadRequest("recipe_id is required")
	}
	item, ok, err := m.store.GetCloudRecipe(ctx, id)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if !ok {
		return nil, actionbase.CodedError(http.StatusNotFound, "cloud_recipe_not_found", "cloud recipe was not found")
	}
	return item, nil
}

func (m *Module) eventsList(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "limit"); err != nil {
		return nil, err
	}
	limit := actionbase.Params(params).Int64("limit")
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	items, err := m.store.ListCloudEvents(ctx, int(limit))
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	return map[string]any{"events": items}, nil
}

func (m *Module) unavailableWrite(_ context.Context, _ map[string]any) (any, *actionbase.Error) {
	return nil, unavailableError()
}

func (m *Module) publish(ctx context.Context, eventType, cloudEventID string, payload map[string]any) {
	if m != nil && m.cfg.Publish != nil {
		_ = m.cfg.Publish(ctx, eventType, cloudEventID, payload)
	}
}

func (m *Module) now() time.Time {
	if m != nil && m.cfg.Now != nil {
		return m.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func (m *Module) ownerMXID() string {
	if m != nil && m.cfg.OwnerMXID != nil {
		return strings.TrimSpace(m.cfg.OwnerMXID())
	}
	return ""
}

func (m *Module) newID(kind string) string {
	if m != nil && m.cfg.NewID != nil {
		return m.cfg.NewID(kind)
	}
	return "cloud_" + kind + "_" + uuid.NewString()
}

func only(params map[string]any, allowed ...string) *actionbase.Error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key := range params {
		if _, ok := allowedSet[key]; !ok {
			return actionbase.CodedError(http.StatusBadRequest, cloudInvalidParamsCode, "cloud action received an unsupported parameter")
		}
	}
	return nil
}

func unavailableError() *actionbase.Error {
	return actionbase.CodedError(http.StatusServiceUnavailable, cloudUnavailableCode, "cloud orchestrator is not configured")
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// digestFields keeps the idempotency comparison structurally unambiguous.
// It is deliberately length-prefixed instead of concatenating user-controlled
// fields with a delimiter.
func digestFields(values ...string) string {
	hash := sha256.New()
	var length [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func goalPayload(summary GoalSummary) map[string]any {
	return map[string]any{
		"goal_id": summary.GoalID, "plan_id": summary.PlanID, "cloud_connection_id": summary.ConnectionID,
		"status": summary.Status, "revision": summary.Revision, "created_at": summary.CreatedAt, "updated_at": summary.UpdatedAt,
	}
}

func planPayload(plan Plan) map[string]any {
	return map[string]any{
		"plan_id": plan.PlanID, "goal_id": plan.GoalID, "cloud_connection_id": plan.ConnectionID,
		"status": plan.Status, "title": plan.Title, "summary": plan.Summary, "recipe_digest": plan.RecipeDigest,
		"quote_id": plan.QuoteID, "plan_hash": plan.PlanHash, "revision": plan.Revision, "updated_at": plan.UpdatedAt,
		"created_at": plan.CreatedAt,
	}
}

func planSummaries(plans []Plan) []Plan {
	for index := range plans {
		plans[index].Quote = nil
	}
	return plans
}

func jobPayload(job Job) map[string]any {
	return map[string]any{
		"job_id": job.JobID, "plan_id": job.PlanID, "deployment_id": job.DeploymentID,
		"kind": job.Kind, "execution_status": job.Execution, "outcome_status": job.Outcome,
		"checkpoint": job.Checkpoint, "error_code": job.ErrorCode, "revision": job.Revision,
		"created_at": job.CreatedAt, "updated_at": job.UpdatedAt,
	}
}

func deploymentPayload(deployment Deployment) map[string]any {
	return map[string]any{
		"deployment_id": deployment.DeploymentID, "plan_id": deployment.PlanID, "cloud_connection_id": deployment.ConnectionID,
		"execution_status": deployment.Execution, "outcome_status": deployment.Outcome, "resource_status": deployment.Resource,
		"revision": deployment.Revision, "created_at": deployment.CreatedAt, "updated_at": deployment.UpdatedAt,
	}
}

func servicePayload(service Service) map[string]any {
	return map[string]any{
		"service_id": service.ServiceID, "deployment_id": service.DeploymentID, "recipe_id": service.RecipeID,
		"name": service.Name, "service_status": service.Status, "integration_status": service.Integration,
		"revision": service.Revision, "created_at": service.CreatedAt, "updated_at": service.UpdatedAt,
	}
}
