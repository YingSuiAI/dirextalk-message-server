// Package recipeexec coordinates a sealed Recipe execution manifest with a
// trusted artifact resolver, an idempotent action driver, and durable
// checkpoints. It deliberately does not execute processes, download content,
// retrieve secrets, or call any cloud API. Those privileges stay in later,
// separately isolated execution components.
package recipeexec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var (
	// ErrExecutorConfiguration means the local host did not provide every
	// required trusted dependency. It is never safe to fall back to a shell or
	// an unverified artifact when one is absent.
	ErrExecutorConfiguration = errors.New("recipe executor is not configured")
	// ErrArtifactDigestMismatch means the resolver did not return the exact
	// compiled artifact locked by the manifest.
	ErrArtifactDigestMismatch = errors.New("resolved recipe artifact digest does not match the manifest")
	// ErrActionUnsupported means the trusted artifact did not declare the
	// requested opaque action identifier.
	ErrActionUnsupported = errors.New("resolved recipe artifact does not support the manifest action")
	// ErrCheckpointBinding means a durable record belongs to another execution
	// or another immutable manifest digest.
	ErrCheckpointBinding = errors.New("recipe checkpoint does not match execution binding")
	// ErrCheckpointState means the durable record cannot represent a legal
	// prefix of the manifest's checkpoint sequence.
	ErrCheckpointState = errors.New("recipe checkpoint state is invalid")
	// ErrCheckpointOutOfOrder prevents a driver from skipping, replaying, or
	// regressing a declared checkpoint.
	ErrCheckpointOutOfOrder = errors.New("recipe checkpoint is not the next declared checkpoint")
	// ErrCheckpointConflict is a sentinel an atomic CheckpointStore may return
	// when another holder advanced the state first.
	ErrCheckpointConflict = errors.New("recipe checkpoint compare-and-swap conflict")
	// ErrExecutionIncomplete means a driver returned successfully without
	// persisting the terminal declared checkpoint.
	ErrExecutionIncomplete      = errors.New("recipe action returned before the terminal checkpoint")
	ErrSecretScope              = errors.New("recipe secret scope does not match the trusted bundle")
	ErrSecretMaterialize        = errors.New("recipe secret materialization failed")
	ErrSecretMaterializePending = errors.New("recipe secret materialization is pending")
	ErrSecretStage              = errors.New("recipe secret staging failed")
)

// Binding is the durable identity for one execution attempt. The manifest
// digest makes a stale checkpoint unusable after any reviewed scope changes.
type Binding struct {
	ExecutionID    string
	ManifestDigest string
}

// Bundle is a trusted resolver result. It contains only the authenticated
// artifact digest and the opaque action identifiers it exposes; it carries no
// command, URL, secret, or arbitrary payload into this coordinator.
type Bundle struct {
	ArtifactDigest string
	ActionIDs      []string
	SecretTargets  []SecretTarget
}

// SecretTarget is trusted local catalog data. Neither the sealed manifest nor
// an Agent may select a host path or environment variable name.
type SecretTarget struct {
	SlotID         string
	FileName       string
	EnvironmentKey string
}

type SecretMaterializeRequest struct {
	TaskID         string
	ExecutionID    string
	ManifestDigest string
	ArtifactDigest string
	SlotID         string
	SecretRef      string
}

type SecretMaterializer interface {
	Materialize(context.Context, SecretMaterializeRequest) ([]byte, error)
}

type MaterializedSecret struct {
	Target SecretTarget
	Value  []byte
}

type SecretDelivery struct {
	Files           map[string]string
	EnvironmentFile string
}

type SecretStager interface {
	Stage(context.Context, string, string, []MaterializedSecret) (SecretDelivery, func(), error)
}

// BundleResolver must authenticate and pin a compiled artifact before
// returning its descriptor. The coordinator independently compares the
// returned digest with the sealed manifest before it calls a driver.
type BundleResolver interface {
	Resolve(ctx context.Context, artifactDigest string) (Bundle, error)
}

// CheckpointState is one durable prefix of the declared checkpoint sequence.
// Index is -1 only before the first checkpoint; Completed is true only at the
// sequence's terminal checkpoint.
type CheckpointState struct {
	Binding    Binding
	Checkpoint string
	Index      int
	Completed  bool
}

// InitialCheckpointState returns the only valid state before any action work
// has been recorded.
func InitialCheckpointState(binding Binding) CheckpointState {
	return CheckpointState{Binding: binding, Index: -1}
}

// CheckpointStore owns durable, compare-and-swap checkpoint persistence. Load
// must return InitialCheckpointState(binding) for a fresh execution; Advance
// must atomically accept next only when the stored state equals previous.
type CheckpointStore interface {
	Load(ctx context.Context, binding Binding) (CheckpointState, error)
	Advance(ctx context.Context, previous, next CheckpointState) error
}

// ActionRequest is the narrow non-secret input exposed to a trusted action
// driver. Stable Binding lets the driver make its external action idempotent
// across a restart; ResumeAfter tells it which semantic checkpoint was
// persisted. Volume/data/secret fields remain opaque slot references.
type ActionRequest struct {
	Binding      Binding
	Artifact     Bundle
	DeploymentID string
	ActionID     string
	RootRequired bool
	Timeout      time.Duration
	ResumeAfter  string
	VolumeSlots  []cloudorchestrator.VolumeSlotV1
	DataSlots    []cloudorchestrator.DataSlotV1
	SecretSlots  []cloudorchestrator.SecretSlotV1
	Secrets      SecretDelivery
}

// CheckpointReporter is the sole way an ActionDriver advances durable
// progress. The coordinator rejects any checkpoint that is not the immediate
// next item in the sealed sequence.
type CheckpointReporter interface {
	Checkpoint(ctx context.Context, name string) error
}

// ActionDriver is intentionally an interface rather than a shell callback.
// Implementations must use Binding as their idempotency key and must report
// every declared checkpoint in order, including the terminal checkpoint.
type ActionDriver interface {
	Execute(ctx context.Context, request ActionRequest, checkpoints CheckpointReporter) error
}

// Executor coordinates one sealed manifest. The task transport can inject it,
// but the production cloud-worker deliberately provides no resolver, store, or
// driver until a separately audited fixed bundle is available.
type Executor struct {
	Resolver                     BundleResolver
	Store                        CheckpointStore
	Driver                       ActionDriver
	RequireSecretMaterialization bool
	Materializer                 SecretMaterializer
	SecretStager                 SecretStager
	SecretRetryDelay             time.Duration
}

// Configured reports whether every trusted execution dependency was supplied.
// Transport loops must check this before claiming work; Execute retains the
// same check as a second fail-closed boundary.
func (executor Executor) Configured() bool {
	return executor.Resolver != nil && executor.Store != nil && executor.Driver != nil &&
		(!executor.RequireSecretMaterialization || (executor.Materializer != nil && executor.SecretStager != nil))
}

// Result reports only non-secret, durable execution progress. Completed does
// not mean the deployed service is externally ready; readiness remains a
// separately verified control-plane concern.
type Result struct {
	ExecutionID    string
	ManifestDigest string
	LastCheckpoint string
	Completed      bool
	Resumed        bool
}

// Execute validates the sealed scope, resolves the exact compiled artifact,
// resumes at its durable checkpoint, and requires the driver to record the
// terminal checkpoint before reporting success. It is the local coordinator
// primitive used by tests and trusted fixed-AMI integration code; a delivered
// Recipe task must use ExecuteTask so the transport binding is checked first.
func (executor Executor) Execute(ctx context.Context, manifest cloudorchestrator.RecipeExecutionManifestV1) (Result, error) {
	return executor.execute(ctx, manifest, nil)
}

// ExecuteTask is the execution entry point for a delivered Recipe task. It
// rejects a task whose manifest differs or whose remote checkpoint is ahead
// of trusted local state before it can call an ActionDriver. Local state may
// be ahead after an accepted action checkpoint whose HTTP event response was
// lost; that safe prefix is returned so transport can replay the missing
// de-secreted events without re-running the action. Secret retrieval is used
// only when the caller explicitly injects the closed materializer and tmpfs
// stager; process execution and cloud control remain outside this coordinator.
func (executor Executor) ExecuteTask(ctx context.Context, task TaskV1, manifest cloudorchestrator.RecipeExecutionManifestV1) (Result, error) {
	if err := task.ValidateForManifest(manifest); err != nil {
		return Result{}, err
	}
	return executor.execute(ctx, manifest, &task)
}

func (executor Executor) execute(ctx context.Context, manifest cloudorchestrator.RecipeExecutionManifestV1, task *TaskV1) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := manifest.Validate(); err != nil {
		return Result{}, fmt.Errorf("recipe execution manifest: %w", err)
	}
	if !executor.Configured() {
		return Result{}, ErrExecutorConfiguration
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		return Result{}, fmt.Errorf("digest recipe execution manifest: %w", err)
	}
	binding := Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest}
	runContext, cancel := context.WithTimeout(ctx, time.Duration(manifest.TimeoutSeconds)*time.Second)
	defer cancel()
	if err := runContext.Err(); err != nil {
		return Result{}, err
	}
	bundle, err := executor.Resolver.Resolve(runContext, manifest.ArtifactDigest)
	if err != nil {
		return Result{}, fmt.Errorf("resolve recipe artifact: %w", err)
	}
	if bundle.ArtifactDigest != manifest.ArtifactDigest {
		return Result{}, ErrArtifactDigestMismatch
	}
	if !bundleSupportsAction(bundle, manifest.ActionID) {
		return Result{}, ErrActionUnsupported
	}

	state, err := executor.Store.Load(runContext, binding)
	if err != nil {
		return Result{}, fmt.Errorf("load recipe checkpoint: %w", err)
	}
	if err := validateCheckpointState(state, binding, manifest.CheckpointSequence); err != nil {
		return Result{}, err
	}
	if task != nil {
		taskIndex := taskCheckpointIndex(task.CheckpointSequence, task.LastCheckpoint)
		if taskIndex > state.Index {
			return Result{}, ErrTaskCheckpointBinding
		}
	}
	resumed := state.Index >= 0
	result := resultForState(binding, state, resumed)
	if state.Completed {
		return result, nil
	}
	secrets, cleanup, err := executor.prepareSecrets(runContext, task, binding, manifest, bundle)
	if err != nil {
		return result, err
	}
	if cleanup != nil {
		defer cleanup()
	}
	if err := runContext.Err(); err != nil {
		return result, err
	}
	reporter := &checkpointReporter{
		store:            executor.Store,
		executionContext: runContext,
		binding:          binding,
		checkpoints:      manifest.CheckpointSequence,
		state:            state,
	}
	driverBundle := cloneBundle(bundle)
	driverSecretSlots := append([]cloudorchestrator.SecretSlotV1(nil), manifest.SecretSlots...)
	if executor.RequireSecretMaterialization {
		driverBundle.SecretTargets = nil
		driverSecretSlots = nil
	}
	request := ActionRequest{
		Binding:      binding,
		Artifact:     driverBundle,
		DeploymentID: manifest.DeploymentID,
		ActionID:     manifest.ActionID,
		RootRequired: manifest.RootRequired,
		Timeout:      time.Duration(manifest.TimeoutSeconds) * time.Second,
		ResumeAfter:  state.Checkpoint,
		VolumeSlots:  append([]cloudorchestrator.VolumeSlotV1(nil), manifest.VolumeSlots...),
		DataSlots:    append([]cloudorchestrator.DataSlotV1(nil), manifest.DataSlots...),
		SecretSlots:  driverSecretSlots,
		Secrets:      secrets,
	}
	if err := executor.Driver.Execute(runContext, request, reporter); err != nil {
		return resultForState(binding, reporter.state, resumed), fmt.Errorf("execute recipe action: %w", err)
	}
	if err := runContext.Err(); err != nil {
		return resultForState(binding, reporter.state, resumed), err
	}
	if !reporter.state.Completed {
		return resultForState(binding, reporter.state, resumed), ErrExecutionIncomplete
	}
	return resultForState(binding, reporter.state, resumed), nil
}

type checkpointReporter struct {
	mu               sync.Mutex
	store            CheckpointStore
	executionContext context.Context
	binding          Binding
	checkpoints      []string
	state            CheckpointState
}

func (reporter *checkpointReporter) Checkpoint(ctx context.Context, name string) error {
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if err := reporter.executionContext.Err(); err != nil {
		return err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if reporter.state.Completed {
		return ErrCheckpointOutOfOrder
	}
	nextIndex := reporter.state.Index + 1
	if nextIndex >= len(reporter.checkpoints) || reporter.checkpoints[nextIndex] != name {
		return ErrCheckpointOutOfOrder
	}
	next := CheckpointState{
		Binding:    reporter.binding,
		Checkpoint: name,
		Index:      nextIndex,
		Completed:  nextIndex == len(reporter.checkpoints)-1,
	}
	if err := reporter.store.Advance(reporter.executionContext, reporter.state, next); err != nil {
		return fmt.Errorf("advance recipe checkpoint: %w", err)
	}
	reporter.state = next
	return nil
}

func validateCheckpointState(state CheckpointState, binding Binding, checkpoints []string) error {
	if state.Binding != binding {
		return ErrCheckpointBinding
	}
	if state.Index < -1 || state.Index >= len(checkpoints) {
		return ErrCheckpointState
	}
	if state.Index == -1 {
		if state.Checkpoint != "" || state.Completed {
			return ErrCheckpointState
		}
		return nil
	}
	if state.Checkpoint != checkpoints[state.Index] || state.Completed != (state.Index == len(checkpoints)-1) {
		return ErrCheckpointState
	}
	return nil
}

func resultForState(binding Binding, state CheckpointState, resumed bool) Result {
	return Result{
		ExecutionID:    binding.ExecutionID,
		ManifestDigest: binding.ManifestDigest,
		LastCheckpoint: state.Checkpoint,
		Completed:      state.Completed,
		Resumed:        resumed,
	}
}

func bundleSupportsAction(bundle Bundle, actionID string) bool {
	for _, candidate := range bundle.ActionIDs {
		if candidate == actionID {
			return true
		}
	}
	return false
}

func cloneBundle(bundle Bundle) Bundle {
	clone := bundle
	clone.ActionIDs = append([]string(nil), bundle.ActionIDs...)
	clone.SecretTargets = append([]SecretTarget(nil), bundle.SecretTargets...)
	return clone
}

func (executor Executor) prepareSecrets(ctx context.Context, task *TaskV1, binding Binding, manifest cloudorchestrator.RecipeExecutionManifestV1, bundle Bundle) (SecretDelivery, func(), error) {
	if !executor.RequireSecretMaterialization {
		if len(manifest.SecretSlots) != 0 {
			return SecretDelivery{}, nil, ErrSecretScope
		}
		return SecretDelivery{}, nil, nil
	}
	if len(manifest.SecretSlots) == 0 && len(bundle.SecretTargets) == 0 {
		return SecretDelivery{}, nil, nil
	}
	if task == nil || executor.Materializer == nil || executor.SecretStager == nil || len(manifest.SecretSlots) != len(bundle.SecretTargets) {
		return SecretDelivery{}, nil, ErrSecretScope
	}
	bySlot := make(map[string]cloudorchestrator.SecretSlotV1, len(manifest.SecretSlots))
	for _, slot := range manifest.SecretSlots {
		bySlot[slot.SlotID] = slot
	}
	materialized := make([]MaterializedSecret, 0, len(bundle.SecretTargets))
	defer func() {
		for i := range materialized {
			clear(materialized[i].Value)
		}
	}()
	for _, target := range bundle.SecretTargets {
		slot, ok := bySlot[target.SlotID]
		if !ok {
			return SecretDelivery{}, nil, ErrSecretScope
		}
		value, err := executor.materializeSecret(ctx, SecretMaterializeRequest{
			TaskID: task.TaskID, ExecutionID: manifest.ExecutionID, ManifestDigest: binding.ManifestDigest,
			ArtifactDigest: manifest.ArtifactDigest, SlotID: target.SlotID, SecretRef: slot.SecretRef,
		})
		if err != nil {
			return SecretDelivery{}, nil, err
		}
		if len(value) == 0 {
			clear(value)
			return SecretDelivery{}, nil, ErrSecretMaterialize
		}
		materialized = append(materialized, MaterializedSecret{Target: target, Value: value})
	}
	delivery, cleanup, err := executor.SecretStager.Stage(ctx, manifest.DeploymentID, manifest.ExecutionID, materialized)
	if err != nil || cleanup == nil {
		if cleanup != nil {
			cleanup()
		}
		return SecretDelivery{}, nil, ErrSecretStage
	}
	return delivery, cleanup, nil
}

func (executor Executor) materializeSecret(ctx context.Context, request SecretMaterializeRequest) ([]byte, error) {
	delay := executor.SecretRetryDelay
	if delay <= 0 {
		delay = 250 * time.Millisecond
	}
	for {
		value, err := executor.Materializer.Materialize(ctx, request)
		if err == nil {
			return value, nil
		}
		clear(value)
		if !errors.Is(err, ErrSecretMaterializePending) {
			return nil, ErrSecretMaterialize
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
