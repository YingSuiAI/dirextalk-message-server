package ociservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const (
	CheckpointArtifactVerified = cloudorchestrator.OCIServiceCheckpointArtifactVerified
	CheckpointContainerCreated = cloudorchestrator.OCIServiceCheckpointContainerCreated
	CheckpointContainerStarted = cloudorchestrator.OCIServiceCheckpointContainerStarted
	CheckpointHealthVerified   = cloudorchestrator.OCIServiceCheckpointHealthVerified

	// SecretStagingRoot is the only host root from which the production Driver
	// accepts staged secrets. The Worker wiring must configure FileSecretStager
	// with this exact root.
	SecretStagingRoot = "/run/dirextalk/cloud-worker/secrets"
)

var (
	ErrDriverConfiguration = errors.New("OCI service driver is not configured")
	ErrUnsupportedScope    = errors.New("OCI service recipe scope is unsupported")
	ErrRootRequired        = errors.New("OCI service installation requires approved root execution")
	ErrDescriptorMismatch  = errors.New("OCI service descriptor does not bind the action request")
	ErrHealthFailed        = errors.New("OCI service loopback health probe failed")

	namedDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	identifierPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// DescriptorResolver returns only a previously authenticated, content-addressed
// descriptor. It cannot return a command, registry URL, mutable tag, or host path.
type DescriptorResolver interface {
	LookupDescriptor(context.Context, string) (cloudorchestrator.OCIServiceBundleV1, error)
}

// ContainerSpec is the complete typed create boundary. Ports are loopback-only
// and SecretMounts are read-only; Host implementations have no generic argv API.
type ContainerSpec struct {
	Name          string
	BindingDigest string
	ImageDigest   string
	LoopbackPorts []uint16
	SecretMounts  []SecretMount
}

type SecretMount struct {
	Source string
	Target string
}

// Host is the only privileged boundary. Every operation is typed and scoped to
// one deterministic container; there is deliberately no shell or command hook.
type Host interface {
	EffectiveUID() int
	VerifyPinnedImage(context.Context, string) error
	EnsureContainer(context.Context, ContainerSpec) error
	StartContainer(context.Context, string) error
	StopContainer(context.Context, string) error
	RemoveContainer(context.Context, string) error
	ProbeLoopback(context.Context, cloudorchestrator.OCIServiceLoopbackProbeV1) error
}

type Driver struct {
	resolver         DescriptorResolver
	host             Host
	secretValidator  func(recipeexec.SecretDelivery) error
	healthRetryDelay time.Duration
	waitHealthRetry  func(context.Context, time.Duration) error
}

var _ recipeexec.ActionDriver = (*Driver)(nil)

func NewDriver(resolver DescriptorResolver, host Host) *Driver {
	return &Driver{resolver: resolver, host: host, secretValidator: recipeexec.ValidateStagedSecretDelivery, healthRetryDelay: 250 * time.Millisecond, waitHealthRetry: waitForHealthRetry}
}

func (driver *Driver) Execute(ctx context.Context, request recipeexec.ActionRequest, reporter recipeexec.CheckpointReporter) error {
	if driver == nil || driver.resolver == nil || driver.host == nil || driver.secretValidator == nil || driver.waitHealthRetry == nil || reporter == nil {
		return recipeexec.PermanentExecutionFailure(ErrDriverConfiguration)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !request.RootRequired || driver.host.EffectiveUID() != 0 {
		return recipeexec.PermanentExecutionFailure(ErrRootRequired)
	}
	if !validRequestIdentity(request) || len(request.VolumeSlots) != 0 || len(request.DataSlots) != 0 {
		return recipeexec.PermanentExecutionFailure(ErrUnsupportedScope)
	}

	bundle, err := driver.resolver.LookupDescriptor(ctx, request.Artifact.ArtifactDigest)
	if err != nil {
		return recipeexec.PermanentExecutionFailure(ErrDescriptorMismatch)
	}
	install, ok := bundle.Action(cloudorchestrator.CompiledRecipeActionInstall)
	if !ok || !descriptorBindsRequest(bundle, install, request) {
		return recipeexec.PermanentExecutionFailure(ErrDescriptorMismatch)
	}
	resumeIndex, ok := checkpointIndex(request.ResumeAfter)
	if !ok {
		return recipeexec.PermanentExecutionFailure(ErrUnsupportedScope)
	}
	container, err := containerSpec(request, bundle, driver.secretValidator)
	if err != nil {
		return recipeexec.PermanentExecutionFailure(err)
	}

	if resumeIndex < 0 {
		if err := driver.host.VerifyPinnedImage(ctx, bundle.ImageDigest); err != nil {
			return fmt.Errorf("verify pinned OCI image: %w", err)
		}
		if err := reporter.Checkpoint(ctx, CheckpointArtifactVerified); err != nil {
			return err
		}
	}
	if resumeIndex < 1 {
		if err := driver.host.EnsureContainer(ctx, container); err != nil {
			return fmt.Errorf("ensure typed OCI container: %w", err)
		}
		if err := reporter.Checkpoint(ctx, CheckpointContainerCreated); err != nil {
			return err
		}
	}
	if resumeIndex < 2 {
		if err := driver.host.StartContainer(ctx, container.Name); err != nil {
			return fmt.Errorf("start typed OCI container: %w", err)
		}
		if err := reporter.Checkpoint(ctx, CheckpointContainerStarted); err != nil {
			return err
		}
	}
	if resumeIndex < 3 {
		if err := driver.waitForHealth(ctx, request.Timeout, bundle.Health); err != nil {
			return err
		}
		return reporter.Checkpoint(ctx, CheckpointHealthVerified)
	}
	return nil
}

func (driver *Driver) waitForHealth(ctx context.Context, timeout time.Duration, health cloudorchestrator.OCIServiceHealthV1) error {
	healthContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	delay := driver.healthRetryDelay
	if delay <= 0 {
		delay = 250 * time.Millisecond
	}
	probes := []cloudorchestrator.OCIServiceLoopbackProbeV1{health.Liveness, health.Readiness, health.Semantic}
	for {
		allHealthy := true
		for _, probe := range probes {
			if err := driver.host.ProbeLoopback(healthContext, probe); err != nil {
				allHealthy = false
				break
			}
		}
		if allHealthy {
			return nil
		}
		if err := driver.waitHealthRetry(healthContext, delay); err != nil {
			return fmt.Errorf("%w: %w", ErrHealthFailed, err)
		}
	}
}

func waitForHealthRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validRequestIdentity(request recipeexec.ActionRequest) bool {
	return identifierPattern.MatchString(request.Binding.ExecutionID) && namedDigestPattern.MatchString(request.Binding.ManifestDigest) &&
		identifierPattern.MatchString(request.DeploymentID) && namedDigestPattern.MatchString(request.Artifact.ArtifactDigest) &&
		request.ActionID != "" && request.Timeout > 0
}

func descriptorBindsRequest(bundle cloudorchestrator.OCIServiceBundleV1, install cloudorchestrator.CompiledRecipeActionV1, request recipeexec.ActionRequest) bool {
	if bundle.Validate() != nil || bundle.ArtifactDigest != request.Artifact.ArtifactDigest || bundle.ImageDigest != request.Artifact.ArtifactDigest ||
		install.ActionID != request.ActionID || !install.RootRequired || request.Timeout != time.Duration(install.TimeoutSeconds)*time.Second ||
		!sameStrings(install.CheckpointSequence, cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()) || !secretTargetsBindSlots(request.Artifact.SecretTargets, request.SecretSlots) {
		return false
	}
	actions := make([]string, len(bundle.Actions))
	for index := range bundle.Actions {
		actions[index] = bundle.Actions[index].ActionID
	}
	return sameStringSet(actions, request.Artifact.ActionIDs)
}

func secretTargetsBindSlots(targets []recipeexec.SecretTarget, slots []cloudorchestrator.SecretSlotV1) bool {
	if len(targets) != len(slots) {
		return false
	}
	want := make([]string, len(targets))
	actual := make([]string, len(slots))
	for index := range targets {
		want[index] = targets[index].SlotID
	}
	for index := range slots {
		actual[index] = slots[index].SlotID
	}
	return sameStringSet(want, actual)
}

func containerSpec(request recipeexec.ActionRequest, bundle cloudorchestrator.OCIServiceBundleV1, validateSecrets func(recipeexec.SecretDelivery) error) (ContainerSpec, error) {
	if validateSecrets == nil || validateSecrets(request.Secrets) != nil {
		return ContainerSpec{}, ErrUnsupportedScope
	}
	binding := sha256.Sum256([]byte("dirextalk.oci-service-container/v1\x00" + request.Binding.ExecutionID + "\x00" + request.Binding.ManifestDigest + "\x00" + request.DeploymentID + "\x00" + bundle.ArtifactDigest))
	bindingDigest := "sha256:" + hex.EncodeToString(binding[:])
	ports := []uint16{bundle.Health.Liveness.Port, bundle.Health.Readiness.Port, bundle.Health.Semantic.Port}
	sort.Slice(ports, func(i, j int) bool { return ports[i] < ports[j] })
	ports = compactPorts(ports)
	spec := ContainerSpec{Name: "dtx-" + hex.EncodeToString(binding[:12]), BindingDigest: bindingDigest, ImageDigest: bundle.ImageDigest, LoopbackPorts: ports}
	directory := path.Join(SecretStagingRoot, request.DeploymentID+"-"+request.Binding.ExecutionID)
	if directory == SecretStagingRoot || !stringsHasPathPrefix(directory, SecretStagingRoot) || request.Secrets.StagingDirectory != "" && request.Secrets.StagingDirectory != directory {
		return ContainerSpec{}, ErrUnsupportedScope
	}
	seenSlots, seenFiles := map[string]struct{}{}, map[string]struct{}{}
	for _, target := range request.Artifact.SecretTargets {
		if _, duplicate := seenSlots[target.SlotID]; duplicate {
			return ContainerSpec{}, ErrUnsupportedScope
		}
		seenSlots[target.SlotID] = struct{}{}
		switch {
		case target.FileName != "" && target.EnvironmentKey == "":
			source, exists := request.Secrets.Files[target.SlotID]
			wantSource := path.Join(directory, target.FileName)
			if !exists || source != wantSource || path.Base(target.FileName) != target.FileName || !secretNamePattern.MatchString(target.FileName) {
				return ContainerSpec{}, ErrUnsupportedScope
			}
			if _, duplicate := seenFiles[target.FileName]; duplicate {
				return ContainerSpec{}, ErrUnsupportedScope
			}
			seenFiles[target.FileName] = struct{}{}
			spec.SecretMounts = append(spec.SecretMounts, SecretMount{Source: source, Target: containerSecret + "/" + target.FileName})
		default:
			return ContainerSpec{}, ErrUnsupportedScope
		}
	}
	if len(request.Secrets.Files) != len(spec.SecretMounts) || request.Secrets.EnvironmentFile != "" {
		return ContainerSpec{}, ErrUnsupportedScope
	}
	if len(request.Artifact.SecretTargets) == 0 && (len(request.SecretSlots) != 0 || request.Secrets.StagingDirectory != "" || len(request.Secrets.Files) != 0 || request.Secrets.EnvironmentFile != "") {
		return ContainerSpec{}, ErrUnsupportedScope
	}
	sort.Slice(spec.SecretMounts, func(i, j int) bool { return spec.SecretMounts[i].Target < spec.SecretMounts[j].Target })
	return spec, nil
}

func stringsHasPathPrefix(value, root string) bool {
	return value != root && len(value) > len(root) && value[:len(root)] == root && value[len(root)] == '/'
}

func checkpointIndex(checkpoint string) (int, bool) {
	if checkpoint == "" {
		return -1, true
	}
	for index, candidate := range cloudorchestrator.OCIServiceInstallCheckpointSequenceV1() {
		if checkpoint == candidate {
			return index, true
		}
	}
	return -1, false
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	left, right = append([]string(nil), left...), append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return sameStrings(left, right)
}

func compactPorts(values []uint16) []uint16 {
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}
