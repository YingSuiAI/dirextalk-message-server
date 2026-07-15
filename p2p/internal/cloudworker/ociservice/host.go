package ociservice

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	podmanPath          = "/usr/bin/podman"
	containerLabel      = "io.dirextalk.binding"
	containerSecret     = "/run/secrets"
	containerInitTarget = "/run/dirextalk/container-init"
	maxProbeBody        = 1 << 20
)

var (
	containerNamePattern          = regexp.MustCompile(`^dtx-[0-9a-f]{24}$`)
	secretNamePattern             = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	storageSourcePattern          = regexp.MustCompile(`^` + regexp.QuoteMeta(StorageRoot) + `/[0-9a-f]{64}/(?:volumes|data)/[0-9a-f]{64}$`)
	serviceSecretPattern          = regexp.MustCompile(`^` + regexp.QuoteMeta(ServiceSecretRoot) + `/[0-9a-f]{64}/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	runtimeEnvironmentNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,63}$`)
	ociRuntimeSecretPathPattern   = regexp.MustCompile(`^/run/secrets/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	ErrProductionHost             = errors.New("OCI service production host is unavailable")
	ErrContainerBinding           = errors.New("existing OCI container has a different binding")
)

type commandRunner interface {
	Run(context.Context, string, []string) ([]byte, error)
}

type exitCoder interface{ ExitCode() int }

type podmanHost struct {
	uid                 int
	runner              commandRunner
	client              *http.Client
	ensureStorage       func(ContainerSpec) error
	refreshSecrets      func(ContainerSpec) error
	containerInitSource string
}

var _ Host = (*podmanHost)(nil)

func newPodmanHost(uid int, runner commandRunner) *podmanHost {
	transport := &http.Transport{
		Proxy:              nil,
		DialContext:        (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		TLSClientConfig:    &tls.Config{MinVersion: tls.VersionTLS12},
		DisableCompression: true,
	}
	return &podmanHost{uid: uid, runner: runner, ensureStorage: ensureProductionStorageDirectories, refreshSecrets: refreshProductionServiceSecrets, client: &http.Client{
		Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (host *podmanHost) EffectiveUID() int { return host.uid }

func (host *podmanHost) EnsurePinnedImage(ctx context.Context, source cloudorchestrator.OCIImageSourceReferenceV1, digest string) error {
	pinnedDigest, sourceErr := source.PinnedDigest()
	if host == nil || host.runner == nil || sourceErr != nil || !namedDigestPattern.MatchString(digest) || pinnedDigest != digest {
		return ErrProductionHost
	}
	_, err := host.runner.Run(ctx, podmanPath, []string{"image", "exists", "--", digest})
	if err == nil {
		return host.verifyImageDigest(ctx, digest, digest)
	}
	var code exitCoder
	if !errors.As(err, &code) || code.ExitCode() != 1 {
		return err
	}
	if _, err = host.runner.Run(ctx, podmanPath, []string{"image", "pull", "--tls-verify=true", "--", string(source)}); err != nil {
		host.discardPulledImage(digest)
		return err
	}
	if err = host.verifyImageDigest(ctx, string(source), digest); err != nil {
		host.discardPulledImage(digest)
		return err
	}
	repoDigests, err := host.runner.Run(ctx, podmanPath, []string{"image", "inspect", "--format={{range .RepoDigests}}{{println .}}{{end}}", "--", string(source)})
	if err != nil {
		host.discardPulledImage(digest)
		return ErrDescriptorMismatch
	}
	for _, candidate := range strings.Fields(string(repoDigests)) {
		if candidate == string(source) {
			return nil
		}
	}
	host.discardPulledImage(digest)
	return ErrDescriptorMismatch
}

func (host *podmanHost) discardPulledImage(digest string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = host.runner.Run(cleanupCtx, podmanPath, []string{"image", "rm", "--force", "--", digest})
}

func (host *podmanHost) verifyImageDigest(ctx context.Context, image, digest string) error {
	output, err := host.runner.Run(ctx, podmanPath, []string{"image", "inspect", "--format={{.Digest}}", "--", image})
	if err != nil || strings.TrimSpace(string(output)) != digest {
		return ErrDescriptorMismatch
	}
	return nil
}

func (host *podmanHost) EnsureContainer(ctx context.Context, spec ContainerSpec) error {
	if host == nil || host.uid != 0 || host.runner == nil || host.ensureStorage == nil || host.refreshSecrets == nil || validateContainerSpec(spec) != nil {
		return ErrProductionHost
	}
	if err := host.ensureStorage(spec); err != nil {
		return err
	}
	if err := host.RefreshServiceSecrets(ctx, spec); err != nil {
		return err
	}
	_, err := host.runner.Run(ctx, podmanPath, []string{"container", "exists", spec.Name})
	if err == nil {
		output, inspectErr := host.runner.Run(ctx, podmanPath, []string{"container", "inspect", "--format={{index .Config.Labels \"" + containerLabel + "\"}}", "--", spec.Name})
		if inspectErr != nil || strings.TrimSpace(string(output)) != spec.BindingDigest {
			return ErrContainerBinding
		}
		return nil
	}
	var code exitCoder
	if !errors.As(err, &code) || code.ExitCode() != 1 {
		return err
	}
	if len(spec.SecretEnvironment) != 0 && !validContainerInitSourcePath(host.containerInitSource) {
		return ErrProductionHost
	}
	_, err = host.runner.Run(ctx, podmanPath, createArgumentsWithInit(spec, host.containerInitSource))
	return err
}

func (host *podmanHost) RefreshServiceSecrets(ctx context.Context, spec ContainerSpec) error {
	if host == nil || host.uid != 0 || host.runner == nil || host.refreshSecrets == nil || validateContainerSpec(spec) != nil {
		return ErrProductionHost
	}
	if ctx == nil {
		return ErrProductionHost
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return host.refreshSecrets(spec)
}

func (host *podmanHost) StartContainer(ctx context.Context, name string) error {
	return host.runNamed(ctx, name, "start")
}

func (host *podmanHost) StopContainer(ctx context.Context, name string) error {
	if host == nil || host.runner == nil || !containerNamePattern.MatchString(name) {
		return ErrProductionHost
	}
	_, err := host.runner.Run(ctx, podmanPath, []string{"container", "stop", "--time=30", "--", name})
	return err
}

func (host *podmanHost) RemoveContainer(ctx context.Context, name string) error {
	if host == nil || host.runner == nil || !containerNamePattern.MatchString(name) {
		return ErrProductionHost
	}
	_, err := host.runner.Run(ctx, podmanPath, []string{"container", "rm", "--force", "--", name})
	return err
}

func (host *podmanHost) runNamed(ctx context.Context, name, action string) error {
	if host == nil || host.runner == nil || !containerNamePattern.MatchString(name) || action != "start" {
		return ErrProductionHost
	}
	_, err := host.runner.Run(ctx, podmanPath, []string{"container", action, "--", name})
	return err
}

func (host *podmanHost) ProbeLoopback(ctx context.Context, probe cloudorchestrator.OCIServiceLoopbackProbeV1) error {
	if host == nil || host.client == nil || validateProbe(probe) != nil {
		return ErrProductionHost
	}
	target := string(probe.Scheme) + "://127.0.0.1:" + strconv.Itoa(int(probe.Port)) + probe.Path
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil || request.URL.Hostname() != "127.0.0.1" {
		return ErrHealthFailed
	}
	response, err := host.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxProbeBody+1))
	if err != nil || len(body) > maxProbeBody || response.StatusCode != int(probe.ExpectedStatus) {
		return ErrHealthFailed
	}
	sum := sha256.Sum256(body)
	if "sha256:"+hex.EncodeToString(sum[:]) != probe.BodySHA256 {
		return ErrHealthFailed
	}
	return nil
}

func createArguments(spec ContainerSpec) []string {
	return createArgumentsWithInit(spec, "")
}

func createArgumentsWithInit(spec ContainerSpec, containerInitSource string) []string {
	arguments := []string{"container", "create", "--name=" + spec.Name, "--label=" + containerLabel + "=" + spec.BindingDigest,
		"--network=bridge", "--read-only", "--cap-drop=all", "--security-opt=no-new-privileges", "--restart=on-failure:5"}
	profile := spec.RuntimeProfile
	if profile != nil {
		for _, capability := range profile.Capabilities {
			arguments = append(arguments, "--cap-add="+string(capability))
		}
		for _, variable := range profile.Environment {
			arguments = append(arguments, "--env="+variable.Name+"="+variable.Value)
		}
		for _, mount := range profile.Tmpfs {
			arguments = append(arguments, "--tmpfs="+mount.ContainerTarget+":rw,noexec,nosuid,nodev,size="+strconv.FormatUint(mount.SizeBytes, 10)+",mode="+strconv.FormatUint(uint64(mount.Mode), 8))
		}
		if profile.RunAs != nil && len(spec.SecretEnvironment) == 0 {
			arguments = append(arguments, "--user="+strconv.FormatUint(uint64(profile.RunAs.UID), 10)+":"+strconv.FormatUint(uint64(profile.RunAs.GID), 10))
		}
		if profile.Entrypoint != "" && len(spec.SecretEnvironment) == 0 {
			arguments = append(arguments, "--entrypoint="+profile.Entrypoint)
		}
	}
	for _, port := range spec.LoopbackPorts {
		value := strconv.Itoa(int(port))
		arguments = append(arguments, "--publish=127.0.0.1:"+value+":"+value+"/tcp")
	}
	for _, mount := range spec.StorageMounts {
		arguments = append(arguments, "--mount=type=bind,source="+mount.Source+",target="+mount.Target+",readonly="+strconv.FormatBool(mount.ReadOnly)+",relabel=private")
	}
	if len(spec.SecretMounts) != 0 {
		arguments = append(arguments, "--mount=type=bind,source="+path.Dir(spec.SecretMounts[0].StableSource)+",target="+containerSecret+",readonly=true,relabel=private")
	}
	if len(spec.SecretEnvironment) != 0 {
		arguments = append(arguments, "--user=0:0", "--mount=type=bind,source="+containerInitSource+",target="+containerInitTarget+",readonly=true,relabel=shared", "--entrypoint="+containerInitTarget)
	}
	arguments = append(arguments, "--", spec.ImageDigest)
	if len(spec.SecretEnvironment) != 0 {
		arguments = append(arguments, "container-init")
		arguments = append(arguments, "--run-as="+strconv.FormatUint(uint64(profile.RunAs.UID), 10)+":"+strconv.FormatUint(uint64(profile.RunAs.GID), 10))
		for _, binding := range spec.SecretEnvironment {
			arguments = append(arguments, "--secret-env="+binding.EnvironmentKey+"="+binding.FilePath)
		}
		arguments = append(arguments, "--", profile.Entrypoint)
	}
	if profile != nil {
		arguments = append(arguments, profile.Argv...)
	}
	return arguments
}

func validateContainerSpec(spec ContainerSpec) error {
	if !containerNamePattern.MatchString(spec.Name) || !namedDigestPattern.MatchString(spec.BindingDigest) || !namedDigestPattern.MatchString(spec.ImageDigest) || len(spec.LoopbackPorts) == 0 || len(spec.LoopbackPorts) > 16 {
		return ErrProductionHost
	}
	if !sort.SliceIsSorted(spec.LoopbackPorts, func(i, j int) bool { return spec.LoopbackPorts[i] < spec.LoopbackPorts[j] }) {
		return ErrProductionHost
	}
	for index, port := range spec.LoopbackPorts {
		if port == 0 || index > 0 && spec.LoopbackPorts[index-1] == port {
			return ErrProductionHost
		}
	}
	seenTargets := map[string]struct{}{}
	seenSources := map[string]struct{}{}
	profile, err := cloudorchestrator.NormalizeOCIServiceRuntimeProfileV1(spec.RuntimeProfile)
	if err != nil || !reflect.DeepEqual(profile, spec.RuntimeProfile) {
		return ErrProductionHost
	}
	if profile != nil {
		for _, mount := range profile.Tmpfs {
			if _, duplicate := seenTargets[mount.ContainerTarget]; duplicate {
				return ErrProductionHost
			}
			seenTargets[mount.ContainerTarget] = struct{}{}
		}
	}
	if !sort.SliceIsSorted(spec.StorageMounts, func(i, j int) bool { return spec.StorageMounts[i].Target < spec.StorageMounts[j].Target }) {
		return ErrProductionHost
	}
	for _, mount := range spec.StorageMounts {
		if !storageSourcePattern.MatchString(mount.Source) || cloudorchestrator.ValidateOCIServiceContainerTarget(mount.Target) != nil || mount.OwnerUID > 65535 || mount.OwnerGID > 65535 {
			return ErrProductionHost
		}
		if _, err := cloudorchestrator.NormalizeOCIServiceStorageDirectoryMode(mount.DirectoryMode); err != nil {
			return ErrProductionHost
		}
		if _, duplicate := seenSources[mount.Source]; duplicate {
			return ErrProductionHost
		}
		if _, duplicate := seenTargets[mount.Target]; duplicate {
			return ErrProductionHost
		}
		seenSources[mount.Source], seenTargets[mount.Target] = struct{}{}, struct{}{}
	}
	if !sort.SliceIsSorted(spec.SecretMounts, func(i, j int) bool { return spec.SecretMounts[i].Target < spec.SecretMounts[j].Target }) {
		return ErrProductionHost
	}
	stableDirectory := ""
	seenStagedSources, seenStableSources := map[string]struct{}{}, map[string]struct{}{}
	for _, mount := range spec.SecretMounts {
		name := path.Base(mount.Target)
		if !validStagedPath(mount.StagedSource, name) || !stringsHasPathPrefix(mount.StagedSource, SecretStagingRoot) || !serviceSecretPattern.MatchString(mount.StableSource) || path.Base(mount.StableSource) != name || !secretNamePattern.MatchString(name) || mount.Target != containerSecret+"/"+name {
			return ErrProductionHost
		}
		if stableDirectory == "" {
			stableDirectory = path.Dir(mount.StableSource)
		} else if path.Dir(mount.StableSource) != stableDirectory {
			return ErrProductionHost
		}
		if _, duplicate := seenStagedSources[mount.StagedSource]; duplicate {
			return ErrProductionHost
		}
		if _, duplicate := seenStableSources[mount.StableSource]; duplicate {
			return ErrProductionHost
		}
		if _, duplicate := seenTargets[mount.Target]; duplicate {
			return ErrProductionHost
		}
		seenStagedSources[mount.StagedSource], seenStableSources[mount.StableSource], seenTargets[mount.Target] = struct{}{}, struct{}{}, struct{}{}
	}
	if profile == nil && len(spec.SecretEnvironment) != 0 || profile != nil && len(spec.SecretEnvironment) != len(profile.SecretEnvironment) {
		return ErrProductionHost
	}
	profileSecretEnvironment := make(map[string]struct{}, len(spec.SecretEnvironment))
	if profile != nil {
		for _, binding := range profile.SecretEnvironment {
			profileSecretEnvironment[binding.EnvironmentKey] = struct{}{}
		}
	}
	secretFiles := make(map[string]struct{}, len(spec.SecretMounts))
	for _, mount := range spec.SecretMounts {
		secretFiles[mount.Target] = struct{}{}
	}
	if profile != nil {
		for _, variable := range profile.Environment {
			if strings.HasSuffix(variable.Name, "_FILE") {
				if _, ok := secretFiles[variable.Value]; !ok {
					return ErrProductionHost
				}
			}
		}
	}
	if !sort.SliceIsSorted(spec.SecretEnvironment, func(i, j int) bool {
		return spec.SecretEnvironment[i].EnvironmentKey < spec.SecretEnvironment[j].EnvironmentKey
	}) {
		return ErrProductionHost
	}
	for index, binding := range spec.SecretEnvironment {
		if index > 0 && spec.SecretEnvironment[index-1].EnvironmentKey == binding.EnvironmentKey || !runtimeEnvironmentNamePattern.MatchString(binding.EnvironmentKey) || !ociRuntimeSecretPathPattern.MatchString(binding.FilePath) {
			return ErrProductionHost
		}
		if _, ok := profileSecretEnvironment[binding.EnvironmentKey]; !ok {
			return ErrProductionHost
		}
		if _, ok := secretFiles[binding.FilePath]; !ok {
			return ErrProductionHost
		}
	}
	return nil
}

func validContainerInitSourcePath(value string) bool {
	return path.IsAbs(value) && path.Clean(value) == value && value != "/" && !strings.ContainsAny(value, "\x00\r\n,")
}

func validStagedPath(value, basename string) bool {
	return path.IsAbs(value) && path.Clean(value) == value && path.Base(value) == basename && basename != "." && basename != ".."
}

func validateProbe(probe cloudorchestrator.OCIServiceLoopbackProbeV1) error {
	if probe.Scheme != cloudorchestrator.OCIServiceProbeHTTP && probe.Scheme != cloudorchestrator.OCIServiceProbeHTTPS || probe.Port == 0 || probe.ExpectedStatus < 100 || probe.ExpectedStatus > 599 || !namedDigestPattern.MatchString(probe.BodySHA256) {
		return ErrHealthFailed
	}
	parsed, err := url.ParseRequestURI(probe.Path)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || !strings.HasPrefix(probe.Path, "/") || strings.ContainsAny(probe.Path, "#\r\n") {
		return ErrHealthFailed
	}
	return nil
}
