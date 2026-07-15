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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	podmanPath      = "/usr/bin/podman"
	containerLabel  = "io.dirextalk.binding"
	containerSecret = "/run/secrets"
	maxProbeBody    = 1 << 20
)

var (
	containerNamePattern = regexp.MustCompile(`^dtx-[0-9a-f]{24}$`)
	secretNamePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	ErrProductionHost    = errors.New("OCI service production host is unavailable")
	ErrContainerBinding  = errors.New("existing OCI container has a different binding")
)

type commandRunner interface {
	Run(context.Context, string, []string) ([]byte, error)
}

type exitCoder interface{ ExitCode() int }

type podmanHost struct {
	uid    int
	runner commandRunner
	client *http.Client
}

var _ Host = (*podmanHost)(nil)

func newPodmanHost(uid int, runner commandRunner) *podmanHost {
	transport := &http.Transport{
		Proxy:              nil,
		DialContext:        (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		TLSClientConfig:    &tls.Config{MinVersion: tls.VersionTLS12},
		DisableCompression: true,
	}
	return &podmanHost{uid: uid, runner: runner, client: &http.Client{
		Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (host *podmanHost) EffectiveUID() int { return host.uid }

func (host *podmanHost) VerifyPinnedImage(ctx context.Context, digest string) error {
	if host == nil || host.runner == nil || !namedDigestPattern.MatchString(digest) {
		return ErrProductionHost
	}
	output, err := host.runner.Run(ctx, podmanPath, []string{"image", "inspect", "--format={{.Digest}}", "--", digest})
	if err != nil || strings.TrimSpace(string(output)) != digest {
		return ErrDescriptorMismatch
	}
	return nil
}

func (host *podmanHost) EnsureContainer(ctx context.Context, spec ContainerSpec) error {
	if host == nil || host.runner == nil || validateContainerSpec(spec) != nil {
		return ErrProductionHost
	}
	if err := validateProductionSecretFiles(spec); err != nil {
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
	_, err = host.runner.Run(ctx, podmanPath, createArguments(spec))
	return err
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
	arguments := []string{"container", "create", "--name=" + spec.Name, "--label=" + containerLabel + "=" + spec.BindingDigest,
		"--network=bridge", "--read-only", "--cap-drop=all", "--security-opt=no-new-privileges", "--restart=on-failure:5"}
	for _, port := range spec.LoopbackPorts {
		value := strconv.Itoa(int(port))
		arguments = append(arguments, "--publish=127.0.0.1:"+value+":"+value+"/tcp")
	}
	for _, mount := range spec.SecretMounts {
		arguments = append(arguments, "--mount=type=bind,source="+mount.Source+",target="+mount.Target+",readonly=true,relabel=private")
	}
	return append(arguments, "--", spec.ImageDigest)
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
	for _, mount := range spec.SecretMounts {
		name := path.Base(mount.Target)
		if !validStagedPath(mount.Source, path.Base(mount.Source)) || !stringsHasPathPrefix(mount.Source, SecretStagingRoot) || !secretNamePattern.MatchString(name) || mount.Target != containerSecret+"/"+name {
			return ErrProductionHost
		}
		if _, duplicate := seenTargets[mount.Target]; duplicate {
			return ErrProductionHost
		}
		seenTargets[mount.Target] = struct{}{}
	}
	return nil
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
