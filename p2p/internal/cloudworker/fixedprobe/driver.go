// Package fixedprobe implements the first deliberately non-business Recipe
// action. Every privileged parameter is compiled into the Worker: a task can
// select neither a command, filesystem path, port, unit name, nor URL.
package fixedprobe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const (
	// ActionID aliases the action in recipeexec's compiled bundle descriptor so
	// the resolver and driver cannot drift to different action identifiers.
	ActionID = recipeexec.FixedProbeActionID

	CheckpointUnitInstalled  = "probe_unit_installed"
	CheckpointServiceStarted = "probe_service_started"
	CheckpointHealthVerified = "probe_health_verified"

	SystemctlPath        = "/usr/bin/systemctl"
	UnitName             = "dirextalk-cloud-worker-probe.service"
	UnitPath             = "/etc/systemd/system/dirextalk-cloud-worker-probe.service"
	ProbePath            = "/usr/local/libexec/dirextalk-cloud-worker-probe-service"
	ReadinessURL         = "http://127.0.0.1:18080/ready"
	ReadinessContentType = "application/json"
	ReadinessBody        = `{"schema":"dirextalk.fixed-probe-readiness/v1","status":"ready"}`

	// UnitContents is immutable and contains no task-derived interpolation.
	UnitContents = `[Unit]
Description=Dirextalk Cloud Worker fixed recipe probe
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/libexec/dirextalk-cloud-worker-probe-service
Restart=on-failure
DynamicUser=yes
NoNewPrivileges=true
PrivateTmp=true
PrivateDevices=true
ProtectHome=true
ProtectSystem=strict
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET
RestrictNamespaces=true
LockPersonality=true
CapabilityBoundingSet=

[Install]
WantedBy=multi-user.target
`
)

var (
	ErrDriverConfiguration = errors.New("fixed probe driver is not configured")
	ErrUnsupportedScope    = errors.New("fixed probe recipe scope is unsupported")
	ErrRootRequired        = errors.New("fixed probe installation requires approved root execution")
	ErrReadinessFailed     = errors.New("fixed probe loopback readiness failed")
)

var fixedCheckpointSequence = []string{
	CheckpointUnitInstalled,
	CheckpointServiceStarted,
	CheckpointHealthVerified,
}

// CheckpointSequence returns a defensive copy of the only sequence accepted
// by this driver. Plan construction must seal this exact sequence.
func CheckpointSequence() []string {
	return append([]string(nil), fixedCheckpointSequence...)
}

// Host is the narrow privileged boundary. Driver always supplies fixed
// constants to these methods; the Recipe task has no corresponding fields.
type Host interface {
	EffectiveUID() int
	WriteFile(path string, contents []byte, mode fs.FileMode) error
	Run(ctx context.Context, executable string, arguments ...string) error
	CheckLoopback(ctx context.Context, url string) error
}

// Driver installs and verifies only the compiled non-business probe service.
// It implements recipeexec.ActionDriver.
type Driver struct{ host Host }

func NewDriver(host Host) *Driver { return &Driver{host: host} }

// NewProductionDriver performs all local trust checks before a Worker is
// allowed to claim a Recipe task. Missing, symlinked, non-regular, or
// non-executable fixed binaries fail closed.
func NewProductionDriver() (*Driver, error) {
	if err := validateProductionHost(runtime.GOOS, os.Geteuid(), os.Lstat); err != nil {
		return nil, err
	}
	return NewDriver(NewLocalHost()), nil
}

func (driver *Driver) Execute(ctx context.Context, request recipeexec.ActionRequest, reporter recipeexec.CheckpointReporter) error {
	if driver == nil || driver.host == nil || reporter == nil {
		return recipeexec.PermanentExecutionFailure(ErrDriverConfiguration)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resumeIndex, ok := checkpointIndex(request.ResumeAfter)
	if request.ActionID != recipeexec.FixedProbeActionID || !fixedBundle(request.Artifact) || !ok || len(request.VolumeSlots) != 0 || len(request.DataSlots) != 0 || len(request.SecretSlots) != 0 {
		return recipeexec.PermanentExecutionFailure(ErrUnsupportedScope)
	}
	if !request.RootRequired || driver.host.EffectiveUID() != 0 {
		return recipeexec.PermanentExecutionFailure(ErrRootRequired)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if resumeIndex < 0 {
		if err := driver.host.WriteFile(UnitPath, []byte(UnitContents), 0o644); err != nil {
			return fmt.Errorf("write fixed probe unit: %w", err)
		}
		if err := driver.host.Run(ctx, SystemctlPath, "daemon-reload"); err != nil {
			return fmt.Errorf("reload fixed systemd units: %w", err)
		}
		if err := reporter.Checkpoint(ctx, CheckpointUnitInstalled); err != nil {
			return err
		}
	}
	if resumeIndex < 1 {
		if err := driver.host.Run(ctx, SystemctlPath, "enable", "--now", UnitName); err != nil {
			return fmt.Errorf("start fixed probe unit: %w", err)
		}
		if err := reporter.Checkpoint(ctx, CheckpointServiceStarted); err != nil {
			return err
		}
	}
	if resumeIndex < 2 {
		if err := driver.host.CheckLoopback(ctx, ReadinessURL); err != nil {
			return fmt.Errorf("%w: %w", ErrReadinessFailed, err)
		}
		if err := reporter.Checkpoint(ctx, CheckpointHealthVerified); err != nil {
			return err
		}
	}
	return nil
}

func checkpointIndex(checkpoint string) (int, bool) {
	if checkpoint == "" {
		return -1, true
	}
	for index, candidate := range fixedCheckpointSequence {
		if checkpoint == candidate {
			return index, true
		}
	}
	return -1, false
}

func fixedBundle(bundle recipeexec.Bundle) bool {
	want := recipeexec.FixedProbeBundle()
	return bundle.ArtifactDigest == want.ArtifactDigest && len(bundle.ActionIDs) == 1 && bundle.ActionIDs[0] == recipeexec.FixedProbeActionID
}

// LocalHost is the production Linux backend. It invokes an absolute systemctl
// binary directly (never a shell) and probes only the compiled loopback URL.
type LocalHost struct {
	client *http.Client
}

func NewLocalHost() *LocalHost {
	transport := &http.Transport{
		Proxy:              nil,
		DialContext:        (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		DisableCompression: true,
	}
	return &LocalHost{client: &http.Client{
		Transport: transport,
		Timeout:   3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func (*LocalHost) EffectiveUID() int { return os.Geteuid() }

func (*LocalHost) WriteFile(path string, contents []byte, mode fs.FileMode) error {
	if path != UnitPath || string(contents) != UnitContents || mode != 0o644 {
		return ErrUnsupportedScope
	}
	if info, err := os.Lstat(UnitPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsupportedScope
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.WriteFile(UnitPath, []byte(UnitContents), 0o644); err != nil {
		return err
	}
	return os.Chmod(UnitPath, 0o644)
}

func (*LocalHost) Run(ctx context.Context, executable string, arguments ...string) error {
	if executable != SystemctlPath || !fixedSystemctlArguments(arguments) {
		return ErrUnsupportedScope
	}
	return exec.CommandContext(ctx, SystemctlPath, arguments...).Run()
}

func (host *LocalHost) CheckLoopback(ctx context.Context, url string) error {
	if host == nil || host.client == nil || url != ReadinessURL {
		return ErrDriverConfiguration
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, ReadinessURL, nil)
	if err != nil {
		return err
	}
	response, err := host.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: HTTP %d", ErrReadinessFailed, response.StatusCode)
	}
	if response.Header.Get("Content-Type") != ReadinessContentType {
		return fmt.Errorf("%w: unexpected content type", ErrReadinessFailed)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(len(ReadinessBody)+1)))
	if err != nil {
		return fmt.Errorf("%w: read response: %v", ErrReadinessFailed, err)
	}
	if string(body) != ReadinessBody {
		return fmt.Errorf("%w: unexpected response body", ErrReadinessFailed)
	}
	return nil
}

func fixedSystemctlArguments(arguments []string) bool {
	if len(arguments) == 1 && arguments[0] == "daemon-reload" {
		return true
	}
	return len(arguments) == 3 && arguments[0] == "enable" && arguments[1] == "--now" && arguments[2] == UnitName
}

func validateProductionHost(goos string, effectiveUID int, stat func(string) (fs.FileInfo, error)) error {
	if goos != "linux" || effectiveUID != 0 || stat == nil {
		return ErrDriverConfiguration
	}
	for _, path := range []string{SystemctlPath, ProbePath} {
		info, err := stat(path)
		if err != nil {
			return fmt.Errorf("%w: fixed executable %q: %v", ErrDriverConfiguration, path, err)
		}
		mode := info.Mode()
		if !mode.IsRegular() || mode&fs.ModeSymlink != 0 || mode.Perm()&0o111 == 0 {
			return fmt.Errorf("%w: fixed executable %q is not a non-symlink executable regular file", ErrDriverConfiguration, path)
		}
	}
	return nil
}
