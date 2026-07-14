// cloud-worker is the deliberately narrow outbound bootstrap process for one
// exclusive Cloud Worker VM. This first stage performs no recipe execution,
// shell invocation, Docker control, AWS SDK call, or cloud mutation.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker"
)

const (
	bootstrapManifestFileEnv = "CLOUD_WORKER_BOOTSTRAP_MANIFEST_FILE"
	expectedConnectionIDEnv  = "CLOUD_WORKER_EXPECTED_CONNECTION_ID"
	expectedEndpointEnv      = "CLOUD_WORKER_EXPECTED_BOOTSTRAP_ENDPOINT"
)

var (
	errConfigInvalid = errors.New("cloud worker configuration is invalid")
	errRunFailed     = errors.New("cloud worker stopped with an error")
)

type commandConfig struct {
	manifestFile       string
	expectedConnection string
	expectedEndpoint   string
	once               bool
	heartbeatInterval  time.Duration
}

type identityProofProvider interface {
	Fetch(context.Context) (cloudworker.InstanceIdentityProof, error)
}

// workerSessionClient is deliberately the small Worker protocol surface that
// the process needs. Keeping it here lets the command own cancellation and
// restart behavior without turning the cloudworker transport into a Recipe
// executor or configurable command runner.
type workerSessionClient interface {
	Claim(context.Context, cloudworker.InstanceIdentityProof) error
	Heartbeat(context.Context) error
	RetryPending(context.Context) error
	RenewIfDue(context.Context, cloudworker.InstanceIdentityProof) error
	ClaimTask(context.Context) (cloudworker.WorkerTask, bool, error)
	RetryPendingTask(context.Context) error
	ReportTask(context.Context, cloudworker.WorkerTask, cloudworker.TaskStatus, string, string, string) error
	Close()
}

type workerSessionClientFactory func(cloudworker.BootstrapManifest, cloudworker.SessionClientConfig) (workerSessionClient, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config, err := parseConfig(os.Args[1:], os.Getenv)
	if err != nil {
		log.Print("cloud-worker: config_invalid")
		os.Exit(2)
	}
	if err := run(ctx, config); err != nil {
		log.Print("cloud-worker: stopped_with_error")
		os.Exit(1)
	}
}

func parseConfig(args []string, getenv func(string) string) (commandConfig, error) {
	if getenv == nil {
		return commandConfig{}, errConfigInvalid
	}
	config := commandConfig{
		manifestFile:       strings.TrimSpace(getenv(bootstrapManifestFileEnv)),
		expectedConnection: strings.TrimSpace(getenv(expectedConnectionIDEnv)),
		expectedEndpoint:   strings.TrimSpace(getenv(expectedEndpointEnv)),
		heartbeatInterval:  30 * time.Second,
	}
	flags := flag.NewFlagSet("cloud-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&config.once, "once", false, "claim one session, send one heartbeat, and process at most one fixed transport probe")
	flags.DurationVar(&config.heartbeatInterval, "heartbeat-interval", config.heartbeatInterval, "outbound heartbeat interval")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return commandConfig{}, errConfigInvalid
	}
	if !validConfigPath(config.manifestFile) || config.expectedConnection == "" || config.expectedEndpoint == "" ||
		config.heartbeatInterval <= 0 {
		return commandConfig{}, errConfigInvalid
	}
	return config, nil
}

func run(ctx context.Context, config commandConfig) error {
	provider, err := cloudworker.NewIMDSv2IdentityProvider()
	if err != nil {
		return errConfigInvalid
	}
	return runWithIdentityProvider(ctx, config, provider)
}

func runWithIdentityProvider(ctx context.Context, config commandConfig, provider identityProofProvider) error {
	return runWithDependencies(ctx, config, provider,
		func(manifest cloudworker.BootstrapManifest, sessionConfig cloudworker.SessionClientConfig) (workerSessionClient, error) {
			return cloudworker.NewSessionClient(manifest, sessionConfig)
		},
		time.Now,
	)
}

func runWithDependencies(
	ctx context.Context,
	config commandConfig,
	provider identityProofProvider,
	newSessionClient workerSessionClientFactory,
	now func() time.Time,
) error {
	if ctx == nil || provider == nil || newSessionClient == nil || now == nil {
		return errConfigInvalid
	}
	manifestBytes, err := readRegularFile(config.manifestFile, 64*1024)
	if err != nil {
		return errConfigInvalid
	}
	manifest, err := cloudworker.ParseBootstrapManifest(manifestBytes, cloudworker.ManifestValidationContext{
		Now:                       now().UTC(),
		MaxLifetime:               10 * time.Minute,
		ExpectedConnectionID:      config.expectedConnection,
		ExpectedBootstrapEndpoint: config.expectedEndpoint,
		AllowExpired:              true,
	})
	if err != nil {
		return errConfigInvalid
	}
	client, err := newSessionClient(manifest, cloudworker.SessionClientConfig{
		ExpectedConnectionID:      config.expectedConnection,
		ExpectedBootstrapEndpoint: config.expectedEndpoint,
		Now:                       now,
		AllowExpiredBootstrap:     true,
	})
	if err != nil || client == nil {
		return errConfigInvalid
	}
	defer client.Close()
	if ctx.Err() != nil {
		return nil
	}
	proof, err := provider.Fetch(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return errRunFailed
	}
	if err := client.Claim(ctx, proof); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return errRunFailed
	}
	if config.once {
		if err := runWorkerCycle(ctx, client, proof, true); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return errRunFailed
		}
		return nil
	}
	ticker := time.NewTicker(config.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_ = runWorkerCycle(ctx, client, proof, false)
			if ctx.Err() != nil {
				return nil
			}
		}
	}
}

// runWorkerCycle keeps regular session telemetry independent from the fixed
// task transport. In non-once mode each transient transport failure is left
// for a later retry; it never starts a shell, container, Recipe, AWS call, or
// dynamic installer as a fallback.
func runWorkerCycle(ctx context.Context, client workerSessionClient, proof cloudworker.InstanceIdentityProof, failFast bool) error {
	if err := client.Heartbeat(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = client.RetryPending(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if failFast {
			return err
		}
	}
	if err := client.RetryPendingTask(ctx); err != nil && !errors.Is(err, cloudworker.ErrNoPendingTaskEvent) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if failFast {
			return err
		}
	}
	if err := client.RenewIfDue(ctx, proof); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if failFast {
			return err
		}
	}
	if err := processFixedTaskProbe(ctx, client); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if failFast {
			return err
		}
	}
	return nil
}

// processFixedTaskProbe is intentionally the entire execution behavior of
// this non-root process. execution_probe reports receipt and transport
// completion for a digest-only task; it never asserts that a user service is
// installed, ready, paired, or healthy.
func processFixedTaskProbe(ctx context.Context, client workerSessionClient) error {
	task, found, err := client.ClaimTask(ctx)
	if err != nil || !found {
		return err
	}
	if task.TaskKind != cloudworker.TaskKindExecutionProbe {
		return errors.New("worker task kind is unsupported")
	}
	switch task.LastSequence {
	case 0:
		if err := client.ReportTask(ctx, task, cloudworker.TaskStatusRunning, cloudworker.ExecutionProbeReceivedCheckpoint, "", task.ExecutionManifestDigest); err != nil {
			return err
		}
		fallthrough
	case 1:
		return client.ReportTask(ctx, task, cloudworker.TaskStatusSucceeded, cloudworker.ExecutionProbeVerifiedCheckpoint, "", task.ExecutionManifestDigest)
	default:
		// The closed probe has only two transitions. A later executor needs an
		// explicit persisted step contract before it can interpret additional
		// sequence values, so this process safely leaves them untouched.
		return nil
	}
}

func validConfigPath(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsAny(value, "\r\n\x00")
}

func readRegularFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, errConfigInvalid
	}
	content, err := os.ReadFile(path)
	if err != nil || int64(len(content)) != info.Size() {
		return nil, errConfigInvalid
	}
	return content, nil
}
