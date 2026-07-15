// cloud-worker is the deliberately narrow outbound bootstrap process for one
// exclusive Cloud Worker VM. Its production default performs no Recipe work;
// the separate Recipe loop exists only behind explicit trusted dependency
// injection and has no shell, Docker, AWS SDK, or cloud-control fallback.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/fixedprobe"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const (
	bootstrapManifestFileEnv = "CLOUD_WORKER_BOOTSTRAP_MANIFEST_FILE"
	expectedConnectionIDEnv  = "CLOUD_WORKER_EXPECTED_CONNECTION_ID"
	expectedEndpointEnv      = "CLOUD_WORKER_EXPECTED_BOOTSTRAP_ENDPOINT"
	fixedProbeRecipeEnv      = "CLOUD_WORKER_FIXED_PROBE_RECIPE_ENABLED"
	recipeCheckpointDirEnv   = "CLOUD_WORKER_RECIPE_CHECKPOINT_DIR"
)

var (
	errConfigInvalid = errors.New("cloud worker configuration is invalid")
	errRunFailed     = errors.New("cloud worker stopped with an error")
)

type commandConfig struct {
	manifestFile        string
	expectedConnection  string
	expectedEndpoint    string
	recipeCheckpointDir string
	fixedProbeRecipe    bool
	once                bool
	heartbeatInterval   time.Duration
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

type recipeTaskProcessor interface {
	ProcessOne(context.Context) error
}

type serviceReadinessProcessor interface {
	ProcessOne(context.Context) error
}

type recipeTaskClientProvider interface {
	NewRecipeTaskClient() (*cloudworker.RecipeTaskClient, error)
}

type serviceReadinessTaskClientProvider interface {
	NewServiceReadinessTaskClient() (*cloudworker.ServiceReadinessTaskClient, error)
}

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
		manifestFile:        strings.TrimSpace(getenv(bootstrapManifestFileEnv)),
		expectedConnection:  strings.TrimSpace(getenv(expectedConnectionIDEnv)),
		expectedEndpoint:    strings.TrimSpace(getenv(expectedEndpointEnv)),
		recipeCheckpointDir: strings.TrimSpace(getenv(recipeCheckpointDirEnv)),
		heartbeatInterval:   30 * time.Second,
	}
	switch strings.ToLower(strings.TrimSpace(getenv(fixedProbeRecipeEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.fixedProbeRecipe = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	flags := flag.NewFlagSet("cloud-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&config.once, "once", false, "claim one session and process at most one pass of each explicitly enabled fixed Worker capability")
	flags.DurationVar(&config.heartbeatInterval, "heartbeat-interval", config.heartbeatInterval, "outbound heartbeat interval")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return commandConfig{}, errConfigInvalid
	}
	if !validConfigPath(config.manifestFile) || config.expectedConnection == "" || config.expectedEndpoint == "" ||
		config.heartbeatInterval <= 0 ||
		(config.fixedProbeRecipe && (!validConfigPath(config.recipeCheckpointDir) || !filepath.IsAbs(config.recipeCheckpointDir))) ||
		(!config.fixedProbeRecipe && config.recipeCheckpointDir != "") {
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
	var recipe recipeTaskProcessor
	var readiness serviceReadinessProcessor
	if config.fixedProbeRecipe {
		recipe, err = newFixedProbeRecipeProcessor(client, config.recipeCheckpointDir)
		if err != nil {
			return errRunFailed
		}
		readiness, err = newFixedProbeReadinessProcessor(client)
		if err != nil {
			return errRunFailed
		}
	}
	if config.once {
		if err := runWorkerCycleWithProcessors(ctx, client, proof, true, recipe, readiness); err != nil {
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
			_ = runWorkerCycleWithProcessors(ctx, client, proof, false, recipe, readiness)
			if ctx.Err() != nil {
				return nil
			}
		}
	}
}

func newFixedProbeRecipeProcessor(client workerSessionClient, checkpointDirectory string) (recipeTaskProcessor, error) {
	provider, ok := client.(recipeTaskClientProvider)
	if !ok {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	bundle := recipeexec.FixedProbeBundle()
	resolver, err := recipeexec.NewFixedBundleResolver([]recipeexec.Bundle{bundle})
	if err != nil {
		return nil, err
	}
	checkpointStore, err := recipeexec.NewFileCheckpointStore(checkpointDirectory)
	if err != nil {
		return nil, err
	}
	driver, err := fixedprobe.NewProductionDriver()
	if err != nil {
		return nil, err
	}
	transport, err := provider.NewRecipeTaskClient()
	if err != nil {
		return nil, err
	}
	return cloudworker.NewRecipeTaskLoop(transport, resolver, checkpointStore, driver)
}

func newFixedProbeReadinessProcessor(client workerSessionClient) (serviceReadinessProcessor, error) {
	provider, ok := client.(serviceReadinessTaskClientProvider)
	if !ok {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	transport, err := provider.NewServiceReadinessTaskClient()
	if err != nil {
		return nil, err
	}
	return cloudworker.NewServiceReadinessTaskLoop(transport, fixedprobe.NewLocalHost())
}

// runWorkerCycle keeps regular session telemetry independent from the fixed
// task transport. In non-once mode each transient transport failure is left
// for a later retry; it never starts a shell, container, Recipe, AWS call, or
// dynamic installer as a fallback.
func runWorkerCycle(ctx context.Context, client workerSessionClient, proof cloudworker.InstanceIdentityProof, failFast bool) error {
	return runWorkerCycleWithRecipe(ctx, client, proof, failFast, nil)
}

// runWorkerCycleWithRecipe preserves the narrower test/injection boundary.
// Production uses runWorkerCycleWithProcessors only after every fixed
// capability dependency has been constructed successfully.
func runWorkerCycleWithRecipe(ctx context.Context, client workerSessionClient, proof cloudworker.InstanceIdentityProof, failFast bool, recipe recipeTaskProcessor) error {
	return runWorkerCycleWithProcessors(ctx, client, proof, failFast, recipe, nil)
}

func runWorkerCycleWithProcessors(ctx context.Context, client workerSessionClient, proof cloudworker.InstanceIdentityProof, failFast bool, recipe recipeTaskProcessor, readiness serviceReadinessProcessor) error {
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
	if recipe != nil {
		if err := recipe.ProcessOne(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if failFast {
				return err
			}
		}
	}
	if readiness != nil {
		if err := readiness.ProcessOne(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if failFast {
				return err
			}
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
