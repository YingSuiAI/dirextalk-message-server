// cloud-orchestrator is an independently supervised, least-privilege Cloud
// coordinator for research, Stack attestation, quotes, Worker bootstrap
// observation, and the closed execution-probe transport. It has no Matrix
// config, Native Agent model key, AWS SDK, Docker socket, or product migration
// capability.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/brokertransport"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/researcher"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/storepg"
)

const (
	databaseURLFileEnv           = "CLOUD_ORCHESTRATOR_DATABASE_URL_FILE"
	researcherURLEnv             = "CLOUD_ORCHESTRATOR_RESEARCHER_URL"
	researcherCAFileEnv          = "CLOUD_ORCHESTRATOR_RESEARCHER_CA_FILE"
	researcherCertFileEnv        = "CLOUD_ORCHESTRATOR_RESEARCHER_CERT_FILE"
	researcherKeyFileEnv         = "CLOUD_ORCHESTRATOR_RESEARCHER_KEY_FILE"
	researcherServerNameEnv      = "CLOUD_ORCHESTRATOR_RESEARCHER_SERVER_NAME"
	nodeSigningKeyFileEnv        = "CLOUD_ORCHESTRATOR_NODE_SIGNING_KEY_FILE"
	workerIDEnv                  = "CLOUD_ORCHESTRATOR_WORKER_ID"
	recipeInstallEnabledEnv      = "CLOUD_ORCHESTRATOR_RECIPE_INSTALL_ENABLED"
	serviceReadinessEnabledEnv   = "CLOUD_ORCHESTRATOR_SERVICE_READINESS_ENABLED"
	serviceDestroyEnabledEnv     = "CLOUD_ORCHESTRATOR_SERVICE_DESTROY_ENABLED"
	serviceOperationEnabledEnv   = "CLOUD_ORCHESTRATOR_SERVICE_OPERATION_ENABLED"
	serviceBackupEnabledEnv      = "CLOUD_ORCHESTRATOR_SERVICE_BACKUP_ENABLED"
	serviceRestorePlanEnabledEnv = "CLOUD_ORCHESTRATOR_SERVICE_RESTORE_PLAN_ENABLED"
	serviceRestoreEnabledEnv     = "CLOUD_ORCHESTRATOR_SERVICE_RESTORE_ENABLED"
)

var (
	errConfigInvalid       = errors.New("cloud orchestrator configuration is invalid")
	errDatabaseUnavailable = errors.New("cloud orchestrator database is unavailable")
	errIterationFailed     = errors.New("cloud orchestrator iteration failed")
)

type commandConfig struct {
	databaseURLFile           string
	researcherURL             string
	researcherCAFile          string
	researcherCertFile        string
	researcherKeyFile         string
	researcherServerName      string
	nodeSigningKeyFile        string
	workerID                  string
	recipeInstallEnabled      bool
	serviceReadinessEnabled   bool
	serviceDestroyEnabled     bool
	serviceOperationEnabled   bool
	serviceBackupEnabled      bool
	serviceRestorePlanEnabled bool
	serviceRestoreEnabled     bool
	once                      bool
	pollInterval              time.Duration
	lease                     time.Duration
	attemptTimeout            time.Duration
	retryDelay                time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config, err := parseConfig(os.Args[1:], os.Getenv, os.Hostname)
	if err != nil {
		log.Print("cloud-orchestrator: config_invalid")
		os.Exit(2)
	}
	if err := run(ctx, config); err != nil {
		log.Print("cloud-orchestrator: stopped_with_error")
		os.Exit(1)
	}
}

func parseConfig(args []string, getenv func(string) string, hostname func() (string, error)) (commandConfig, error) {
	if getenv == nil || hostname == nil {
		return commandConfig{}, errConfigInvalid
	}
	defaultWorkerID, err := hostname()
	if err != nil {
		return commandConfig{}, errConfigInvalid
	}
	defaultWorkerID = "cloud-orchestrator-" + strings.TrimSpace(defaultWorkerID)
	if configuredWorkerID := strings.TrimSpace(getenv(workerIDEnv)); configuredWorkerID != "" {
		defaultWorkerID = configuredWorkerID
	}
	config := commandConfig{
		databaseURLFile:      strings.TrimSpace(getenv(databaseURLFileEnv)),
		researcherURL:        strings.TrimSpace(getenv(researcherURLEnv)),
		researcherCAFile:     strings.TrimSpace(getenv(researcherCAFileEnv)),
		researcherCertFile:   strings.TrimSpace(getenv(researcherCertFileEnv)),
		researcherKeyFile:    strings.TrimSpace(getenv(researcherKeyFileEnv)),
		researcherServerName: strings.TrimSpace(getenv(researcherServerNameEnv)),
		nodeSigningKeyFile:   strings.TrimSpace(getenv(nodeSigningKeyFileEnv)),
		workerID:             defaultWorkerID,
		pollInterval:         2 * time.Second,
		lease:                2 * time.Minute,
		attemptTimeout:       90 * time.Second,
		retryDelay:           time.Minute,
	}
	switch strings.ToLower(strings.TrimSpace(getenv(recipeInstallEnabledEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.recipeInstallEnabled = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	switch strings.ToLower(strings.TrimSpace(getenv(serviceReadinessEnabledEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.serviceReadinessEnabled = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	switch strings.ToLower(strings.TrimSpace(getenv(serviceDestroyEnabledEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.serviceDestroyEnabled = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	switch strings.ToLower(strings.TrimSpace(getenv(serviceOperationEnabledEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.serviceOperationEnabled = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	switch strings.ToLower(strings.TrimSpace(getenv(serviceBackupEnabledEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.serviceBackupEnabled = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	switch strings.ToLower(strings.TrimSpace(getenv(serviceRestorePlanEnabledEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.serviceRestorePlanEnabled = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	switch strings.ToLower(strings.TrimSpace(getenv(serviceRestoreEnabledEnv))) {
	case "", "false", "0":
	case "true", "1":
		config.serviceRestoreEnabled = true
	default:
		return commandConfig{}, errConfigInvalid
	}
	flags := flag.NewFlagSet("cloud-orchestrator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&config.once, "once", false, "process one pass for every explicitly enabled control-plane runner")
	flags.StringVar(&config.researcherURL, "researcher-url", config.researcherURL, "exact HTTPS private researcher endpoint")
	flags.StringVar(&config.workerID, "worker-id", config.workerID, "non-secret worker identifier")
	flags.DurationVar(&config.pollInterval, "poll-interval", config.pollInterval, "idle polling interval")
	flags.DurationVar(&config.lease, "lease", config.lease, "research claim lease")
	flags.DurationVar(&config.attemptTimeout, "attempt-timeout", config.attemptTimeout, "maximum single research attempt")
	flags.DurationVar(&config.retryDelay, "retry-delay", config.retryDelay, "retry delay after an iteration failure")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return commandConfig{}, errConfigInvalid
	}
	config.databaseURLFile = strings.TrimSpace(config.databaseURLFile)
	config.researcherURL = strings.TrimSpace(config.researcherURL)
	config.researcherCAFile = strings.TrimSpace(config.researcherCAFile)
	config.researcherCertFile = strings.TrimSpace(config.researcherCertFile)
	config.researcherKeyFile = strings.TrimSpace(config.researcherKeyFile)
	config.researcherServerName = strings.TrimSpace(config.researcherServerName)
	config.nodeSigningKeyFile = strings.TrimSpace(config.nodeSigningKeyFile)
	config.workerID = strings.TrimSpace(config.workerID)
	if !validConfigPath(config.databaseURLFile) || config.researcherURL == "" || !validConfigPath(config.researcherCAFile) || !validConfigPath(config.researcherCertFile) || !validConfigPath(config.researcherKeyFile) || !validConfigPath(config.nodeSigningKeyFile) || !validResearcherServerName(config.researcherServerName) || !validWorkerID(config.workerID) ||
		config.pollInterval <= 0 || config.lease <= 0 || config.lease > 5*time.Minute ||
		config.attemptTimeout <= 0 || config.attemptTimeout >= config.lease || config.retryDelay <= 0 ||
		(config.serviceReadinessEnabled && !config.recipeInstallEnabled) {
		return commandConfig{}, errConfigInvalid
	}
	if _, err := researcher.NewHTTP(researcher.HTTPConfig{Endpoint: config.researcherURL}); err != nil {
		return commandConfig{}, errConfigInvalid
	}
	return config, nil
}

func validWorkerID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validConfigPath(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsAny(value, "\r\n\x00")
}

func validResearcherServerName(value string) bool {
	if value == "" || len(value) > 253 || strings.ContainsAny(value, " \r\n\t\x00") {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func readDatabaseURL(path string) (string, error) {
	info, err := os.Stat(strings.TrimSpace(path))
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 8192 {
		return "", errConfigInvalid
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", errConfigInvalid
	}
	databaseURL := strings.TrimSpace(string(content))
	parsed, err := url.Parse(databaseURL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" || strings.ContainsAny(databaseURL, "\r\n\x00") {
		return "", errConfigInvalid
	}
	return databaseURL, nil
}

// readNodeSigningKey accepts exactly one PKCS#8 Ed25519 private key from a
// regular mounted file. Its path may be configured, but the key bytes never
// enter an environment variable, log line, database record, or command flag.
func readNodeSigningKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Stat(strings.TrimSpace(path))
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 16*1024 {
		return nil, errConfigInvalid
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errConfigInvalid
	}
	block, rest := pem.Decode(content)
	if block == nil || block.Type != "PRIVATE KEY" || len(strings.TrimSpace(string(rest))) != 0 {
		return nil, errConfigInvalid
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if err != nil || !ok || len(privateKey) != ed25519.PrivateKeySize {
		return nil, errConfigInvalid
	}
	return append(ed25519.PrivateKey(nil), privateKey...), nil
}

func run(ctx context.Context, config commandConfig) error {
	client, err := researcher.NewMutualTLSClient(researcher.MutualTLSClientConfig{
		CAFile: config.researcherCAFile, CertificateFile: config.researcherCertFile, KeyFile: config.researcherKeyFile,
		ServerName: config.researcherServerName,
	})
	if err != nil {
		return errConfigInvalid
	}
	planner, err := researcher.NewHTTP(researcher.HTTPConfig{Endpoint: config.researcherURL, Client: client})
	if err != nil {
		return errConfigInvalid
	}
	nodeSigningKey, err := readNodeSigningKey(config.nodeSigningKeyFile)
	if err != nil {
		return errConfigInvalid
	}
	brokerTransport, err := brokertransport.New(nodeSigningKey, time.Now)
	if err != nil {
		return errConfigInvalid
	}
	databaseURL, err := readDatabaseURL(config.databaseURLFile)
	if err != nil {
		return errConfigInvalid
	}
	startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	store, err := storepg.Open(startCtx, databaseURL, storepg.Config{})
	cancel()
	if err != nil {
		return errDatabaseUnavailable
	}
	defer store.Close()
	researchRunner := runtime.New(store, planner, runtime.Config{
		WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay,
	})
	quoteRunner := runtime.NewQuoteRunner(store, brokerTransport, runtime.Config{
		WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay,
	})
	registrationRunner := runtime.NewConnectionRegistrationRunner(store, brokerTransport, runtime.Config{
		WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay,
	})
	// deployment.create remains deliberately disabled in this process. The
	// Connection Stack can now independently verify Worker bootstrap evidence,
	// so its read-only observer is safe to run continuously; a future executor
	// stage must still prove an install/readiness contract before any billable
	// provision outbox is allowed to leave this process.
	var deploymentRunner iterationRunner
	workerBootstrapObservationRunner := runtime.NewWorkerBootstrapObservationRunner(store, brokerTransport, runtime.Config{
		WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay,
	})
	// execution_probe is a closed, digest-only task transport verification. It
	// may issue and observe that task, but it cannot execute a Recipe, create
	// EC2, access Worker credentials, or treat the result as service readiness.
	executionProbeRunner := runtime.NewExecutionProbeRunner(store, brokerTransport, runtime.Config{
		WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay,
	})
	// Secret observation is read-only and only claims an already-approved,
	// pending bootstrap ledger entry. It never creates a session, uploads
	// material, or broadens the ProductCore action surface.
	serviceSecretObserver := runtime.NewServiceSecretObserver(store, brokerTransport, runtime.Config{
		WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay,
	})
	var recipeInstallRunner iterationRunner
	var serviceReadinessRunner iterationRunner
	var serviceDestroyRunner iterationRunner
	var serviceOperationRunner iterationRunner
	var serviceBackupRunner iterationRunner
	var serviceRestorePlanRunner iterationRunner
	var serviceRestoreRunner iterationRunner
	if config.recipeInstallEnabled {
		recipeInstallRunner = runtime.NewRecipeInstallRunner(store, brokerTransport, runtime.Config{WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay})
	}
	if config.serviceReadinessEnabled {
		serviceReadinessRunner = runtime.NewServiceReadinessRunner(store, brokerTransport, runtime.Config{WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay})
	}
	if config.serviceDestroyEnabled {
		serviceDestroyRunner = runtime.NewServiceDestroyRunner(store, brokerTransport, runtime.Config{WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay})
	}
	if config.serviceOperationEnabled {
		serviceOperationRunner = runtime.NewServiceOperationRunner(store, brokerTransport, runtime.Config{WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay})
	}
	if config.serviceBackupEnabled {
		serviceBackupRunner = runtime.NewServiceBackupRunner(store, brokerTransport, runtime.Config{WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay})
	}
	if config.serviceRestorePlanEnabled {
		serviceRestorePlanRunner = runtime.NewServiceRestorePlanRunner(store, brokerTransport, runtime.Config{WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay})
	}
	if config.serviceRestoreEnabled {
		serviceRestoreRunner = runtime.NewServiceRestoreRunner(store, brokerTransport, runtime.Config{WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay})
	}
	if config.once {
		if _, err := runIteration(ctx, researchRunner, registrationRunner, quoteRunner, deploymentRunner, workerBootstrapObservationRunner, executionProbeRunner, recipeInstallRunner, serviceReadinessRunner, serviceOperationRunner, serviceBackupRunner, serviceRestorePlanRunner, serviceRestoreRunner, serviceDestroyRunner, serviceSecretObserver); err != nil {
			return errIterationFailed
		}
		return nil
	}
	for {
		processed, err := runIteration(ctx, researchRunner, registrationRunner, quoteRunner, deploymentRunner, workerBootstrapObservationRunner, executionProbeRunner, recipeInstallRunner, serviceReadinessRunner, serviceOperationRunner, serviceBackupRunner, serviceRestorePlanRunner, serviceRestoreRunner, serviceDestroyRunner, serviceSecretObserver)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if !wait(ctx, config.retryDelay) {
				return nil
			}
			continue
		}
		if processed {
			continue
		}
		if !wait(ctx, config.pollInterval) {
			return nil
		}
	}
}

type iterationRunner interface {
	RunOnce(context.Context) (bool, error)
}

// runIteration gives each independent control-plane loop one chance per poll.
// A failure in one durable control-plane runner must not starve the others;
// all errors are returned together for the next retry backoff.
func runIteration(ctx context.Context, researchRunner, registrationRunner, quoteRunner, deploymentRunner, workerBootstrapObservationRunner, executionProbeRunner, recipeInstallRunner, serviceReadinessRunner, serviceOperationRunner, serviceBackupRunner, serviceRestorePlanRunner, serviceRestoreRunner, serviceDestroyRunner, serviceSecretObserver iterationRunner) (bool, error) {
	researched, researchErr := researchRunner.RunOnce(ctx)
	registered, registrationErr := registrationRunner.RunOnce(ctx)
	quoted, quoteErr := quoteRunner.RunOnce(ctx)
	var deployed bool
	var deploymentErr error
	if deploymentRunner != nil {
		deployed, deploymentErr = deploymentRunner.RunOnce(ctx)
	}
	var observed bool
	var observationErr error
	if workerBootstrapObservationRunner != nil {
		observed, observationErr = workerBootstrapObservationRunner.RunOnce(ctx)
	}
	var executionProbed bool
	var executionProbeErr error
	if executionProbeRunner != nil {
		executionProbed, executionProbeErr = executionProbeRunner.RunOnce(ctx)
	}
	var recipeInstalled bool
	var recipeInstallErr error
	if recipeInstallRunner != nil {
		recipeInstalled, recipeInstallErr = recipeInstallRunner.RunOnce(ctx)
	}
	var readinessObserved bool
	var serviceReadinessErr error
	if serviceReadinessRunner != nil {
		readinessObserved, serviceReadinessErr = serviceReadinessRunner.RunOnce(ctx)
	}
	var serviceDestroyed bool
	var serviceDestroyErr error
	if serviceDestroyRunner != nil {
		serviceDestroyed, serviceDestroyErr = serviceDestroyRunner.RunOnce(ctx)
	}
	var serviceOperated bool
	var serviceOperationErr error
	if serviceOperationRunner != nil {
		serviceOperated, serviceOperationErr = serviceOperationRunner.RunOnce(ctx)
	}
	var serviceBackedUp bool
	var serviceBackupErr error
	if serviceBackupRunner != nil {
		serviceBackedUp, serviceBackupErr = serviceBackupRunner.RunOnce(ctx)
	}
	var serviceRestorePlanned bool
	var serviceRestorePlanErr error
	if serviceRestorePlanRunner != nil {
		serviceRestorePlanned, serviceRestorePlanErr = serviceRestorePlanRunner.RunOnce(ctx)
	}
	var serviceRestored bool
	var serviceRestoreErr error
	if serviceRestoreRunner != nil {
		serviceRestored, serviceRestoreErr = serviceRestoreRunner.RunOnce(ctx)
	}
	var secretObserved bool
	var secretObserveErr error
	if serviceSecretObserver != nil {
		secretObserved, secretObserveErr = serviceSecretObserver.RunOnce(ctx)
	}
	return researched || registered || quoted || deployed || observed || executionProbed || recipeInstalled || readinessObserved || serviceOperated || serviceBackedUp || serviceRestorePlanned || serviceRestored || serviceDestroyed || secretObserved, errors.Join(researchErr, registrationErr, quoteErr, deploymentErr, observationErr, executionProbeErr, recipeInstallErr, serviceReadinessErr, serviceOperationErr, serviceBackupErr, serviceRestorePlanErr, serviceRestoreErr, serviceDestroyErr, secretObserveErr)
}

func wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
