// cloud-orchestrator is an independently supervised, least-privilege Cloud
// research worker. It has no Matrix config, Native Agent model key, AWS SDK,
// Docker socket, or product migration capability.
package main

import (
	"context"
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

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/researcher"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/storepg"
)

const (
	databaseURLFileEnv = "CLOUD_ORCHESTRATOR_DATABASE_URL_FILE"
	researcherURLEnv   = "CLOUD_ORCHESTRATOR_RESEARCHER_URL"
	workerIDEnv        = "CLOUD_ORCHESTRATOR_WORKER_ID"
)

var (
	errConfigInvalid       = errors.New("cloud orchestrator configuration is invalid")
	errDatabaseUnavailable = errors.New("cloud orchestrator database is unavailable")
	errIterationFailed     = errors.New("cloud orchestrator iteration failed")
)

type commandConfig struct {
	databaseURLFile string
	researcherURL   string
	workerID        string
	once            bool
	pollInterval    time.Duration
	lease           time.Duration
	attemptTimeout  time.Duration
	retryDelay      time.Duration
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
		databaseURLFile: strings.TrimSpace(getenv(databaseURLFileEnv)),
		researcherURL:   strings.TrimSpace(getenv(researcherURLEnv)),
		workerID:        defaultWorkerID,
		pollInterval:    2 * time.Second,
		lease:           2 * time.Minute,
		attemptTimeout:  90 * time.Second,
		retryDelay:      time.Minute,
	}
	flags := flag.NewFlagSet("cloud-orchestrator", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&config.once, "once", false, "process at most one research request")
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
	config.workerID = strings.TrimSpace(config.workerID)
	if config.databaseURLFile == "" || config.researcherURL == "" || !validWorkerID(config.workerID) ||
		config.pollInterval <= 0 || config.lease <= 0 || config.lease > 5*time.Minute ||
		config.attemptTimeout <= 0 || config.attemptTimeout >= config.lease || config.retryDelay <= 0 {
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

func run(ctx context.Context, config commandConfig) error {
	planner, err := researcher.NewHTTP(researcher.HTTPConfig{Endpoint: config.researcherURL})
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
	runner := runtime.New(store, planner, runtime.Config{
		WorkerID: config.workerID, Lease: config.lease, AttemptTimeout: config.attemptTimeout, RetryDelay: config.retryDelay,
	})
	if config.once {
		if _, err := runner.RunOnce(ctx); err != nil {
			return errIterationFailed
		}
		return nil
	}
	for {
		processed, err := runner.RunOnce(ctx)
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
