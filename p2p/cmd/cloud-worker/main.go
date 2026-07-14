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
	identityDocumentFileEnv  = "CLOUD_WORKER_IDENTITY_DOCUMENT_FILE"
	identitySignatureFileEnv = "CLOUD_WORKER_IDENTITY_SIGNATURE_FILE"
)

var (
	errConfigInvalid = errors.New("cloud worker configuration is invalid")
	errRunFailed     = errors.New("cloud worker stopped with an error")
)

type commandConfig struct {
	manifestFile       string
	expectedConnection string
	expectedEndpoint   string
	identityDocument   string
	identitySignature  string
	once               bool
	heartbeatInterval  time.Duration
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
		manifestFile:       strings.TrimSpace(getenv(bootstrapManifestFileEnv)),
		expectedConnection: strings.TrimSpace(getenv(expectedConnectionIDEnv)),
		expectedEndpoint:   strings.TrimSpace(getenv(expectedEndpointEnv)),
		identityDocument:   strings.TrimSpace(getenv(identityDocumentFileEnv)),
		identitySignature:  strings.TrimSpace(getenv(identitySignatureFileEnv)),
		heartbeatInterval:  30 * time.Second,
	}
	flags := flag.NewFlagSet("cloud-worker", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&config.once, "once", false, "claim once and send one heartbeat")
	flags.DurationVar(&config.heartbeatInterval, "heartbeat-interval", config.heartbeatInterval, "outbound heartbeat interval")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return commandConfig{}, errConfigInvalid
	}
	if !validConfigPath(config.manifestFile) || !validConfigPath(config.identityDocument) || !validConfigPath(config.identitySignature) ||
		config.expectedConnection == "" || config.expectedEndpoint == "" || config.heartbeatInterval <= 0 {
		return commandConfig{}, errConfigInvalid
	}
	return config, nil
}

func run(ctx context.Context, config commandConfig) error {
	manifestBytes, err := readRegularFile(config.manifestFile, 64*1024)
	if err != nil {
		return errConfigInvalid
	}
	now := time.Now
	manifest, err := cloudworker.ParseBootstrapManifest(manifestBytes, cloudworker.ManifestValidationContext{
		Now:                       now().UTC(),
		MaxLifetime:               10 * time.Minute,
		ExpectedConnectionID:      config.expectedConnection,
		ExpectedBootstrapEndpoint: config.expectedEndpoint,
	})
	if err != nil {
		return errConfigInvalid
	}
	document, err := readRegularFile(config.identityDocument, 64*1024)
	if err != nil {
		return errConfigInvalid
	}
	signature, err := readRegularFile(config.identitySignature, 32*1024)
	if err != nil {
		return errConfigInvalid
	}
	client, err := cloudworker.NewSessionClient(manifest, cloudworker.SessionClientConfig{
		ExpectedConnectionID:      config.expectedConnection,
		ExpectedBootstrapEndpoint: config.expectedEndpoint,
		Now:                       now,
	})
	if err != nil {
		return errConfigInvalid
	}
	proof := cloudworker.InstanceIdentityProof{DocumentB64: string(document), SignatureB64: string(signature)}
	if err := client.Claim(ctx, proof); err != nil {
		return errRunFailed
	}
	if config.once {
		if err := client.Heartbeat(ctx); err != nil {
			return errRunFailed
		}
		return nil
	}
	ticker := time.NewTicker(config.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			client.Close()
			return nil
		case <-ticker.C:
			if err := client.Heartbeat(ctx); err != nil {
				if retryErr := client.RetryPending(ctx); retryErr != nil {
					return errRunFailed
				}
			}
		}
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
