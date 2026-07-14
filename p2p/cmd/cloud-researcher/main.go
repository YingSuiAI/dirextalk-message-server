// cloud-researcher is the private model boundary for Cloud Orchestrator
// research. It accepts only mTLS-authenticated typed inputs and has no Matrix,
// AWS SDK, Docker socket, ProductCore, or Worker capability.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/researcher"
)

const (
	listenAddrEnv      = "CLOUD_RESEARCHER_LISTEN_ADDR"
	tlsCertFileEnv     = "CLOUD_RESEARCHER_TLS_CERT_FILE"
	tlsKeyFileEnv      = "CLOUD_RESEARCHER_TLS_KEY_FILE"
	clientCAFileEnv    = "CLOUD_RESEARCHER_CLIENT_CA_FILE"
	modelEndpointEnv   = "CLOUD_RESEARCHER_MODEL_ENDPOINT"
	modelIDEnv         = "CLOUD_RESEARCHER_MODEL_ID"
	modelAPIKeyFileEnv = "CLOUD_RESEARCHER_MODEL_API_KEY_FILE"
)

var (
	errConfigInvalid = errors.New("cloud researcher configuration is invalid")
	errStartFailed   = errors.New("cloud researcher could not start")
)

type commandConfig struct {
	listenAddr      string
	tlsCertFile     string
	tlsKeyFile      string
	clientCAFile    string
	modelEndpoint   string
	modelID         string
	modelAPIKeyFile string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	config, err := parseConfig(os.Args[1:], os.Getenv)
	if err != nil {
		log.Print("cloud-researcher: config_invalid")
		os.Exit(2)
	}
	if err := run(ctx, config); err != nil {
		log.Print("cloud-researcher: stopped_with_error")
		os.Exit(1)
	}
}

func parseConfig(args []string, getenv func(string) string) (commandConfig, error) {
	if getenv == nil {
		return commandConfig{}, errConfigInvalid
	}
	config := commandConfig{
		listenAddr:      strings.TrimSpace(getenv(listenAddrEnv)),
		tlsCertFile:     strings.TrimSpace(getenv(tlsCertFileEnv)),
		tlsKeyFile:      strings.TrimSpace(getenv(tlsKeyFileEnv)),
		clientCAFile:    strings.TrimSpace(getenv(clientCAFileEnv)),
		modelEndpoint:   strings.TrimSpace(getenv(modelEndpointEnv)),
		modelID:         strings.TrimSpace(getenv(modelIDEnv)),
		modelAPIKeyFile: strings.TrimSpace(getenv(modelAPIKeyFileEnv)),
	}
	if config.listenAddr == "" {
		config.listenAddr = "127.0.0.1:8443"
	}
	flags := flag.NewFlagSet("cloud-researcher", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&config.listenAddr, "listen", config.listenAddr, "private mTLS listen address")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return commandConfig{}, errConfigInvalid
	}
	config.listenAddr = strings.TrimSpace(config.listenAddr)
	if !validListenAddress(config.listenAddr) || !validConfigPath(config.tlsCertFile) || !validConfigPath(config.tlsKeyFile) || !validConfigPath(config.clientCAFile) || !validConfigPath(config.modelAPIKeyFile) {
		return commandConfig{}, errConfigInvalid
	}
	if _, err := researcher.NewOpenAICompatiblePlanner(researcher.OpenAICompatibleConfig{Endpoint: config.modelEndpoint, Model: config.modelID, APIKey: "configuration-check"}); err != nil {
		return commandConfig{}, errConfigInvalid
	}
	return config, nil
}

func run(ctx context.Context, config commandConfig) error {
	apiKey, err := researcher.ReadModelAPIKeyFile(config.modelAPIKeyFile)
	if err != nil {
		return errConfigInvalid
	}
	planner, err := researcher.NewOpenAICompatiblePlanner(researcher.OpenAICompatibleConfig{Endpoint: config.modelEndpoint, Model: config.modelID, APIKey: apiKey})
	if err != nil {
		return errConfigInvalid
	}
	tlsConfig, err := researcher.LoadMutualTLSServerConfig(researcher.MutualTLSServerConfig{CertificateFile: config.tlsCertFile, KeyFile: config.tlsKeyFile, ClientCAFile: config.clientCAFile})
	if err != nil {
		return errConfigInvalid
	}
	listener, err := tls.Listen("tcp", config.listenAddr, tlsConfig)
	if err != nil {
		return errStartFailed
	}
	defer listener.Close()
	server := &http.Server{
		Handler:           researcher.NewResearchHTTPHandler(planner),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		case <-stopped:
		}
	}()
	err = server.Serve(listener)
	close(stopped)
	if errors.Is(err, http.ErrServerClosed) || ctx.Err() != nil {
		return nil
	}
	return errStartFailed
}

func validConfigPath(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsAny(value, "\r\n\x00")
}

func validListenAddress(value string) bool {
	if value == "" || len(value) > 512 || strings.ContainsAny(value, "\r\n\t\x00") {
		return false
	}
	_, port, err := net.SplitHostPort(value)
	if err != nil || port == "" {
		return false
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	return err == nil && parsedPort > 0
}
