package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionbootstrap"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "connection-bootstrap: server stopped")
		os.Exit(1)
	}
}
func run(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("connection-bootstrap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "strict fixed bootstrap configuration JSON")
	controllerAddress := flags.String("controller-listen", "", "mTLS controller listen address")
	uploadAddress := flags.String("upload-listen", "", "public TLS upload listen address")
	certificatePath := flags.String("tls-cert", "", "TLS certificate PEM")
	keyPath := flags.String("tls-key", "", "TLS private key PEM")
	clientCAPath := flags.String("controller-client-ca", "", "controller client CA PEM")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *configPath == "" || *controllerAddress == "" || *uploadAddress == "" || *certificatePath == "" || *keyPath == "" || *clientCAPath == "" || *controllerAddress == *uploadAddress {
		return connectionbootstrap.ErrInvalid
	}
	configRaw, err := readRegular(*configPath, 1<<20)
	if err != nil {
		return err
	}
	config, err := connectionbootstrap.ParseConfig(configRaw)
	clear(configRaw)
	if err != nil {
		return err
	}
	factory, err := connectionbootstrap.NewAWSClientFactory(config.Region)
	if err != nil {
		return err
	}
	service, err := connectionbootstrap.NewService(config, factory, nil, nil)
	if err != nil {
		return err
	}
	certificate, err := tls.LoadX509KeyPair(*certificatePath, *keyPath)
	if err != nil {
		return err
	}
	caRaw, err := readRegular(*clientCAPath, 1<<20)
	if err != nil {
		return err
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caRaw) {
		clear(caRaw)
		return connectionbootstrap.ErrInvalid
	}
	clear(caRaw)
	silent := log.New(io.Discard, "", 0)
	controller := &http.Server{Addr: *controllerAddress, Handler: service.ControllerHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, ErrorLog: silent, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs}}
	upload := &http.Server{Addr: *uploadAddress, Handler: service.UploadHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, ErrorLog: silent, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, ClientAuth: tls.NoClientCert}}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsChannel := make(chan error, 2)
	go func() { errorsChannel <- controller.ListenAndServeTLS("", "") }()
	go func() { errorsChannel <- upload.ListenAndServeTLS("", "") }()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = controller.Shutdown(shutdownContext)
			_ = upload.Shutdown(shutdownContext)
			return nil
		case err := <-errorsChannel:
			if err == http.ErrServerClosed {
				return nil
			}
			_ = controller.Close()
			_ = upload.Close()
			return err
		case <-ticker.C:
			service.CleanupExpired()
		}
	}
}
func readRegular(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, connectionbootstrap.ErrInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || opened.Size() != info.Size() {
		return nil, connectionbootstrap.ErrInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(raw)) != opened.Size() {
		return nil, connectionbootstrap.ErrInvalid
	}
	return raw, nil
}
