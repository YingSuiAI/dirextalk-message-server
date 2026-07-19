// cloud-worker-probe-service is the fixed, non-business workload used to
// validate the first sealed Recipe execution path. It has no configuration,
// outbound network access, filesystem access, secrets, cloud credentials, or
// dynamic routing surface and listens on loopback only.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	probeListenAddress = "127.0.0.1:18080"
	probeReadyPath     = "/ready"
	probeReadyBody     = `{"schema":"dirextalk.fixed-probe-readiness/v1","status":"ready"}`
)

func main() {
	if len(os.Args) != 1 {
		log.Print("cloud-worker-probe-service: arguments_not_allowed")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Addr:              probeListenAddress,
		Handler:           probeHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    8 * 1024,
	}
	result := make(chan error, 1)
	go func() { result <- server.ListenAndServe() }()
	select {
	case err := <-result:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Print("cloud-worker-probe-service: serve_failed")
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Print("cloud-worker-probe-service: shutdown_failed")
			os.Exit(1)
		}
	}
}

func probeHandler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("Content-Security-Policy", "default-src 'none'")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		if request.URL.Path != probeReadyPath || request.URL.RawQuery != "" {
			http.NotFound(response, request)
			return
		}
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, "method_not_allowed", http.StatusMethodNotAllowed)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte(probeReadyBody))
	})
}
