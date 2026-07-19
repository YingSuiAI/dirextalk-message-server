package cloudworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

const maxSemanticReadinessBodyBytes = 1 << 20

// LocalServiceReadinessProbe is intentionally not a generic HTTP client. The
// host is fixed to loopback and the complete request/response contract is the
// validated, catalog-bound ServiceReadinessProbeV1.
type LocalServiceReadinessProbe struct {
	client *http.Client
}

func NewLocalServiceReadinessProbe() *LocalServiceReadinessProbe {
	return &LocalServiceReadinessProbe{client: &http.Client{Timeout: 30 * time.Second}}
}

func (probe *LocalServiceReadinessProbe) CheckLoopback(ctx context.Context, contract ServiceReadinessProbeV1) error {
	if probe == nil || probe.client == nil || contract.validate() != nil {
		return errors.New("service readiness probe is invalid")
	}
	target := contract.Scheme + "://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(int(contract.Port))) + contract.Path
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	response, err := probe.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != int(contract.ExpectedStatus) {
		return fmt.Errorf("semantic readiness status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxSemanticReadinessBodyBytes+1))
	if err != nil || len(body) > maxSemanticReadinessBodyBytes {
		return errors.New("semantic readiness body is invalid")
	}
	sum := sha256.Sum256(body)
	if "sha256:"+hex.EncodeToString(sum[:]) != contract.BodySHA256 {
		return errors.New("semantic readiness evidence does not match")
	}
	return nil
}
