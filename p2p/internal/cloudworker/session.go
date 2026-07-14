package cloudworker

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	defaultHTTPTimeout       = 30 * time.Second
	maxWorkerSessionResponse = 64 * 1024
	workerSessionRenewalLead = time.Minute
)

var (
	ErrSessionNotClaimed = errors.New("worker session is not claimed")
	ErrSessionClaimed    = errors.New("worker session is already claimed")
	ErrPendingEvent      = errors.New("worker session has a pending event")
	ErrNoPendingEvent    = errors.New("worker session has no pending event")
)

type SessionState string

const (
	SessionStateUnclaimed SessionState = "unclaimed"
	SessionStateActive    SessionState = "active"
	SessionStateClosed    SessionState = "closed"
)

// SessionClientConfig contains only launch-time, non-secret pins. The event
// bearer token is accepted later from the one-time claim response and remains
// private to SessionClient memory.
type SessionClientConfig struct {
	ExpectedConnectionID      string
	ExpectedBootstrapEndpoint string
	HTTPClient                *http.Client
	Now                       func() time.Time
	// AllowExpiredBootstrap is only used by the cloud-worker reconnect path.
	// The Broker still decides whether the immutable bootstrap session was
	// already activated and can be reauthenticated.
	AllowExpiredBootstrap bool
}

// SessionSnapshot is deliberately safe to project or test: it never carries
// the claim token, identity document, endpoints, or raw response data.
type SessionSnapshot struct {
	State                    SessionState
	LeaseEpoch               uint64
	LastAcknowledgedSequence uint64
	PendingSequence          uint64
}

type SessionClient struct {
	manifest BootstrapManifest
	endpoint *url.URL
	client   *http.Client
	now      func() time.Time

	mu             sync.Mutex
	state          SessionState
	access         string
	epoch          uint64
	leaseExpiresAt time.Time
	lastAck        uint64
	pending        *SessionEvent
}

// NewSessionClient validates the immutable manifest before any HTTP request.
// Its transport has no environment proxy and never follows redirects.
func NewSessionClient(manifest BootstrapManifest, config SessionClientConfig) (*SessionClient, error) {
	now := config.Now
	if now == nil {
		now = time.Now
	}
	if err := manifest.Validate(ManifestValidationContext{
		Now:                       now().UTC(),
		MaxLifetime:               maxBootstrapManifestLifetime,
		ExpectedConnectionID:      config.ExpectedConnectionID,
		ExpectedBootstrapEndpoint: config.ExpectedBootstrapEndpoint,
		AllowExpired:              config.AllowExpiredBootstrap,
	}); err != nil {
		return nil, err
	}
	endpoint, err := url.Parse(config.ExpectedBootstrapEndpoint)
	if err != nil {
		return nil, errors.New("worker bootstrap endpoint is invalid")
	}
	client, err := secureHTTPClient(config.HTTPClient)
	if err != nil {
		return nil, err
	}
	return &SessionClient{manifest: manifest, endpoint: endpoint, client: client, now: now, state: SessionStateUnclaimed}, nil
}

// Claim consumes no local secret. The server must verify the opaque instance
// identity proof and atomically consume the bootstrap session; this client
// only accepts a response bound to the manifest.
func (client *SessionClient) Claim(ctx context.Context, proof InstanceIdentityProof) error {
	if client == nil {
		return ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state == SessionStateClosed {
		return ErrSessionNotClaimed
	}
	if client.state != SessionStateUnclaimed {
		return ErrSessionClaimed
	}
	return client.claimLocked(ctx, proof, false)
}

// RenewIfDue presents the VM identity proof again once the bearer lease is
// close to expiring. A successful renewal must rotate both the lease epoch and
// bearer token, so prior telemetry cannot be sent under the new lease.
func (client *SessionClient) RenewIfDue(ctx context.Context, proof InstanceIdentityProof) error {
	if client == nil {
		return ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.leaseExpiresAt.IsZero() {
		return ErrSessionNotClaimed
	}
	if client.now().UTC().Before(client.leaseExpiresAt.Add(-workerSessionRenewalLead)) {
		return nil
	}
	return client.claimLocked(ctx, proof, true)
}

func (client *SessionClient) claimLocked(ctx context.Context, proof InstanceIdentityProof, renewing bool) error {
	request, err := NewClaimRequest(client.manifest, proof)
	if err != nil {
		return err
	}
	body, err := client.post(ctx, "claim", request, "")
	if err != nil {
		return err
	}
	var response claimResponse
	now := client.now().UTC()
	if err := decodeStrictObject(body, &response); err != nil || response.ValidateFor(client.manifest, now) != nil {
		return errors.New("worker claim response is invalid")
	}
	leaseExpiresAt, err := parseCanonicalInstant(response.LeaseExpiresAt)
	if err != nil {
		return errors.New("worker claim response is invalid")
	}
	if renewing && (response.LeaseEpoch <= client.epoch || response.AccessToken == client.access) {
		return errors.New("worker session renewal did not rotate its lease")
	}
	client.access = response.AccessToken
	client.epoch = response.LeaseEpoch
	client.leaseExpiresAt = leaseExpiresAt
	// Events from the prior epoch are not valid under the rotated bearer. This
	// Worker currently emits telemetry only; later recipe execution will own
	// durable checkpoint recovery separately from this transport reset.
	client.lastAck = 0
	client.pending = nil
	client.state = SessionStateActive
	return nil
}

func (client *SessionClient) Heartbeat(ctx context.Context) error {
	return client.emit(ctx, EventKindHeartbeat, "", "", "", "")
}

func (client *SessionClient) Checkpoint(ctx context.Context, checkpoint, evidenceDigest string) error {
	return client.emit(ctx, EventKindCheckpoint, checkpoint, "", "", evidenceDigest)
}

func (client *SessionClient) Report(ctx context.Context, status ReportStatus, errorCode, evidenceDigest string) error {
	return client.emit(ctx, EventKindReport, "", status, errorCode, evidenceDigest)
}

// RetryPending preserves the original epoch, sequence, timestamp, and body.
// A Connection Stack can therefore return an idempotent receipt after a lost
// HTTP response without accidentally receiving a second effect.
func (client *SessionClient) RetryPending(ctx context.Context) error {
	if client == nil {
		return ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive {
		return ErrSessionNotClaimed
	}
	if client.pending == nil {
		return ErrNoPendingEvent
	}
	return client.sendPending(ctx)
}

func (client *SessionClient) Close() {
	if client == nil {
		return
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.access = ""
	client.pending = nil
	client.state = SessionStateClosed
}

func (client *SessionClient) Snapshot() SessionSnapshot {
	if client == nil {
		return SessionSnapshot{State: SessionStateClosed}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	snapshot := SessionSnapshot{State: client.state, LeaseEpoch: client.epoch, LastAcknowledgedSequence: client.lastAck}
	if client.pending != nil {
		snapshot.PendingSequence = client.pending.Sequence
	}
	return snapshot
}

func (client *SessionClient) emit(ctx context.Context, kind EventKind, checkpoint string, status ReportStatus, errorCode, evidenceDigest string) error {
	if client == nil {
		return ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive {
		return ErrSessionNotClaimed
	}
	if client.pending != nil {
		return ErrPendingEvent
	}
	event := SessionEvent{
		Schema:             WorkerEventV1Schema,
		ConnectionID:       client.manifest.ConnectionID,
		DeploymentID:       client.manifest.DeploymentID,
		BootstrapSessionID: client.manifest.BootstrapSessionID,
		LeaseEpoch:         client.epoch,
		Sequence:           client.lastAck + 1,
		Kind:               kind,
		Checkpoint:         checkpoint,
		ReportStatus:       status,
		ErrorCode:          errorCode,
		EvidenceDigest:     evidenceDigest,
		OccurredAt:         canonicalInstant(client.now()),
	}
	if err := event.Validate(); err != nil {
		return err
	}
	client.pending = &event
	return client.sendPending(ctx)
}

func (client *SessionClient) sendPending(ctx context.Context) error {
	if client.pending == nil || client.access == "" {
		return ErrSessionNotClaimed
	}
	body, err := client.post(ctx, "events", *client.pending, client.access)
	if err != nil {
		return err
	}
	var receipt EventReceipt
	if err := decodeStrictObject(body, &receipt); err != nil || receipt.ValidateFor(*client.pending) != nil {
		return errors.New("worker event receipt is invalid")
	}
	client.lastAck = client.pending.Sequence
	client.pending = nil
	return nil
}

func (client *SessionClient) post(ctx context.Context, operation string, payload any, token string) ([]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.New("worker session request is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.operationURL(operation), bytes.NewReader(encoded))
	if err != nil {
		return nil, errors.New("worker session request is invalid")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("worker session is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return nil, errors.New("worker session request was rejected")
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]))
	if contentType != "application/json" {
		return nil, errors.New("worker session response is invalid")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxWorkerSessionResponse+1))
	if err != nil || len(body) == 0 || len(body) > maxWorkerSessionResponse {
		return nil, errors.New("worker session response is invalid")
	}
	return body, nil
}

func (client *SessionClient) operationURL(operation string) string {
	endpoint := *client.endpoint
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + "/" + client.manifest.BootstrapSessionID + "/" + operation
	endpoint.RawPath = ""
	return endpoint.String()
}

func secureHTTPClient(source *http.Client) (*http.Client, error) {
	if source == nil {
		source = &http.Client{}
	}
	copy := *source
	var transport *http.Transport
	switch configured := source.Transport.(type) {
	case nil:
		transport = http.DefaultTransport.(*http.Transport).Clone()
	case *http.Transport:
		transport = configured.Clone()
	default:
		return nil, errors.New("worker HTTP transport is invalid")
	}
	transport.Proxy = nil
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
			transport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
	}
	copy.Transport = transport
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copy.Timeout <= 0 {
		copy.Timeout = defaultHTTPTimeout
	}
	return &copy, nil
}
