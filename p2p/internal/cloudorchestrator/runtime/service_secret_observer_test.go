package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestServiceSecretObserverPersistsBeforeIOAndReplaysExactEnvelopeAfterDisconnect(t *testing.T) {
	now := time.Date(2026, 7, 15, 3, 0, 0, 0, time.UTC)
	store := newSecretObserveStore(secretObserveClaim())
	transport := &fakeSecretObserveTransport{signed: testSignedSecretObserve(now), errs: []error{errors.New("disconnect"), nil}, results: []ServiceSecretObservation{{}, {SessionID: "secret-session-0001", Status: "completed", ProviderVersion: "version-1", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}}}
	observer := NewServiceSecretObserver(store, transport, testObserverConfig(now))
	if worked, err := observer.RunOnce(t.Context()); !worked || err != nil || store.persisted.EnvelopeJSON == "" || store.deferred != 1 {
		t.Fatalf("first run worked=%v err=%v store=%#v", worked, err, store)
	}
	store.claim.Command.SignedEnvelope = store.persisted.EnvelopeJSON
	store.claim.Command.PayloadJSON = store.persisted.PayloadJSON
	store.claim.Command.PayloadSHA256 = store.persisted.PayloadSHA256
	store.claim.Command.RequestSHA256 = store.persisted.RequestSHA256
	store.claim.Command.IssuedAt = store.persisted.IssuedAt
	store.claim.Command.ExpiresAt = store.persisted.ExpiresAt
	if worked, err := observer.RunOnce(t.Context()); !worked || err != nil || store.completed != 1 || transport.buildCalls != 1 || len(transport.envelopes) != 2 || transport.envelopes[0] != transport.envelopes[1] {
		t.Fatalf("replay worked=%v err=%v build=%d completed=%d envelopes=%v", worked, err, transport.buildCalls, store.completed, transport.envelopes)
	}
}

func TestServiceSecretObserverRejectsOldLeaseLateAndDuplicateUnsafeObservations(t *testing.T) {
	now := time.Date(2026, 7, 15, 3, 0, 0, 0, time.UTC)
	store := newSecretObserveStore(secretObserveClaim())
	transport := &fakeSecretObserveTransport{signed: testSignedSecretObserve(now), results: []ServiceSecretObservation{{SessionID: "secret-session-0001", Status: "completed", ProviderVersion: "version-1", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}}, beforeReturn: func() { store.currentLease = "new-lease" }}
	observer := NewServiceSecretObserver(store, transport, testObserverConfig(now))
	if _, err := observer.RunOnce(t.Context()); err == nil || store.completed != 0 {
		t.Fatalf("late response advanced stale lease: err=%v completed=%d", err, store.completed)
	}
	store.currentLease = store.claim.LeaseToken
	transport.beforeReturn = nil
	transport.results = []ServiceSecretObservation{{SessionID: "secret-session-0001", Status: "completed", ProviderVersion: "version-1", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}, {SessionID: "secret-session-0001", Status: "completed", ProviderVersion: "version-1", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}}
	if _, err := observer.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := observer.RunOnce(t.Context()); err != nil || store.completed != 1 {
		t.Fatalf("duplicate observation err=%v completed=%d", err, store.completed)
	}
}

func TestServiceSecretObserverStatusAndCanaryFailuresAreDeSecreted(t *testing.T) {
	now := time.Date(2026, 7, 15, 3, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		result  ServiceSecretObservation
		err     error
		want    string
		expired bool
	}{
		{"pending", ServiceSecretObservation{SessionID: "secret-session-0001", Status: "pending_upload", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}, nil, "defer", false},
		{"expired", ServiceSecretObservation{SessionID: "secret-session-0001", Status: "expired", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}, nil, "expire", false},
		{"missing_before_expiry", ServiceSecretObservation{}, ServiceSecretObserveUnavailable(errors.New("secret_ref:model-token canary-value")), "defer", false},
		{"missing_after_expiry", ServiceSecretObservation{}, ServiceSecretObserveUnavailable(errors.New("secret_ref:model-token canary-value")), "expire", true},
		{"pending_after_expiry", ServiceSecretObservation{SessionID: "secret-session-0001", Status: "pending_upload", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}, nil, "expire", true},
		{"completed_after_expiry", ServiceSecretObservation{SessionID: "secret-session-0001", Status: "completed", ProviderVersion: "version-1", BindingDigest: "sha256:" + strings.Repeat("b", 64), UpdatedMarker: strings.Repeat("c", 64)}, nil, "complete", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim := secretObserveClaim()
			if tc.expired {
				claim.ApprovalExpiresAt = now
			}
			store := newSecretObserveStore(claim)
			transport := &fakeSecretObserveTransport{signed: testSignedSecretObserve(now), results: []ServiceSecretObservation{tc.result}, errs: []error{tc.err}}
			observer := NewServiceSecretObserver(store, transport, testObserverConfig(now))
			_, err := observer.RunOnce(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			switch tc.want {
			case "defer":
				if store.deferred != 1 {
					t.Fatal("not deferred")
				}
			case "expire":
				if store.expired != 1 {
					t.Fatal("not expired")
				}
			case "fail":
				if store.failed != "service_secret_observe_unavailable" || strings.Contains(store.failed, "model-token") || strings.Contains(store.failed, "canary") {
					t.Fatalf("unsafe failure=%q", store.failed)
				}
			case "complete":
				if store.completed != 1 {
					t.Fatal("not completed")
				}
			}
		})
	}
}

type fakeSecretObserveStore struct {
	claim                        ServiceSecretObserveClaim
	currentLease                 string
	persisted                    SignedServiceSecretObserveCommand
	deferred, expired, completed int
	failed, completedMarker      string
}

func newSecretObserveStore(c ServiceSecretObserveClaim) *fakeSecretObserveStore {
	return &fakeSecretObserveStore{claim: c, currentLease: c.LeaseToken}
}
func (s *fakeSecretObserveStore) ClaimPendingServiceSecretObserve(context.Context, string, time.Duration) (ServiceSecretObserveClaim, bool, error) {
	return s.claim, true, nil
}
func (s *fakeSecretObserveStore) check(c ServiceSecretObserveClaim) error {
	if c.LeaseToken != s.currentLease {
		return errors.New("stale lease")
	}
	return nil
}
func (s *fakeSecretObserveStore) PersistServiceSecretObserveCommand(_ context.Context, c ServiceSecretObserveClaim, v SignedServiceSecretObserveCommand) error {
	if err := s.check(c); err != nil {
		return err
	}
	s.persisted = v
	return nil
}
func (s *fakeSecretObserveStore) CompleteServiceSecretObserve(_ context.Context, c ServiceSecretObserveClaim, v ServiceSecretObservation) error {
	if err := s.check(c); err != nil {
		return err
	}
	if s.completedMarker != "" && s.completedMarker != v.UpdatedMarker {
		return errors.New("observation conflict")
	}
	s.completedMarker = v.UpdatedMarker
	s.completed = 1
	return nil
}
func (s *fakeSecretObserveStore) DeferServiceSecretObserve(_ context.Context, c ServiceSecretObserveClaim, _ string, _ time.Time) error {
	if err := s.check(c); err != nil {
		return err
	}
	s.deferred++
	return nil
}
func (s *fakeSecretObserveStore) ExpireServiceSecretObserve(_ context.Context, c ServiceSecretObserveClaim) error {
	if err := s.check(c); err != nil {
		return err
	}
	s.expired++
	return nil
}
func (s *fakeSecretObserveStore) FailServiceSecretObserve(_ context.Context, c ServiceSecretObserveClaim, code string) error {
	if err := s.check(c); err != nil {
		return err
	}
	s.failed = code
	return nil
}

type fakeSecretObserveTransport struct {
	signed       SignedServiceSecretObserveCommand
	buildCalls   int
	envelopes    []string
	results      []ServiceSecretObservation
	errs         []error
	beforeReturn func()
}

func (t *fakeSecretObserveTransport) BuildServiceSecretObserveCommand(ServiceSecretObserveCommand, ServiceSecretObserveRequest, time.Time) (SignedServiceSecretObserveCommand, error) {
	t.buildCalls++
	return t.signed, nil
}
func (t *fakeSecretObserveTransport) RequestServiceSecretObserve(_ context.Context, _ string, _ ServiceSecretObserveCommand, s SignedServiceSecretObserveCommand, _ ServiceSecretObserveRequest) (ServiceSecretObservation, error) {
	t.envelopes = append(t.envelopes, s.EnvelopeJSON)
	if t.beforeReturn != nil {
		t.beforeReturn()
	}
	var result ServiceSecretObservation
	if len(t.results) > 0 {
		result = t.results[0]
		t.results = t.results[1:]
	}
	var err error
	if len(t.errs) > 0 {
		err = t.errs[0]
		t.errs = t.errs[1:]
	}
	return result, err
}

func secretObserveClaim() ServiceSecretObserveClaim {
	return ServiceSecretObserveClaim{LeaseToken: "lease-1", BrokerEndpoint: "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/commands", Region: "us-east-1", ApprovalExpiresAt: time.Date(2026, 7, 15, 3, 10, 0, 0, time.UTC), Request: ServiceSecretObserveRequest{SessionID: "secret-session-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:" + strings.Repeat("a", 64), SecretRef: "secret_ref:model-token", ContextDigest: "sha256:" + strings.Repeat("b", 64)}, Command: ServiceSecretObserveCommand{CommandID: "command-secret-observe-0001", ConnectionID: "connection-0001", NodeKeyID: "node-key-1", ExpectedGeneration: 2, NodeCounter: 41, Attempt: 1, Action: ServiceSecretObserveAction}}
}
func testSignedSecretObserve(now time.Time) SignedServiceSecretObserveCommand {
	request := secretObserveClaim().Request
	raw, _ := json.Marshal(request)
	sum := sha256.Sum256(raw)
	return SignedServiceSecretObserveCommand{EnvelopeJSON: `{"signed":"fixed-canary-free-envelope"}`, PayloadJSON: string(raw), PayloadSHA256: hex.EncodeToString(sum[:]), RequestSHA256: strings.Repeat("d", 64), IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute)}
}
func testObserverConfig(now time.Time) Config {
	return Config{WorkerID: "observer-1", Lease: time.Minute, AttemptTimeout: 10 * time.Second, RetryDelay: time.Second, Now: func() time.Time { return now }}
}
