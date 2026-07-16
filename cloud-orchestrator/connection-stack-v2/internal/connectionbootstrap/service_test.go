package connectionbootstrap

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/connectionfoundation"
)

func TestConcurrentUploadConsumesCredentialsOnceAndReplayIsStable(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
	factory := &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:sts::123456789012:assumed-role/bootstrap/operator", UserID: "operator"}}
	service, _ := NewService(configFixture(), factory, rand.Reader, clock)
	createRequest := createRequestFixture("connection-concurrent-0001")
	response, err := service.CreateSession(createRequest)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := service.CreateSession(createRequest)
	if err != nil || replay.SessionID != response.SessionID || replay.UploadBearer != response.UploadBearer || replay.Status != "awaiting_upload" {
		t.Fatalf("awaiting replay=%#v err=%v", replay, err)
	}
	envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("s", 40), SessionToken: "session-token"})
	const workers = 16
	receipts := make(chan Receipt, workers)
	failures := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			receipt, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope)
			if err != nil {
				failures <- err
				return
			}
			receipts <- receipt
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("concurrent Upload: %v", err)
	}
	close(receipts)
	stackID := ""
	for receipt := range receipts {
		if stackID == "" {
			stackID = receipt.StackID
		}
		if receipt.StackID != stackID || receipt.Status != "accepted" {
			t.Fatalf("unstable receipt: %#v", receipt)
		}
	}
	if got := factory.createCalls.Load(); got != 1 {
		t.Fatalf("CreateStack calls=%d", got)
	}
	if !allZero(factory.seen.AccessKeyID) || !allZero(factory.seen.SecretAccessKey) || !allZero(factory.seen.SessionToken) {
		t.Fatal("credential buffers were retained after AWS acceptance")
	}
	acceptedReplay, err := service.CreateSession(createRequest)
	if err != nil || acceptedReplay.Status != "accepted" || acceptedReplay.UploadBearer != "" || acceptedReplay.Receipt == nil || acceptedReplay.Receipt.StackID != stackID {
		t.Fatalf("accepted create replay=%#v err=%v", acceptedReplay, err)
	}
	changed := createRequest
	changed.RolePlan.NodeKeyID = "node-key-changed-0001"
	changed.RolePlan.FixedParameters["NodeKeyId"] = changed.RolePlan.NodeKeyID
	if _, err := service.CreateSession(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed create replay err=%v", err)
	}
	different := envelope
	different.Nonce = base64.StdEncoding.EncodeToString([]byte("123456789012"))
	if _, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, different); !errors.Is(err, ErrConflict) {
		t.Fatalf("different replay err=%v", err)
	}
}

func TestUploadRejectsTamperAndExpiredEnvelope(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*UploadEnvelope, *fakeClock, *fakeFactory)
		want   error
	}{{"tamper", func(envelope *UploadEnvelope, _ *fakeClock, _ *fakeFactory) {
		raw, _ := base64.StdEncoding.DecodeString(envelope.Ciphertext)
		raw[0] ^= 1
		envelope.Ciphertext = base64.StdEncoding.EncodeToString(raw)
	}, ErrInvalid}, {"expired", func(_ *UploadEnvelope, clock *fakeClock, _ *fakeFactory) { clock.Advance(11 * time.Minute) }, ErrExpired}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
			factory := &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:role/bootstrap", UserID: "role"}}
			service, _ := NewService(configFixture(), factory, rand.Reader, clock)
			response := createSessionFixture(t, service, "connection-"+test.name+"-0001")
			envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("x", 40)})
			test.mutate(&envelope, clock, factory)
			if _, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope); !errors.Is(err, test.want) {
				t.Fatalf("Upload err=%v want=%v", err, test.want)
			}
			if factory.createCalls.Load() != 0 {
				t.Fatal("CreateStack called for rejected upload")
			}
		})
	}

}

func TestUploadRejectsRootIdentityUnlessRolePlanPermitsRootBootstrap(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
	factory := &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"}}
	service, _ := NewService(configFixture(), factory, rand.Reader, clock)
	response := createSessionFixture(t, service, "connection-root-disabled-0001")
	envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("r", 40)})

	if _, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("root bootstrap err=%v", err)
	}
	if factory.createCalls.Load() != 0 {
		t.Fatalf("CreateStack calls=%d", factory.createCalls.Load())
	}
}

func TestUploadAcceptsRootIdentityOnlyForOneTimeConnectionStackBootstrap(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
	factory := &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"}}
	config := rootConfigFixture()
	resolvedPlan := rootFoundationPlan()
	resolvedPlan.Network.SubnetID = "subnet-0123456789abcdeff"
	resolvedPlan.Worker.AMIID = "ami-0123456789abcdeff"
	resolver := &fakeFoundationResolver{resolution: rootBootstrapResolutionFixture(resolvedPlan)}
	service, _ := NewService(config, factory, rand.Reader, clock, WithFoundationResolver(resolver))
	request := rootCreateRequestFixture("connection-root-bootstrap-0001")
	response, err := service.CreateSession(request)
	if err != nil {
		t.Fatal(err)
	}
	envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("r", 40)})

	receipt, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope)
	if err != nil || receipt.Status != "accepted" || receipt.ConnectionID != response.ConnectionID || receipt.StackID == "" {
		t.Fatalf("root bootstrap receipt=%#v err=%v", receipt, err)
	}
	if factory.createCalls.Load() != 1 {
		t.Fatalf("CreateStack calls=%d", factory.createCalls.Load())
	}
	if resolver.calls.Load() != 1 || resolver.request.ConnectionID != response.ConnectionID || resolver.request.BootstrapID != request.RolePlan.BootstrapID || resolver.request.Region != config.Region || resolver.request.AccountID != factory.identity.AccountID {
		t.Fatalf("root foundation resolver request=%#v calls=%d", resolver.request, resolver.calls.Load())
	}
	resolverRequestRaw, err := json.Marshal(resolver.request)
	if err != nil || strings.Contains(strings.ToLower(string(resolverRequestRaw)), "access") || strings.Contains(strings.ToLower(string(resolverRequestRaw)), "secret") || strings.Contains(strings.ToLower(string(resolverRequestRaw)), "token") || strings.Contains(string(resolverRequestRaw), "connection_template") {
		t.Fatalf("foundation resolver request contained a credential field or failed to marshal: %v", err)
	}
	if resolver.accessKeyLength == 0 || resolver.secretAccessKeyLength == 0 || !allZero(resolver.credentials.AccessKeyID) || !allZero(resolver.credentials.SecretAccessKey) || !allZero(resolver.credentials.SessionToken) {
		t.Fatal("resolver credential buffers were not zeroed after the one-time root bootstrap")
	}
	stackRequest := factory.lastStackRequest()
	if stackRequest.Parameters["WorkerSubnetId"] != resolvedPlan.Network.SubnetID || stackRequest.Parameters["WorkerBaseAmiId"] != resolvedPlan.Worker.AMIID || stackRequest.Template != resolver.resolution.ConnectionTemplate {
		t.Fatalf("CreateStack did not use resolved foundation: %#v", stackRequest.Parameters)
	}
	receiptRaw, err := json.Marshal(receipt)
	if err != nil || strings.Contains(string(receiptRaw), "access_key") || strings.Contains(string(receiptRaw), "secret_access_key") || strings.Contains(string(receiptRaw), "session_token") {
		t.Fatalf("receipt leaked credential material or failed to marshal: %v", err)
	}
	if !allZero(factory.seen.AccessKeyID) || !allZero(factory.seen.SecretAccessKey) || !allZero(factory.seen.SessionToken) {
		t.Fatal("root bootstrap credential buffers were retained after AWS acceptance")
	}
	replay, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope)
	if err != nil || replay != receipt || factory.createCalls.Load() != 1 {
		t.Fatalf("root bootstrap replay=%#v err=%v create_calls=%d", replay, err, factory.createCalls.Load())
	}
}

func TestRootFoundationResolverRunsOnlyAfterAllowedRootIdentity(t *testing.T) {
	tests := []struct {
		name          string
		identity      CallerIdentity
		allowRoot     bool
		want          error
		resolverCalls int64
		createCalls   int64
	}{
		{
			name:          "role_uses_static_foundation",
			identity:      CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:role/bootstrap", UserID: "role"},
			want:          nil,
			resolverCalls: 0,
			createCalls:   1,
		},
		{
			name:          "root_without_approved_role_plan",
			identity:      CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"},
			allowRoot:     false,
			want:          ErrUnauthorized,
			resolverCalls: 0,
			createCalls:   0,
		},
		{
			name:          "root_with_invalid_identity",
			identity:      CallerIdentity{ARN: "arn:aws:iam::123456789012:root", UserID: "root"},
			allowRoot:     true,
			want:          ErrInvalid,
			resolverCalls: 0,
			createCalls:   0,
		},
		{
			name:          "approved_root_uses_resolver",
			identity:      CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"},
			allowRoot:     true,
			want:          nil,
			resolverCalls: 1,
			createCalls:   1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
			factory := &fakeFactory{identity: test.identity}
			resolver := &fakeFoundationResolver{resolution: rootBootstrapResolutionFixture(rootFoundationPlan())}
			config := configFixture()
			request := createRequestFixture("connection-root-resolver-" + strings.ReplaceAll(test.name, "_", "-"))
			if test.allowRoot {
				config = rootConfigFixture()
				request = rootCreateRequestFixture("connection-root-resolver-" + strings.ReplaceAll(test.name, "_", "-"))
			}
			service, err := NewService(config, factory, rand.Reader, clock, WithFoundationResolver(resolver))
			if err != nil {
				t.Fatal(err)
			}
			response, err := service.CreateSession(request)
			if err != nil {
				t.Fatal(err)
			}
			envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("r", 40)})
			_, err = service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope)
			if !errors.Is(err, test.want) {
				t.Fatalf("Upload() error=%v want=%v", err, test.want)
			}
			if resolver.calls.Load() != test.resolverCalls || factory.createCalls.Load() != test.createCalls {
				t.Fatalf("resolver calls=%d create calls=%d", resolver.calls.Load(), factory.createCalls.Load())
			}
		})
	}
}

func TestRootFoundationResolverMayReplaceAbsentStaticPlanButRoleCannot(t *testing.T) {
	for _, test := range []struct {
		name          string
		identity      CallerIdentity
		allowRoot     bool
		want          error
		resolverCalls int64
		createCalls   int64
	}{
		{
			name:          "approved_root",
			identity:      CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"},
			allowRoot:     true,
			resolverCalls: 1,
			createCalls:   1,
		},
		{
			name:          "role_requires_static_plan",
			identity:      CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:role/bootstrap", UserID: "role"},
			want:          ErrInvalid,
			resolverCalls: 0,
			createCalls:   0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
			factory := &fakeFactory{identity: test.identity}
			config := rootConfigFixture()
			resolver := &fakeFoundationResolver{resolution: rootBootstrapResolutionFixture(rootFoundationPlan())}
			service, err := NewService(config, factory, rand.Reader, clock, WithFoundationResolver(resolver))
			if err != nil {
				t.Fatal(err)
			}
			request := createRequestFixture("connection-no-static-" + test.name)
			if test.allowRoot {
				request = rootCreateRequestFixture("connection-no-static-" + test.name)
			}
			response, err := service.CreateSession(request)
			if !test.allowRoot {
				if !errors.Is(err, ErrInvalid) || resolver.calls.Load() != 0 || factory.createCalls.Load() != 0 {
					t.Fatalf("role CreateSession() err=%v resolver calls=%d create calls=%d", err, resolver.calls.Load(), factory.createCalls.Load())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("r", 40)})

			_, err = service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope)
			if !errors.Is(err, test.want) {
				t.Fatalf("Upload() error=%v want=%v", err, test.want)
			}
			if resolver.calls.Load() != test.resolverCalls || factory.createCalls.Load() != test.createCalls {
				t.Fatalf("resolver calls=%d create calls=%d", resolver.calls.Load(), factory.createCalls.Load())
			}
		})
	}
}

func TestRootFoundationResolverFailureNeverFallsBackToStaticFoundation(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
	factory := &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"}}
	resolver := &fakeFoundationResolver{err: errors.New("foundation unavailable")}
	service, err := NewService(rootConfigFixture(), factory, rand.Reader, clock, WithFoundationResolver(resolver))
	if err != nil {
		t.Fatal(err)
	}
	request := rootCreateRequestFixture("connection-root-resolver-failure-0001")
	response, err := service.CreateSession(request)
	if err != nil {
		t.Fatal(err)
	}
	envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("r", 40)})

	if _, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Upload() error=%v", err)
	}
	if resolver.calls.Load() != 1 || factory.createCalls.Load() != 0 {
		t.Fatalf("resolver calls=%d create calls=%d", resolver.calls.Load(), factory.createCalls.Load())
	}
	if resolver.accessKeyLength == 0 || resolver.secretAccessKeyLength == 0 || !allZero(resolver.credentials.AccessKeyID) || !allZero(resolver.credentials.SecretAccessKey) || !allZero(resolver.credentials.SessionToken) {
		t.Fatal("resolver failure retained root credential buffers")
	}
}

func TestRootFoundationResolverResolutionMustBindPublishedArtifactsToFoundationAndIntent(t *testing.T) {
	resolutions := map[string]RootBootstrapResolution{
		"missing_contract": {},
		"wrong_region": func() RootBootstrapResolution {
			value := rootBootstrapResolutionFixture(rootFoundationPlan())
			value.FoundationPlan.Region = "us-west-2"
			return value
		}(),
		"foundation_bucket_mismatch": func() RootBootstrapResolution {
			value := rootBootstrapResolutionFixture(rootFoundationPlan())
			value.FoundationPlan.Worker.Artifact.Bucket = "another-foundation-artifacts"
			return value
		}(),
		"template_cross_bucket": func() RootBootstrapResolution {
			value := rootBootstrapResolutionFixture(rootFoundationPlan())
			value.ConnectionTemplate.Bucket = "another-foundation-artifacts"
			return value
		}(),
		"template_cross_kms_key": func() RootBootstrapResolution {
			value := rootBootstrapResolutionFixture(rootFoundationPlan())
			value.ConnectionTemplate.KMSKeyID = "arn:aws:kms:us-east-1:123456789012:key/11111111-2222-3333-4444-555555555555"
			return value
		}(),
		"foundation_kms_wrong_region": func() RootBootstrapResolution {
			value := rootBootstrapResolutionFixture(rootFoundationPlan())
			value.FoundationArtifact.KMSKeyID = "arn:aws:kms:us-west-2:123456789012:key/01234567-89ab-cdef-0123-456789abcdef"
			return value
		}(),
		"template_does_not_match_publish_intent": func() RootBootstrapResolution {
			value := rootBootstrapResolutionFixture(rootFoundationPlan())
			value.ConnectionTemplate.SHA256 = "sha256:" + strings.Repeat("e", 64)
			return value
		}(),
		"broker_cross_bucket": func() RootBootstrapResolution {
			value := rootBootstrapResolutionFixture(rootFoundationPlan())
			value.BrokerArtifact.Bucket = "another-foundation-artifacts"
			return value
		}(),
	}
	for name, resolution := range resolutions {
		t.Run(name, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
			factory := &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"}}
			resolver := &fakeFoundationResolver{resolution: resolution}
			service, err := NewService(rootConfigFixture(), factory, rand.Reader, clock, WithFoundationResolver(resolver))
			if err != nil {
				t.Fatal(err)
			}
			request := rootCreateRequestFixture("connection-root-invalid-foundation-" + name)
			response, err := service.CreateSession(request)
			if err != nil {
				t.Fatal(err)
			}
			envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("r", 40)})

			if _, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Upload() error=%v", err)
			}
			if resolver.calls.Load() != 1 || factory.createCalls.Load() != 0 {
				t.Fatalf("resolver calls=%d create calls=%d", resolver.calls.Load(), factory.createCalls.Load())
			}
		})
	}
}

func TestApprovedRootBootstrapWithoutFoundationResolverDoesNotUseStaticFoundation(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}
	factory := &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:root", UserID: "root"}}
	service, err := NewService(configFixture(), factory, rand.Reader, clock)
	if err != nil {
		t.Fatal(err)
	}
	request := rootCreateRequestFixture("connection-root-no-resolver-0001")
	response, err := service.CreateSession(request)
	if err != nil {
		t.Fatal(err)
	}
	envelope := encryptFixture(t, response, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("r", 40)})

	if _, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Upload() error=%v", err)
	}
	if factory.createCalls.Load() != 0 {
		t.Fatalf("CreateStack calls=%d", factory.createCalls.Load())
	}
}

func TestConfigRequiresTypedFoundationBoundToControllerRegion(t *testing.T) {
	missing := configFixture()
	missing.FoundationPlan = connectionfoundation.Plan{}
	if err := missing.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing foundation Validate() error=%v", err)
	}

	mismatched := configFixture()
	mismatched.FoundationPlan.Region = "us-west-2"
	if err := mismatched.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched foundation Validate() error=%v", err)
	}
}

func TestRootResolverConfigParserDefersOnlyStaticFoundationRequirement(t *testing.T) {
	config := rootConfigFixture()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["connection_template"]; present {
		t.Fatal("resolver-only Config serialized an absent connection_template")
	}
	if _, err := ParseConfig(raw); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseConfig() error=%v", err)
	}
	parsed, err := ParseConfigForFoundationResolver(raw)
	if err != nil {
		t.Fatalf("ParseConfigForFoundationResolver() error=%v", err)
	}
	if _, err := NewService(parsed, &fakeFactory{}, rand.Reader, &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewService without resolver error=%v", err)
	}
	if _, err := NewService(parsed, &fakeFactory{}, rand.Reader, &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}, WithFoundationResolver(&fakeFoundationResolver{resolution: rootBootstrapResolutionFixture(rootFoundationPlan())})); err != nil {
		t.Fatalf("NewService with resolver error=%v", err)
	}
}

func TestRootResolverConfigAllowsOnlyMatchingPublishIntent(t *testing.T) {
	config := rootIntentConfigFixture()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseConfig(raw); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseConfig() error=%v", err)
	}
	parsed, err := ParseConfigForFoundationResolver(raw)
	if err != nil {
		t.Fatalf("ParseConfigForFoundationResolver() error=%v", err)
	}
	service, err := NewService(parsed, &fakeFactory{}, rand.Reader, &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)}, WithFoundationResolver(&fakeFoundationResolver{resolution: rootBootstrapResolutionFixture(rootFoundationPlan())}))
	if err != nil {
		t.Fatal(err)
	}
	request := rootCreateRequestFixture("connection-root-config-intent-0001")
	if _, err := service.CreateSession(request); err != nil {
		t.Fatalf("matching root intent CreateSession() error=%v", err)
	}
	mismatched := rootCreateRequestFixture("connection-root-config-intent-0002")
	mismatched.RolePlan.ConnectionTemplate.PublishIntent.Version = "v1.1.0-cloud-mvp.20260716.2"
	if _, err := service.CreateSession(mismatched); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched root intent CreateSession() error=%v", err)
	}
	role := createRequestFixture("connection-root-config-role-0001")
	if _, err := service.CreateSession(role); !errors.Is(err, ErrInvalid) {
		t.Fatalf("role with root-only Config CreateSession() error=%v", err)
	}

	mixed := config
	mixed.ArtifactPolicy = configFixture().ArtifactPolicy
	if err := mixed.validate(true); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mixed root intent Config validate() error=%v", err)
	}
}

func TestConfigRequiresImmutableBrokerArtifactReference(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"missing_version": func(config *Config) { config.BrokerArtifact.VersionID = "" },
		"mutable_version": func(config *Config) { config.BrokerArtifact.VersionID = "null" },
		"release_alias":   func(config *Config) { config.BrokerArtifact.Version = "v1.1.0-latest" },
		"formal_tag":      func(config *Config) { config.BrokerArtifact.Version = "v1.0.3" },
		"bad_digest":      func(config *Config) { config.BrokerArtifact.SHA256 = "sha256:bad" },
		"path_escape":     func(config *Config) { config.BrokerArtifact.Key = "broker/../broker.zip" },
	} {
		t.Run(name, func(t *testing.T) {
			config := configFixture()
			mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() error=%v", err)
			}
		})
	}
}

func TestConfigRequiresImmutableConnectionTemplateReference(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"missing_version": func(config *Config) { config.ConnectionTemplate.Binding.VersionID = "" },
		"mutable_version": func(config *Config) { config.ConnectionTemplate.Binding.VersionID = "null" },
		"release_alias":   func(config *Config) { config.ConnectionTemplate.Binding.Version = "v1.1.0-latest" },
		"formal_tag":      func(config *Config) { config.ConnectionTemplate.Binding.Version = "v1.0.3" },
		"bad_digest":      func(config *Config) { config.ConnectionTemplate.Binding.SHA256 = "sha256:bad" },
		"wrong_kind":      func(config *Config) { config.ConnectionTemplate.Binding.Kind = string(artifactpublish.KindBrokerZIP) },
	} {
		t.Run(name, func(t *testing.T) {
			config := configFixture()
			mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() error=%v", err)
			}
		})
	}
}

func TestParseConfigRequiresPinnedArtifactsAndRejectsRetiredLooseFields(t *testing.T) {
	raw, err := json.Marshal(configFixture())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseConfig(raw); err != nil {
		t.Fatalf("ParseConfig(pinned reference): %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"worker_ami_id":           "ami-0123456789abcdef0",
		"broker_artifact_bucket":  "dirextalk-artifacts",
		"broker_artifact_version": "mutable-version",
		"template_url":            "https://mutable.example.invalid/connection-stack.yaml",
		"template_digest":         "sha256:" + strings.Repeat("c", 64),
	} {
		t.Run(name, func(t *testing.T) {
			changed := make(map[string]any, len(fields)+1)
			for key, field := range fields {
				changed[key] = field
			}
			changed[name] = value
			candidate, err := json.Marshal(changed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ParseConfig(candidate); !errors.Is(err, ErrInvalid) {
				t.Fatalf("legacy configuration ParseConfig() error=%v", err)
			}
		})
	}
}

func TestStackRequestUsesFoundationAndClosedLifecycleParameters(t *testing.T) {
	config := configFixture()
	config.DeploymentDestroyEnabled = true
	config.ServiceSecretsEnabled = true
	service, err := NewService(config, &fakeFactory{}, rand.Reader, &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	plan := createRequestFixture("connection-feature-flags-0001").RolePlan
	resolution, err := config.staticStackResolution()
	if err != nil {
		t.Fatal(err)
	}
	request, err := service.stackRequest(Identity{
		BootstrapID: plan.BootstrapID, ConnectionID: plan.ConnectionID, StackName: plan.StackName,
		NodeKeyID: plan.NodeKeyID, NodeEd25519PublicKey: plan.NodeEd25519PublicKey,
		DeviceKeyID: plan.DeviceKeyID, DeviceEd25519PublicKey: plan.DeviceEd25519PublicKey,
		FixedParameters: cloneStringMap(plan.FixedParameters),
	}, "aws-bootstrap-feature-flags-0001", "fingerprint", resolution)
	if err != nil {
		t.Fatal(err)
	}
	foundationParameters, err := config.FoundationPlan.TemplateParameters()
	if err != nil {
		t.Fatal(err)
	}
	if len(foundationParameters) != 7 {
		t.Fatalf("foundation parameter count=%d", len(foundationParameters))
	}
	for key, want := range foundationParameters {
		if got := request.Parameters[key]; got != want {
			t.Fatalf("foundation parameter %q=%q want=%q", key, got, want)
		}
	}
	if request.Parameters["EnableDeploymentDestroy"] != "true" || request.Parameters["EnableServiceSecrets"] != "true" || request.Parameters["EnableServiceBackup"] != "false" || request.Parameters["EnableServiceRestorePlan"] != "false" || request.Parameters["EnableServiceRestore"] != "false" || request.Parameters["Environment"] != "" {
		t.Fatalf("closed lifecycle feature flags=%#v", request.Parameters)
	}
	if request.Region != config.Region || request.Template != resolution.ConnectionTemplate {
		t.Fatalf("request used an unpinned connection template: %#v", request)
	}
	if request.Tags[stackTagManaged] != "true" || request.Tags[stackTagConnectionID] != plan.ConnectionID || request.Tags[stackTagRegion] != config.Region || request.Tags[stackTagTemplateBinding] != connectionTemplateBindingFingerprint(resolution.ConnectionTemplate) {
		t.Fatalf("request lacks immutable recovery tags: %#v", request.Tags)
	}
}

func TestControllerHandlerRequiresVerifiedMTLSAndReturnsOnlyFixedSessionContract(t *testing.T) {
	service, _ := NewService(configFixture(), &fakeFactory{identity: CallerIdentity{AccountID: "1", ARN: "arn:aws:iam::1:role/test"}}, rand.Reader, &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)})
	requestValue := createRequestFixture("connection-handler-0001")
	raw, _ := json.Marshal(requestValue)
	request := httptest.NewRequest(http.MethodPost, "https://controller.example/v1/aws-bootstrap/sessions", strings.NewReader(string(raw)))
	recorder := httptest.NewRecorder()
	service.ControllerHandler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("without mTLS status=%d", recorder.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "https://controller.example/v1/aws-bootstrap/sessions", strings.NewReader(string(raw)))
	request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{}}, VerifiedChains: [][]*x509.Certificate{{{}}}}
	recorder = httptest.NewRecorder()
	service.ControllerHandler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("mTLS status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response CreateResponse
	if json.Unmarshal(recorder.Body.Bytes(), &response) != nil || response.UploadBearer == "" || !strings.HasPrefix(response.UploadURL, configFixture().UploadBaseURL) {
		t.Fatalf("response=%#v", response)
	}
}

func TestPublicUploadHandlerAcceptsNoClientCertificateAndReplaysReceipt(t *testing.T) {
	service, _ := NewService(configFixture(), &fakeFactory{identity: CallerIdentity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:role/bootstrap"}}, rand.Reader, &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)})
	session := createSessionFixture(t, service, "connection-public-upload-0001")
	envelope := encryptFixture(t, session, credentialWire{Schema: CredentialSchema, AccessKeyID: "AKIAABCDEFGHIJKLMNOP", SecretAccessKey: strings.Repeat("z", 40)})
	raw, _ := json.Marshal(envelope)
	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(http.MethodPut, session.UploadURL, strings.NewReader(string(raw)))
		request.Header.Set("Authorization", "Bearer "+session.UploadBearer)
		recorder := httptest.NewRecorder()
		service.UploadHandler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("attempt %d status=%d body=%s", attempt, recorder.Code, recorder.Body.String())
		}
		var receipt Receipt
		if json.Unmarshal(recorder.Body.Bytes(), &receipt) != nil || receipt.Status != "accepted" {
			t.Fatalf("receipt=%#v", receipt)
		}
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *fakeClock) Now() time.Time { clock.mu.Lock(); defer clock.mu.Unlock(); return clock.now }
func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

type fakeFactory struct {
	identity         CallerIdentity
	seen             Credentials
	createCalls      atomic.Int64
	stackRequestMu   sync.Mutex
	seenStackRequest StackRequest
}

func (factory *fakeFactory) Clients(credentials Credentials) (STSClient, StackClient, error) {
	factory.seen = credentials
	return fakeSTS{factory.identity}, &fakeStack{factory: factory}, nil
}

type fakeSTS struct{ identity CallerIdentity }

func (client fakeSTS) GetCallerIdentity(context.Context) (CallerIdentity, error) {
	return client.identity, nil
}

type fakeStack struct{ factory *fakeFactory }

func (client *fakeStack) CreateStack(_ context.Context, request StackRequest) (string, error) {
	client.factory.createCalls.Add(1)
	client.factory.stackRequestMu.Lock()
	client.factory.seenStackRequest = request
	client.factory.stackRequestMu.Unlock()
	templatePolicy := artifactpublish.Policy{Bucket: request.Template.Bucket, KMSKeyID: request.Template.KMSKeyID}
	if request.StackName == "" || request.Region != configFixture().Region || request.Template.ValidateFor(templatePolicy) != nil || len(request.Parameters) != 24 || request.Parameters["ConnectionId"] == "" || request.Parameters["StageName"] != "prod" || request.Parameters["BrokerArtifactBucket"] == "" || request.Parameters["BrokerArtifactVersion"] == "" || request.Parameters["WorkerSubnetId"] == "" || request.Parameters["WorkerSecurityGroupId"] == "" || request.Parameters["EnableDeploymentCreate"] != "false" || request.Parameters["EnableDeploymentDestroy"] != "false" || request.Parameters["EnableServiceBackup"] != "false" || request.Parameters["EnableServiceRestorePlan"] != "false" || request.Parameters["EnableServiceRestore"] != "false" || request.Parameters["EnableServiceSecrets"] != "false" || request.Parameters["EnableDynamicArtifacts"] != "false" || request.Parameters["Environment"] != "" || request.Tags[stackTagManaged] != "true" || request.Tags[stackTagParameterBinding] != stackParameterBindingFingerprint(request.Parameters) || !strings.HasPrefix(request.ClientRequestToken, "dtx-") {
		return "", ErrInvalid
	}
	return "arn:aws:cloudformation:us-east-1:123456789012:stack/accepted/stack-id", nil
}

func (factory *fakeFactory) lastStackRequest() StackRequest {
	factory.stackRequestMu.Lock()
	defer factory.stackRequestMu.Unlock()
	return factory.seenStackRequest
}

type fakeFoundationResolver struct {
	resolution            RootBootstrapResolution
	err                   error
	calls                 atomic.Int64
	request               FoundationResolveRequest
	credentials           Credentials
	accessKeyLength       int
	secretAccessKeyLength int
}

func (resolver *fakeFoundationResolver) ResolveFoundation(_ context.Context, request FoundationResolveRequest, credentials Credentials) (RootBootstrapResolution, error) {
	resolver.calls.Add(1)
	resolver.request = request
	resolver.credentials = credentials
	resolver.accessKeyLength = len(credentials.AccessKeyID)
	resolver.secretAccessKeyLength = len(credentials.SecretAccessKey)
	return resolver.resolution, resolver.err
}

func configFixture() Config {
	artifactPolicy := artifactpublish.Policy{Bucket: "dirextalk-artifacts", KMSKeyID: "arn:aws:kms:us-east-1:123456789012:key/01234567-89ab-cdef-0123-456789abcdef"}
	brokerArtifact, err := artifactpublish.NewBrokerArtifactReference(artifactPolicy, "v1.1.0-cloud-mvp.20260716.1", "sha256:"+strings.Repeat("a", 64), 128, "version-00000001")
	if err != nil {
		panic(err)
	}
	template, err := artifactpublish.NewConnectionTemplateReference(artifactPolicy, "v1.1.0-cloud-mvp.20260716.1", "sha256:"+strings.Repeat("c", 64), 512, "version-00000002")
	if err != nil {
		panic(err)
	}
	return Config{Region: "us-east-1", SourceTreeDigest: "sha256:" + strings.Repeat("d", 64), UploadBaseURL: "https://bootstrap.example.invalid", ArtifactPolicy: artifactPolicy, ConnectionTemplate: connectionTemplateReferenceFixture(template), BrokerArtifact: brokerArtifact, FoundationPlan: testFoundationPlan(), FixedParameters: map[string]string{"Environment": "test"}, FixedTags: map[string]string{"product": "dirextalk"}}
}

func connectionTemplateReferenceFixture(template artifactpublish.ConnectionTemplateReference) ConnectionTemplateReference {
	binding := template.Binding
	return ConnectionTemplateReference{Schema: connectionTemplateReferenceSchema, Mode: connectionTemplateModeS3Binding, Binding: &ConnectionTemplateBinding{
		Schema: binding.Schema, Kind: string(binding.Kind), Version: binding.Version,
		Bucket: binding.Bucket, Key: binding.Key, VersionID: binding.VersionID, SHA256: binding.SHA256,
		SizeBytes: binding.SizeBytes, ContentType: binding.ContentType, KMSKeyID: binding.KMSKeyID,
	}}
}

func rootConfigFixture() Config {
	config := configFixture()
	config.ArtifactPolicy = artifactpublish.Policy{}
	config.ConnectionTemplate = ConnectionTemplateReference{}
	config.BrokerArtifact = artifactpublish.BrokerArtifactReference{}
	config.FoundationPlan = connectionfoundation.Plan{}
	return config
}

func rootIntentConfigFixture() Config {
	config := rootConfigFixture()
	config.ConnectionTemplate = rootCreateRequestFixture("connection-root-config-intent-fixture").RolePlan.ConnectionTemplate
	return config
}

func rootFoundationPlan() connectionfoundation.Plan {
	plan := testFoundationPlan()
	plan.Worker.Artifact.Bucket = "dirextalk-artifacts"
	return plan
}

func rootBootstrapResolutionFixture(plan connectionfoundation.Plan) RootBootstrapResolution {
	config := configFixture()
	template, err := config.ConnectionTemplate.ArtifactReference(config.ArtifactPolicy)
	if err != nil {
		panic(err)
	}
	return RootBootstrapResolution{
		FoundationPlan:     plan,
		FoundationArtifact: config.ArtifactPolicy,
		ConnectionTemplate: template,
		BrokerArtifact:     config.BrokerArtifact,
	}
}

var (
	foundationFixtureOnce  sync.Once
	foundationFixtureValue connectionfoundation.Plan
)

func testFoundationPlan() connectionfoundation.Plan {
	foundationFixtureOnce.Do(func() {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		if err != nil {
			panic(err)
		}
		foundationFixtureValue = connectionfoundation.Plan{
			SchemaVersion: connectionfoundation.PlanSchema,
			Region:        "us-east-1",
			Network: connectionfoundation.Network{
				VPCID:                 "vpc-0123456789abcdef0",
				SubnetID:              "subnet-0123456789abcdef0",
				WorkerSecurityGroupID: "sg-0123456789abcdef0",
				AvailabilityZone:      "us-east-1a",
				PrivateSubnet:         true,
				EgressProfile:         connectionfoundation.PrivateNATHTTPSDNSEgress,
				PublicIngressMode:     connectionfoundation.NoPublicIngress,
			},
			Worker: connectionfoundation.Worker{
				AMIID: "ami-0123456789abcdef0",
				Artifact: connectionfoundation.VersionedArtifact{
					Version:                      "v1.1.0-cloud-mvp.20260715.1",
					Bucket:                       "dirextalk-worker-artifacts",
					ObjectKey:                    "worker/v1.1.0-cloud-mvp.20260715.1/artifact.tar",
					ObjectVersionID:              "3Lg5kqtJlcpXroDTDmJ+.yKk6aYxEtR2",
					ArchiveSHA256:                "sha256:" + strings.Repeat("a", 64),
					ImageManifestSHA256:          "sha256:" + strings.Repeat("b", 64),
					WorkerResourceManifestDigest: "sha256:" + strings.Repeat("c", 64),
				},
				IIDVerifier: connectionfoundation.IIDVerifier{
					Algorithm:       connectionfoundation.EC2IIDRSASHA256Verifier,
					RSAPublicKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})),
				},
			},
		}
	})
	return foundationFixtureValue
}
func createRequestFixture(connectionID string) CreateRequest {
	public, _, _ := ed25519.GenerateKey(strings.NewReader(strings.Repeat("k", 32)))
	keyRaw, _ := x509.MarshalPKIXPublicKey(public)
	key := base64.StdEncoding.EncodeToString(keyRaw)
	config := configFixture()
	parameters := map[string]string{"ConnectionId": connectionID, "ConnectionGeneration": "1", "NodeKeyId": "node-key-0001", "NodePublicKeySpkiBase64": key, "DeviceApprovalKeyId": "device-key-0001", "DeviceApprovalPublicKeySpkiBase64": key, "StageName": "prod"}
	return CreateRequest{Schema: CreateRequestSchema, RequestID: "019f6a80-1234-7abc-8def-0123456789ab", RolePlan: RolePlan{BootstrapID: "bootstrap-session-0001", ConnectionID: connectionID, Region: config.Region, StackName: DeterministicStackName(connectionID), ConnectionTemplate: config.ConnectionTemplate, SourceTreeDigest: config.SourceTreeDigest, FixedParameters: parameters, NodeKeyID: "node-key-0001", NodeEd25519PublicKey: key, DeviceKeyID: "device-key-0001", DeviceEd25519PublicKey: key, ExpiresAt: "2026-07-16T01:20:00Z"}}
}

func rootCreateRequestFixture(connectionID string) CreateRequest {
	request := createRequestFixture(connectionID)
	binding := request.RolePlan.ConnectionTemplate.Binding
	request.RolePlan.AllowRootCredentialBootstrap = true
	request.RolePlan.ConnectionTemplate = ConnectionTemplateReference{Schema: connectionTemplateReferenceSchema, Mode: connectionTemplateModePublishIntent, PublishIntent: &ConnectionTemplatePublishIntent{
		Kind: binding.Kind, Version: binding.Version, SHA256: binding.SHA256, SizeBytes: binding.SizeBytes, ContentType: binding.ContentType,
	}}
	return request
}
func createSessionFixture(t *testing.T, service *Service, connectionID string) CreateResponse {
	t.Helper()
	response, err := service.CreateSession(createRequestFixture(connectionID))
	if err != nil {
		t.Fatal(err)
	}
	return response
}
func encryptFixture(t *testing.T, response CreateResponse, credentials credentialWire) UploadEnvelope {
	t.Helper()
	serverRaw, err := base64.StdEncoding.DecodeString(response.ServerX25519PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ecdh.X25519().NewPublicKey(serverRaw)
	if err != nil {
		t.Fatal(err)
	}
	client, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	shared, err := client.ECDH(server)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(shared)
	key, err := hkdf.Key(sha256.New, shared, []byte(response.SessionID), hkdfInfo, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, aead.NonceSize())
	_, _ = rand.Read(nonce)
	plaintext, _ := json.Marshal(credentials)
	defer clear(plaintext)
	ciphertext := aead.Seal(nil, nonce, plaintext, EnvelopeAAD(response.SessionID, response.ConnectionID, response.ExpiresAt))
	return UploadEnvelope{Schema: UploadEnvelopeSchema, SessionID: response.SessionID, ClientX25519PublicKey: base64.StdEncoding.EncodeToString(client.PublicKey().Bytes()), Nonce: base64.StdEncoding.EncodeToString(nonce), Ciphertext: base64.StdEncoding.EncodeToString(ciphertext), ExpiresAt: response.ExpiresAt}
}
func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
