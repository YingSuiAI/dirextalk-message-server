package connectionbootstrap

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	config := configFixture()
	service, _ := NewService(config, factory, rand.Reader, clock)
	request := createRequestFixture("connection-root-bootstrap-0001")
	request.RolePlan.AllowRootCredentialBootstrap = true
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
	if !allZero(factory.seen.AccessKeyID) || !allZero(factory.seen.SecretAccessKey) || !allZero(factory.seen.SessionToken) {
		t.Fatal("root bootstrap credential buffers were retained after AWS acceptance")
	}
	replay, err := service.Upload(context.Background(), response.SessionID, response.UploadBearer, envelope)
	if err != nil || replay != receipt || factory.createCalls.Load() != 1 {
		t.Fatalf("root bootstrap replay=%#v err=%v create_calls=%d", replay, err, factory.createCalls.Load())
	}
}

func TestStackRequestUsesOnlyClosedLifecycleFeatureFlags(t *testing.T) {
	config := configFixture()
	config.DeploymentDestroyEnabled = true
	config.ServiceSecretsEnabled = true
	service, err := NewService(config, &fakeFactory{}, rand.Reader, &fakeClock{now: time.Date(2026, time.July, 16, 1, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	plan := createRequestFixture("connection-feature-flags-0001").RolePlan
	request := service.stackRequest(Identity{
		BootstrapID: plan.BootstrapID, ConnectionID: plan.ConnectionID, StackName: plan.StackName,
		NodeKeyID: plan.NodeKeyID, NodeEd25519PublicKey: plan.NodeEd25519PublicKey,
		DeviceKeyID: plan.DeviceKeyID, DeviceEd25519PublicKey: plan.DeviceEd25519PublicKey,
		FixedParameters: cloneStringMap(plan.FixedParameters),
	}, "aws-bootstrap-feature-flags-0001", "fingerprint")
	if request.Parameters["EnableDeploymentDestroy"] != "true" || request.Parameters["EnableServiceSecrets"] != "true" || request.Parameters["Environment"] != "" {
		t.Fatalf("closed lifecycle feature flags=%#v", request.Parameters)
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
	identity    CallerIdentity
	seen        Credentials
	createCalls atomic.Int64
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
	if request.StackName == "" || request.TemplateURL != configFixture().TemplateURL || len(request.Parameters) != 20 || request.Parameters["ConnectionId"] == "" || request.Parameters["StageName"] != "prod" || request.Parameters["BrokerArtifactBucket"] == "" || request.Parameters["WorkerSubnetId"] == "" || request.Parameters["EnableDeploymentCreate"] != "false" || request.Parameters["EnableDeploymentDestroy"] != "false" || request.Parameters["EnableServiceSecrets"] != "false" || request.Parameters["EnableDynamicArtifacts"] != "false" || request.Parameters["Environment"] != "" || request.Tags["dirextalk:managed"] != "true" || !strings.HasPrefix(request.ClientRequestToken, "dtx-") {
		return "", ErrInvalid
	}
	return "arn:aws:cloudformation:us-east-1:123456789012:stack/accepted/stack-id", nil
}

func configFixture() Config {
	return Config{Region: "us-east-1", TemplateURL: "https://artifacts.example.invalid/connection-stack-v2.yaml", TemplateDigest: "sha256:" + strings.Repeat("c", 64), SourceTreeDigest: "sha256:" + strings.Repeat("d", 64), UploadBaseURL: "https://bootstrap.example.invalid", BrokerArtifact: "sha256:" + strings.Repeat("a", 64), BrokerArtifactBucket: "dirextalk-artifacts", BrokerArtifactKey: "broker/v1/broker.zip", WorkerAMIID: "ami-0123456789abcdef0", NetworkID: "vpc-0123456789abcdef0", WorkerSubnetID: "subnet-0123456789abcdef0", WorkerAvailabilityZone: "us-east-1a", ArtifactManifestDigest: "sha256:" + strings.Repeat("b", 64), FixedParameters: map[string]string{"Environment": "test"}, FixedTags: map[string]string{"product": "dirextalk"}}
}
func createRequestFixture(connectionID string) CreateRequest {
	public, _, _ := ed25519.GenerateKey(strings.NewReader(strings.Repeat("k", 32)))
	keyRaw, _ := x509.MarshalPKIXPublicKey(public)
	key := base64.StdEncoding.EncodeToString(keyRaw)
	config := configFixture()
	parameters := map[string]string{"ConnectionId": connectionID, "ConnectionGeneration": "1", "NodeKeyId": "node-key-0001", "NodePublicKeySpkiBase64": key, "DeviceApprovalKeyId": "device-key-0001", "DeviceApprovalPublicKeySpkiBase64": key, "StageName": "prod"}
	return CreateRequest{Schema: CreateRequestSchema, RequestID: "019f6a80-1234-7abc-8def-0123456789ab", RolePlan: RolePlan{BootstrapID: "bootstrap-session-0001", ConnectionID: connectionID, Region: config.Region, StackName: DeterministicStackName(connectionID), TemplateURL: config.TemplateURL, TemplateDigest: config.TemplateDigest, SourceTreeDigest: config.SourceTreeDigest, FixedParameters: parameters, NodeKeyID: "node-key-0001", NodeEd25519PublicKey: key, DeviceKeyID: "device-key-0001", DeviceEd25519PublicKey: key, ExpiresAt: "2026-07-16T01:20:00Z"}}
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
