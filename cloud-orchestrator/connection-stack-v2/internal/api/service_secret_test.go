package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestServiceSecretSessionUploadCompleteAndExactReplay(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	public, private, _ := ed25519.GenerateKey(bytes.NewReader(bytes.Repeat([]byte{7}, 64)))
	proof := serviceSecretProof(t, private, now)
	manifest := serviceSecretManifest(proof)
	manifestJSON, _ := manifest.CanonicalJSON()
	recipeTasks := &memoryRecipeTaskStore{tasks: map[string]commandstore.RecipeTaskRecord{proof.DeploymentID + "\x00" + proof.TaskID: {ConnectionID: proof.ConnectionID, DeploymentID: proof.DeploymentID, TaskID: proof.TaskID, ExecutionID: proof.ExecutionID, RecipeExecutionManifestDigest: proof.ManifestDigest, ManifestJSON: manifestJSON, Status: "queued"}}, receipts: newMemoryCommandStore()}
	deploymentStore := newMemoryCommandStore()
	deploymentStore.deployments[proof.ConnectionID+"\x00"+proof.DeploymentID] = commandstore.DeploymentReservation{ConnectionID: proof.ConnectionID, DeploymentID: proof.DeploymentID, PlanHash: "sha256:" + repeat("3", 64), RecipeDigest: proof.RecipeDigest, SecretScope: []commandstore.ApprovedSecretReference{{SecretRef: proof.SecretRef, Purpose: proof.Purpose, Delivery: proof.Delivery}}, State: "finalized"}
	secretStore := newMemoryServiceSecretStore()
	secretStore.finalizeFailures = 1 // provider succeeded, durable response was lost
	provider := &capturingServiceSecretProvider{}
	sealer := &xorServiceSecretSealer{}
	broker := Broker{ServiceSecretsEnabled: true, ApprovalResolver: StaticApprovalKeyResolver{ConnectionID: proof.ConnectionID, SignerKeyID: proof.SignerKeyID, PublicKey: public}, DeploymentStore: deploymentStore, RecipeTasks: recipeTasks, ServiceSecretStore: secretStore, ServiceSecretProvider: provider, ServiceSecretKeySealer: sealer, ServiceSecretRandom: bytes.NewReader(append(sequential(0, 32), sequential(64, 32)...)), Now: func() time.Time { return now }}
	proofJSON, _ := json.Marshal(proof)
	created := serveJSON(t, broker, http.MethodPost, serviceSecretSessionsPath, proofJSON, "")
	if created.Code != 201 {
		t.Fatalf("create=%d %s", created.Code, created.Body.String())
	}
	createReplay := serveJSON(t, broker, http.MethodPost, serviceSecretSessionsPath, proofJSON, "")
	if createReplay.Code != 201 || createReplay.Body.String() != created.Body.String() {
		t.Fatalf("lost create replay=%d body=%s", createReplay.Code, createReplay.Body.String())
	}
	sealer.failTokenUnseal = true
	failedReplay := serveJSON(t, broker, http.MethodPost, serviceSecretSessionsPath, proofJSON, "")
	if failedReplay.Code != 503 {
		t.Fatalf("token unseal must fail closed: %d", failedReplay.Code)
	}
	sealer.failTokenUnseal = false
	for name, mutate := range map[string]func(*contract.ServiceSecretApprovalProof){
		"artifact": func(p *contract.ServiceSecretApprovalProof) {
			p.SessionID = "secret-session-artifact"
			p.ArtifactDigest = "sha256:" + repeat("9", 64)
		},
		"slot": func(p *contract.ServiceSecretApprovalProof) {
			p.SessionID = "secret-session-slot"
			p.SlotID = "other_token"
		},
	} {
		t.Run("reject task scope "+name, func(t *testing.T) {
			changed := proof
			mutate(&changed)
			resignServiceSecretProof(t, &changed, private)
			raw, _ := json.Marshal(changed)
			rejected := serveJSON(t, broker, http.MethodPost, serviceSecretSessionsPath, raw, "")
			if rejected.Code != http.StatusConflict {
				t.Fatalf("status=%d body=%s", rejected.Code, rejected.Body.String())
			}
		})
	}
	var createMap map[string]any
	_ = json.Unmarshal(created.Body.Bytes(), &createMap)
	token := createMap["upload_token"].(string)
	serverPublic, _ := base64.RawURLEncoding.DecodeString(createMap["server_public_key_b64"].(string))
	plaintext := []byte("SERVICE_SECRET_CANARY_DO_NOT_PERSIST")
	envelope, err := contract.EncryptServiceSecret(proof.Context(), serverPublic, sequential(32, 32), sequential(0, 12), plaintext)
	if err != nil {
		t.Fatal(err)
	}
	envelopeJSON, _ := envelope.CanonicalJSON()
	uploadPath := serviceSecretSessionsPath + "/" + proof.SessionID + "/encrypted-upload"
	upload := serveJSON(t, broker, http.MethodPut, uploadPath, envelopeJSON, token)
	if upload.Code != 503 || provider.calls != 1 || provider.mutations != 1 || !bytes.Equal(provider.plaintext, plaintext) {
		t.Fatalf("upload=%d calls=%d body=%s", upload.Code, provider.calls, upload.Body.String())
	}
	replay := serveJSON(t, broker, http.MethodPut, uploadPath, envelopeJSON, token)
	if replay.Code != 200 || provider.calls != 2 || provider.mutations != 1 {
		t.Fatalf("replay=%d calls=%d mutations=%d", replay.Code, provider.calls, provider.mutations)
	}
	secondReplay := serveJSON(t, broker, http.MethodPut, uploadPath, envelopeJSON, token)
	if secondReplay.Code != 200 || provider.calls != 2 {
		t.Fatalf("finalized replay=%d calls=%d", secondReplay.Code, provider.calls)
	}
	different, _ := contract.EncryptServiceSecret(proof.Context(), serverPublic, sequential(32, 32), sequential(1, 12), plaintext)
	differentJSON, _ := different.CanonicalJSON()
	conflict := serveJSON(t, broker, http.MethodPut, uploadPath, differentJSON, token)
	if conflict.Code != http.StatusConflict || provider.calls != 2 {
		t.Fatalf("different envelope=%d calls=%d", conflict.Code, provider.calls)
	}
	completeRaw := []byte(`{"schema":"dirextalk.service-secret-complete/v1","envelope_digest":"` + secretStore.sessions[proof.SessionID].EnvelopeDigest + `"}`)
	now = proof.ExpiresAt.Add(time.Second)
	expiredComplete := serveJSON(t, broker, http.MethodPost, serviceSecretSessionsPath+"/"+proof.SessionID+"/complete", completeRaw, token)
	if expiredComplete.Code != http.StatusGone || secretStore.sessions[proof.SessionID].State != commandstore.ServiceSecretUploaded {
		t.Fatalf("expired unfinished complete=%d state=%s", expiredComplete.Code, secretStore.sessions[proof.SessionID].State)
	}
	now = proof.ExpiresAt.Add(-time.Second)
	completed := serveJSON(t, broker, http.MethodPost, serviceSecretSessionsPath+"/"+proof.SessionID+"/complete", completeRaw, token)
	if completed.Code != 200 || secretStore.sessions[proof.SessionID].State != commandstore.ServiceSecretCompleted {
		t.Fatalf("complete=%d %s", completed.Code, completed.Body.String())
	}
	now = proof.ExpiresAt.Add(time.Second)
	lostUploadReceiptReplay := serveJSON(t, broker, http.MethodPut, uploadPath, envelopeJSON, token)
	if lostUploadReceiptReplay.Code != http.StatusOK || provider.calls != 2 {
		t.Fatalf("completed upload replay after expiry=%d calls=%d", lostUploadReceiptReplay.Code, provider.calls)
	}
	expiredDifferentReplay := serveJSON(t, broker, http.MethodPut, uploadPath, differentJSON, token)
	if expiredDifferentReplay.Code != http.StatusConflict || provider.calls != 2 {
		t.Fatalf("completed conflicting replay after expiry=%d calls=%d", expiredDifferentReplay.Code, provider.calls)
	}
	stored, _ := json.Marshal(secretStore.sessions[proof.SessionID])
	for _, output := range [][]byte{stored, upload.Body.Bytes(), replay.Body.Bytes(), completed.Body.Bytes()} {
		if bytes.Contains(output, plaintext) || bytes.Contains(output, envelopeJSON) || bytes.Contains(output, []byte(token)) {
			t.Fatal("secret or envelope persisted/reflected")
		}
	}
}

func TestServiceSecretRouteDefaultsOff(t *testing.T) {
	response := serveJSON(t, Broker{}, http.MethodPost, serviceSecretSessionsPath, []byte(`{}`), "")
	if response.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d", response.Code)
	}
}

func serviceSecretProof(t *testing.T, key ed25519.PrivateKey, now time.Time) contract.ServiceSecretApprovalProof {
	t.Helper()
	p := contract.ServiceSecretApprovalProof{SchemaVersion: "cloud-orchestrator/v1", Intent: contract.ServiceSecretApprovalIntent, ApprovalID: "approval-secret-0001", ChallengeID: "challenge-secret-0001", SignerKeyID: "device-secret-0001", SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-secret-0001", ExecutionID: "execution-0001", RecipeDigest: "sha256:" + repeat("1", 64), ArtifactDigest: "sha256:" + repeat("2", 64), SlotID: "model_token", SecretRef: "secret_ref:model-token-001", Purpose: "model inference", Delivery: "environment", IssuedAt: now, ExpiresAt: now.Add(10 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
	p.ManifestDigest, _ = serviceSecretManifest(p).Digest()
	resignServiceSecretProof(t, &p, key)
	return p
}
func resignServiceSecretProof(t *testing.T, p *contract.ServiceSecretApprovalProof, key ed25519.PrivateKey) {
	t.Helper()
	p.ContextDigest, _ = p.Context().Digest()
	p.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	payload, err := p.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	p.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
}
func serviceSecretManifest(p contract.ServiceSecretApprovalProof) contract.RecipeExecutionManifestV1 {
	return contract.RecipeExecutionManifestV1{SchemaVersion: contract.RecipeExecutionManifestSchema, ExecutionID: p.ExecutionID, DeploymentID: p.DeploymentID, PlanID: "plan-secret-0001", PlanHash: "sha256:" + repeat("3", 64), PlanRevision: 1, RecipeDigest: p.RecipeDigest, WorkerResourceManifestDigest: "sha256:" + repeat("4", 64), ArtifactDigest: p.ArtifactDigest, ActionID: "install_service", RootRequired: true, TimeoutSeconds: 900, CheckpointSequence: []string{"artifact_verified", "health_verified"}, SecretSlots: []contract.RecipeSecretSlotV1{{SlotID: p.SlotID, SecretRef: p.SecretRef}}}
}
func repeat(s string, n int) string { return string(bytes.Repeat([]byte(s), n)) }
func sequential(start byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = start + byte(i)
	}
	return out
}
func serveJSON(t *testing.T, b Broker, method, path string, body []byte, token string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	return w
}

type xorServiceSecretSealer struct{ failTokenUnseal bool }

func (xorServiceSecretSealer) SealServiceSecretKey(_ context.Context, key, _ []byte) (string, error) {
	sealed := append([]byte(nil), key...)
	for i := range sealed {
		sealed[i] ^= 0xa5
	}
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}
func (xorServiceSecretSealer) UnsealServiceSecretKey(_ context.Context, sealed string, _ []byte) ([]byte, error) {
	key, _ := base64.RawURLEncoding.DecodeString(sealed)
	for i := range key {
		key[i] ^= 0xa5
	}
	return key, nil
}
func (xorServiceSecretSealer) SealServiceSecretToken(_ context.Context, token, _ []byte) (string, error) {
	sealed := append([]byte(nil), token...)
	for i := range sealed {
		sealed[i] ^= 0x5a
	}
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}
func (s xorServiceSecretSealer) UnsealServiceSecretToken(_ context.Context, sealed string, _ []byte) ([]byte, error) {
	if s.failTokenUnseal {
		return nil, commandstore.NewError("test_unseal_failed")
	}
	token, _ := base64.RawURLEncoding.DecodeString(sealed)
	for i := range token {
		token[i] ^= 0x5a
	}
	return token, nil
}

type capturingServiceSecretProvider struct {
	calls     int
	mutations int
	plaintext []byte
	versions  map[string]string
	readValue []byte
	getCalls  int
	getErr    error
}

func (p *capturingServiceSecretProvider) PutServiceSecret(_ context.Context, binding ServiceSecretProviderBinding, value []byte) (string, error) {
	p.calls++
	if p.versions == nil {
		p.versions = map[string]string{}
	}
	if version, ok := p.versions[binding.EnvelopeDigest]; ok {
		return version, nil
	}
	p.mutations++
	p.plaintext = append([]byte(nil), value...)
	p.versions[binding.EnvelopeDigest] = "opaque-version-0001"
	return p.versions[binding.EnvelopeDigest], nil
}
func (p *capturingServiceSecretProvider) GetServiceSecret(_ context.Context, _ ServiceSecretReadBinding) ([]byte, error) {
	p.getCalls++
	if p.getErr != nil {
		return nil, p.getErr
	}
	if p.readValue != nil {
		return append([]byte(nil), p.readValue...), nil
	}
	return append([]byte(nil), p.plaintext...), nil
}

type memoryServiceSecretStore struct {
	mu               sync.Mutex
	sessions         map[string]commandstore.ServiceSecretSession
	finalizeFailures int
}

func newMemoryServiceSecretStore() *memoryServiceSecretStore {
	return &memoryServiceSecretStore{sessions: map[string]commandstore.ServiceSecretSession{}}
}
func (s *memoryServiceSecretStore) LookupServiceSecret(_ context.Context, id string) (commandstore.ServiceSecretSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.sessions[id]
	return v, ok, nil
}
func (s *memoryServiceSecretStore) LookupCompletedServiceSecret(_ context.Context, connectionID, deploymentID, recipeDigest, artifactDigest, slotID, secretRef string) (commandstore.ServiceSecretSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.sessions {
		if v.State == commandstore.ServiceSecretCompleted && v.ConnectionID == connectionID && v.DeploymentID == deploymentID && v.RecipeDigest == recipeDigest && v.ArtifactDigest == artifactDigest && v.SlotID == slotID && v.SecretRef == secretRef {
			return v, true, nil
		}
	}
	return commandstore.ServiceSecretSession{}, false, nil
}
func (s *memoryServiceSecretStore) CreateServiceSecret(_ context.Context, v commandstore.ServiceSecretSession) (commandstore.ServiceSecretSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.sessions[v.SessionID]; ok {
		return old, false, nil
	}
	s.sessions[v.SessionID] = v
	return v, true, nil
}
func (s *memoryServiceSecretStore) ClaimServiceSecretEnvelope(_ context.Context, id, digest string) (commandstore.ServiceSecretSession, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.sessions[id]
	if v.EnvelopeDigest != "" && v.EnvelopeDigest != digest {
		return v, false, commandstore.NewError("service_secret_envelope_conflict")
	}
	claimed := v.EnvelopeDigest == ""
	v.EnvelopeDigest = digest
	if v.State == commandstore.ServiceSecretPending {
		v.State = commandstore.ServiceSecretProcessing
	}
	s.sessions[id] = v
	return v, claimed, nil
}
func (s *memoryServiceSecretStore) FinalizeServiceSecretUpload(_ context.Context, id, digest, version string) (commandstore.ServiceSecretSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.sessions[id]
	if s.finalizeFailures > 0 {
		s.finalizeFailures--
		return v, commandstore.NewError("connection_stack_store_unavailable")
	}
	if v.EnvelopeDigest != digest {
		return v, commandstore.NewError("service_secret_envelope_conflict")
	}
	v.State = commandstore.ServiceSecretUploaded
	v.ProviderVersion = version
	s.sessions[id] = v
	return v, nil
}
func (s *memoryServiceSecretStore) CompleteServiceSecret(_ context.Context, id, digest string) (commandstore.ServiceSecretSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.sessions[id]
	if v.EnvelopeDigest != digest || v.State == commandstore.ServiceSecretPending || v.State == commandstore.ServiceSecretProcessing {
		return v, commandstore.NewError("service_secret_not_uploaded")
	}
	v.State = commandstore.ServiceSecretCompleted
	s.sessions[id] = v
	return v, nil
}
