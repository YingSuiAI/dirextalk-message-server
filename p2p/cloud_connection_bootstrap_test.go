package p2p

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
)

func TestCloudConnectionRolePlanQueuesOnlyVerifiedRegistration(t *testing.T) {
	nodePublic := testEd25519SPKIBase64(t)
	devicePublic := testEd25519SPKIBase64(t)
	service := NewService(Config{
		ServerName: "example.com",
		CloudConnectionStack: CloudConnectionStackConfig{
			TemplateDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ConnectionTemplate:      testS3ConnectionTemplate(),
			SourceTreeDigest:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			NodeKeyID:               "node-key-1",
			NodePublicKeySPKIBase64: nodePublic,
			RolePlanTTL:             15 * time.Minute,
		},
	})
	router := newP2PTestRouter(service)
	rolePlanKey := uuid.NewString()
	rolePlan := cloudCommand(t, router, service, "cloud.connections.role_plan", map[string]any{
		"provider": "aws", "region": "ap-northeast-1", "device_approval_key_id": "device-key-1",
		"device_approval_public_key_spki_base64": devicePublic, "idempotency_key": rolePlanKey,
	})
	plan, ok := rolePlan["role_plan"].(map[string]any)
	if !ok || plan["provider"] != "aws" || plan["region"] != "ap-northeast-1" || plan["status"] != cloudmodule.ConnectionBootstrapAwaitingStack || plan["bootstrap_id"] == "" || plan["cloud_connection_id"] == "" || plan["source_tree_digest"] != "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" || plan["allow_root_credential_bootstrap"] != false || plan["template_url"] != testS3ConnectionTemplateURL("ap-northeast-1") {
		t.Fatalf("role plan = %#v", rolePlan)
	}
	if template, ok := plan["connection_template"].(map[string]any); !ok || template["mode"] != "s3_binding" {
		t.Fatalf("role plan omitted immutable connection template: %#v", rolePlan)
	}
	params, ok := plan["cloudformation_parameters"].(map[string]any)
	if !ok || params["ConnectionId"] != plan["cloud_connection_id"] || params["NodeKeyId"] != "node-key-1" || params["DeviceApprovalKeyId"] != "device-key-1" || params["StageName"] != "prod" {
		t.Fatalf("role plan parameters = %#v", plan)
	}
	encodedRolePlan := mustMarshalJSON(t, rolePlan)
	for _, forbidden := range []string{"secret", "access_key", "private_key", "broker_command_url", "stack_arn"} {
		if strings.Contains(strings.ToLower(encodedRolePlan), forbidden) {
			t.Fatalf("role plan leaked %q: %s", forbidden, encodedRolePlan)
		}
	}

	replay := cloudCommand(t, router, service, "cloud.connections.role_plan", map[string]any{
		"provider": "aws", "region": "ap-northeast-1", "device_approval_key_id": "device-key-1",
		"device_approval_public_key_spki_base64": devicePublic, "idempotency_key": rolePlanKey,
	})
	if replay["role_plan"].(map[string]any)["bootstrap_id"] != plan["bootstrap_id"] {
		t.Fatalf("role-plan replay changed its bootstrap: first=%#v replay=%#v", rolePlan, replay)
	}

	invalidEndpoint := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.connections.registration.complete",
		"params": map[string]any{
			"bootstrap_id": plan["bootstrap_id"], "expected_revision": 1, "idempotency_key": uuid.NewString(),
			"broker_command_url": "https://example.invalid/prod/v2/commands",
			"stack_arn":          validConnectionStackARN,
		},
	})
	invalidEndpoint.Header.Set("Authorization", "Bearer "+service.AccessToken())
	invalidRecorder := httptest.NewRecorder()
	router.ServeHTTP(invalidRecorder, invalidEndpoint)
	if invalidRecorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid registration endpoint = %d body=%s", invalidRecorder.Code, invalidRecorder.Body.String())
	}

	completed := cloudCommand(t, router, service, "cloud.connections.registration.complete", map[string]any{
		"bootstrap_id": plan["bootstrap_id"], "expected_revision": 1, "idempotency_key": uuid.NewString(),
		"broker_command_url": validConnectionBrokerURL, "stack_arn": validConnectionStackARN,
	})
	registration, ok := completed["registration"].(map[string]any)
	if !ok || registration["bootstrap_id"] != plan["bootstrap_id"] || registration["cloud_connection_id"] != plan["cloud_connection_id"] || registration["status"] != cloudmodule.ConnectionBootstrapVerificationQueued || registration["job_id"] == "" {
		t.Fatalf("registration completion = %#v", completed)
	}
	encodedRegistration := mustMarshalJSON(t, completed)
	for _, forbidden := range []string{"broker_command_url", "stack_arn", "secret", "private_key"} {
		if strings.Contains(strings.ToLower(encodedRegistration), forbidden) {
			t.Fatalf("registration response leaked %q: %s", forbidden, encodedRegistration)
		}
	}
	connections := cloudCommand(t, router, service, "cloud.connections.list", map[string]any{})
	if got, ok := connections["connections"].([]any); !ok || len(got) != 0 {
		t.Fatalf("unverified connection must not be public: %#v", connections)
	}
	bootstrap := cloudCommand(t, router, service, "cloud.bootstrap", map[string]any{})
	jobs, ok := bootstrap["jobs"].([]any)
	if !ok || len(jobs) != 1 || jobs[0].(map[string]any)["kind"] != "connection_registration" || jobs[0].(map[string]any)["checkpoint"] != "connection_verification_queued" {
		t.Fatalf("registration job = %#v", bootstrap["jobs"])
	}
}

func TestCloudConnectionRolePlanBindsRootCredentialBootstrapPermit(t *testing.T) {
	nodePublic := testEd25519SPKIBase64(t)
	devicePublic := testEd25519SPKIBase64(t)
	service := NewService(Config{ServerName: "example.com", CloudConnectionStack: CloudConnectionStackConfig{
		TemplateDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ConnectionTemplate: testPublishIntentConnectionTemplate(),
		SourceTreeDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", NodeKeyID: "node-key-1",
		NodePublicKeySPKIBase64: nodePublic, RolePlanTTL: 15 * time.Minute,
	}})
	router := newP2PTestRouter(service)
	idempotencyKey := uuid.NewString()
	params := map[string]any{
		"provider": "aws", "region": "ap-northeast-1", "device_approval_key_id": "device-key-root-1",
		"device_approval_public_key_spki_base64": devicePublic, "allow_root_credential_bootstrap": true, "idempotency_key": idempotencyKey,
	}
	result := cloudCommand(t, router, service, "cloud.connections.role_plan", params)
	plan := result["role_plan"].(map[string]any)
	if plan["allow_root_credential_bootstrap"] != true {
		t.Fatalf("root permit missing from role plan: %#v", plan)
	}
	if plan["template_url"] != "" {
		t.Fatalf("root role plan must not invent a pre-foundation template URL: %#v", plan)
	}
	if template, ok := plan["connection_template"].(map[string]any); !ok || template["mode"] != "publish_intent" {
		t.Fatalf("root role plan omitted immutable publish intent: %#v", plan)
	}
	if replay := cloudCommand(t, router, service, "cloud.connections.role_plan", params); replay["role_plan"].(map[string]any)["bootstrap_id"] != plan["bootstrap_id"] {
		t.Fatalf("root permit replay changed plan: %#v", replay)
	}
	changed := make(map[string]any, len(params))
	for key, value := range params {
		changed[key] = value
	}
	changed["allow_root_credential_bootstrap"] = false
	request := jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.role_plan", "params": changed})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("ordinary role path must reject the root publish intent: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	changed["idempotency_key"] = uuid.NewString()
	changed["allow_root_credential_bootstrap"] = "true"
	request = jsonRequest(t, "/_p2p/command", map[string]any{"action": "cloud.connections.role_plan", "params": changed})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("non-boolean root permit status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestCloudConnectionRolePlanFailsClosedWithoutPinnedSourceTree(t *testing.T) {
	service := NewService(Config{
		ServerName: "example.com",
		CloudConnectionStack: CloudConnectionStackConfig{
			TemplateDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ConnectionTemplate:      testS3ConnectionTemplate(),
			NodeKeyID:               "node-key-1",
			NodePublicKeySPKIBase64: testEd25519SPKIBase64(t),
			RolePlanTTL:             15 * time.Minute,
		},
	})
	router := newP2PTestRouter(service)
	request := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.connections.role_plan",
		"params": map[string]any{
			"provider": "aws", "region": "ap-northeast-1", "device_approval_key_id": "device-key-1",
			"device_approval_public_key_spki_base64": testEd25519SPKIBase64(t), "idempotency_key": uuid.NewString(),
		},
	})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "cloud_connection_stack_unavailable") {
		t.Fatalf("missing source-tree digest must fail closed: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestCloudConnectionRolePlanRejectsLegacyRawTemplateURLConfig(t *testing.T) {
	service := NewService(Config{ServerName: "example.com", CloudConnectionStack: CloudConnectionStackConfig{
		TemplateURL:             testS3ConnectionTemplateURL("ap-northeast-1"),
		TemplateDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ConnectionTemplate:      testS3ConnectionTemplate(),
		SourceTreeDigest:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		NodeKeyID:               "node-key-1",
		NodePublicKeySPKIBase64: testEd25519SPKIBase64(t),
		RolePlanTTL:             15 * time.Minute,
	}})
	router := newP2PTestRouter(service)
	request := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.connections.role_plan",
		"params": map[string]any{
			"provider": "aws", "region": "ap-northeast-1", "device_approval_key_id": "device-key-1",
			"device_approval_public_key_spki_base64": testEd25519SPKIBase64(t), "idempotency_key": uuid.NewString(),
		},
	})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "cloud_connection_stack_unavailable") {
		t.Fatalf("legacy raw template URL must be rejected before role-plan issuance: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

const (
	validConnectionBrokerURL = "https://a1b2c3d4e5.execute-api.ap-northeast-1.amazonaws.com/prod/v2/commands"
	validConnectionStackARN  = "arn:aws:cloudformation:ap-northeast-1:123456789012:stack/dirextalk-test/12345678-1234-1234-1234-123456789012"
)

func testS3ConnectionTemplate() cloudmodule.ConnectionTemplateReference {
	return cloudmodule.ConnectionTemplateReference{
		Schema: "dirextalk.connection-template-reference/v1", Mode: "s3_binding",
		Binding: &cloudmodule.ConnectionTemplateBinding{
			Schema: "dirextalk.immutable-artifact-binding/v1", Kind: "connection_stack_template", Version: "v1.1.0-cloud-mvp.20260716.1",
			Bucket: "dirextalk-artifacts", Key: "releases/connection-stack/v1.1.0-cloud-mvp.20260716.1/connection-stack-v1.1.0-cloud-mvp.20260716.1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.yaml",
			VersionID: "version-00000001", SHA256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 512,
			ContentType: "application/x-yaml", KMSKeyID: "alias/dirextalk-artifacts",
		},
	}
}

func testPublishIntentConnectionTemplate() cloudmodule.ConnectionTemplateReference {
	return cloudmodule.ConnectionTemplateReference{
		Schema: "dirextalk.connection-template-reference/v1", Mode: "publish_intent",
		PublishIntent: &cloudmodule.ConnectionTemplatePublishIntent{
			Kind: "connection_stack_template", Version: "v1.1.0-cloud-mvp.20260716.1",
			SHA256: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 512, ContentType: "application/x-yaml",
		},
	}
}

func testS3ConnectionTemplateURL(region string) string {
	if region == "ap-northeast-1" {
		return "https://s3.ap-northeast-1.amazonaws.com/dirextalk-artifacts/releases/connection-stack/v1.1.0-cloud-mvp.20260716.1/connection-stack-v1.1.0-cloud-mvp.20260716.1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.yaml?versionId=version-00000001"
	}
	return ""
}

func cloudCommand(t *testing.T, router http.Handler, service *Service, action string, params map[string]any) map[string]any {
	t.Helper()
	request := jsonRequest(t, "/_p2p/command", map[string]any{"action": action, "params": params})
	request.Header.Set("Authorization", "Bearer "+service.AccessToken())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("%s = %d body=%s", action, recorder.Code, recorder.Body.String())
	}
	return decodeJSONMap(t, recorder.Body.String())
}

func testEd25519SPKIBase64(t *testing.T) string {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(der)
}

func mustMarshalJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
