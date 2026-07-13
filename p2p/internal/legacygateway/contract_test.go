package legacygateway

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	fixtureTenant       = "01890f00-0000-7000-8000-000000000301"
	fixtureRequest      = "01890f00-0000-7000-8000-000000000310"
	fixtureInstallation = "01890f00-0000-7000-8000-000000000311"
	fixtureConversation = "01890f00-0000-7000-8000-000000000312"
	fixtureRequestEvent = "01890f00-0000-7000-8000-000000000313"
	fixtureConnector    = "01890f00-0000-7000-8000-000000000314"
	fixtureRoom         = "!legacy:example.test"
)

func TestParseInvocationContentIsStrictAndDropsRawIdempotencyKey(t *testing.T) {
	content := `{
		"request_id":"01890f00-0000-7000-8000-000000000310",
		"installation_id":"01890f00-0000-7000-8000-000000000311",
		"preferred_connector_id":"01890f00-0000-7000-8000-000000000314",
		"dispatch_mode":"single",
		"grant_version":4,
		"input_event_id":"$prompt:example.test",
		"required_capabilities":["tool.read","chat.streaming"],
		"idempotency_key":"once-01"
	}`
	invocation, err := ParseInvocationContent(fixtureTenant, fixtureRoom, []byte(content))
	if err != nil {
		t.Fatalf("ParseInvocationContent() error = %v", err)
	}
	if got, want := invocation.RequiredCapabilities, []string{"chat.streaming", "tool.read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("capabilities = %v, want %v", got, want)
	}
	if _, exists := reflect.TypeOf(invocation).FieldByName("IdempotencyKey"); exists {
		t.Fatal("Invocation must not return the raw idempotency key")
	}
	if got := hex.EncodeToString(invocation.IdempotencyDigest[:]); got != "64c1af57ad01ed35ddc024958d56c8aec12405d975429f3bedb7a72af87200d3" {
		t.Fatalf("idempotency digest = %s", got)
	}

	invalid := []string{
		strings.Replace(content, `"idempotency_key":"once-01"`, `"idempotency_key":"once-01","unknown":true`, 1),
		content + `{}`,
		strings.Replace(content, `"grant_version":4`, `"grant_version":9007199254740992`, 1),
		strings.Replace(content, `"tool.read","chat.streaming"`, `"tool.read","tool.read"`, 1),
		strings.Replace(content, `"tool.read","chat.streaming"`, `"tool.Read","chat.streaming"`, 1),
		strings.Replace(content, `"idempotency_key":"once-01"`, `"idempotency_key":""`, 1),
		strings.Replace(content, fixtureRequest, "01890f00-0000-4000-8000-000000000310", 1),
		strings.Replace(content, `"idempotency_key":"once-01"`, `"request_id":"`+fixtureRequest+`","idempotency_key":"once-01"`, 1),
	}
	for index, candidate := range invalid {
		if _, err := ParseInvocationContent(fixtureTenant, fixtureRoom, []byte(candidate)); err == nil {
			t.Fatalf("invalid content %d was accepted", index)
		}
	}
}

func TestParseInvocationContentRequiresCapabilitiesFieldButAllowsEmptyArray(t *testing.T) {
	content := `{
		"request_id":"01890f00-0000-7000-8000-000000000310",
		"installation_id":"01890f00-0000-7000-8000-000000000311",
		"preferred_connector_id":null,
		"dispatch_mode":"single",
		"grant_version":4,
		"input_event_id":"$prompt:example.test",
		"required_capabilities":[],
		"idempotency_key":"once-01"
	}`

	invocation, err := ParseInvocationContent(fixtureTenant, fixtureRoom, []byte(content))
	if err != nil {
		t.Fatalf("ParseInvocationContent() explicit empty capabilities error = %v", err)
	}
	if len(invocation.RequiredCapabilities) != 0 {
		t.Fatalf("capabilities = %#v, want empty", invocation.RequiredCapabilities)
	}

	invalid := []string{
		strings.Replace(content, "\n\t\t\"required_capabilities\":[],", "", 1),
		strings.Replace(content, `"required_capabilities":[]`, `"required_capabilities":null`, 1),
	}
	for index, candidate := range invalid {
		if _, err := ParseInvocationContent(fixtureTenant, fixtureRoom, []byte(candidate)); err == nil {
			t.Fatalf("invalid capabilities content %d was accepted", index)
		}
	}
}

func TestGatewayDigestsMatchFrozenCrossLanguageVector(t *testing.T) {
	idempotencyDigest, err := IdempotencyDigest(fixtureTenant, fixtureRoom, "once-01")
	if err != nil {
		t.Fatalf("IdempotencyDigest() error = %v", err)
	}
	if got := hex.EncodeToString(idempotencyDigest[:]); got != "64c1af57ad01ed35ddc024958d56c8aec12405d975429f3bedb7a72af87200d3" {
		t.Fatalf("idempotency digest = %s", got)
	}
	request := CreateRunRequest{
		RequestID:            fixtureRequest,
		IdempotencyDigest:    idempotencyDigest,
		InstallationID:       fixtureInstallation,
		ConversationID:       fixtureConversation,
		RequestEventID:       fixtureRequestEvent,
		PreferredConnectorID: fixtureConnector,
		RequiredCapabilities: []string{"chat.streaming", "tool.read"},
		DispatchMode:         DispatchSingle,
		GrantVersion:         4,
	}
	requestDigest, err := RequestDigest(fixtureTenant, request)
	if err != nil {
		t.Fatalf("RequestDigest() error = %v", err)
	}
	if got := hex.EncodeToString(requestDigest[:]); got != "a49fbeb70ef8a0e7e1cdf1961f5873f450a22112306c52d5992be9cbfb6acf80" {
		t.Fatalf("request digest = %s", got)
	}
}

func TestBuildCandidateKeepsSourceIdentityStableAcrossCrashRetry(t *testing.T) {
	idempotencyDigest, err := IdempotencyDigest(fixtureTenant, fixtureRoom, "once-01")
	if err != nil {
		t.Fatal(err)
	}
	invocation := Invocation{
		RequestID:            fixtureRequest,
		InstallationID:       fixtureInstallation,
		PreferredConnectorID: fixtureConnector,
		RequiredCapabilities: []string{"chat.streaming"},
		DispatchMode:         DispatchSingle,
		GrantVersion:         4,
		MatrixInputEventID:   "$prompt:example.test",
		IdempotencyDigest:    idempotencyDigest,
	}
	first, err := BuildCandidate(
		fixtureTenant, fixtureRoom, "$invoke:example.test", fixtureConversation,
		fixtureRequestEvent, invocation, time.Unix(100, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildCandidate(
		fixtureTenant, fixtureRoom, "$invoke:example.test", fixtureConversation,
		"01890f00-0000-7000-8000-000000000315", invocation, time.Unix(200, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceDigest != second.SourceDigest {
		t.Fatal("locally regenerated request event/time changed source replay identity")
	}
	if first.RequestDigest == second.RequestDigest {
		t.Fatal("request digest must bind the stored opaque request event id")
	}
}

func TestVendoredGatewayProtocolSHA(t *testing.T) {
	content, err := os.ReadFile("agentgatewayv1/agent_gateway.proto")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	if got := hex.EncodeToString(digest[:]); got != "4f8ab24ec3b39c729e2b21f6e81aaa2f4359c39c3d80892d29af76bb978dc1cf" {
		t.Fatalf("vendored agent_gateway.proto sha256 = %s", got)
	}
}
