package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/google/uuid"
)

type secretBootstrapUploadPortStub struct {
	authorizedToken string
	calls           int
	request         SecretBootstrapUploadRequest
	response        any
	err             *actionbase.Error
}

func (stub *secretBootstrapUploadPortStub) OwnerAuthorized(token string) bool {
	return token == stub.authorizedToken
}

func (stub *secretBootstrapUploadPortStub) UploadEncryptedSecret(_ context.Context, request SecretBootstrapUploadRequest) (any, *actionbase.Error) {
	stub.calls++
	stub.request = SecretBootstrapUploadRequest{
		SessionID: request.SessionID, UploadToken: append([]byte(nil), request.UploadToken...),
		ClientPublicKey: append([]byte(nil), request.ClientPublicKey...), Nonce: append([]byte(nil), request.Nonce...),
		Ciphertext: append([]byte(nil), request.Ciphertext...), IdempotencyKey: request.IdempotencyKey,
		ExpectedRevision: request.ExpectedRevision,
	}
	return stub.response, stub.err
}

func TestSecretBootstrapUploadHandlerRequiresOwnerAndForwardsOnlyDecodedCiphertext(t *testing.T) {
	uploadToken := append(make([]byte, 31), 1)
	clientPublicKey := append(make([]byte, 31), 2)
	nonce := append(make([]byte, 11), 3)
	ciphertext := append(make([]byte, 16), 4)
	sessionID, idempotencyKey := uuid.NewString(), uuid.NewString()
	stub := &secretBootstrapUploadPortStub{
		authorizedToken: "owner-token",
		response: map[string]any{"session": map[string]any{
			"session_id": sessionID, "status": "uploaded", "revision": int64(2),
		}},
	}
	body := map[string]any{
		"session_id": sessionID, "upload_token": rawURL(uploadToken), "client_public_key": rawURL(clientPublicKey),
		"nonce": rawURL(nonce), "ciphertext": rawURL(ciphertext), "idempotency_key": idempotencyKey, "expected_revision": 1,
	}
	encoded, _ := json.Marshal(body)

	unauthorized := httptest.NewRequest(http.MethodPost, "/_p2p/cloud/secret-bootstrap/upload", strings.NewReader(string(encoded)))
	unauthorizedRecorder := httptest.NewRecorder()
	SecretBootstrapUploadHandler(stub).ServeHTTP(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized || stub.calls != 0 {
		t.Fatalf("unauthorized status=%d calls=%d", unauthorizedRecorder.Code, stub.calls)
	}

	request := httptest.NewRequest(http.MethodPost, "/_p2p/cloud/secret-bootstrap/upload", strings.NewReader(string(encoded)))
	request.Header.Set("Authorization", "Bearer owner-token")
	recorder := httptest.NewRecorder()
	SecretBootstrapUploadHandler(stub).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("upload status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	if stub.calls != 1 || stub.request.SessionID != sessionID || stub.request.IdempotencyKey != idempotencyKey || stub.request.ExpectedRevision != 1 ||
		!equalHTTPBytes(stub.request.UploadToken, uploadToken) || !equalHTTPBytes(stub.request.ClientPublicKey, clientPublicKey) ||
		!equalHTTPBytes(stub.request.Nonce, nonce) || !equalHTTPBytes(stub.request.Ciphertext, ciphertext) {
		t.Fatalf("decoded request = %#v", stub.request)
	}
	response := recorder.Body.String()
	for _, forbidden := range []string{rawURL(uploadToken), rawURL(clientPublicKey), rawURL(nonce), rawURL(ciphertext), "upload_token", "ciphertext"} {
		if strings.Contains(response, forbidden) {
			t.Fatalf("secret upload material leaked in response: %s", response)
		}
	}
}

func TestSecretBootstrapUploadHandlerRejectsNonCanonicalOrUnexpectedInput(t *testing.T) {
	valid := map[string]any{
		"session_id": uuid.NewString(), "upload_token": rawURL(make([]byte, 32)), "client_public_key": rawURL(append(make([]byte, 31), 1)),
		"nonce": rawURL(make([]byte, 12)), "ciphertext": rawURL(make([]byte, 17)), "idempotency_key": uuid.NewString(), "expected_revision": 1,
	}
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "unknown field", mutate: func(value map[string]any) { value["plaintext"] = "forbidden" }},
		{name: "padded base64", mutate: func(value map[string]any) { value["nonce"] = value["nonce"].(string) + "=" }},
		{name: "bad token size", mutate: func(value map[string]any) { value["upload_token"] = rawURL(make([]byte, 31)) }},
		{name: "empty ciphertext", mutate: func(value map[string]any) { value["ciphertext"] = rawURL(make([]byte, 16)) }},
		{name: "zero revision", mutate: func(value map[string]any) { value["expected_revision"] = 0 }},
		{name: "noncanonical uuid", mutate: func(value map[string]any) { value["session_id"] = strings.ToUpper(value["session_id"].(string)) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := make(map[string]any, len(valid)+1)
			for key, item := range valid {
				body[key] = item
			}
			test.mutate(body)
			encoded, _ := json.Marshal(body)
			stub := &secretBootstrapUploadPortStub{authorizedToken: "owner-token"}
			request := httptest.NewRequest(http.MethodPost, "/_p2p/cloud/secret-bootstrap/upload", strings.NewReader(string(encoded)))
			request.Header.Set("Authorization", "Bearer owner-token")
			recorder := httptest.NewRecorder()
			SecretBootstrapUploadHandler(stub).ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || stub.calls != 0 || !strings.Contains(recorder.Body.String(), secretBootstrapInvalidCode) {
				t.Fatalf("status=%d calls=%d body=%s", recorder.Code, stub.calls, recorder.Body.String())
			}
		})
	}
}

func rawURL(value []byte) string { return base64.RawURLEncoding.EncodeToString(value) }

func equalHTTPBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
