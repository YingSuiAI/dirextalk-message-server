package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/google/uuid"
)

const (
	maxSecretBootstrapUploadBodyBytes int64 = 1_500_000
	maxSecretBootstrapCiphertextBytes       = 1024*1024 + 16
	secretBootstrapInvalidCode              = "cloud_secret_bootstrap_invalid"
)

type SecretBootstrapUploadRequest struct {
	SessionID        string
	UploadToken      []byte
	ClientPublicKey  []byte
	Nonce            []byte
	Ciphertext       []byte
	IdempotencyKey   string
	ExpectedRevision int64
}

type SecretBootstrapUploadPort interface {
	OwnerAuthorized(token string) bool
	UploadEncryptedSecret(context.Context, SecretBootstrapUploadRequest) (any, *actionbase.Error)
}

type secretBootstrapUploadEnvelope struct {
	SessionID        string `json:"session_id"`
	UploadToken      string `json:"upload_token"`
	ClientPublicKey  string `json:"client_public_key"`
	Nonce            string `json:"nonce"`
	Ciphertext       string `json:"ciphertext"`
	IdempotencyKey   string `json:"idempotency_key"`
	ExpectedRevision int64  `json:"expected_revision"`
}

// SecretBootstrapUploadHandler is a dedicated owner-only ciphertext tunnel.
// It is intentionally separate from ProductCore and websocket dispatch so the
// sensitive token and ciphertext body cannot enter action instrumentation,
// retries, durable operations, or event payloads.
func SecretBootstrapUploadHandler(port SecretBootstrapUploadPort) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		SetCORSHeaders(w, r)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			WriteError(w, actionbase.StatusError(http.StatusMethodNotAllowed, "method not allowed"))
			return
		}
		if port == nil || !port.OwnerAuthorized(BearerToken(r.Header.Get("Authorization"))) {
			WriteError(w, actionbase.StatusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN"))
			return
		}

		var envelope secretBootstrapUploadEnvelope
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSecretBootstrapUploadBodyBytes))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&envelope); err != nil {
			writeSecretBootstrapInvalid(w)
			return
		}
		if err := requireJSONEOF(decoder); err != nil || !canonicalHTTPUUID(envelope.SessionID) ||
			!canonicalHTTPUUID(envelope.IdempotencyKey) || envelope.ExpectedRevision <= 0 {
			writeSecretBootstrapInvalid(w)
			return
		}

		uploadToken, ok := decodeRawURLExact(envelope.UploadToken, 32)
		if !ok {
			writeSecretBootstrapInvalid(w)
			return
		}
		defer wipeHTTPBytes(uploadToken)
		clientPublicKey, ok := decodeRawURLExact(envelope.ClientPublicKey, 32)
		if !ok {
			writeSecretBootstrapInvalid(w)
			return
		}
		defer wipeHTTPBytes(clientPublicKey)
		nonce, ok := decodeRawURLExact(envelope.Nonce, 12)
		if !ok {
			writeSecretBootstrapInvalid(w)
			return
		}
		defer wipeHTTPBytes(nonce)
		ciphertext, ok := decodeRawURLRange(envelope.Ciphertext, 17, maxSecretBootstrapCiphertextBytes)
		if !ok {
			writeSecretBootstrapInvalid(w)
			return
		}
		defer wipeHTTPBytes(ciphertext)

		response, apiErr := port.UploadEncryptedSecret(r.Context(), SecretBootstrapUploadRequest{
			SessionID: envelope.SessionID, UploadToken: uploadToken, ClientPublicKey: clientPublicKey,
			Nonce: nonce, Ciphertext: ciphertext, IdempotencyKey: envelope.IdempotencyKey,
			ExpectedRevision: envelope.ExpectedRevision,
		})
		if apiErr != nil {
			WriteError(w, apiErr)
			return
		}
		WriteJSON(w, http.StatusOK, response)
	}
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("trailing json value")
		}
		return err
	}
	return nil
}

func decodeRawURLExact(value string, size int) ([]byte, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != size || base64.RawURLEncoding.EncodeToString(decoded) != value {
		wipeHTTPBytes(decoded)
		return nil, false
	}
	return decoded, true
}

func decodeRawURLRange(value string, minimum, maximum int) ([]byte, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) < minimum || len(decoded) > maximum || base64.RawURLEncoding.EncodeToString(decoded) != value {
		wipeHTTPBytes(decoded)
		return nil, false
	}
	return decoded, true
}

func canonicalHTTPUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

func writeSecretBootstrapInvalid(w http.ResponseWriter) {
	WriteError(w, actionbase.CodedError(http.StatusBadRequest, secretBootstrapInvalidCode, "cloud secret bootstrap upload is invalid"))
}

func wipeHTTPBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
