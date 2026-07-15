package contract

import (
	"crypto/ecdh"
	"encoding/hex"
	"testing"
)

func TestServiceSecretEnvelopeDeterministicCrossLanguageVector(t *testing.T) {
	context := ServiceSecretContextV1{SchemaVersion: ServiceSecretContextSchema, SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-secret-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333", RecipeDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111", ArtifactDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222", SlotID: "model_token", SecretRef: "secret_ref:model-token-001", Purpose: "model inference", Delivery: "environment", ExpiresAt: "2026-07-15T12:10:00.000Z"}
	serverPrivate, ephemeralPrivate, nonce := sequentialBytes(0, 32), sequentialBytes(0x20, 32), sequentialBytes(0, 12)
	plaintext := []byte("test-only-service-secret-value")
	serverKey, _ := ecdh.X25519().NewPrivateKey(serverPrivate)
	envelope, err := EncryptServiceSecret(context, serverKey.PublicKey().Bytes(), ephemeralPrivate, nonce, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := DecryptServiceSecret(context, serverPrivate, envelope)
	if err != nil || string(decrypted) != string(plaintext) {
		t.Fatalf("decrypt=%q err=%v", decrypted, err)
	}
	contextCBOR, _ := context.CanonicalCBOR()
	contextDigest, _ := context.Digest()
	envelopeJSON, _ := envelope.CanonicalJSON()
	envelopeDigest, _ := envelope.Digest()
	ephemeralKey, _ := ecdh.X25519().NewPrivateKey(ephemeralPrivate)
	assertGolden(t, "context CBOR", hex.EncodeToString(contextCBOR), "ae67707572706f73656f6d6f64656c20696e666572656e636567736c6f745f69646b6d6f64656c5f746f6b656e677461736b5f6964707461736b2d7365637265742d303030316864656c69766572796b656e7669726f6e6d656e746a657870697265735f61747818323032362d30372d31355431323a31303a30302e3030305a6a7365637265745f726566781a7365637265745f7265663a6d6f64656c2d746f6b656e2d3030316a73657373696f6e5f6964737365637265742d73657373696f6e2d303030316c657865637574696f6e5f69646e657865637574696f6e2d303030316d636f6e6e656374696f6e5f69646f636f6e6e656374696f6e2d303030316d6465706c6f796d656e745f69646f6465706c6f796d656e742d303030316d7265636970655f64696765737478477368613235363a313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131316e736368656d615f76657273696f6e7823646972657874616c6b2e736572766963652d7365637265742d636f6e746578742f76316f61727469666163745f64696765737478477368613235363a323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232326f6d616e69666573745f64696765737478477368613235363a33333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333")
	assertGolden(t, "context digest", contextDigest, "sha256:43a1feb8f2ef1a20fd795d184b1ff29317c5e699730d68e6649d4b534305b211")
	assertGolden(t, "server public key", hex.EncodeToString(serverKey.PublicKey().Bytes()), "8f40c5adb68f25624ae5b214ea767a6ec94d829d3d7b5e1ad1ba6f3e2138285f")
	assertGolden(t, "ephemeral public key", hex.EncodeToString(ephemeralKey.PublicKey().Bytes()), "358072d6365880d1aeea329adf9121383851ed21a28e3b75e965d0d2cd166254")
	assertGolden(t, "envelope JSON", string(envelopeJSON), `{"schema_version":"dirextalk.service-secret-envelope/v1","session_id":"secret-session-0001","context_digest":"sha256:43a1feb8f2ef1a20fd795d184b1ff29317c5e699730d68e6649d4b534305b211","ephemeral_public_key_b64":"NYBy1jZYgNGu6jKa35EhODhR7SGijjt16WXQ0s0WYlQ","nonce_b64":"AAECAwQFBgcICQoL","ciphertext_b64":"VTxpxdp7bm8fK7GWhUEk10ZBRjuXS49OjFmBbm7nSuaKD2K4fU0maXLZ7XOXjw"}`)
	assertGolden(t, "envelope digest", envelopeDigest, "sha256:1efa9580d2927657e0c3f2b0a9fe31bf8b2b387cc8ccfe8df8d097090eea56bb")
}

func assertGolden(t *testing.T, label, actual, expected string) {
	t.Helper()
	if actual != expected {
		t.Fatalf("%s drifted: got %s", label, actual)
	}
}
func sequentialBytes(start byte, count int) []byte {
	value := make([]byte, count)
	for index := range value {
		value[index] = start + byte(index)
	}
	return value
}
