package main

import (
	"testing"
)

func TestParseConfigRequiresMountedTLSAndModelSecretFiles(t *testing.T) {
	env := map[string]string{
		"CLOUD_RESEARCHER_TLS_CERT_FILE":      "server.pem",
		"CLOUD_RESEARCHER_TLS_KEY_FILE":       "server.key",
		"CLOUD_RESEARCHER_CLIENT_CA_FILE":     "client-ca.pem",
		"CLOUD_RESEARCHER_MODEL_ENDPOINT":     "https://model.example/v1/chat/completions",
		"CLOUD_RESEARCHER_MODEL_ID":           "research-model",
		"CLOUD_RESEARCHER_MODEL_API_KEY_FILE": "model-api-key",
	}
	config, err := parseConfig([]string{"--listen", "127.0.0.1:9443"}, func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if config.listenAddr != "127.0.0.1:9443" || config.modelAPIKeyFile != "model-api-key" || config.modelEndpoint != env["CLOUD_RESEARCHER_MODEL_ENDPOINT"] {
		t.Fatalf("config = %#v", config)
	}
	delete(env, "CLOUD_RESEARCHER_MODEL_API_KEY_FILE")
	if _, err := parseConfig(nil, func(key string) string { return env[key] }); err == nil {
		t.Fatal("researcher must reject a missing mounted model secret file")
	}
	if _, err := parseConfig([]string{"--api-key", "must-not-be-a-flag"}, func(key string) string { return env[key] }); err == nil {
		t.Fatal("researcher must not accept a model API key through flags")
	}
	env["CLOUD_RESEARCHER_MODEL_API_KEY_FILE"] = "model-api-key"
	if _, err := parseConfig([]string{"--listen", "127.0.0.1:notaport"}, func(key string) string { return env[key] }); err == nil {
		t.Fatal("researcher must reject a nonnumeric listen port")
	}
}
