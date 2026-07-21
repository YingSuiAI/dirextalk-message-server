package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestVolcVoiceChatClientReplacesDynamicSessionFields(t *testing.T) {
	var calls []map[string]any
	client := &volcVoiceChatOpenAPIClient{
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Header.Get("Authorization") == "" || req.Header.Get("X-Date") == "" || req.Header.Get("X-Content-Sha256") == "" {
				t.Fatalf("request was not signed: headers=%#v", req.Header)
			}
			if req.URL.Query().Get("Version") != volcVoiceChatVersion {
				t.Fatalf("unexpected version query: %s", req.URL.RawQuery)
			}
			var body map[string]any
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatalf("decode request body: %v", err)
			}
			body["_action"] = req.URL.Query().Get("Action")
			calls = append(calls, body)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"ResponseMetadata":{"RequestId":"req"}}`))),
				Header:     make(http.Header),
			}, nil
		})},
		host:            "rtc.volcengineapi.com",
		region:          "cn-north-1",
		accessKeyID:     "ak",
		secretAccessKey: "sk",
		webhookURL:      "https://www.wenson.art/_p2p/agent/voice/webhook",
		configTemplate: parseVoiceChatTemplate(`{
			"AppId":"template-app",
			"RoomId":"template-room",
			"TaskId":"template-task",
			"Config":{"CallbackUrl":"${VOLC_VOICE_WEBHOOK_URL}","ASRConfig":{"Provider":"volcano"}},
			"AgentConfig":{"TargetUserId":["template-user"],"UserId":"template-ai","WelcomeMessage":"hello"}
		}`),
	}
	session := voiceSession{
		SessionID:      "voice_1",
		TaskID:         "voice_1",
		AppID:          "123456781234567812345678",
		VoiceChatAppID: "123456781234567812345678",
		RoomID:         "dirextalk_voice_room",
		UserID:         "owner_user",
		AIUserID:       "dirextalk_ai_user",
	}
	if err := client.StartVoiceChat(context.Background(), session); err != nil {
		t.Fatalf("StartVoiceChat: %v", err)
	}
	if err := client.StopVoiceChat(context.Background(), session); err != nil {
		t.Fatalf("StopVoiceChat: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("unexpected call count: %#v", calls)
	}
	start := calls[0]
	if start["_action"] != "StartVoiceChat" || start["AppId"] != session.VoiceChatAppID || start["RoomId"] != session.RoomID || start["TaskId"] != session.TaskID {
		t.Fatalf("start payload did not use dynamic ids: %#v", start)
	}
	config := start["Config"].(map[string]any)
	if config["CallbackUrl"] != "https://www.wenson.art/_p2p/agent/voice/webhook" {
		t.Fatalf("webhook placeholder was not replaced: %#v", config)
	}
	agentConfig := start["AgentConfig"].(map[string]any)
	targets := agentConfig["TargetUserId"].([]any)
	if len(targets) != 1 || targets[0] != session.UserID || agentConfig["UserId"] != session.AIUserID {
		t.Fatalf("agent config did not use dynamic users: %#v", agentConfig)
	}
	stop := calls[1]
	if stop["_action"] != "StopVoiceChat" || stop["AppId"] != session.VoiceChatAppID || stop["RoomId"] != session.RoomID || stop["TaskId"] != session.TaskID {
		t.Fatalf("stop payload did not use dynamic ids: %#v", stop)
	}
}

func TestVolcVoiceChatClientReportsNoPermissionWithHint(t *testing.T) {
	client := &volcVoiceChatOpenAPIClient{
		httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(`{
					"ResponseMetadata": {
						"RequestId": "req-no-permission",
						"Error": {
							"Code": "NoPermissionForApp",
							"Message": "no permission for app"
						}
					}
				}`)),
				Header: make(http.Header),
			}, nil
		})},
		host:            "rtc.volcengineapi.com",
		region:          "cn-north-1",
		accessKeyID:     "ak",
		secretAccessKey: "sk",
		configTemplate:  parseVoiceChatTemplate(`{"Config":{},"AgentConfig":{}}`),
	}
	err := client.StartVoiceChat(context.Background(), voiceSession{
		SessionID:      "voice_1",
		TaskID:         "voice_1",
		AppID:          "rtc-app",
		VoiceChatAppID: "rtc-app",
		RoomID:         "room",
		UserID:         "user",
		AIUserID:       "ai",
	})
	if err == nil {
		t.Fatal("expected NoPermissionForApp error")
	}
	text := err.Error()
	for _, want := range []string{"NoPermissionForApp", "req-no-permission", "VOLC_VOICE_CHAT_APP_ID"} {
		if !strings.Contains(text, want) {
			t.Fatalf("error %q does not contain %q", text, want)
		}
	}
}

func TestVolcRTCTokenSignerProducesScopedToken(t *testing.T) {
	signer := volcRTCTokenSigner{}
	token, err := signer.SignRTC(
		"123456781234567812345678",
		"app-key",
		"room",
		"user",
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("SignRTC: %v", err)
	}
	if !strings.HasPrefix(token, "001123456781234567812345678") {
		t.Fatalf("unexpected token prefix: %q", token)
	}
}
