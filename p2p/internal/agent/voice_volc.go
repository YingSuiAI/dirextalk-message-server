package agent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	volcRTCOpenAPIHost    = "rtc.volcengineapi.com"
	volcRTCOpenAPIRegion  = "cn-north-1"
	volcRTCOpenAPIService = "rtc"
	volcVoiceChatVersion  = "2025-06-01"
)

type voiceTokenSigner interface {
	SignRTC(appID, appKey, roomID, userID string, expiresAt time.Time) (string, error)
}

type voiceChatClient interface {
	StartVoiceChat(context.Context, voiceSession) error
	StopVoiceChat(context.Context, voiceSession) error
}

type volcRTCTokenSigner struct{}

func (volcRTCTokenSigner) SignRTC(appID, appKey, roomID, userID string, expiresAt time.Time) (string, error) {
	token := volcRTCToken{
		AppID:      strings.TrimSpace(appID),
		AppKey:     strings.TrimSpace(appKey),
		RoomID:     strings.TrimSpace(roomID),
		UserID:     strings.TrimSpace(userID),
		IssuedAt:   uint32(time.Now().UTC().Unix()),
		ExpireAt:   uint32(expiresAt.UTC().Unix()),
		Nonce:      secureUint32(),
		Privileges: map[uint16]uint32{},
	}
	if len(token.AppID) != 24 {
		return "", fmt.Errorf("VOLC_RTC_APP_ID must be 24 characters")
	}
	if token.AppKey == "" {
		return "", fmt.Errorf("VOLC_RTC_APP_KEY is required")
	}
	privilegeExpiresAt := token.ExpireAt
	for _, privilege := range []uint16{0, 1, 2, 3, 4, 5} {
		token.addPrivilege(privilege, privilegeExpiresAt)
	}
	return token.serialize()
}

type volcRTCToken struct {
	AppID      string
	AppKey     string
	RoomID     string
	UserID     string
	IssuedAt   uint32
	ExpireAt   uint32
	Nonce      uint32
	Privileges map[uint16]uint32
}

func (t *volcRTCToken) addPrivilege(privilege uint16, expiresAt uint32) {
	if t.Privileges == nil {
		t.Privileges = map[uint16]uint32{}
	}
	t.Privileges[privilege] = expiresAt
}

func (t volcRTCToken) serialize() (string, error) {
	message := new(bytes.Buffer)
	for _, value := range []uint32{t.Nonce, t.IssuedAt, t.ExpireAt} {
		if err := binary.Write(message, binary.LittleEndian, value); err != nil {
			return "", err
		}
	}
	for _, value := range []string{t.RoomID, t.UserID} {
		if err := writeVolcString(message, value); err != nil {
			return "", err
		}
	}
	if err := writeVolcPrivileges(message, t.Privileges); err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, []byte(t.AppKey))
	if _, err := mac.Write(message.Bytes()); err != nil {
		return "", err
	}
	signature := mac.Sum(nil)
	content := new(bytes.Buffer)
	if err := writeVolcString(content, string(message.Bytes())); err != nil {
		return "", err
	}
	if err := writeVolcString(content, string(signature)); err != nil {
		return "", err
	}
	return "001" + t.AppID + base64.StdEncoding.EncodeToString(content.Bytes()), nil
}

func writeVolcString(w io.Writer, value string) error {
	if len(value) > 65535 {
		return fmt.Errorf("value exceeds Volc RTC token string limit")
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(len(value))); err != nil {
		return err
	}
	_, err := io.WriteString(w, value)
	return err
}

func writeVolcPrivileges(w io.Writer, privileges map[uint16]uint32) error {
	if len(privileges) > 65535 {
		return fmt.Errorf("too many Volc RTC token privileges")
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(len(privileges))); err != nil {
		return err
	}
	keys := make([]int, 0, len(privileges))
	for key := range privileges {
		keys = append(keys, int(key))
	}
	sort.Ints(keys)
	for _, key := range keys {
		if err := binary.Write(w, binary.LittleEndian, uint16(key)); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, privileges[uint16(key)]); err != nil {
			return err
		}
	}
	return nil
}

func secureUint32() uint32 {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return uint32(time.Now().UTC().UnixNano())
	}
	return binary.LittleEndian.Uint32(buf[:])
}

type volcVoiceChatOpenAPIClient struct {
	httpClient      *http.Client
	host            string
	region          string
	accessKeyID     string
	secretAccessKey string
	webhookURL      string
	configTemplate  map[string]any
}

func newVolcVoiceChatOpenAPIClient(cfg voiceConfig) *volcVoiceChatOpenAPIClient {
	if strings.TrimSpace(cfg.OpenAPIAccessKeyID) == "" || strings.TrimSpace(cfg.OpenAPISecretAccessKey) == "" {
		return nil
	}
	return &volcVoiceChatOpenAPIClient{
		httpClient:      http.DefaultClient,
		host:            fallback(cfg.OpenAPIHost, volcRTCOpenAPIHost),
		region:          fallback(cfg.OpenAPIRegion, volcRTCOpenAPIRegion),
		accessKeyID:     strings.TrimSpace(cfg.OpenAPIAccessKeyID),
		secretAccessKey: strings.TrimSpace(cfg.OpenAPISecretAccessKey),
		webhookURL:      strings.TrimSpace(cfg.WebhookURL),
		configTemplate:  parseVoiceChatTemplate(cfg.VoiceChatConfigJSON),
	}
}

func (c *volcVoiceChatOpenAPIClient) StartVoiceChat(ctx context.Context, session voiceSession) error {
	if c == nil {
		return nil
	}
	return c.call(ctx, "StartVoiceChat", c.voiceChatPayload(session))
}

func (c *volcVoiceChatOpenAPIClient) StopVoiceChat(ctx context.Context, session voiceSession) error {
	if c == nil {
		return nil
	}
	return c.call(ctx, "StopVoiceChat", map[string]any{
		"AppId":  session.voiceChatAppID(),
		"RoomId": session.RoomID,
		"TaskId": session.TaskID,
	})
}

func (c *volcVoiceChatOpenAPIClient) voiceChatPayload(session voiceSession) map[string]any {
	payload := deepCloneMap(c.configTemplate)
	replaceVoiceChatPlaceholders(payload, map[string]string{
		"VOLC_RTC_APP_ID":        session.AppID,
		"VOLC_VOICE_CHAT_APP_ID": session.voiceChatAppID(),
		"VOLC_VOICE_WEBHOOK_URL": c.webhookURL,
		"VOICE_SESSION_ID":       session.SessionID,
		"VOICE_TASK_ID":          session.TaskID,
		"VOICE_ROOM_ID":          session.RoomID,
		"VOICE_USER_ID":          session.UserID,
		"VOICE_AI_USER_ID":       session.AIUserID,
	})
	payload["AppId"] = session.voiceChatAppID()
	payload["RoomId"] = session.RoomID
	payload["TaskId"] = session.TaskID
	config := mapValue(payload["Config"])
	if config == nil {
		config = map[string]any{}
		payload["Config"] = config
	}
	agentConfig := mapValue(payload["AgentConfig"])
	if agentConfig == nil {
		agentConfig = map[string]any{}
		payload["AgentConfig"] = agentConfig
	}
	agentConfig["TargetUserId"] = []any{session.UserID}
	agentConfig["UserId"] = session.AIUserID
	if _, ok := agentConfig["WelcomeMessage"]; !ok {
		agentConfig["WelcomeMessage"] = "我是 Dirextalk 语音助手，有什么需要我帮你查找的吗？"
	}
	agentConfig["EnableConversationStateCallback"] = true
	return payload
}

func (s voiceSession) voiceChatAppID() string {
	return fallback(s.VoiceChatAppID, s.AppID)
}

func (c *volcVoiceChatOpenAPIClient) call(ctx context.Context, action string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s payload: %w", action, err)
	}
	endpoint := url.URL{
		Scheme: "https",
		Host:   c.host,
		Path:   "/",
		RawQuery: url.Values{
			"Action":  []string{action},
			"Version": []string{volcVoiceChatVersion},
		}.Encode(),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	signVolcOpenAPIRequest(req, body, volcOpenAPICredentials{
		AccessKeyID:     c.accessKeyID,
		SecretAccessKey: c.secretAccessKey,
		Region:          c.region,
		Service:         volcRTCOpenAPIService,
	})
	client := c.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	logFields := logrus.Fields{
		"component": "volc_voice_chat",
		"action":    action,
		"host":      c.host,
		"region":    c.region,
		"app_id":    logString(payload["AppId"]),
		"room_id":   logString(payload["RoomId"]),
		"task_id":   logString(payload["TaskId"]),
	}
	if agentConfig := mapValue(payload["AgentConfig"]); agentConfig != nil {
		logFields["agent_user_id"] = logString(agentConfig["UserId"])
		if targets, ok := agentConfig["TargetUserId"].([]any); ok && len(targets) > 0 {
			logFields["target_user_id"] = logString(targets[0])
		}
	}
	for key, value := range summarizeVoiceChatPayload(payload) {
		logFields[key] = value
	}
	logrus.WithFields(logFields).Info("calling Volc VoiceChat OpenAPI")
	resp, err := client.Do(req)
	if err != nil {
		logrus.WithError(err).WithFields(logFields).Warn("Volc VoiceChat OpenAPI request failed")
		return fmt.Errorf("%s request failed: %w", action, err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logFields["http_status"] = resp.StatusCode
		logFields["response"] = sanitizedVolcResponse(responseBody)
		logrus.WithFields(logFields).Warn("Volc VoiceChat OpenAPI returned non-2xx")
		return fmt.Errorf("%s failed with status %d: %s", action, resp.StatusCode, sanitizedVolcResponse(responseBody))
	}
	decoded, decodeOK := decodeVolcOpenAPIResponse(responseBody)
	if decodeOK {
		if decoded.ResponseMetadata.RequestID != "" {
			logFields["request_id"] = decoded.ResponseMetadata.RequestID
		}
		if decoded.ResponseMetadata.Error != nil {
			logFields["error_code"] = decoded.ResponseMetadata.Error.Code
			logFields["error_message"] = decoded.ResponseMetadata.Error.Message
			logrus.WithFields(logFields).Warn("Volc VoiceChat OpenAPI returned business error")
			return fmt.Errorf("%s failed: %s", action, formatVolcOpenAPIError(decoded))
		}
	}
	logrus.WithFields(logFields).Info("Volc VoiceChat OpenAPI call succeeded")
	return nil
}

type volcOpenAPIResponse struct {
	ResponseMetadata struct {
		RequestID string `json:"RequestId"`
		Error     *struct {
			Code    string `json:"Code"`
			Message string `json:"Message"`
		} `json:"Error"`
	} `json:"ResponseMetadata"`
}

func decodeVolcOpenAPIResponse(body []byte) (volcOpenAPIResponse, bool) {
	var decoded volcOpenAPIResponse
	if len(bytes.TrimSpace(body)) == 0 {
		return decoded, false
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return decoded, false
	}
	return decoded, true
}

func formatVolcOpenAPIError(decoded volcOpenAPIResponse) string {
	if decoded.ResponseMetadata.Error == nil {
		return "unknown"
	}
	code := strings.TrimSpace(decoded.ResponseMetadata.Error.Code)
	message := strings.TrimSpace(decoded.ResponseMetadata.Error.Message)
	requestID := strings.TrimSpace(decoded.ResponseMetadata.RequestID)
	parts := []string{}
	if code != "" {
		parts = append(parts, code)
	}
	if message != "" {
		parts = append(parts, message)
	}
	if requestID != "" {
		parts = append(parts, "RequestId="+requestID)
	}
	if code == "NoPermissionForApp" {
		parts = append(parts, "check VOLC_VOICE_CHAT_APP_ID/VOLC_RTC_APP_ID and enable VoiceChat permission for that Volc app")
	}
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, ": ")
}

type volcOpenAPICredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Service         string
}

func signVolcOpenAPIRequest(req *http.Request, body []byte, credentials volcOpenAPICredentials) {
	now := time.Now().UTC()
	xDate := now.Format("20060102T150405Z")
	shortDate := xDate[:8]
	bodyHash := sha256Hex(body)
	req.Header.Set("Host", req.Host)
	req.Header.Set("X-Date", xDate)
	req.Header.Set("X-Content-Sha256", bodyHash)
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	signedHeaders := []string{"content-type", "host", "x-content-sha256", "x-date"}
	canonicalHeaders := "content-type:" + strings.TrimSpace(req.Header.Get("Content-Type")) + "\n" +
		"host:" + req.Host + "\n" +
		"x-content-sha256:" + bodyHash + "\n" +
		"x-date:" + xDate + "\n"
	canonicalRequest := strings.Join([]string{
		req.Method,
		"/",
		normalizeVolcQuery(req.URL.Query()),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		bodyHash,
	}, "\n")
	scope := strings.Join([]string{shortDate, credentials.Region, credentials.Service, "request"}, "/")
	stringToSign := strings.Join([]string{
		"HMAC-SHA256",
		xDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signingKey := volcSigningKey(credentials.SecretAccessKey, shortDate, credentials.Region, credentials.Service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	req.Header.Set("Authorization", "HMAC-SHA256 Credential="+credentials.AccessKeyID+"/"+scope+", SignedHeaders="+strings.Join(signedHeaders, ";")+", Signature="+signature)
}

func volcSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte(secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("request"))
}

func hmacSHA256(key, content []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(content)
	return mac.Sum(nil)
}

func sha256Hex(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func normalizeVolcQuery(values url.Values) string {
	return strings.ReplaceAll(values.Encode(), "+", "%20")
}

func sanitizedVolcResponse(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	text := string(body)
	for _, key := range []string{"AccessKeyId", "SecretAccessKey", "Authorization", "Token"} {
		text = strings.ReplaceAll(text, key, "redacted")
	}
	if len(text) > 512 {
		return text[:512]
	}
	return text
}

func summarizeVoiceChatPayload(payload map[string]any) logrus.Fields {
	fields := logrus.Fields{
		"has_config":       mapValue(payload["Config"]) != nil,
		"has_agent_config": mapValue(payload["AgentConfig"]) != nil,
		"has_rtc_config":   mapValue(payload["RTCConfig"]) != nil,
	}
	if config := mapValue(payload["Config"]); config != nil {
		if asrConfig := mapValue(config["ASRConfig"]); asrConfig != nil {
			fields["asr_provider"] = logString(asrConfig["Provider"])
			if providerParams := mapValue(asrConfig["ProviderParams"]); providerParams != nil {
				fields["asr_mode"] = logString(providerParams["Mode"])
				fields["asr_resource"] = logString(providerParams["ApiResourceId"])
			}
		}
		if llmConfig := mapValue(config["LLMConfig"]); llmConfig != nil {
			fields["llm_mode"] = logString(llmConfig["Mode"])
			fields["llm_model"] = fallback(logString(llmConfig["ModelName"]), logString(llmConfig["EndPointId"]))
		}
		if ttsConfig := mapValue(config["TTSConfig"]); ttsConfig != nil {
			fields["tts_provider"] = logString(ttsConfig["Provider"])
		}
		if subtitleConfig := mapValue(config["SubtitleConfig"]); subtitleConfig != nil {
			fields["subtitle_mode"] = logString(subtitleConfig["SubtitleMode"])
			fields["subtitle_disable_rts"] = logString(subtitleConfig["DisableRTSSubtitle"])
		}
	}
	return fields
}

func parseVoiceChatTemplate(raw string) map[string]any {
	if strings.TrimSpace(raw) == "" {
		return defaultVoiceChatTemplate()
	}
	var template map[string]any
	if err := json.Unmarshal([]byte(raw), &template); err != nil || template == nil {
		return defaultVoiceChatTemplate()
	}
	return template
}

func deepCloneMap(values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	data, err := json.Marshal(values)
	if err != nil {
		return cloneMap(values)
	}
	var cloned map[string]any
	if err := json.Unmarshal(data, &cloned); err != nil || cloned == nil {
		return cloneMap(values)
	}
	return cloned
}

func replaceVoiceChatPlaceholders(value any, replacements map[string]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if text, ok := child.(string); ok {
				typed[key] = replaceVoiceChatString(text, replacements)
				continue
			}
			replaceVoiceChatPlaceholders(child, replacements)
		}
	case []any:
		for index, child := range typed {
			if text, ok := child.(string); ok {
				typed[index] = replaceVoiceChatString(text, replacements)
				continue
			}
			replaceVoiceChatPlaceholders(child, replacements)
		}
	}
}

func replaceVoiceChatString(value string, replacements map[string]string) string {
	result := value
	for key, replacement := range replacements {
		result = strings.ReplaceAll(result, "${"+key+"}", replacement)
		result = strings.ReplaceAll(result, "{{"+key+"}}", replacement)
	}
	return result
}

func defaultVoiceChatTemplate() map[string]any {
	return map[string]any{
		"Config": map[string]any{
			"ASRConfig": map[string]any{
				"Provider": "volcano",
				"ProviderParams": map[string]any{
					"Mode":                 "bigmodel",
					"ApiResourceId":        "volc.seedasr.sauc.duration",
					"StreamMode":           2,
					"VolcanoASRParameters": "{\"request\":{\"enable_nonstream\":true}}",
				},
				"VADConfig": map[string]any{
					"SilenceTime": 600,
				},
				"InterruptConfig": map[string]any{
					"InterruptKeywords":       []any{},
					"InterruptSpeechDuration": 0,
				},
			},
			"LLMConfig": map[string]any{
				"Mode":          "ArkV3",
				"ModelName":     "doubao-seed-character-251128",
				"ThinkingType":  "disabled",
				"VisionConfig":  map[string]any{},
				"HistoryLength": 10,
				"Temperature":   0.1,
				"TopP":          0.3,
				"MaxTokens":     1024,
				"SystemMessages": []any{
					"你是 Dirextalk 语音助手。回答要简洁、准确、友好；涉及群聊、频道或帖子时，优先提示用户查看 Dirextalk 给出的引用卡片。",
				},
			},
			"TTSConfig": map[string]any{
				"Provider": "volcano_bidirection",
				"ProviderParams": map[string]any{
					"Credential": map[string]any{
						"ResourceId": "seed-tts-1.0",
					},
					"VolcanoTTSParameters": "{\"req_params\":{\"speaker\":\"zh_female_yuanqinvyou_moon_bigtts\",\"audio_params\":{\"speech_rate\":0,\"loudness_rate\":0},\"additions\":{\"post_process\":{\"pitch\":0}}}}",
				},
			},
			"InterruptMode": 0,
			"SubtitleConfig": map[string]any{
				"DisableRTSSubtitle": false,
				"SubtitleMode":       0,
			},
			"FunctionCallingConfig": map[string]any{},
			"WebSearchAgentConfig":  map[string]any{},
			"MemoryConfig":          map[string]any{},
			"MusicAgentConfig":      map[string]any{},
		},
		"AgentConfig": map[string]any{
			"WelcomeMessage":                  "我是你的 AI 助手，有什么需要我为您效劳的吗？",
			"EnableConversationStateCallback": true,
			"VoicePrint": map[string]any{
				"MetaList":       nil,
				"VoicePrintList": nil,
			},
		},
	}
}

func mapValue(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	default:
		return nil
	}
}

func logString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}
