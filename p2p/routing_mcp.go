package p2p

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

const (
	mcpProtocolVersion = "2025-06-18"

	jsonRPCParseError     = -32700
	jsonRPCInvalidRequest = -32600
	jsonRPCMethodNotFound = -32601
	jsonRPCInvalidParams  = -32602
)

type mcpJSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  map[string]any  `json:"params,omitempty"`
}

type mcpJSONRPCResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      json.RawMessage  `json:"id,omitempty"`
	Result  any              `json:"result,omitempty"`
	Error   *mcpJSONRPCError `json:"error,omitempty"`
}

type mcpJSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func handleMCP(service *Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if err := rejectMCPQueryTokens(r); err != nil {
			writeError(w, err)
			return
		}
		if err := validateMCPOrigin(r); err != nil {
			writeError(w, err)
			return
		}
		switch r.Method {
		case http.MethodOptions:
			w.WriteHeader(http.StatusNoContent)
			return
		case http.MethodGet:
			writeError(w, statusError(http.StatusMethodNotAllowed, "MCP server-to-client streaming is not enabled"))
			return
		case http.MethodPost:
		default:
			writeError(w, statusError(http.StatusMethodNotAllowed, "method not allowed"))
			return
		}
		token := bearerToken(r.Header.Get("Authorization"))
		if !mcpHTTPTokenAuthorized(service, token) {
			writeError(w, statusError(http.StatusUnauthorized, "M_UNKNOWN_TOKEN"))
			return
		}

		var req mcpJSONRPCRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024))
		decoder.UseNumber()
		if err := decoder.Decode(&req); err != nil {
			writeMCPJSONRPCError(w, http.StatusBadRequest, nil, jsonRPCParseError, "parse error")
			return
		}
		if strings.TrimSpace(req.JSONRPC) != "2.0" || strings.TrimSpace(req.Method) == "" {
			writeMCPJSONRPCError(w, http.StatusOK, req.ID, jsonRPCInvalidRequest, "invalid request")
			return
		}
		if req.Params == nil {
			req.Params = map[string]any{}
		}

		switch strings.TrimSpace(req.Method) {
		case "initialize":
			writeMCPJSONRPCResult(w, req.ID, mcpInitializeResult())
		case "tools/list":
			writeMCPJSONRPCResult(w, req.ID, mcpToolsListResult(service))
		case "tools/call":
			result, rpcErr := mcpToolsCallResult(r, service, req.Params)
			if rpcErr != nil {
				writeMCPJSONRPCError(w, http.StatusOK, req.ID, rpcErr.Code, rpcErr.Message)
				return
			}
			writeMCPJSONRPCResult(w, req.ID, result)
		default:
			writeMCPJSONRPCError(w, http.StatusOK, req.ID, jsonRPCMethodNotFound, "method not found")
		}
	}
}

func mcpHTTPTokenAuthorized(service *Service, token string) bool {
	return service != nil && token != "" && token == service.AgentToken()
}

func rejectMCPQueryTokens(r *http.Request) *apiError {
	for key := range r.URL.Query() {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "access_token", "agent_token", "token", "authorization", "auth_token":
			return statusError(http.StatusBadRequest, "access tokens are not accepted in query string")
		}
	}
	return nil
}

func validateMCPOrigin(r *http.Request) *apiError {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return nil
	}
	originURL, err := url.Parse(origin)
	if err != nil || originURL.Scheme == "" || originURL.Host == "" {
		return statusError(http.StatusForbidden, "origin is not allowed")
	}
	requestURL, err := url.Parse(requestBaseURL(r))
	if err != nil || requestURL.Scheme == "" || requestURL.Host == "" {
		return statusError(http.StatusForbidden, "origin is not allowed")
	}
	if strings.EqualFold(originURL.Scheme, requestURL.Scheme) && strings.EqualFold(originURL.Host, requestURL.Host) {
		return nil
	}
	return statusError(http.StatusForbidden, "origin is not allowed")
}

func mcpInitializeResult() map[string]any {
	buildInfo := internal.CurrentBuildInfo()
	return map[string]any{
		"protocolVersion": mcpProtocolVersion,
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    "dirextalk-message-server",
			"version": buildInfo.Version,
		},
	}
}

func mcpToolsListResult(service *Service) map[string]any {
	capabilityService := service.dirextalkMCPService()
	tools := make([]map[string]any, 0, len(capabilityService.Tools()))
	for _, tool := range capabilityService.Tools() {
		tools = append(tools, map[string]any{
			"name":        tool.Name,
			"description": tool.Description,
			"inputSchema": tool.InputSchema,
		})
	}
	return map[string]any{"tools": tools}
}

func mcpToolsCallResult(r *http.Request, service *Service, params map[string]any) (map[string]any, *mcpJSONRPCError) {
	name, ok := params["name"].(string)
	if !ok || strings.TrimSpace(name) == "" {
		return nil, &mcpJSONRPCError{Code: jsonRPCInvalidParams, Message: "tool name is required"}
	}
	action, ok := dirextalkmcp.NativeToolAction(name)
	if !ok {
		return nil, &mcpJSONRPCError{Code: jsonRPCInvalidParams, Message: "unknown tool"}
	}
	arguments := map[string]any{}
	if rawArguments, ok := params["arguments"]; ok && rawArguments != nil {
		rawMap, ok := rawArguments.(map[string]any)
		if !ok {
			return nil, &mcpJSONRPCError{Code: jsonRPCInvalidParams, Message: "tool arguments must be an object"}
		}
		arguments = cloneAnyMap(rawMap)
	}
	value, mcpErr := service.dirextalkMCPService().Invoke(r.Context(), action, arguments)
	if mcpErr != nil {
		return map[string]any{
			"isError": true,
			"content": []map[string]string{
				{"type": "text", "text": mcpErr.Message},
			},
		}, nil
	}
	return map[string]any{
		"isError":           false,
		"structuredContent": value,
		"content": []map[string]string{
			{"type": "text", "text": mcpResultText(value)},
		},
	}, nil
}

func mcpResultText(value any) string {
	text := jsonValue(value)
	if text == "" {
		return "null"
	}
	return text
}

func writeMCPJSONRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	writeMCPJSONRPCResponse(w, http.StatusOK, mcpJSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	})
}

func writeMCPJSONRPCError(w http.ResponseWriter, status int, id json.RawMessage, code int, message string) {
	writeMCPJSONRPCResponse(w, status, mcpJSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &mcpJSONRPCError{Code: code, Message: message},
	})
}

func writeMCPJSONRPCResponse(w http.ResponseWriter, status int, response mcpJSONRPCResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}
