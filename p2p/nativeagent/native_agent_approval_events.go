package nativeagent

import (
	"encoding/json"
	"strings"

	"github.com/cloudwego/eino/schema"
)

func approvalsFromEinoMessages(messages []*schema.Message) []map[string]any {
	approvals := make([]map[string]any, 0)
	seen := map[string]bool{}
	for _, message := range messages {
		if message == nil || message.Role != schema.Tool {
			continue
		}
		var payload map[string]any
		if json.Unmarshal([]byte(strings.TrimSpace(message.Content)), &payload) != nil {
			continue
		}
		result := nestedAnyMap(payload["result"])
		if trimString(result["status"]) != "confirmation_required" {
			continue
		}
		approval := nestedAnyMap(result["approval"])
		id := strings.TrimSpace(trimString(approval["id"]))
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		approvals = append(approvals, cloneAnyMap(approval))
	}
	return approvals
}
