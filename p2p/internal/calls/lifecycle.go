package calls

import (
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func callTimeParam(values ...any) string {
	for _, value := range values {
		switch typed := value.(type) {
		case string:
			text := strings.TrimSpace(typed)
			if text == "" {
				continue
			}
			if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
				return parsed.UTC().Format(time.RFC3339Nano)
			}
		default:
			if milliseconds := actionbase.Int64(typed); milliseconds > 0 {
				return time.UnixMilli(milliseconds).UTC().Format(time.RFC3339Nano)
			}
		}
	}
	return ""
}

func applyCallLifecycle(call *dirextalkdomain.CallRecord, event string, params actionbase.Params, now time.Time, localUserID string) {
	switch event {
	case "connected":
		if answeredAt := callTimeParam(params.Raw("answered_at"), params.Raw("answered_at_ms")); answeredAt != "" {
			call.AnsweredAt = answeredAt
		} else if call.AnsweredAt == "" {
			call.AnsweredAt = now.UTC().Format(time.RFC3339Nano)
		}
	case "ended", "rejected", "missed", "failed":
		if endedAt := callTimeParam(params.Raw("ended_at"), params.Raw("ended_at_ms")); endedAt != "" {
			call.EndedAt = endedAt
		} else if call.EndedAt == "" {
			call.EndedAt = now.UTC().Format(time.RFC3339Nano)
		}
		call.EndedByMXID = fallback(params.String("ended_by_mxid"), localUserID)
		call.EndReason = params.String("reason")
		if durationMS := params.Int64("duration_ms"); durationMS > 0 {
			call.DurationMS = durationMS
		} else if call.DurationMS <= 0 {
			call.DurationMS = callDurationMS(call.AnsweredAt, call.EndedAt)
		}
	}
}

func callDurationMS(start, end string) int64 {
	startTime, startErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(start))
	endTime, endErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(end))
	if startErr != nil || endErr != nil {
		return 0
	}
	duration := endTime.Sub(startTime)
	if duration < 0 {
		return 0
	}
	return duration.Milliseconds()
}
