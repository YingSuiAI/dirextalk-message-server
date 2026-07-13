package realtimews

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func (m *Module) streamEvents(
	ctx context.Context,
	conn *websocket.Conn,
	role string,
	since int64,
	readDone <-chan struct{},
	outbound <-chan map[string]any,
) {
	if m == nil || m.events == nil {
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "server.error", "error": "event service is unavailable"})
		return
	}
	cursorStatus, err := m.events.CursorStatus(ctx, since)
	if err != nil {
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "server.error", "error": err.Error()})
		return
	}
	if cursorStatus.Expired {
		if err := wsjson.Write(ctx, conn, map[string]any{
			"type":     "server.cursor_reset",
			"since":    cursorStatus.Since,
			"min_seq":  cursorStatus.Bounds.MinSeq,
			"max_seq":  cursorStatus.Bounds.MaxSeq,
			"count":    cursorStatus.Bounds.Count,
			"recovery": "bootstrap_required",
		}); err != nil {
			return
		}
	}
	for {
		events, err := m.events.List(ctx, since, BatchLimit)
		if err != nil {
			_ = wsjson.Write(ctx, conn, map[string]any{"type": "server.error", "error": err.Error()})
			return
		}
		if len(events) > 0 {
			for _, event := range events {
				if eventVisible(role, event) {
					if err := wsjson.Write(ctx, conn, map[string]any{"type": "server.event", "event": event}); err != nil {
						return
					}
				}
				since = event.Seq
			}
			continue
		}

		// Preserve the existing List-then-Waiter ordering. Making subscription
		// atomic is a separate behavior fix with its own concurrency contract.
		waitForEvent := m.events.Waiter()
		select {
		case <-ctx.Done():
			return
		case <-readDone:
			return
		case frame := <-outbound:
			if err := wsjson.Write(ctx, conn, frame); err != nil {
				return
			}
		case <-waitForEvent:
		}
	}
}

func eventVisible(string, dirextalkdomain.Event) bool { return true }
