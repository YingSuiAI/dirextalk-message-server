package agenthistory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/matrixhistory"
	roomserverAPI "github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	rstypes "github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/internal"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/storage"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/synctypes"
	"github.com/YingSuiAI/dirextalk-message-server/syncapi/types"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type Reader struct {
	DB     storage.Database
	RSAPI  roomserverAPI.SyncRoomserverAPI
	UserID string
}

func NewReader(db storage.Database, rsAPI roomserverAPI.SyncRoomserverAPI, userID string) *Reader {
	return &Reader{
		DB:     db,
		RSAPI:  rsAPI,
		UserID: strings.TrimSpace(userID),
	}
}

func (r *Reader) ListOrdinaryMessages(ctx context.Context, roomID string, fromTS, toTS int64, limit int) ([]matrixhistory.MessageSummary, error) {
	if r == nil || r.DB == nil {
		return nil, fmt.Errorf("sync DB reader is unavailable")
	}
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return nil, fmt.Errorf("room_id is required")
	}
	if limit <= 0 {
		limit = 50
	}
	snapshot, err := r.DB.NewDatabaseSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer snapshot.Rollback()

	maxPos, err := snapshot.MaxStreamPositionForPDUs(ctx)
	if err != nil {
		return nil, err
	}
	if maxPos <= 0 {
		return nil, nil
	}
	from, err := snapshot.StreamToTopologicalPosition(ctx, roomID, maxPos, true)
	if err != nil {
		return nil, err
	}
	to := types.TopologyToken{}
	eventType := "m.room.message"
	filter := synctypes.DefaultRoomEventFilter()
	filter.Limit = limit * 3
	if filter.Limit < limit {
		filter.Limit = limit
	}
	filter.Types = &[]string{eventType}
	streamEvents, _, _, err := snapshot.GetEventsInTopologicalRange(ctx, &from, &to, roomID, &filter, true)
	if err != nil {
		return nil, err
	}
	events := snapshot.StreamEventsToEvents(ctx, nil, streamEvents, r.RSAPI)
	events, err = r.filterVisibleEvents(ctx, snapshot, roomID, events)
	if err != nil {
		return nil, err
	}
	messages := make([]matrixhistory.MessageSummary, 0, limit)
	for _, event := range events {
		if event == nil || event.Type() != eventType || !matrixhistory.InTimeRange(int64(event.OriginServerTS()), fromTS, toTS) {
			continue
		}
		content := map[string]any{}
		if err := json.Unmarshal(event.Content(), &content); err != nil {
			continue
		}
		if trimString(content["p2p_kind"]) != "" {
			continue
		}
		body := trimString(content["body"])
		if body == "" {
			continue
		}
		sender := senderMXID(event)
		localpart, domain := splitMXID(sender)
		messages = append(messages, matrixhistory.MessageSummary{
			TS:              int64(event.OriginServerTS()),
			Sender:          displayNameFromMXID(sender),
			SenderMXID:      sender,
			SenderDomain:    domain,
			SenderLocalpart: localpart,
			Msg:             body,
		})
		if len(messages) >= limit {
			break
		}
	}
	return messages, nil
}

func (r *Reader) filterVisibleEvents(ctx context.Context, snapshot storage.DatabaseTransaction, roomID string, events []*rstypes.HeaderedEvent) ([]*rstypes.HeaderedEvent, error) {
	userID, err := spec.NewUserID(r.UserID, true)
	if err != nil {
		return nil, err
	}
	filtered := events
	if r.RSAPI != nil {
		filtered, err = internal.ApplyHistoryVisibilityFilter(ctx, snapshot, r.RSAPI, events, nil, *userID, "agent_history")
		if err != nil {
			return nil, err
		}
	}
	return snapshot.FilterLocalHiddenEvents(ctx, userID.String(), roomID, filtered)
}

func senderMXID(event *rstypes.HeaderedEvent) string {
	if event == nil {
		return ""
	}
	if event.UserID.String() != "" {
		return event.UserID.String()
	}
	return string(event.SenderID())
}

func trimString(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return ""
}

func displayNameFromMXID(mxid string) string {
	localpart, _ := splitMXID(mxid)
	if strings.TrimSpace(localpart) == "" {
		return strings.TrimSpace(mxid)
	}
	return localpart
}

func splitMXID(mxid string) (localpart, domain string) {
	trimmed := strings.TrimSpace(mxid)
	withoutSigil := strings.TrimPrefix(trimmed, "@")
	if idx := strings.Index(withoutSigil, ":"); idx >= 0 {
		localpart = strings.TrimSpace(withoutSigil[:idx])
		domain = strings.TrimSpace(withoutSigil[idx+1:])
		return localpart, domain
	}
	return strings.TrimSpace(withoutSigil), ""
}
