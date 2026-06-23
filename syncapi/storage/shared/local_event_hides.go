package shared

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	rstypes "github.com/YingSuiAI/direxio-message-server/roomserver/types"
	"github.com/YingSuiAI/direxio-message-server/syncapi/types"
)

func (d *Database) HideLocalEvents(ctx context.Context, userID, roomID string, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	if d.LocalEventHides == nil {
		return fmt.Errorf("local event hide storage is not configured")
	}
	hiddenAt := time.Now().UTC().Format(time.RFC3339Nano)
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.LocalEventHides.InsertLocalEventHides(ctx, txn, userID, roomID, eventIDs, hiddenAt)
	})
}

func (d *Database) ClearLocalRoom(ctx context.Context, userID, roomID string, throughStreamPos types.StreamPosition) error {
	if d.LocalEventHides == nil {
		return fmt.Errorf("local event hide storage is not configured")
	}
	hiddenAt := time.Now().UTC().Format(time.RFC3339Nano)
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.LocalEventHides.UpsertLocalRoomClear(ctx, txn, userID, roomID, throughStreamPos, hiddenAt)
	})
}

func (d *DatabaseTransaction) FilterLocalHiddenStreamEvents(
	ctx context.Context, userID, roomID string, events []types.StreamEvent,
) ([]types.StreamEvent, error) {
	if len(events) == 0 || userID == "" || d.LocalEventHides == nil {
		return events, nil
	}

	eventsByRoom := make(map[string][]types.StreamEvent)
	eventIDsByRoom := make(map[string][]string)
	for _, event := range events {
		if event.HeaderedEvent == nil {
			continue
		}
		eventRoomID := roomID
		if eventRoomID == "" {
			eventRoomID = event.RoomID().String()
		}
		if eventRoomID == "" {
			continue
		}
		eventsByRoom[eventRoomID] = append(eventsByRoom[eventRoomID], event)
		eventIDsByRoom[eventRoomID] = append(eventIDsByRoom[eventRoomID], event.EventID())
	}
	if len(eventIDsByRoom) == 0 {
		return events, nil
	}

	hiddenEventIDs := make(map[string]struct{})
	for eventRoomID, eventIDs := range eventIDsByRoom {
		state, err := d.LocalEventHides.SelectLocalEventHideState(ctx, d.txn, userID, eventRoomID, eventIDs)
		if err != nil {
			return nil, err
		}
		for _, event := range eventsByRoom[eventRoomID] {
			if state.IsHidden(event) {
				hiddenEventIDs[event.EventID()] = struct{}{}
			}
		}
	}
	if len(hiddenEventIDs) == 0 {
		return events, nil
	}

	filtered := make([]types.StreamEvent, 0, len(events))
	for _, event := range events {
		if event.HeaderedEvent == nil {
			filtered = append(filtered, event)
			continue
		}
		if _, hidden := hiddenEventIDs[event.EventID()]; hidden {
			continue
		}
		filtered = append(filtered, event)
	}
	return filtered, nil
}

func (d *DatabaseTransaction) FilterLocalHiddenEvents(
	ctx context.Context, userID, roomID string, events []*rstypes.HeaderedEvent,
) ([]*rstypes.HeaderedEvent, error) {
	if len(events) == 0 || userID == "" || d.LocalEventHides == nil {
		return events, nil
	}

	eventIDs := make([]string, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		eventIDs = append(eventIDs, event.EventID())
	}
	if len(eventIDs) == 0 {
		return events, nil
	}

	selected, err := d.OutputEvents.SelectEvents(ctx, d.txn, eventIDs, nil, true)
	if err != nil {
		return nil, err
	}
	streamByEventID := make(map[string]types.StreamEvent, len(selected))
	for _, event := range selected {
		if event.HeaderedEvent == nil {
			continue
		}
		streamByEventID[event.EventID()] = event
	}

	streamEvents := make([]types.StreamEvent, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		streamEvent, ok := streamByEventID[event.EventID()]
		if !ok {
			streamEvent = types.StreamEvent{HeaderedEvent: event}
		} else if streamEvent.HeaderedEvent == nil {
			streamEvent.HeaderedEvent = event
		}
		streamEvents = append(streamEvents, streamEvent)
	}

	filteredStreamEvents, err := d.FilterLocalHiddenStreamEvents(ctx, userID, roomID, streamEvents)
	if err != nil {
		return nil, err
	}
	filtered := make([]*rstypes.HeaderedEvent, 0, len(filteredStreamEvents))
	for _, event := range filteredStreamEvents {
		if event.HeaderedEvent != nil {
			filtered = append(filtered, event.HeaderedEvent)
		}
	}
	return filtered, nil
}
