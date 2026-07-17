package projector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkstate"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// projectRoomCreate is the only projection path allowed to assign a room
// creator. Profile state may be written later by any authorized moderator and
// therefore cannot identify who created the room.
func (m *Module) projectRoomCreate(ctx context.Context, event *types.HeaderedEvent) error {
	stateKey := event.StateKey()
	if stateKey == nil || *stateKey != "" {
		return nil
	}

	content := map[string]any{}
	if err := json.Unmarshal(event.Content(), &content); err != nil {
		return err
	}
	kind := conversationKindFromStateKind(dirextalkstate.RoomKindFromRoomType(textValue(content["type"])))
	if m.dependencies.Conversations == nil {
		if groupOrChannelConversation(kind) {
			return errors.New("conversation projection port is not configured")
		}
		return nil
	}

	roomID := event.RoomID().String()
	record, exists, err := m.dependencies.Conversations.GetRecord(ctx, "", roomID)
	if err != nil {
		return err
	}
	if exists {
		if !groupOrChannelConversation(record.Kind) {
			return nil
		}
		if groupOrChannelConversation(kind) && kind != record.Kind {
			return fmt.Errorf("conversation kind conflict for room %s: existing %s, create event %s", roomID, record.Kind, kind)
		}
		kind = record.Kind
	}
	if !groupOrChannelConversation(kind) {
		return nil
	}

	if !exists {
		at := m.eventTime(event).UnixMilli()
		if err := m.dependencies.Conversations.Save(ctx, dirextalkdomain.ConversationRecord{
			MatrixRoomID:     roomID,
			Kind:             kind,
			Lifecycle:        dirextalkdomain.ConversationLifecycleActive,
			ProjectionState:  dirextalkdomain.ConversationProjectionPending,
			ProjectionReason: "room_profile_pending",
			CreatedAt:        at,
			UpdatedAt:        at,
		}); err != nil {
			return err
		}
	}

	creatorMXID := authoritativeRoomCreatorMXID(event, content)
	if creatorMXID == "" {
		return nil
	}
	return m.dependencies.Conversations.SetCreator(ctx, roomID, creatorMXID)
}

func groupOrChannelConversation(kind dirextalkdomain.ConversationKind) bool {
	return kind == dirextalkdomain.ConversationKindGroup || kind == dirextalkdomain.ConversationKindChannel
}

func authoritativeRoomCreatorMXID(event *types.HeaderedEvent, content map[string]any) string {
	if event != nil {
		if mxid := validFullMXID(string(event.SenderID())); mxid != "" {
			return mxid
		}
	}
	return validFullMXID(textValue(content["creator"]))
}

func validFullMXID(value string) string {
	value = strings.TrimSpace(value)
	userID, err := spec.NewUserID(value, true)
	if err != nil || userID == nil {
		return ""
	}
	return userID.String()
}
