// Package reports owns report submission, durable report creation, and the
// related Matrix system-notification projection.
package reports

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalktransport"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const (
	actionSubmit   = "reports.submit"
	SystemRoomName = "System Notification"
)

// Store is the durable report repository used by Module.
type Store interface {
	InsertReport(context.Context, dirextalkdomain.ReportRecord) error
}

// Target is the normalized report target resolved from the group or channel
// owning module.
type Target struct {
	RoomID    string
	ChannelID string
	Name      string
}

// TargetPort resolves the durable product target before any report side
// effects occur.
type TargetPort interface {
	Group(context.Context, string) (Target, bool, error)
	Channel(context.Context, string, string) (Target, bool, error)
}

// SystemRoom is the Matrix room and sender used for owner notifications.
type SystemRoom struct {
	RoomID     string
	SenderMXID string
}

// SystemRoomPort ensures that the system room and its portal state are
// durable before a report message is sent.
type SystemRoomPort interface {
	Ensure(context.Context) (SystemRoom, error)
}

// MatrixPort is the narrow Matrix write boundary required by reports.
type MatrixPort interface {
	SendMessage(context.Context, dirextalktransport.SendMessageRequest) (dirextalktransport.SendMessageResult, error)
}

// ConversationPort owns the durable system-conversation projection.
type ConversationPort interface {
	Save(context.Context, dirextalkdomain.ConversationRecord) error
}

// Config contains deterministic identity, clock, and error-mapping seams.
type Config struct {
	NewReportID       func() string
	Now               func() time.Time
	MapTransportError func(error) *actionbase.Error
}

// Module implements the reports ProductCore action over narrow domain ports.
type Module struct {
	store         Store
	targets       TargetPort
	systemRooms   SystemRoomPort
	matrix        MatrixPort
	conversations ConversationPort
	config        Config
}

func New(store Store, targets TargetPort, systemRooms SystemRoomPort, matrix MatrixPort, conversations ConversationPort, cfg Config) *Module {
	return &Module{
		store: store, targets: targets, systemRooms: systemRooms,
		matrix: matrix, conversations: conversations, config: cfg,
	}
}

// Handlers returns the exact ProductCore action surface owned by the module.
func (m *Module) Handlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{actionSubmit: m.Submit}
}

func (m *Module) Submit(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	values := actionbase.Params(params)
	targetType := strings.ToLower(values.FirstString("target_type", "room_type"))
	roomID := values.String("room_id")
	channelID := values.String("channel_id")
	if roomID == "" && channelID == "" {
		return nil, actionbase.BadRequest("room_id or channel_id is required")
	}
	if targetType == "" {
		if channelID != "" {
			targetType = "channel"
		} else {
			targetType = "group"
		}
	}

	target, actionErr := m.resolveTarget(ctx, targetType, channelID, roomID)
	if actionErr != nil {
		return nil, actionErr
	}
	systemRoom, err := m.systemRooms.Ensure(ctx)
	if err != nil {
		return nil, actionbase.InternalError(err)
	}
	if strings.TrimSpace(systemRoom.RoomID) == "" {
		return nil, actionbase.InternalError(errors.New("system room is not available"))
	}

	now := m.now()
	report := dirextalkdomain.ReportRecord{
		ReportID:            m.newReportID(),
		TargetType:          targetType,
		TargetRoomID:        target.RoomID,
		TargetChannelID:     target.ChannelID,
		TargetName:          target.Name,
		ReporterMXID:        values.FirstString("reporter_mxid", "user_mxid"),
		ReporterDisplayName: values.FirstString("reporter_display_name", "display_name"),
		Reason:              values.FirstString("reason", "report_reason"),
		Body:                values.String("body"),
		ImageURLs:           values.Strings("image_urls"),
		SystemRoomID:        systemRoom.RoomID,
		CreatedAt:           now.Format(time.RFC3339Nano),
	}
	result, err := m.matrix.SendMessage(ctx, dirextalktransport.SendMessageRequest{
		SenderMXID:  systemRoom.SenderMXID,
		RoomID:      systemRoom.RoomID,
		EventType:   "m.room.message",
		MessageType: "m.notice",
		Content:     notificationContent(report),
		Timestamp:   now,
	})
	if err != nil {
		return nil, m.transportError(err)
	}
	report.EventID = strings.TrimSpace(result.EventID)
	report.OriginServerTS = result.OriginServerTS
	if report.OriginServerTS <= 0 {
		report.OriginServerTS = now.UnixMilli()
	}
	if m.store != nil {
		if err := m.store.InsertReport(ctx, report); err != nil {
			return nil, actionbase.InternalError(err)
		}
	}
	if err := m.conversations.Save(ctx, dirextalkdomain.ConversationRecord{
		MatrixRoomID:    systemRoom.RoomID,
		Kind:            dirextalkdomain.ConversationKindSystem,
		Lifecycle:       dirextalkdomain.ConversationLifecycleActive,
		Title:           SystemRoomName,
		LastEventID:     report.EventID,
		LastMessage:     fallback(report.Body, "A report has been submitted for a group or channel you manage."),
		LastActivityAt:  report.OriginServerTS,
		ProjectionState: dirextalkdomain.ConversationProjectionReady,
		CreatedAt:       now.UnixMilli(),
		UpdatedAt:       now.UnixMilli(),
	}); err != nil {
		return nil, actionbase.InternalError(err)
	}
	return report, nil
}

func (m *Module) resolveTarget(ctx context.Context, targetType, channelID, roomID string) (Target, *actionbase.Error) {
	switch targetType {
	case "group":
		target, ok, err := m.targets.Group(ctx, roomID)
		if err != nil {
			return Target{}, actionbase.InternalError(err)
		}
		if !ok {
			return Target{}, actionbase.StatusError(http.StatusNotFound, "group not found")
		}
		return target, nil
	case "channel":
		target, ok, err := m.targets.Channel(ctx, channelID, roomID)
		if err != nil {
			return Target{}, actionbase.InternalError(err)
		}
		if !ok {
			return Target{}, actionbase.StatusError(http.StatusNotFound, "channel not found")
		}
		return target, nil
	default:
		return Target{}, actionbase.BadRequest("target_type must be group or channel")
	}
}

func (m *Module) now() time.Time {
	if m.config.Now == nil {
		return time.Now().UTC()
	}
	return m.config.Now().UTC()
}

func (m *Module) newReportID() string {
	if m.config.NewReportID == nil {
		return "report_" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return m.config.NewReportID()
}

func (m *Module) transportError(err error) *actionbase.Error {
	if m.config.MapTransportError != nil {
		return m.config.MapTransportError(err)
	}
	return actionbase.InternalError(err)
}

func notificationContent(report dirextalkdomain.ReportRecord) map[string]any {
	return map[string]any{
		"msgtype":               "m.notice",
		"body":                  fallback(report.Body, "A report has been submitted for a group or channel you manage."),
		"msg_type":              "report",
		"client_type":           "report",
		"p2p_kind":              "system_report",
		"report_id":             report.ReportID,
		"target_type":           report.TargetType,
		"target_room_id":        report.TargetRoomID,
		"target_channel_id":     report.TargetChannelID,
		"target_name":           report.TargetName,
		"reporter_mxid":         report.ReporterMXID,
		"reporter_display_name": report.ReporterDisplayName,
		"reason":                report.Reason,
		"report_time":           report.CreatedAt,
		"image_urls":            append([]string(nil), report.ImageURLs...),
	}
}

func fallback(value, defaultValue string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return defaultValue
}
