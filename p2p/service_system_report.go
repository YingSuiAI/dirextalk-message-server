package p2p

import (
	"context"
	"errors"
	"strings"
	"time"
)

const systemRoomName = "System Notification"

type reportStore interface {
	InsertReport(ctx context.Context, report reportRecord) error
}

func (s *Service) reportStore() reportStore {
	if s.store == nil {
		return nil
	}
	return s.store
}

func (s *Service) ensureSystemRoom(ctx context.Context) (bool, error) {
	s.mu.Lock()
	currentRoomID := strings.TrimSpace(s.systemRoomID)
	ownerMXID := s.ownerMXID
	ownerDisplayName := s.profile.DisplayName
	ownerAvatarURL := s.profile.AvatarURL
	s.mu.Unlock()
	if currentRoomID != "" || s.transport == nil {
		return false, nil
	}
	res, err := s.transport.CreateRoom(ctx, CreateRoomRequest{
		CreatorMXID:        ownerMXID,
		CreatorDisplayName: ownerDisplayName,
		CreatorAvatarURL:   ownerAvatarURL,
		Name:               systemRoomName,
		Topic:              "Dirextalk system notifications",
		Visibility:         "private",
		RoomType:           DirextalkRoomTypeSystem,
		IsDirect:           false,
	})
	if err != nil {
		return false, err
	}
	roomID := strings.TrimSpace(res.RoomID)
	if roomID == "" {
		return false, nil
	}
	s.mu.Lock()
	s.systemRoomID = roomID
	s.mu.Unlock()
	return roomID != currentRoomID, nil
}

func (s *Service) reportSubmit(ctx context.Context, params map[string]any) (any, *apiError) {
	targetType := strings.ToLower(trimString(params["target_type"]))
	if targetType == "" {
		targetType = strings.ToLower(trimString(params["room_type"]))
	}
	roomID := trimString(params["room_id"])
	channelID := trimString(params["channel_id"])
	if roomID == "" && channelID == "" {
		return nil, badRequest("room_id or channel_id is required")
	}
	if targetType == "" {
		if channelID != "" {
			targetType = "channel"
		} else {
			targetType = "group"
		}
	}
	var targetName string
	switch targetType {
	case "group":
		group, ok, err := s.groupByRoom(ctx, roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if !ok {
			return nil, statusError(404, "group not found")
		}
		roomID = group.RoomID
		targetName = group.Name
	case "channel":
		ch, ok, err := s.channelByIDOrRoom(ctx, channelID, roomID)
		if err != nil {
			return nil, internalError(err)
		}
		if !ok {
			return nil, statusError(404, "channel not found")
		}
		roomID = ch.RoomID
		channelID = ch.ChannelID
		targetName = ch.Name
	default:
		return nil, badRequest("target_type must be group or channel")
	}
	changed, err := s.ensureSystemRoom(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	if changed && s.store != nil {
		s.mu.Lock()
		state := s.portalStateLocked()
		s.mu.Unlock()
		if err := s.store.SavePortal(ctx, state); err != nil {
			return nil, internalError(err)
		}
	}
	s.mu.Lock()
	systemRoomID := s.systemRoomID
	senderMXID := s.ownerMXID
	s.mu.Unlock()
	if strings.TrimSpace(systemRoomID) == "" {
		return nil, internalError(errors.New("system room is not available"))
	}
	now := time.Now().UTC()
	report := reportRecord{
		ReportID:            "report_" + randomToken("report"),
		TargetType:          targetType,
		TargetRoomID:        roomID,
		TargetChannelID:     channelID,
		TargetName:          targetName,
		ReporterMXID:        trimString(params["reporter_mxid"]),
		ReporterDisplayName: trimString(params["reporter_display_name"]),
		Reason:              trimString(params["reason"]),
		Body:                trimString(params["body"]),
		ImageURLs:           stringSliceParam(params["image_urls"]),
		SystemRoomID:        systemRoomID,
		CreatedAt:           now.Format(time.RFC3339Nano),
	}
	if report.ReporterMXID == "" {
		report.ReporterMXID = trimString(params["user_mxid"])
	}
	if report.ReporterDisplayName == "" {
		report.ReporterDisplayName = trimString(params["display_name"])
	}
	if report.Reason == "" {
		report.Reason = trimString(params["report_reason"])
	}
	content := reportNotificationContent(report)
	result, err := s.transport.SendMessage(ctx, SendMessageRequest{
		SenderMXID:  senderMXID,
		RoomID:      systemRoomID,
		EventType:   "m.room.message",
		MessageType: "m.notice",
		Content:     content,
		Timestamp:   now,
	})
	if err != nil {
		return nil, transportWriteError(err)
	}
	report.EventID = strings.TrimSpace(result.EventID)
	report.OriginServerTS = result.OriginServerTS
	if report.OriginServerTS <= 0 {
		report.OriginServerTS = now.UnixMilli()
	}
	if store := s.reportStore(); store != nil {
		if err := store.InsertReport(ctx, report); err != nil {
			return nil, internalError(err)
		}
	}
	if err := s.saveConversation(ctx, conversationRecord{
		MatrixRoomID:    systemRoomID,
		Kind:            conversationKindSystem,
		Lifecycle:       conversationLifecycleActive,
		Title:           systemRoomName,
		LastEventID:     report.EventID,
		LastMessage:     fallbackString(report.Body, "A report has been submitted for a group or channel you manage."),
		LastActivityAt:  report.OriginServerTS,
		ProjectionState: conversationProjectionReady,
		CreatedAt:       now.UnixMilli(),
		UpdatedAt:       now.UnixMilli(),
	}); err != nil {
		return nil, internalError(err)
	}
	return report, nil
}

func reportNotificationContent(report reportRecord) map[string]any {
	images := append([]string(nil), report.ImageURLs...)
	return map[string]any{
		"msgtype":               "m.notice",
		"body":                  fallbackString(report.Body, "A report has been submitted for a group or channel you manage."),
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
		"image_urls":            images,
	}
}
