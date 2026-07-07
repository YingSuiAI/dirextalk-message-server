package storage

import (
	"context"
	"database/sql"
	"encoding/json"
)

func (s *DatabaseStore) InsertReport(ctx context.Context, report reportRecord) error {
	imagesJSON, err := json.Marshal(report.ImageURLs)
	if err != nil {
		return err
	}
	return s.writer.Do(nil, nil, func(txn *sql.Tx) error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO p2p_reports (
				report_id, target_type, target_room_id, target_channel_id, target_name,
				reporter_mxid, reporter_display_name, reason, body, image_urls_json,
				system_room_id, event_id, origin_server_ts, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		`, report.ReportID, report.TargetType, report.TargetRoomID, report.TargetChannelID, report.TargetName,
			report.ReporterMXID, report.ReporterDisplayName, report.Reason, report.Body, string(imagesJSON),
			report.SystemRoomID, report.EventID, report.OriginServerTS, report.CreatedAt)
		return err
	})
}

func (s *DatabaseStore) ListReports(ctx context.Context, targetRoomID string, limit int) ([]reportRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows *sql.Rows
	var err error
	if targetRoomID != "" {
		rows, err = s.db.QueryContext(ctx, reportSelect+` WHERE target_room_id = $1 ORDER BY created_at DESC, report_id DESC LIMIT $2`, targetRoomID, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, reportSelect+` ORDER BY created_at DESC, report_id DESC LIMIT $1`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer closeResource(rows)
	reports := make([]reportRecord, 0)
	for rows.Next() {
		report, err := scanReport(rows)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}

const reportSelect = `SELECT report_id, target_type, target_room_id, target_channel_id, target_name,
	reporter_mxid, reporter_display_name, reason, body, image_urls_json,
	system_room_id, event_id, origin_server_ts, created_at FROM p2p_reports`

type reportScanner interface {
	Scan(dest ...any) error
}

func scanReport(scanner reportScanner) (reportRecord, error) {
	var report reportRecord
	var imagesJSON string
	if err := scanner.Scan(
		&report.ReportID,
		&report.TargetType,
		&report.TargetRoomID,
		&report.TargetChannelID,
		&report.TargetName,
		&report.ReporterMXID,
		&report.ReporterDisplayName,
		&report.Reason,
		&report.Body,
		&imagesJSON,
		&report.SystemRoomID,
		&report.EventID,
		&report.OriginServerTS,
		&report.CreatedAt,
	); err != nil {
		return reportRecord{}, err
	}
	if imagesJSON != "" {
		if err := json.Unmarshal([]byte(imagesJSON), &report.ImageURLs); err != nil {
			return reportRecord{}, err
		}
	}
	if report.ImageURLs == nil {
		report.ImageURLs = []string{}
	}
	return report, nil
}
