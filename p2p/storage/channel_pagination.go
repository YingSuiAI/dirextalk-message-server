package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func listChannelPage[T any](
	ctx context.Context,
	db *sql.DB,
	selectSQL, filterField, filterValue, idField string,
	fromTS, snapshotTS, cursorTS int64,
	cursorID string,
	limit int,
	scan func(channelScanner) (T, error),
) ([]T, bool, error) {
	query, args := channelPageQuery(
		selectSQL,
		filterField,
		filterValue,
		idField,
		fromTS,
		snapshotTS,
		cursorTS,
		cursorID,
		limit,
	)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer closeResource(rows)

	var records []T
	for rows.Next() {
		record, err := scan(rows)
		if err != nil {
			return nil, false, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
	}
	return records, hasMore, nil
}

func channelPageQuery(
	selectSQL, filterField, filterValue, idField string,
	fromTS, snapshotTS, cursorTS int64,
	cursorID string,
	limit int,
) (string, []any) {
	query := selectSQL + ` WHERE origin_server_ts >= $1 AND origin_server_ts <= $2`
	args := []any{fromTS, snapshotTS}
	nextArg := 3
	if strings.TrimSpace(filterValue) != "" {
		query += fmt.Sprintf(` AND %s = $%d`, filterField, nextArg)
		args = append(args, filterValue)
		nextArg++
	}
	if cursorTS > 0 {
		query += fmt.Sprintf(` AND (origin_server_ts < $%d OR (origin_server_ts = $%d AND %s < $%d))`, nextArg, nextArg, idField, nextArg+1)
		args = append(args, cursorTS, cursorID)
		nextArg += 2
	}
	query += fmt.Sprintf(` ORDER BY origin_server_ts DESC, %s DESC LIMIT $%d`, idField, nextArg)
	args = append(args, limit+1)
	return query, args
}
