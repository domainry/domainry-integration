package integration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/domainry/domainry-orm/query"
)

func (s *OperationsStore) ProcessDueEvents(ctx context.Context, workerID string, limit int) (int, error) {
	if strings.TrimSpace(workerID) == "" {
		return 0, fmt.Errorf("Integration event worker ID is required")
	}
	if limit <= 0 || limit > 500 {
		limit = 25
	}
	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)
	due, args, err := query.NewSelectBuilder(s.dialect, "_integration_events").
		Columns("id", "workspace_id", "status", "fencing_token").
		Where(query.Or(
			query.And(query.In("status", "received", "failed"), query.Or(query.Equal("next_retry_at", ""), query.LessThanOrEqual("next_retry_at", nowText))),
			query.And(query.Equal("status", "processing"), query.Or(query.Equal("lease_expires_at", ""), query.LessThanOrEqual("lease_expires_at", nowText))),
		)).OrderBy(query.Ascending("received_at")).Limit(limit).Build()
	if err != nil {
		return 0, err
	}
	rows, err := s.database.QueryContext(ctx, due, args...)
	if err != nil {
		return 0, fmt.Errorf("list due Integration events: %w", err)
	}
	type candidate struct {
		id, workspaceID, status string
		fencingToken            int64
	}
	candidates := []candidate{}
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.id, &value.workspaceID, &value.status, &value.fencingToken); err != nil {
			_ = rows.Close()
			return 0, err
		}
		candidates = append(candidates, value)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	processed := 0
	var firstErr error
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		claim, claimArgs, err := query.NewUpdateBuilder(s.dialect, "_integration_events").
			Set("status", "processing").Set("lease_owner", workerID).Set("lease_expires_at", now.Add(30*time.Second).Format(time.RFC3339Nano)).
			Set("fencing_token", candidate.fencingToken+1).Set("updated_at", nowText).
			Where(query.And(query.Equal("id", candidate.id), query.Equal("workspace_id", candidate.workspaceID), query.Equal("status", candidate.status), query.Equal("fencing_token", candidate.fencingToken))).Build()
		if err != nil {
			return processed, err
		}
		result, err := s.database.ExecContext(ctx, claim, claimArgs...)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			continue
		}
		event, err := s.GetEvent(ctx, candidate.workspaceID, candidate.id)
		if err == nil {
			_, err = s.processEvent(ctx, event, nil)
		}
		processed++
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return processed, firstErr
}
