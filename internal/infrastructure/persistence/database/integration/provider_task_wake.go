package integration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

func wakeProviderTasks(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, connectionKey string, taskKeys []string, now time.Time) (int64, error) {
	sequences, err := wakeProviderTasksWithSequences(ctx, database, dialect, subjectLifecycle, workspaceID, connectionKey, taskKeys, now)
	return int64(len(sequences)), err
}

func wakeProviderTasksWithSequences(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, subjectLifecycle *SubjectLifecyclePersistence, workspaceID, connectionKey string, taskKeys []string, now time.Time) (map[string]int64, error) {
	workspaceID, connectionKey = strings.TrimSpace(workspaceID), strings.TrimSpace(connectionKey)
	if workspaceID == "" || connectionKey == "" {
		return nil, fmt.Errorf("Integration Provider task wake target is incomplete")
	}
	seen := map[string]bool{}
	keys := make([]string, 0, len(taskKeys))
	for _, key := range taskKeys {
		key = strings.TrimSpace(key)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		keys = append(keys, key)
	}
	if len(keys) == 0 || len(keys) > 10 {
		return nil, fmt.Errorf("Integration Provider task wake set is invalid")
	}
	sort.Strings(keys)
	if err := guardSubjectWrite(ctx, database, dialect, subjectLifecycle, workspaceID, subjectFenceReference{"connection", "", connectionKey}); err != nil {
		return nil, err
	}
	nowMillis := now.UTC().UnixMilli()
	sequences := make(map[string]int64, len(keys))
	for _, key := range keys {
		base := query.And(
			subjectRowsWriteAllowed(subjectLifecycle, dialect, workspaceID, providerRunTable),
			query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey), query.Equal("run_kind", providerRunKindState), query.Equal("run_key", key),
		)
		processing, args, err := query.NewUpdateBuilder(dialect, providerRunTable).
			Set("due_at", nowMillis).SetExpression("wake_sequence", query.Add(query.Column("wake_sequence"), query.Value(1))).Set("updated_at", nowMillis).
			Where(query.And(base, query.Equal("status", "processing"))).Build()
		if err != nil {
			return nil, err
		}
		result, err := database.ExecContext(ctx, processing, args...)
		if err != nil {
			return nil, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected == 0 {
			ready, readyArgs, buildErr := query.NewUpdateBuilder(dialect, providerRunTable).
				Set("status", "ready").Set("due_at", nowMillis).Set("attempt_count", 0).
				Set("lease_owner", "").Set("lease_expires_at", int64(0)).
				SetExpression("wake_sequence", query.Add(query.Column("wake_sequence"), query.Value(1))).Set("updated_at", nowMillis).
				Where(query.And(base, query.In("status", "ready", "failed", "dead_letter"))).Build()
			if buildErr != nil {
				return nil, buildErr
			}
			result, err = database.ExecContext(ctx, ready, readyArgs...)
			if err != nil {
				return nil, err
			}
			affected, err = result.RowsAffected()
			if err != nil {
				return nil, err
			}
		}
		if affected == 0 {
			continue
		}
		if affected != 1 {
			return nil, fmt.Errorf("Integration Provider task wake changed %d rows", affected)
		}
		statement, args, err := query.NewSelectBuilder(dialect, providerRunTable).Columns("wake_sequence").Where(query.And(
			query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey), query.Equal("run_kind", providerRunKindState), query.Equal("run_key", key),
		)).Build()
		if err != nil {
			return nil, err
		}
		var sequence int64
		if err := database.QueryRowContext(ctx, statement, args...).Scan(&sequence); err != nil {
			return nil, err
		}
		sequences[key] = sequence
	}
	return sequences, nil
}
