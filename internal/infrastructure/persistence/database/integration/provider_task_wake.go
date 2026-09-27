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
	workspaceID, connectionKey = strings.TrimSpace(workspaceID), strings.TrimSpace(connectionKey)
	if workspaceID == "" || connectionKey == "" {
		return 0, fmt.Errorf("Integration Provider task wake target is incomplete")
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
		return 0, fmt.Errorf("Integration Provider task wake set is invalid")
	}
	sort.Strings(keys)
	values := make([]any, len(keys))
	for index := range keys {
		values[index] = keys[index]
	}
	if err := guardSubjectWrite(ctx, database, dialect, subjectLifecycle, workspaceID, subjectFenceReference{"connection", "", connectionKey}); err != nil {
		return 0, err
	}
	base := query.And(
		subjectRowsWriteAllowed(subjectLifecycle, dialect, workspaceID, providerRunTable),
		query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey), query.Equal("run_kind", providerRunKindState), query.In("run_key", values...),
	)
	nowMillis := now.UTC().UnixMilli()
	processing, args, err := query.NewUpdateBuilder(dialect, providerRunTable).
		Set("due_at", nowMillis).SetExpression("wake_sequence", query.Add(query.Column("wake_sequence"), query.Value(1))).Set("updated_at", nowMillis).
		Where(query.And(base, query.Equal("status", "processing"))).Build()
	if err != nil {
		return 0, err
	}
	result, err := database.ExecContext(ctx, processing, args...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	ready, args, err := query.NewUpdateBuilder(dialect, providerRunTable).
		Set("status", "ready").Set("due_at", nowMillis).Set("attempt_count", 0).
		Set("lease_owner", "").Set("lease_expires_at", int64(0)).
		SetExpression("wake_sequence", query.Add(query.Column("wake_sequence"), query.Value(1))).Set("updated_at", nowMillis).
		Where(query.And(base, query.In("status", "ready", "failed", "dead_letter"))).Build()
	if err != nil {
		return 0, err
	}
	result, err = database.ExecContext(ctx, ready, args...)
	if err != nil {
		return 0, err
	}
	current, _ := result.RowsAffected()
	return affected + current, nil
}
