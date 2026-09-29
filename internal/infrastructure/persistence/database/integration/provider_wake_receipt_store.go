package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

const providerWakeReceiptTable = "_integration_provider_wake_receipts"

func recordProviderWakeReceipts(
	ctx context.Context,
	database sqlhost.DBTX,
	dialect modulehost.Dialect,
	workspaceID, connectionKey, connectorKey, providerKey, inboxID string,
	publishedAt int64,
	receivedAt time.Time,
	sequences map[string]int64,
) error {
	for taskKey, sequence := range sequences {
		digest := sha256.Sum256([]byte(workspaceID + "\x00" + inboxID + "\x00" + connectionKey + "\x00" + taskKey))
		id := "provider_wake_" + hex.EncodeToString(digest[:16])
		statement, args, err := query.NewInsertBuilder(dialect, providerWakeReceiptTable).
			Columns("id", "workspace_id", "inbox_id", "connector_key", "provider_key", "connection_key", "task_key", "wake_sequence", "status", "published_at", "received_at", "completed_at", "created_at", "updated_at").
			Values(id, workspaceID, inboxID, connectorKey, providerKey, connectionKey, taskKey, sequence, "pending", publishedAt, receivedAt.UTC().UnixMilli(), int64(0), receivedAt.UTC().UnixMilli(), receivedAt.UTC().UnixMilli()).
			OnConflictDoNothing("workspace_id", "inbox_id", "connection_key", "task_key").Build()
		if err != nil {
			return err
		}
		if _, err := database.ExecContext(ctx, statement, args...); err != nil {
			return fmt.Errorf("record Integration Provider wake receipt: %w", err)
		}
	}
	return nil
}

func completeProviderWakeReceipts(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, candidate providerTaskCandidate, now time.Time) error {
	statement, args, err := query.NewUpdateBuilder(dialect, providerWakeReceiptTable).
		Set("status", "completed").Set("completed_at", now.UTC().UnixMilli()).Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(
			query.Equal("workspace_id", candidate.workspaceID), query.Equal("connection_key", candidate.connectionKey), query.Equal("task_key", candidate.taskKey),
			query.Equal("status", "pending"), query.LessThanOrEqual("wake_sequence", candidate.wakeSequence),
		)).Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("complete Integration Provider wake receipts: %w", err)
	}
	return nil
}

func cleanupProviderWakeReceipts(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) error {
	cutoff := now.UTC().Add(-providerMetricRetention).UnixMilli()
	statement, args, err := query.NewDeleteBuilder(dialect, providerWakeReceiptTable).Where(query.LessThan("received_at", cutoff)).Build()
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, statement, args...)
	return err
}

func deleteProviderWakeReceipts(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, workspaceID, connectionKey string) error {
	statement, args, err := query.NewDeleteBuilder(dialect, providerWakeReceiptTable).Where(query.And(
		query.Equal("workspace_id", workspaceID), query.Equal("connection_key", connectionKey),
	)).Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("delete Integration Provider wake receipts: %w", err)
	}
	return nil
}
