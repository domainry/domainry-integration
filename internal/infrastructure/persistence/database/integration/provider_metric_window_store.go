package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

const (
	providerMetricWindowTable = "_integration_provider_metric_windows"
	googleHTTP429MetricKey    = "google_http_429"
	providerMetricWindowSize  = 5 * time.Minute
	providerMetricRetention   = 7 * 24 * time.Hour
	providerMetricShards      = 64
)

// The task failure update and this increment share one transaction, so a
// competing or stale worker cannot create a metric event without owning the
// state transition. Shards keep simultaneous 429 failures off a single row.
func recordGoogleHTTP429(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, taskID string, now time.Time) error {
	window := now.UTC().Truncate(providerMetricWindowSize).UnixMilli()
	hash := sha256.Sum256([]byte(taskID))
	shard := int64(hash[0] % providerMetricShards)
	id := ownerID("provider_metric_", "_platform", fmt.Sprintf("%s:%d:%d", googleHTTP429MetricKey, window, shard))
	insert, args, err := query.NewInsertBuilder(dialect, providerMetricWindowTable).
		Columns("id", "workspace_id", "metric_key", "shard", "window_started_at", "event_count", "updated_at").
		Values(id, "_platform", googleHTTP429MetricKey, shard, window, int64(0), now.UTC().UnixMilli()).
		OnConflictDoNothing("metric_key", "window_started_at", "shard").Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, insert, args...); err != nil {
		return err
	}
	update, args, err := query.NewUpdateBuilder(dialect, providerMetricWindowTable).
		SetExpression("event_count", query.Add(query.Column("event_count"), query.Value(int64(1)))).
		Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(query.Equal("metric_key", googleHTTP429MetricKey), query.Equal("window_started_at", window), query.Equal("shard", shard))).Build()
	if err != nil {
		return err
	}
	result, err := database.ExecContext(ctx, update, args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("Integration provider metric window was not updated: affected=%d", affected)
	}
	return nil
}

func readGoogleHTTP429Events(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	window := now.UTC().Truncate(providerMetricWindowSize).UnixMilli()
	first := window - 11*providerMetricWindowSize.Milliseconds()
	statement, args, err := query.NewSelectBuilder(dialect, providerMetricWindowTable).
		Projections(
			query.Project(query.Coalesce(query.Sum(query.CaseWhen(query.Equal("window_started_at", window), query.Column("event_count")).Else(int64(0))), query.Value(int64(0)))),
			query.Project(query.Coalesce(query.Sum(query.Column("event_count")), query.Value(int64(0)))),
		).
		Where(query.And(query.Equal("metric_key", googleHTTP429MetricKey), query.GreaterThanOrEqual("window_started_at", first), query.LessThanOrEqual("window_started_at", window))).Build()
	if err != nil {
		return 0, 0, err
	}
	err = database.QueryRowContext(ctx, statement, args...).Scan(&current, &lastTwelve)
	return current, lastTwelve, err
}

func cleanupProviderMetricWindows(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) error {
	cutoff := now.UTC().Add(-providerMetricRetention).Truncate(providerMetricWindowSize).UnixMilli()
	statement, args, err := query.NewDeleteBuilder(dialect, providerMetricWindowTable).
		Where(query.And(query.Equal("metric_key", googleHTTP429MetricKey), query.LessThan("window_started_at", cutoff))).Build()
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, statement, args...)
	return err
}
