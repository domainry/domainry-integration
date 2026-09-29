package integration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

const googleSyncDelayThreshold = 5 * time.Minute

func observeGoogleSyncCompletion(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time, snapshot *integrationsdk.ProviderRunSnapshot) error {
	completedPredicate := query.And(
		query.Equal("connector_key", "google_workspace"), query.Equal("provider_key", "google"), query.Equal("status", "completed"),
		query.GreaterThanOrEqual("completed_at", now.Add(-time.Hour).UnixMilli()), query.LessThanOrEqual("completed_at", now.UnixMilli()),
	)
	delay := query.Subtract(query.Column("completed_at"), query.Column("published_at"))
	timed := query.And(query.GreaterThan("published_at", int64(0)), query.LessThanOrEqualExpressions(query.Column("published_at"), query.Column("completed_at")))
	statement, args, err := query.NewSelectBuilder(dialect, providerWakeReceiptTable).Projections(
		query.Project(query.CountAll()),
		query.Project(query.Coalesce(query.Sum(query.CaseWhen(timed, int64(1)).Else(int64(0))), query.Value(int64(0)))),
		query.Project(query.Coalesce(query.Sum(query.CaseWhen(query.And(timed, query.GreaterThanOrEqualValue(delay, googleSyncDelayThreshold.Milliseconds())), int64(1)).Else(int64(0))), query.Value(int64(0)))),
		query.Project(query.Max(query.CaseWhen(timed, delay))),
	).Where(completedPredicate).Build()
	if err != nil {
		return err
	}
	var maximum sql.NullInt64
	if err := database.QueryRowContext(ctx, statement, args...).Scan(
		&snapshot.GoogleSyncCompletedHour,
		&snapshot.GoogleSyncTimedHour,
		&snapshot.GoogleSyncDelayed5mHour,
		&maximum,
	); err != nil {
		return fmt.Errorf("query Google synchronization completion latency: %w", err)
	}
	if maximum.Valid {
		snapshot.GoogleSyncMaxEndToEndDelayMsHour = maximum.Int64
	}

	pendingPredicate := query.And(
		query.Equal("connector_key", "google_workspace"), query.Equal("provider_key", "google"), query.Equal("status", "pending"),
		query.LessThanOrEqual("received_at", now.UnixMilli()),
	)
	statement, args, err = query.NewSelectBuilder(dialect, providerWakeReceiptTable).Projections(
		query.Project(query.CountAll()),
		query.Project(query.Coalesce(query.Sum(query.CaseWhen(query.LessThanOrEqual("received_at", now.Add(-googleSyncDelayThreshold).UnixMilli()), int64(1)).Else(int64(0))), query.Value(int64(0)))),
		query.Project(query.Min(query.Column("received_at"))),
	).Where(pendingPredicate).Build()
	if err != nil {
		return err
	}
	var oldest sql.NullInt64
	if err := database.QueryRowContext(ctx, statement, args...).Scan(&snapshot.GoogleSyncPending, &snapshot.GoogleSyncPendingOver5m, &oldest); err != nil {
		return fmt.Errorf("query pending Google synchronization receipts: %w", err)
	}
	if oldest.Valid {
		snapshot.GoogleSyncOldestPendingAgeMs = now.UnixMilli() - oldest.Int64
	}
	return nil
}
