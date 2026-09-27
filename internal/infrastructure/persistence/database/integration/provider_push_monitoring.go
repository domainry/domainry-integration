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

const googlePushDelayThreshold = 5 * time.Minute

// observeGooglePush aggregates only deduplicated, verified Google Pub/Sub
// receipts. A notification with no task wake is not evidence that Gmail failed
// to notify us.
func observeGooglePush(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time, snapshot *integrationsdk.ProviderRunSnapshot) error {
	predicate := query.And(
		query.Equal("connector_key", "google_workspace"),
		query.Equal("provider_key", "google"),
		query.In("status", "routed", "unmatched"),
		query.GreaterThanOrEqual("received_at", now.Add(-time.Hour).UnixMilli()),
		query.LessThanOrEqual("received_at", now.UnixMilli()),
	)
	delay := query.Subtract(query.Column("received_at"), query.Column("published_at"))
	timed := query.And(query.GreaterThan("published_at", int64(0)), query.LessThanOrEqualExpressions(query.Column("published_at"), query.Column("received_at")))
	statement, args, err := query.NewSelectBuilder(dialect, providerInboxTable).Projections(
		query.Project(query.CountAll()),
		query.Project(query.Coalesce(query.Sum(query.CaseWhen(query.Equal("status", "unmatched"), int64(1)).Else(int64(0))), query.Value(int64(0)))),
		query.Project(query.Coalesce(query.Sum(query.CaseWhen(timed, int64(1)).Else(int64(0))), query.Value(int64(0)))),
		query.Project(query.Coalesce(query.Sum(query.CaseWhen(query.And(timed, query.GreaterThanOrEqualValue(delay, googlePushDelayThreshold.Milliseconds())), int64(1)).Else(int64(0))), query.Value(int64(0)))),
		query.Project(query.Max(query.CaseWhen(timed, delay))),
	).Where(predicate).Build()
	if err != nil {
		return err
	}
	var maxDelay sql.NullInt64
	if err := database.QueryRowContext(ctx, statement, args...).Scan(
		&snapshot.GooglePushReceivedHour,
		&snapshot.GooglePushNoWakeHour,
		&snapshot.GooglePushTimedHour,
		&snapshot.GooglePushDelayed5mHour,
		&maxDelay,
	); err != nil {
		return fmt.Errorf("query Google Push inbox latency: %w", err)
	}
	if maxDelay.Valid {
		snapshot.GooglePushMaxDelayMsHour = maxDelay.Int64
	}
	if snapshot.GooglePushReceivedHour == 0 {
		return nil
	}
	statement, args, err = query.NewSelectBuilder(dialect, providerInboxTable).
		Projections(query.Project(query.Column("route_bucket")), query.Project(query.CountAll())).
		Where(predicate).
		GroupBy(query.Column("route_bucket")).
		OrderBy(query.DescendingExpression(query.CountAll()), query.Ascending("route_bucket")).
		Limit(1).Build()
	if err != nil {
		return err
	}
	var bucket int
	if err := database.QueryRowContext(ctx, statement, args...).Scan(&bucket, &snapshot.GooglePushHotBucketEventsHour); err != nil {
		return fmt.Errorf("query Google Push route bucket hotspot: %w", err)
	}
	snapshot.GooglePushHotBucket = &bucket
	return nil
}
