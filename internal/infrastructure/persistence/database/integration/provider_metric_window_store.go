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
	providerMetricWindowTable                      = "_integration_provider_metric_windows"
	googleHTTP429MetricKey                         = "google_http_429"
	googleHTTP429ForegroundMetricKey               = "google_http_429_foreground"
	googleGmailProjectRateLimitMetricKey           = "google_gmail_project_rate_limit"
	googleGmailProjectRateLimitForegroundMetricKey = "google_gmail_project_rate_limit_foreground"
	googleGmailUserRateLimitMetricKey              = "google_gmail_user_rate_limit"
	googleGmailUserRateLimitForegroundMetricKey    = "google_gmail_user_rate_limit_foreground"
	feishuRateLimitMetricKey                       = "feishu_rate_limit"
	feishuRateLimitForegroundMetricKey             = "feishu_rate_limit_foreground"
	googleGmailHistoryMessagesWithoutPushMetricKey = "google_gmail_history_messages_without_push"
	googleGmailHistoryCursorExpiredMetricKey       = "google_gmail_history_cursor_expired"
	providerMetricWindowSize                       = 5 * time.Minute
	providerMetricRetention                        = 7 * 24 * time.Hour
	providerMetricShards                           = 64
)

// The task failure update and this increment share one transaction, so a
// competing or stale worker cannot create a metric event without owning the
// state transition. Shards keep simultaneous 429 failures off a single row.
func recordGoogleHTTP429(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, taskID string, now time.Time) error {
	return recordProviderMetricEvent(ctx, database, dialect, googleHTTP429MetricKey, taskID, now)
}

func recordGoogleHTTP429Foreground(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, invocationID string, now time.Time) error {
	return recordProviderMetricEvent(ctx, database, dialect, googleHTTP429ForegroundMetricKey, invocationID, now)
}

func isGoogleGmailRateLimitCode(code string) bool {
	_, ok := googleGmailRateLimitMetricKey(code, false)
	return ok
}

func googleGmailRateLimitMetricKey(code string, foreground bool) (string, bool) {
	switch code {
	case "google.http_403.rate_limit_exceeded":
		if foreground {
			return googleGmailProjectRateLimitForegroundMetricKey, true
		}
		return googleGmailProjectRateLimitMetricKey, true
	case "google.http_403.user_rate_limit_exceeded":
		if foreground {
			return googleGmailUserRateLimitForegroundMetricKey, true
		}
		return googleGmailUserRateLimitMetricKey, true
	default:
		return "", false
	}
}

func recordGoogleGmailRateLimit(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, code, eventID string, foreground bool, now time.Time) error {
	metricKey, ok := googleGmailRateLimitMetricKey(code, foreground)
	if !ok {
		return fmt.Errorf("unsupported Google Gmail rate-limit code %q", code)
	}
	return recordProviderMetricEvent(ctx, database, dialect, metricKey, eventID, now)
}

func isFeishuRateLimitCode(code string) bool {
	switch code {
	case "feishu.rate_limit_exceeded", "feishu_calendar.rate_limit_exceeded", "feishu_calendar.oauth.rate_limit_exceeded":
		return true
	default:
		return false
	}
}

func recordFeishuRateLimit(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, eventID string, foreground bool, now time.Time) error {
	metricKey := feishuRateLimitMetricKey
	if foreground {
		metricKey = feishuRateLimitForegroundMetricKey
	}
	return recordProviderMetricEvent(ctx, database, dialect, metricKey, eventID, now)
}

func recordProviderMetricEvent(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, metricKey, eventID string, now time.Time) error {
	return recordProviderMetricValue(ctx, database, dialect, metricKey, eventID, 1, now)
}

func recordProviderMetricValue(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, metricKey, eventID string, value int64, now time.Time) error {
	if value <= 0 {
		return fmt.Errorf("Integration provider metric value must be positive")
	}
	window := now.UTC().Truncate(providerMetricWindowSize).UnixMilli()
	hash := sha256.Sum256([]byte(eventID))
	shard := int64(hash[0] % providerMetricShards)
	insert, args, err := query.NewInsertBuilder(dialect, providerMetricWindowTable).
		Columns("workspace_id", "metric_key", "shard", "window_started_at", "event_count", "updated_at").
		Values("_platform", metricKey, shard, window, int64(0), now.UTC().UnixMilli()).
		OnConflictDoNothing("metric_key", "window_started_at", "shard").Build()
	if err != nil {
		return err
	}
	if _, err := database.ExecContext(ctx, insert, args...); err != nil {
		return err
	}
	update, args, err := query.NewUpdateBuilder(dialect, providerMetricWindowTable).
		SetExpression("event_count", query.Add(query.Column("event_count"), query.Value(value))).
		Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(query.Equal("metric_key", metricKey), query.Equal("window_started_at", window), query.Equal("shard", shard))).Build()
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
	return readProviderMetricEvents(ctx, database, dialect, googleHTTP429MetricKey, now)
}

func readGoogleHTTP429ForegroundEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, googleHTTP429ForegroundMetricKey, now)
}

func readGoogleGmailProjectRateLimitEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, googleGmailProjectRateLimitMetricKey, now)
}

func readGoogleGmailProjectRateLimitForegroundEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, googleGmailProjectRateLimitForegroundMetricKey, now)
}

func readGoogleGmailUserRateLimitEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, googleGmailUserRateLimitMetricKey, now)
}

func readGoogleGmailUserRateLimitForegroundEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, googleGmailUserRateLimitForegroundMetricKey, now)
}

func readFeishuRateLimitEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, feishuRateLimitMetricKey, now)
}

func readFeishuRateLimitForegroundEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, feishuRateLimitForegroundMetricKey, now)
}

func readGoogleGmailHistoryMessagesWithoutPush(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, googleGmailHistoryMessagesWithoutPushMetricKey, now)
}

func readGoogleGmailHistoryCursorExpired(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) (current, lastTwelve int64, err error) {
	return readProviderMetricEvents(ctx, database, dialect, googleGmailHistoryCursorExpiredMetricKey, now)
}

func readProviderMetricEvents(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, metricKey string, now time.Time) (current, lastTwelve int64, err error) {
	window := now.UTC().Truncate(providerMetricWindowSize).UnixMilli()
	first := window - 11*providerMetricWindowSize.Milliseconds()
	statement, args, err := query.NewSelectBuilder(dialect, providerMetricWindowTable).
		Projections(
			query.Project(query.Coalesce(query.Sum(query.CaseWhen(query.Equal("window_started_at", window), query.Column("event_count")).Else(sqlIntegerValue(dialect, 0))), query.Value(int64(0)))),
			query.Project(query.Coalesce(query.Sum(query.Column("event_count")), query.Value(int64(0)))),
		).
		Where(query.And(query.Equal("metric_key", metricKey), query.GreaterThanOrEqual("window_started_at", first), query.LessThanOrEqual("window_started_at", window))).Build()
	if err != nil {
		return 0, 0, err
	}
	err = database.QueryRowContext(ctx, statement, args...).Scan(&current, &lastTwelve)
	return current, lastTwelve, err
}

func cleanupProviderMetricWindows(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, now time.Time) error {
	cutoff := now.UTC().Add(-providerMetricRetention).Truncate(providerMetricWindowSize).UnixMilli()
	statement, args, err := query.NewDeleteBuilder(dialect, providerMetricWindowTable).
		Where(query.LessThan("window_started_at", cutoff)).Build()
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, statement, args...)
	return err
}
