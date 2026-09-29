package integration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

var _ integrationsdk.ProviderRunMonitoring = (*ManagementStore)(nil)

func (s *ManagementStore) ProviderRunSnapshot(ctx context.Context) (integrationsdk.ProviderRunSnapshot, error) {
	return s.providerRunSnapshot(ctx, time.Now().UTC())
}

func (s *ManagementStore) providerRunSnapshot(ctx context.Context, now time.Time) (integrationsdk.ProviderRunSnapshot, error) {
	now = now.UTC()
	result := integrationsdk.ProviderRunSnapshot{
		ObservedAt: now.Format(time.RFC3339Nano), GlobalRateLimit: providerGlobalRatePerMinute,
		GoogleOfficialQuota: integrationsdk.ProviderQuotaSnapshot{
			Source: "google_cloud_monitoring", Status: integrationsdk.ProviderQuotaStatusNotConfigured, ObservedAt: now.Format(time.RFC3339Nano),
		},
	}
	if s.providerQuota != nil {
		quota, observeErr := s.providerQuota.ObserveProviderQuota(ctx, now)
		if observeErr != nil {
			result.GoogleOfficialQuota = integrationsdk.ProviderQuotaSnapshot{
				Source: "google_cloud_monitoring", Status: integrationsdk.ProviderQuotaStatusUnavailable,
				ObservedAt: now.Format(time.RFC3339Nano), ErrorCode: "google.monitoring_unavailable",
			}
		} else {
			result.GoogleOfficialQuota = quota
		}
	}
	var oldest int64
	for _, group := range []struct {
		status string
		due    bool
		count  *int64
	}{
		{status: "ready", due: true, count: &result.ReadyDue},
		{status: "failed", due: false, count: &result.Failed},
		{status: "failed", due: true, count: &result.FailedDue},
		{status: "processing", due: false, count: &result.Processing},
		{status: "processing", due: true, count: &result.ExpiredProcessing},
		{status: "dead_letter", due: false, count: &result.DeadLetter},
	} {
		predicate := query.And(query.Equal("run_kind", providerRunKindState), query.Equal("status", group.status))
		if group.due {
			predicate = query.And(predicate, query.LessThanOrEqual("due_at", now.UnixMilli()), query.Or(query.Equal("lease_expires_at", int64(0)), query.LessThanOrEqual("lease_expires_at", now.UnixMilli())))
		}
		statement, args, err := query.NewSelectBuilder(s.dialect, providerRunTable).
			Projections(query.Project(query.CountAll()), query.Project(query.Min(query.Column("due_at")))).Where(predicate).Build()
		if err != nil {
			return integrationsdk.ProviderRunSnapshot{}, err
		}
		var firstDue sql.NullInt64
		if err := s.database.QueryRowContext(ctx, statement, args...).Scan(group.count, &firstDue); err != nil {
			return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration provider run %s: %w", group.status, err)
		}
		if group.due && firstDue.Valid && (oldest == 0 || firstDue.Int64 < oldest) {
			oldest = firstDue.Int64
		}
	}
	if oldest > 0 {
		result.OldestRunnableDueAt = time.UnixMilli(oldest).UTC().Format(time.RFC3339Nano)
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, providerRunTable).
		Projections(query.Project(query.CountAll())).Where(query.And(
		query.Equal("run_kind", providerRunKindState), query.In("status", "failed", "dead_letter"), query.Equal("last_error_code", "google.http_429"),
	)).Build()
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, err
	}
	if err := s.database.QueryRowContext(ctx, statement, args...).Scan(&result.GoogleHTTP429Failures); err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Google HTTP 429 failures: %w", err)
	}
	current429, hour429, err := readGoogleHTTP429Events(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Google HTTP 429 event windows: %w", err)
	}
	result.GoogleHTTP429Events5m = current429
	result.GoogleHTTP429EventsHour = hour429
	result.GoogleHTTP429WindowStart = now.Truncate(providerMetricWindowSize).Format(time.RFC3339Nano)
	foregroundCurrent429, foregroundHour429, err := readGoogleHTTP429ForegroundEvents(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration foreground Google HTTP 429 event windows: %w", err)
	}
	result.GoogleHTTP429ForegroundEvents5m = foregroundCurrent429
	result.GoogleHTTP429ForegroundEventsHour = foregroundHour429
	for _, group := range []struct {
		code  string
		count *int64
	}{
		{code: "google.http_403.rate_limit_exceeded", count: &result.GoogleGmailProjectRateLimitFailures},
		{code: "google.http_403.user_rate_limit_exceeded", count: &result.GoogleGmailUserRateLimitFailures},
	} {
		statement, args, err = query.NewSelectBuilder(s.dialect, providerRunTable).
			Projections(query.Project(query.CountAll())).Where(query.And(
			query.Equal("run_kind", providerRunKindState), query.In("status", "failed", "dead_letter"), query.Equal("last_error_code", group.code),
		)).Build()
		if err != nil {
			return integrationsdk.ProviderRunSnapshot{}, err
		}
		if err := s.database.QueryRowContext(ctx, statement, args...).Scan(group.count); err != nil {
			return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Google Gmail rate-limit failures: %w", err)
		}
	}
	result.GoogleGmailProjectRateLimitEvents5m, result.GoogleGmailProjectRateLimitEventsHour, err = readGoogleGmailProjectRateLimitEvents(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Google Gmail project rate-limit event windows: %w", err)
	}
	result.GoogleGmailProjectRateLimitForegroundEvents5m, result.GoogleGmailProjectRateLimitForegroundEventsHour, err = readGoogleGmailProjectRateLimitForegroundEvents(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration foreground Google Gmail project rate-limit event windows: %w", err)
	}
	result.GoogleGmailUserRateLimitEvents5m, result.GoogleGmailUserRateLimitEventsHour, err = readGoogleGmailUserRateLimitEvents(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Google Gmail user rate-limit event windows: %w", err)
	}
	result.GoogleGmailUserRateLimitForegroundEvents5m, result.GoogleGmailUserRateLimitForegroundEventsHour, err = readGoogleGmailUserRateLimitForegroundEvents(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration foreground Google Gmail user rate-limit event windows: %w", err)
	}
	result.GoogleGmailRateLimitWindowStart = now.Truncate(providerMetricWindowSize).Format(time.RFC3339Nano)
	statement, args, err = query.NewSelectBuilder(s.dialect, providerRunTable).
		Projections(query.Project(query.CountAll())).Where(query.And(
		query.Equal("run_kind", providerRunKindState), query.In("status", "failed", "dead_letter"),
		query.Or(
			query.Equal("last_error_code", "feishu.rate_limit_exceeded"),
			query.Equal("last_error_code", "feishu_calendar.rate_limit_exceeded"),
			query.Equal("last_error_code", "feishu_calendar.oauth.rate_limit_exceeded"),
		),
	)).Build()
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, err
	}
	if err := s.database.QueryRowContext(ctx, statement, args...).Scan(&result.FeishuRateLimitFailures); err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Feishu rate-limit failures: %w", err)
	}
	result.FeishuRateLimitEvents5m, result.FeishuRateLimitEventsHour, err = readFeishuRateLimitEvents(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Feishu rate-limit event windows: %w", err)
	}
	result.FeishuRateLimitForegroundEvents5m, result.FeishuRateLimitForegroundEventsHour, err = readFeishuRateLimitForegroundEvents(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration foreground Feishu rate-limit event windows: %w", err)
	}
	result.FeishuRateLimitWindowStart = now.Truncate(providerMetricWindowSize).Format(time.RFC3339Nano)
	_, result.GoogleGmailHistoryMessagesWithoutPushHour, err = readGoogleGmailHistoryMessagesWithoutPush(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Google Gmail history messages without Push: %w", err)
	}
	_, result.GoogleGmailHistoryCursorExpiredHour, err = readGoogleGmailHistoryCursorExpired(ctx, s.database, s.dialect, now)
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Google Gmail expired history cursors: %w", err)
	}
	if err := observeGooglePush(ctx, s.database, s.dialect, now, &result); err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Google Push notifications: %w", err)
	}
	if err := observeGoogleSyncCompletion(ctx, s.database, s.dialect, now, &result); err != nil {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration Google synchronization completion: %w", err)
	}
	statement, args, err = query.NewSelectBuilder(s.dialect, providerRateLimitTable).
		Columns("used_count", "window_started_at").Where(query.And(query.Equal("scope_kind", "global"), query.Equal("scope_key", "all"))).Build()
	if err != nil {
		return integrationsdk.ProviderRunSnapshot{}, err
	}
	var used, window int64
	if err := s.database.QueryRowContext(ctx, statement, args...).Scan(&used, &window); err != nil && err != sql.ErrNoRows {
		return integrationsdk.ProviderRunSnapshot{}, fmt.Errorf("observe Integration provider global rate window: %w", err)
	}
	if window == now.Truncate(time.Minute).UnixMilli() {
		result.GlobalRateUsed = used
		result.RateWindowStartedAt = time.UnixMilli(window).UTC().Format(time.RFC3339Nano)
	}
	return result, nil
}
