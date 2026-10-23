package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

const (
	providerConnectionLeaseTable = "_integration_provider_connection_leases"
	providerRateLimitTable       = "_integration_provider_rate_limits"
	providerTaskLeaseDuration    = 6 * time.Minute

	providerGlobalRatePerMinute = int64(20000)
	// The provider ceiling is an Integration scheduler admission limit, not a
	// claim about a vendor's official quota. Matching the global ceiling keeps
	// the 100k-account baseline from being rejected locally; provider-specific
	// quota telemetry and adaptive admission remain separate operational work.
	providerKindRatePerMinute       = int64(20000)
	providerWorkspaceRatePerMinute  = int64(1000)
	providerConnectionRatePerMinute = int64(120)
)

type providerRateScope struct {
	workspaceID string
	kind        string
	key         string
	limit       int64
}

func providerTaskRateScopes(candidate providerTaskCandidate) []providerRateScope {
	return []providerRateScope{
		{workspaceID: "_platform", kind: "global", key: "all", limit: providerGlobalRatePerMinute},
		{workspaceID: "_platform", kind: "provider", key: ownerID("rate_key_", candidate.connectorKey, candidate.providerKey), limit: providerKindRatePerMinute},
		{workspaceID: candidate.workspaceID, kind: "workspace", key: ownerID("rate_key_", candidate.workspaceID, "workspace"), limit: providerWorkspaceRatePerMinute},
		{workspaceID: candidate.workspaceID, kind: "connection", key: ownerID("rate_key_", candidate.workspaceID, candidate.connectionKey), limit: providerConnectionRatePerMinute},
	}
}

func consumeProviderRate(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, scope providerRateScope, now time.Time) (bool, error) {
	window := now.UTC().Truncate(time.Minute).UnixMilli()
	insert, args, err := query.NewInsertBuilder(dialect, providerRateLimitTable).
		Columns("workspace_id", "scope_kind", "scope_key", "window_started_at", "used_count", "updated_at").
		Values(scope.workspaceID, scope.kind, scope.key, window, 0, now.UTC().UnixMilli()).
		OnConflictDoNothing("scope_kind", "scope_key").Build()
	if err != nil {
		return false, err
	}
	if _, err = database.ExecContext(ctx, insert, args...); err != nil {
		return false, err
	}
	expired := query.LessThan("window_started_at", window)
	update, args, err := query.NewUpdateBuilder(dialect, providerRateLimitTable).
		SetExpression("window_started_at", query.CaseWhen(expired, sqlIntegerValue(dialect, window)).Else(query.Column("window_started_at"))).
		SetExpression("used_count", query.CaseWhen(expired, sqlIntegerValue(dialect, 1)).Else(query.Add(query.Column("used_count"), query.Value(1)))).
		Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(query.Equal("scope_kind", scope.kind), query.Equal("scope_key", scope.key), query.Or(expired, query.LessThan("used_count", scope.limit)))).Build()
	if err != nil {
		return false, err
	}
	result, err := database.ExecContext(ctx, update, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func acquireProviderConnectionLease(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, candidate providerTaskCandidate, leaseOwner string, now time.Time) (bool, error) {
	insert, args, err := query.NewInsertBuilder(dialect, providerConnectionLeaseTable).
		Columns("workspace_id", "connection_key", "lease_owner", "lease_expires_at", "fencing_token", "updated_at").
		Values(candidate.workspaceID, candidate.connectionKey, "", int64(0), int64(0), now.UTC().UnixMilli()).
		OnConflictDoNothing("workspace_id", "connection_key").Build()
	if err != nil {
		return false, err
	}
	if _, err = database.ExecContext(ctx, insert, args...); err != nil {
		return false, err
	}
	claim, args, err := query.NewUpdateBuilder(dialect, providerConnectionLeaseTable).
		Set("lease_owner", leaseOwner).Set("lease_expires_at", now.Add(providerTaskLeaseDuration).UnixMilli()).
		SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(query.Equal("workspace_id", candidate.workspaceID), query.Equal("connection_key", candidate.connectionKey), query.Or(query.Equal("lease_owner", ""), query.LessThanOrEqual("lease_expires_at", now.UTC().UnixMilli())))).Build()
	if err != nil {
		return false, err
	}
	result, err := database.ExecContext(ctx, claim, args...)
	if err != nil {
		return false, err
	}
	affected, _ := result.RowsAffected()
	return affected == 1, nil
}

func (s *WorkerStore) acquireProviderTask(ctx context.Context, candidate providerTaskCandidate, now time.Time) (string, bool, error) {
	tx, err := s.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Rollback() }()
	for _, scope := range providerTaskRateScopes(candidate) {
		allowed, rateErr := consumeProviderRate(ctx, tx, s.dialect, scope, now)
		if rateErr != nil {
			return "", false, rateErr
		}
		if !allowed {
			return "", false, nil
		}
	}
	leaseOwner := ownerID("provider_lease_owner_", s.workerID, strconv.FormatInt(candidate.id, 10))
	acquired, err := acquireProviderConnectionLease(ctx, tx, s.dialect, candidate, leaseOwner, now)
	if err != nil || !acquired {
		return "", false, err
	}
	claimed, err := s.claimProviderTaskWith(ctx, tx, candidate, now)
	if err != nil || !claimed {
		return "", false, err
	}
	if err = tx.Commit(); err != nil {
		return "", false, err
	}
	return leaseOwner, true, nil
}

func (s *WorkerStore) releaseProviderConnectionLease(ctx context.Context, candidate providerTaskCandidate, leaseOwner string, now time.Time) error {
	statement, args, err := query.NewUpdateBuilder(s.dialect, providerConnectionLeaseTable).
		Set("lease_owner", "").Set("lease_expires_at", int64(0)).Set("updated_at", now.UTC().UnixMilli()).
		Where(query.And(query.Equal("workspace_id", candidate.workspaceID), query.Equal("connection_key", candidate.connectionKey), query.Equal("lease_owner", leaseOwner))).Build()
	if err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("Integration Provider connection lease %q was lost", candidate.connectionKey)
	}
	return nil
}
