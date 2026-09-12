package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

var (
	errConnectionAccountNotFound = errors.New("Integration connection account was not found")
	errConnectionAccountChanged  = errors.New("Integration connection account changed")
)

func normalizedConnectionAccountSubject(subject integrationsdk.ConnectionAccountSubject) (integrationsdk.ConnectionAccountSubject, error) {
	subject.WorkspaceID, subject.UserID = strings.TrimSpace(subject.WorkspaceID), strings.TrimSpace(subject.UserID)
	return subject, subject.Validate()
}

func requireConnectionAccountSubjectScope(ctx context.Context, subject integrationsdk.ConnectionAccountSubject) error {
	if scope, scoped := integrationmodel.AccessScopeFromContext(ctx); scoped {
		if !scope.Valid() || scope.WorkspaceID != subject.WorkspaceID || scope.ActorID != subject.UserID {
			return fmt.Errorf("Integration connection account subject does not match the authorized principal")
		}
		personal, workspace := scope.ConnectionAccountAccess()
		if subject.Access.Personal && !personal || subject.Access.Workspace && !workspace {
			return fmt.Errorf("Integration connection account access exceeds the authorized scope")
		}
	}
	return nil
}

func connectionAccountProjection() []query.Projection {
	return []query.Projection{
		query.Project(query.QualifiedColumn("c", "connection_key")),
		query.Project(query.QualifiedColumn("c", "workspace_id")),
		query.Project(query.QualifiedColumn("c", "connector_key")),
		query.Project(query.QualifiedColumn("c", "provider_key")),
		query.Project(query.QualifiedColumn("c", "name")),
		query.Project(query.QualifiedColumn("a", "scope")),
		query.Project(query.QualifiedColumn("a", "owner_user_id")),
		query.Project(query.QualifiedColumn("c", "status")),
		query.Project(query.QualifiedColumn("c", "created_at")),
		query.Project(query.QualifiedColumn("c", "updated_at")),
	}
}

func connectionAccountJoin() query.Join {
	return query.InnerJoin("_integration_connections", "c", query.And(
		query.EqualExpressions(query.QualifiedColumn("a", "workspace_id"), query.QualifiedColumn("c", "workspace_id")),
		query.EqualExpressions(query.QualifiedColumn("a", "connection_key"), query.QualifiedColumn("c", "connection_key")),
	))
}

func scanConnectionAccount(row rowScanner) (integrationsdk.ConnectionAccount, error) {
	var value integrationsdk.ConnectionAccount
	var name sql.NullString
	if err := row.Scan(&value.Key, &value.WorkspaceID, &value.ConnectorKey, &value.ProviderKey, &name, &value.Scope, &value.OwnerUserID, &value.Status, &value.CreatedAt, &value.UpdatedAt); err != nil {
		return value, err
	}
	value.Name = name.String
	if !value.Scope.Valid() || value.Scope == integrationsdk.ConnectionAccountScopePersonal && strings.TrimSpace(value.OwnerUserID) == "" || value.Scope == integrationsdk.ConnectionAccountScopeWorkspace && strings.TrimSpace(value.OwnerUserID) != "" {
		return integrationsdk.ConnectionAccount{}, fmt.Errorf("Integration connection account ownership is invalid")
	}
	return value, nil
}

func connectionAccountAccess(subject integrationsdk.ConnectionAccountSubject) query.Predicate {
	personal, workspace := query.AlwaysFalse(), query.AlwaysFalse()
	if subject.Access.Personal {
		personal = query.And(
			query.EqualValue(query.QualifiedColumn("a", "scope"), string(integrationsdk.ConnectionAccountScopePersonal)),
			query.EqualValue(query.QualifiedColumn("a", "owner_user_id"), subject.UserID))
	}
	if subject.Access.Workspace {
		workspace = query.EqualValue(query.QualifiedColumn("a", "scope"), string(integrationsdk.ConnectionAccountScopeWorkspace))
	}
	return query.And(
		query.EqualValue(query.QualifiedColumn("a", "workspace_id"), subject.WorkspaceID),
		query.Or(personal, workspace),
	)
}

func (s *ManagementStore) ListConnectionAccounts(ctx context.Context, subject integrationsdk.ConnectionAccountSubject) ([]integrationsdk.ConnectionAccount, error) {
	var err error
	subject, err = normalizedConnectionAccountSubject(subject)
	if err != nil {
		return nil, err
	}
	if err = requireConnectionAccountSubjectScope(ctx, subject); err != nil {
		return nil, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connection_accounts").Alias("a").
		Projections(connectionAccountProjection()...).Join(connectionAccountJoin()).Where(connectionAccountAccess(subject)).
		OrderBy(query.AscendingExpression(query.QualifiedColumn("c", "connection_key"))).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration connection accounts: %w", err)
	}
	defer rows.Close()
	values := []integrationsdk.ConnectionAccount{}
	for rows.Next() {
		value, scanErr := scanConnectionAccount(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		values = append(values, value)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	// Release the cursor before owner-side credential queries (one-connection hosts).
	for i := range values {
		if err = s.enrichAccountReadiness(ctx, &values[i]); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (s *ManagementStore) connectionAccount(ctx context.Context, workspaceID, key string, access query.Predicate) (integrationsdk.ConnectionAccount, error) {
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	key, err = requiredOwnerValue("connection account key", key)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	predicates := []query.Predicate{
		query.EqualValue(query.QualifiedColumn("a", "workspace_id"), workspaceID),
		query.EqualValue(query.QualifiedColumn("a", "connection_key"), key),
	}
	if access != nil {
		predicates = append(predicates, access)
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connection_accounts").Alias("a").
		Projections(connectionAccountProjection()...).Join(connectionAccountJoin()).Where(query.And(predicates...)).Limit(1).Build()
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	value, err := scanConnectionAccount(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return integrationsdk.ConnectionAccount{}, errConnectionAccountNotFound
	}
	return value, err
}

func (s *ManagementStore) GetConnectionAccount(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, key string) (integrationsdk.ConnectionAccount, error) {
	var err error
	subject, err = normalizedConnectionAccountSubject(subject)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	if err = requireConnectionAccountSubjectScope(ctx, subject); err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	value, err := s.connectionAccount(ctx, subject.WorkspaceID, key, connectionAccountAccess(subject))
	if err == nil {
		err = s.enrichAccountReadiness(ctx, &value)
	}
	return value, err
}

func (s *ManagementStore) TestConnectionAccount(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, key string, request integrationsdk.ConnectionTestRequest) (integrationsdk.ConnectionAccountTestResult, error) {
	var normalizeErr error
	subject, normalizeErr = normalizedConnectionAccountSubject(subject)
	if normalizeErr != nil {
		return integrationsdk.ConnectionAccountTestResult{}, normalizeErr
	}
	if operation := strings.TrimSpace(request.Operation); operation != "" && operation != "test_connection" || len(request.Payload) != 0 || len(request.Input) != 0 || request.Confirm {
		return integrationsdk.ConnectionAccountTestResult{}, fmt.Errorf("Integration account test accepts no business operation or input")
	}
	account, err := s.GetConnectionAccount(ctx, subject, key)
	if err != nil {
		return integrationsdk.ConnectionAccountTestResult{}, err
	}
	if account.Status != "active" {
		return integrationsdk.ConnectionAccountTestResult{}, fmt.Errorf("Integration connection account is not active")
	}
	if account.Readiness != nil && account.Readiness.Test != nil && !account.Readiness.Test.Allowed {
		return integrationsdk.ConnectionAccountTestResult{}, fmt.Errorf("Integration connection account test scope is not granted")
	}
	trusted := integrationmodel.WithAccessScope(ctx, integrationmodel.AccessScope{WorkspaceID: subject.WorkspaceID, PermissionKey: "integration.connection_accounts.test", ActorID: subject.UserID, Unrestricted: true})
	result, err := s.TestConnection(trusted, subject.WorkspaceID, account.Key, integrationsdk.ConnectionTestRequest{})
	if err != nil {
		return integrationsdk.ConnectionAccountTestResult{}, fmt.Errorf("Integration connection account test failed")
	}
	current, err := s.GetConnectionAccount(ctx, subject, key)
	if err != nil || current.Status != "active" || current.UpdatedAt != account.UpdatedAt {
		return integrationsdk.ConnectionAccountTestResult{}, errConnectionAccountChanged
	}
	return integrationsdk.ConnectionAccountTestResult{Account: current, Operation: "test_connection", Connected: result.Receipt.Status == integrationsdk.DeliveryStatusSucceeded, Receipt: result.Receipt}, nil
}

func (s *ManagementStore) RegisterConnectionAccount(ctx context.Context, workspaceID, key, actorID string, input integrationsdk.ConnectionAccountRegistration) (integrationsdk.ConnectionAccount, error) {
	workspaceID, key, actorID = strings.TrimSpace(workspaceID), strings.TrimSpace(key), strings.TrimSpace(actorID)
	if err := requireAllDataScope(ctx, workspaceID); err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	if err := input.Validate(); err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	input.OwnerUserID = strings.TrimSpace(input.OwnerUserID)
	actorID, _ = scopeOwner(ctx, actorID)
	if actorID == "" {
		return integrationsdk.ConnectionAccount{}, fmt.Errorf("Integration connection account actor is required")
	}
	if s.transactions != nil {
		var value integrationsdk.ConnectionAccount
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.RegisterConnectionAccount(ctx, workspaceID, key, actorID, input)
			return operationErr
		})
		return value, err
	}
	connection, err := s.GetConnection(ctx, workspaceID, key)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	now := ownerNow()
	where := query.And(query.Equal("workspace_id", workspaceID), query.Equal("connection_key", key))
	lookup, args, err := query.NewSelectBuilder(s.dialect, "_integration_connection_accounts").Columns("id").Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	var id string
	lookupErr := s.database.QueryRowContext(ctx, lookup, args...).Scan(&id)
	switch lookupErr {
	case sql.ErrNoRows:
		id = ownerID("connection_account_", workspaceID, key)
		statement, values, buildErr := query.NewInsertBuilder(s.dialect, "_integration_connection_accounts").Columns("id", "workspace_id", "connection_key", "scope", "owner_user_id", "created_by", "created_at", "updated_at").Values(id, workspaceID, key, input.Scope, input.OwnerUserID, actorID, now, now).Build()
		if buildErr != nil {
			return integrationsdk.ConnectionAccount{}, buildErr
		}
		if _, err = s.database.ExecContext(ctx, statement, values...); err != nil {
			return integrationsdk.ConnectionAccount{}, fmt.Errorf("insert Integration connection account: %w", err)
		}
	case nil:
		existing, readErr := s.connectionAccount(ctx, workspaceID, key, nil)
		if readErr != nil {
			return integrationsdk.ConnectionAccount{}, readErr
		}
		if existing.Scope != input.Scope || existing.OwnerUserID != input.OwnerUserID {
			return integrationsdk.ConnectionAccount{}, fmt.Errorf("Integration connection account ownership is immutable; create a separate connection")
		}
		return existing, nil
	default:
		return integrationsdk.ConnectionAccount{}, lookupErr
	}
	if err = s.syncConnectionAccountSecrets(ctx, connection); err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	return s.connectionAccount(ctx, workspaceID, key, nil)
}

func materialSecretKeys(refs map[string]string) ([]string, error) {
	seen := map[string]bool{}
	values := make([]string, 0, len(refs))
	for field, reference := range refs {
		reference = strings.TrimSpace(reference)
		if !strings.HasPrefix(reference, "secret:") || strings.TrimSpace(strings.TrimPrefix(reference, "secret:")) == "" {
			return nil, fmt.Errorf("Integration connection account secret %q must use managed material", field)
		}
		key := strings.TrimSpace(strings.TrimPrefix(reference, "secret:"))
		if !seen[key] {
			seen[key] = true
			values = append(values, key)
		}
	}
	sort.Strings(values)
	return values, nil
}

func (s *ManagementStore) syncConnectionAccountSecrets(ctx context.Context, connection integrationsdk.Connection) error {
	if err := s.rejectReservedAccountSecrets(ctx, connection); err != nil {
		return err
	}
	accountWhere := query.And(query.Equal("workspace_id", connection.WorkspaceID), query.Equal("connection_key", connection.Key))
	lookup, args, err := query.NewSelectBuilder(s.dialect, "_integration_connection_accounts").Columns("id").Where(accountWhere).Limit(1).Build()
	if err != nil {
		return err
	}
	var accountID string
	if err = s.database.QueryRowContext(ctx, lookup, args...).Scan(&accountID); err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return err
	}
	keys, err := materialSecretKeys(connection.SecretRefs)
	if err != nil {
		return err
	}
	if err = s.rejectExistingCredentialAliases(ctx, connection, keys); err != nil {
		return err
	}
	for _, key := range keys {
		statement, values, buildErr := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("status", "value_ref").Where(query.And(query.Equal("workspace_id", connection.WorkspaceID), query.Equal("secret_key", key))).Limit(1).Build()
		if buildErr != nil {
			return buildErr
		}
		var status string
		var valueRef sql.NullString
		if err = s.database.QueryRowContext(ctx, statement, values...).Scan(&status, &valueRef); err != nil {
			return fmt.Errorf("read Integration connection account secret %q: %w", key, err)
		}
		if status != "active" || valueRef.String != "material:"+key {
			return fmt.Errorf("Integration connection account secret %q is not active managed material", key)
		}
	}
	remove, values, err := query.NewDeleteBuilder(s.dialect, "_integration_connection_account_secrets").Where(accountWhere).Build()
	if err != nil {
		return err
	}
	if _, err = s.database.ExecContext(ctx, remove, values...); err != nil {
		return err
	}
	for _, key := range keys {
		id := ownerID("connection_account_secret_", connection.WorkspaceID, connection.Key+"\x00"+key)
		statement, values, buildErr := query.NewInsertBuilder(s.dialect, "_integration_connection_account_secrets").Columns("id", "workspace_id", "connection_key", "secret_key", "created_at").Values(id, connection.WorkspaceID, connection.Key, key, ownerNow()).Build()
		if buildErr != nil {
			return buildErr
		}
		if _, err = s.database.ExecContext(ctx, statement, values...); err != nil {
			return fmt.Errorf("reserve Integration connection account secret %q: %w", key, err)
		}
	}
	return nil
}

func (s *ManagementStore) RevokeConnectionAccount(ctx context.Context, subject integrationsdk.ConnectionAccountSubject, key, expectedUpdatedAt string) (integrationsdk.ConnectionAccount, error) {
	var err error
	subject, err = normalizedConnectionAccountSubject(subject)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	if err = requireConnectionAccountSubjectScope(ctx, subject); err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	expectedUpdatedAt = strings.TrimSpace(expectedUpdatedAt)
	if expectedUpdatedAt == "" {
		return integrationsdk.ConnectionAccount{}, fmt.Errorf("Integration connection account revision is required")
	}
	if s.transactions != nil {
		var value integrationsdk.ConnectionAccount
		err = s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.RevokeConnectionAccount(ctx, subject, key, expectedUpdatedAt)
			return operationErr
		})
		return value, err
	}
	account, err := s.GetConnectionAccount(ctx, subject, key)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	if account.Status == "revoked" {
		return account, nil
	}
	if account.UpdatedAt != expectedUpdatedAt {
		return integrationsdk.ConnectionAccount{}, errConnectionAccountChanged
	}
	now := ownerNow()
	connectionWhere := query.And(query.Equal("workspace_id", subject.WorkspaceID), query.Equal("connection_key", account.Key), query.Equal("updated_at", expectedUpdatedAt))
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_connections").Set("status", "revoked").Set("updated_at", now).Where(connectionWhere).Build()
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return integrationsdk.ConnectionAccount{}, errConnectionAccountChanged
	}
	secretQuery, secretArgs, err := query.NewSelectBuilder(s.dialect, "_integration_connection_account_secrets").Columns("secret_key").Where(query.And(query.Equal("workspace_id", subject.WorkspaceID), query.Equal("connection_key", account.Key))).OrderBy(query.Ascending("secret_key")).Build()
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	rows, err := s.database.QueryContext(ctx, secretQuery, secretArgs...)
	if err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	var secrets []string
	for rows.Next() {
		var secret string
		if err = rows.Scan(&secret); err != nil {
			_ = rows.Close()
			return integrationsdk.ConnectionAccount{}, err
		}
		secrets = append(secrets, secret)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return integrationsdk.ConnectionAccount{}, err
	}
	if err = rows.Close(); err != nil {
		return integrationsdk.ConnectionAccount{}, err
	}
	for _, secret := range secrets {
		secretWhere := query.And(query.Equal("workspace_id", subject.WorkspaceID), query.Equal("secret_key", secret), query.Equal("status", "active"))
		update, values, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("status", "revoked").Set("revoked_at", now).Set("updated_at", now).Where(secretWhere).Build()
		if buildErr != nil {
			return integrationsdk.ConnectionAccount{}, buildErr
		}
		if _, err = s.database.ExecContext(ctx, update, values...); err != nil {
			return integrationsdk.ConnectionAccount{}, err
		}
	}
	return s.GetConnectionAccount(ctx, subject, account.Key)
}

var _ integrationsdk.ConnectionAccounts = (*ManagementStore)(nil)
var _ integrationsdk.ConnectionAccountAdministration = (*ManagementStore)(nil)

// Legacy connections stay outside the user catalog, but cannot alias credentials
// reserved for a published account. Both checks run inside the connection write
// transaction; existing aliases reject publication without changing ownership.
func (s *ManagementStore) rejectReservedAccountSecrets(ctx context.Context, connection integrationsdk.Connection) error {
	for _, ref := range connection.SecretRefs {
		if !strings.HasPrefix(strings.TrimSpace(ref), "secret:") {
			continue
		}
		key := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(ref), "secret:"))
		statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connection_account_secrets").Columns("connection_key").Where(query.And(query.Equal("workspace_id", connection.WorkspaceID), query.Equal("secret_key", key))).Limit(1).Build()
		if err != nil {
			return err
		}
		var owner string
		err = s.database.QueryRowContext(ctx, statement, args...).Scan(&owner)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return err
		}
		if owner != connection.Key {
			return fmt.Errorf("Integration credential belongs to another connection account")
		}
	}
	return nil
}

func (s *ManagementStore) rejectExistingCredentialAliases(ctx context.Context, connection integrationsdk.Connection, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_connections").Columns("connection_key", "secret_refs_json").Where(query.Equal("workspace_id", connection.WorkspaceID)).Build()
	if err != nil {
		return err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	reserved := map[string]bool{}
	for _, key := range keys {
		reserved["secret:"+key] = true
	}
	for rows.Next() {
		var key string
		var raw sql.NullString
		if err = rows.Scan(&key, &raw); err != nil {
			return err
		}
		if key == connection.Key {
			continue
		}
		var refs map[string]string
		if raw.Valid && raw.String != "" {
			if err = json.Unmarshal([]byte(raw.String), &refs); err != nil {
				return err
			}
		}
		for _, ref := range refs {
			if reserved[strings.TrimSpace(ref)] {
				return fmt.Errorf("Integration account credential is already used by another connection")
			}
		}
	}
	return rows.Err()
}
