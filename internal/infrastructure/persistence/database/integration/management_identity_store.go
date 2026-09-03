package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

func (s *ManagementStore) ListExternalIdentities(ctx context.Context, workspaceID string) ([]integrationsdk.ExternalIdentity, error) {
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return nil, err
	}
	where, err := scopedWhere(ctx, workspaceID, "", "")
	if err != nil {
		return nil, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_external_identities").Columns(
		"identity_key", "workspace_id", "provider", "external_subject", "external_subject_type", "external_name", "external_organization", "external_department", "external_group", "external_bot_id", "actor_id", "role_key", "status", "last_resolved_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(where).OrderBy(query.Ascending("identity_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration external identities: %w", err)
	}
	defer rows.Close()
	values := []integrationsdk.ExternalIdentity{}
	for rows.Next() {
		value, err := scanExternalIdentity(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func scanExternalIdentity(row rowScanner) (integrationsdk.ExternalIdentity, error) {
	var value integrationsdk.ExternalIdentity
	var subjectType, name, organization, department, group, botID, lastResolved, createdBy, disabledAt sql.NullString
	if err := row.Scan(&value.Key, &value.WorkspaceID, &value.Provider, &value.ExternalSubject, &subjectType, &name, &organization, &department, &group, &botID, &value.ActorID, &value.RoleKey, &value.Status, &lastResolved, &createdBy, &value.CreatedAt, &value.UpdatedAt, &disabledAt); err != nil {
		return value, err
	}
	value.ExternalSubjectType, value.ExternalName, value.ExternalOrganization = subjectType.String, name.String, organization.String
	value.ExternalDepartment, value.ExternalGroup, value.ExternalBotID = department.String, group.String, botID.String
	value.LastResolvedAt, value.CreatedBy, value.DisabledAt = lastResolved.String, createdBy.String, disabledAt.String
	return value, nil
}

func (s *ManagementStore) getExternalIdentity(ctx context.Context, workspaceID, key string) (integrationsdk.ExternalIdentity, error) {
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("identity_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_external_identities").Columns(
		"identity_key", "workspace_id", "provider", "external_subject", "external_subject_type", "external_name", "external_organization", "external_department", "external_group", "external_bot_id", "actor_id", "role_key", "status", "last_resolved_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	value, err := scanExternalIdentity(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return value, fmt.Errorf("Integration external identity %q was not found", key)
	}
	return value, err
}

func (s *ManagementStore) UpsertExternalIdentity(ctx context.Context, workspaceID, key, actorID string, input integrationsdk.ExternalIdentityInput) (integrationsdk.ExternalIdentity, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.ExternalIdentity
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.UpsertExternalIdentity(ctx, workspaceID, key, actorID, input)
			return operationErr
		})
		return value, err
	}
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	key, err = requiredOwnerValue("external identity key", key)
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	input.Provider, err = requiredOwnerValue("external identity provider", input.Provider)
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	input.ExternalSubject, err = requiredOwnerValue("external subject", input.ExternalSubject)
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	input.ActorID, err = requiredOwnerValue("external identity actor ID", input.ActorID)
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	input.RoleKey, err = requiredOwnerValue("external identity role key", input.RoleKey)
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	if strings.TrimSpace(input.Status) == "" {
		input.Status = "active"
	}
	now := ownerNow()
	where, err := scopedWhere(ctx, workspaceID, "", "", query.Equal("identity_key", key))
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	lookup, lookupArgs, err := query.NewSelectBuilder(s.dialect, "_integration_external_identities").Columns("id").Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	var id string
	lookupErr := s.database.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&id)
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return integrationsdk.ExternalIdentity{}, lookupErr
	}
	if lookupErr == sql.ErrNoRows {
		actorID, _ = scopeOwner(ctx, actorID)
		statement, args, buildErr := query.NewInsertBuilder(s.dialect, "_integration_external_identities").Columns(
			"id", "identity_key", "workspace_id", "provider", "external_subject", "external_subject_type", "external_name", "external_organization", "external_department", "external_group", "external_bot_id", "actor_id", "role_key", "status", "last_resolved_at", "created_by", "created_at", "updated_at", "disabled_at",
		).Values(ownerID("external_identity_", workspaceID, key), key, workspaceID, input.Provider, input.ExternalSubject, input.ExternalSubjectType, input.ExternalName, input.ExternalOrganization, input.ExternalDepartment, input.ExternalGroup, input.ExternalBotID, input.ActorID, input.RoleKey, input.Status, "", actorID, now, now, "").Build()
		if buildErr != nil {
			return integrationsdk.ExternalIdentity{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.ExternalIdentity{}, fmt.Errorf("insert Integration external identity: %w", err)
		}
	} else {
		statement, args, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_external_identities").Set("provider", input.Provider).Set("external_subject", input.ExternalSubject).Set("external_subject_type", input.ExternalSubjectType).Set("external_name", input.ExternalName).Set("external_organization", input.ExternalOrganization).Set("external_department", input.ExternalDepartment).Set("external_group", input.ExternalGroup).Set("external_bot_id", input.ExternalBotID).Set("actor_id", input.ActorID).Set("role_key", input.RoleKey).Set("status", input.Status).Set("disabled_at", "").Set("updated_at", now).Where(where).Build()
		if buildErr != nil {
			return integrationsdk.ExternalIdentity{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.ExternalIdentity{}, fmt.Errorf("update Integration external identity: %w", err)
		}
	}
	return s.getExternalIdentity(ctx, workspaceID, key)
}

func (s *ManagementStore) DisableExternalIdentity(ctx context.Context, workspaceID, key, _ string) (integrationsdk.ExternalIdentity, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.ExternalIdentity
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.DisableExternalIdentity(ctx, workspaceID, key, "")
			return operationErr
		})
		return value, err
	}
	if _, err := s.getExternalIdentity(ctx, workspaceID, key); err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	now := ownerNow()
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("identity_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_external_identities").Set("status", "disabled").Set("disabled_at", now).Set("updated_at", now).Where(where).Build()
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return integrationsdk.ExternalIdentity{}, fmt.Errorf("Integration external identity %q was not found", key)
	}
	return s.getExternalIdentity(ctx, workspaceID, key)
}

func (s *ManagementStore) ResolveExternalIdentity(ctx context.Context, workspaceID, provider, externalSubject string) (integrationsdk.ExternalIdentity, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.ExternalIdentity
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.ResolveExternalIdentity(ctx, workspaceID, provider, externalSubject)
			return operationErr
		})
		return value, err
	}
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("provider", strings.TrimSpace(provider)), query.Equal("external_subject", strings.TrimSpace(externalSubject)), query.Equal("status", "active"))
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_external_identities").Columns(
		"identity_key", "workspace_id", "provider", "external_subject", "external_subject_type", "external_name", "external_organization", "external_department", "external_group", "external_bot_id", "actor_id", "role_key", "status", "last_resolved_at", "created_by", "created_at", "updated_at", "disabled_at",
	).Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.ExternalIdentity{}, err
	}
	value, err := scanExternalIdentity(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return value, fmt.Errorf("Integration external identity was not found")
	}
	if err != nil {
		return value, err
	}
	now := ownerNow()
	updateWhere, whereErr := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("identity_key", value.Key))
	if whereErr != nil {
		return value, whereErr
	}
	update, updateArgs, err := query.NewUpdateBuilder(s.dialect, "_integration_external_identities").Set("last_resolved_at", now).Set("updated_at", now).Where(updateWhere).Build()
	if err == nil {
		_, err = s.database.ExecContext(ctx, update, updateArgs...)
	}
	if err != nil {
		return value, err
	}
	value.LastResolvedAt, value.UpdatedAt = now, now
	return value, nil
}
