package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

func (s *ManagementStore) ListSecrets(ctx context.Context, workspaceID string) ([]integrationsdk.Secret, error) {
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return nil, err
	}
	where, err := scopedWhere(ctx, workspaceID, "", "")
	if err != nil {
		return nil, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns(
		"secret_key", "workspace_id", "kind", "status", "description", "value_ref", "fingerprint", "created_by", "created_at", "updated_at", "disabled_at", "expires_at", "rotated_at", "revoked_at", "last_tested_at", "last_test_status", "last_test_error",
	).Where(query.And(where, query.Equal("credential_type", "secret"))).OrderBy(query.Ascending("secret_key")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list Integration secrets: %w", err)
	}
	defer rows.Close()
	values := []integrationsdk.Secret{}
	for rows.Next() {
		value, err := scanSecret(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func scanSecret(row rowScanner) (integrationsdk.Secret, error) {
	var value integrationsdk.Secret
	var description, valueRef, fingerprint, createdBy, disabledAt, expiresAt, rotatedAt, revokedAt, lastTestedAt, lastTestStatus, lastTestError sql.NullString
	err := row.Scan(&value.Key, &value.WorkspaceID, &value.Kind, &value.Status, &description, &valueRef, &fingerprint, &createdBy, &value.CreatedAt, &value.UpdatedAt, &disabledAt, &expiresAt, &rotatedAt, &revokedAt, &lastTestedAt, &lastTestStatus, &lastTestError)
	if err != nil {
		return value, err
	}
	value.Description, value.ValueRef, value.Fingerprint, value.CreatedBy = description.String, valueRef.String, fingerprint.String, createdBy.String
	value.Configured = valueRef.String != "" || fingerprint.String != ""
	value.DisabledAt, value.ExpiresAt, value.RotatedAt, value.RevokedAt = disabledAt.String, expiresAt.String, rotatedAt.String, revokedAt.String
	value.LastTestedAt, value.LastTestStatus, value.LastTestError = lastTestedAt.String, lastTestStatus.String, lastTestError.String
	return value, nil
}

func (s *ManagementStore) getSecret(ctx context.Context, workspaceID, key string) (integrationsdk.Secret, error) {
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("credential_type", "secret"), query.Equal("secret_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns(
		"secret_key", "workspace_id", "kind", "status", "description", "value_ref", "fingerprint", "created_by", "created_at", "updated_at", "disabled_at", "expires_at", "rotated_at", "revoked_at", "last_tested_at", "last_test_status", "last_test_error",
	).Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	value, err := scanSecret(s.database.QueryRowContext(ctx, statement, args...))
	if err == sql.ErrNoRows {
		return value, fmt.Errorf("Integration secret %q was not found", key)
	}
	return value, err
}

func (s *ManagementStore) UpsertSecret(ctx context.Context, workspaceID, key, actorID string, input integrationsdk.SecretInput) (integrationsdk.Secret, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.Secret{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.Secret
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.UpsertSecret(ctx, workspaceID, key, actorID, input)
			return operationErr
		})
		return value, err
	}
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, workspaceID, subjectFenceReference{"secret", "", key}, subjectFenceReference{"subject", "", actorID}); err != nil {
		return integrationsdk.Secret{}, err
	}
	workspaceID, err := requiredOwnerValue("workspace ID", workspaceID)
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	key, err = requiredOwnerValue("secret key", key)
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	input.Kind, err = requiredOwnerValue("secret kind", input.Kind)
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	now, fingerprint, valueRef, ciphertext := ownerNow(), "", "", ""
	hasNewMaterial := input.Value != ""
	if hasNewMaterial {
		if s.cipher == nil {
			return integrationsdk.Secret{}, fmt.Errorf("Integration secret cipher is unavailable")
		}
		ciphertext, err = s.cipher.EncryptSecretMaterial(ctx, workspaceID, key, input.Value)
		if err != nil {
			return integrationsdk.Secret{}, fmt.Errorf("encrypt Integration secret material: %w", err)
		}
		digest := sha256.Sum256([]byte(input.Value))
		fingerprint, valueRef = hex.EncodeToString(digest[:]), "material:"+key
	}
	where, err := scopedWhere(ctx, workspaceID, "", "", query.Equal("credential_type", "secret"), query.Equal("secret_key", key))
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	lookup, lookupArgs, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("id", "value_ref", "fingerprint", "created_by").Where(where).Limit(1).Build()
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	var id string
	var previousRef, previousFingerprint, previousCreatedBy sql.NullString
	lookupErr := s.database.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&id, &previousRef, &previousFingerprint, &previousCreatedBy)
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return integrationsdk.Secret{}, lookupErr
	}
	if valueRef == "" {
		valueRef, fingerprint = previousRef.String, previousFingerprint.String
	}
	if hasNewMaterial {
		if err := s.upsertSecretMaterial(ctx, workspaceID, key, ciphertext, now); err != nil {
			return integrationsdk.Secret{}, err
		}
	}
	if lookupErr == sql.ErrNoRows {
		id = ownerID("secret_", workspaceID, key)
		actorID, _ = scopeOwner(ctx, actorID)
		statement, args, buildErr := query.NewInsertBuilder(s.dialect, "_integration_secrets").Columns(
			"id", "secret_key", "workspace_id", "credential_type", "kind", "status", "description", "value_ref", "fingerprint", "scopes_json", "created_by", "created_at", "updated_at", "disabled_at", "expires_at", "rotated_at", "revoked_at", "last_used_at", "last_tested_at", "last_test_status", "last_test_error",
		).Values(id, key, workspaceID, "secret", input.Kind, "active", input.Description, valueRef, fingerprint, "[]", actorID, now, now, "", input.ExpiresAt, "", "", "", "", "", "").Build()
		if buildErr != nil {
			return integrationsdk.Secret{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.Secret{}, fmt.Errorf("insert Integration secret: %w", err)
		}
	} else {
		statement, args, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("kind", input.Kind).Set("status", "active").Set("description", input.Description).Set("value_ref", valueRef).Set("fingerprint", fingerprint).Set("expires_at", input.ExpiresAt).Set("disabled_at", "").Set("revoked_at", "").Set("updated_at", now).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, workspaceID, "_integration_secrets", id), where)).Build()
		if buildErr != nil {
			return integrationsdk.Secret{}, buildErr
		}
		if _, err := s.database.ExecContext(ctx, statement, args...); err != nil {
			return integrationsdk.Secret{}, fmt.Errorf("update Integration secret: %w", err)
		}
	}
	return s.getSecret(ctx, workspaceID, key)
}

func (s *ManagementStore) upsertSecretMaterial(ctx context.Context, workspaceID, key, ciphertext, now string) error {
	if err := guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, workspaceID, subjectFenceReference{"secret", "", key}); err != nil {
		return err
	}
	lookup, lookupArgs, err := query.NewSelectBuilder(s.dialect, "_integration_secret_materials").Columns("id").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", key))).Limit(1).Build()
	if err != nil {
		return err
	}
	var id string
	lookupErr := s.database.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&id)
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return lookupErr
	}
	if lookupErr == sql.ErrNoRows {
		statement, args, buildErr := query.NewInsertBuilder(s.dialect, "_integration_secret_materials").Columns("id", "workspace_id", "secret_key", "ciphertext", "created_at", "updated_at").Values(ownerID("secret_material_", workspaceID, key), workspaceID, key, ciphertext, now, now).Build()
		if buildErr != nil {
			return buildErr
		}
		_, err = s.database.ExecContext(ctx, statement, args...)
		return err
	}
	statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_secret_materials").Set("ciphertext", ciphertext).Set("updated_at", now).Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, workspaceID, "_integration_secret_materials", id), query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", key)))).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, statement, args...)
	return err
}

func (s *ManagementStore) TransitionSecret(ctx context.Context, workspaceID, key, transition, _ string) (integrationsdk.Secret, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.Secret{}, err
	}
	if s.transactions != nil {
		var value integrationsdk.Secret
		err := s.withTransaction(ctx, func(store *ManagementStore) error {
			var operationErr error
			value, operationErr = store.TransitionSecret(ctx, workspaceID, key, transition, "")
			return operationErr
		})
		return value, err
	}
	if _, err := s.getSecret(ctx, workspaceID, key); err != nil {
		return integrationsdk.Secret{}, err
	}
	now := ownerNow()
	builder := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("updated_at", now)
	switch strings.TrimSpace(transition) {
	case "disable", "disabled":
		builder.Set("status", "disabled").Set("disabled_at", now)
	case "expire", "expired":
		builder.Set("status", "expired").Set("expires_at", now)
	case "revoke", "revoked":
		builder.Set("status", "revoked").Set("revoked_at", now)
	case "rotate", "rotated":
		builder.Set("status", "active").Set("rotated_at", now)
	default:
		return integrationsdk.Secret{}, fmt.Errorf("Integration secret transition %q is unsupported", transition)
	}
	where, err := scopedWhere(ctx, strings.TrimSpace(workspaceID), "", "", query.Equal("credential_type", "secret"), query.Equal("secret_key", strings.TrimSpace(key)))
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	statement, args, err := builder.Where(query.And(subjectRowsWriteAllowed(s.subjectLifecycle, s.dialect, strings.TrimSpace(workspaceID), "_integration_secrets", ownerID("secret_", workspaceID, key)), where)).Build()
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	result, err := s.database.ExecContext(ctx, statement, args...)
	if err != nil {
		return integrationsdk.Secret{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return integrationsdk.Secret{}, fmt.Errorf("Integration secret %q was not found", key)
	}
	return s.getSecret(ctx, workspaceID, key)
}

// RotateSecret keeps material replacement and the rotation transition in one
// owner-database transaction. It is intentionally an optional local extension
// to the public Management contract so the HTTP adapter never exposes a
// partially rotated secret.
func (s *ManagementStore) RotateSecret(ctx context.Context, workspaceID, key, actorID string, input integrationsdk.SecretInput) (integrationsdk.Secret, error) {
	if err := requireAllDataScope(ctx, strings.TrimSpace(workspaceID)); err != nil {
		return integrationsdk.Secret{}, err
	}
	var value integrationsdk.Secret
	err := s.withTransaction(ctx, func(store *ManagementStore) error {
		var operationErr error
		value, operationErr = store.UpsertSecret(ctx, workspaceID, key, actorID, input)
		if operationErr != nil {
			return operationErr
		}
		value, operationErr = store.TransitionSecret(ctx, workspaceID, value.Key, "rotate", actorID)
		return operationErr
	})
	return value, err
}
