package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

type SecretResolver struct {
	database     sqlhost.DBTX
	transactions modulehost.Database
	dialect      modulehost.Dialect
	cipher       modulehost.SecretMaterialCipher
}

func (s *SecretResolver) ApplySecretUpdates(ctx context.Context, workspaceID string, references, updates map[string]string) error {
	return s.applySecretUpdates(ctx, workspaceID, references, nil, updates)
}

func (s *SecretResolver) ApplySecretUpdatesIfCurrent(ctx context.Context, workspaceID string, references map[string]string, versions map[string]providerSecretVersion, updates map[string]string) error {
	if versions == nil {
		return fmt.Errorf("Integration Provider secret update snapshot is required")
	}
	return s.applySecretUpdates(ctx, workspaceID, references, versions, updates)
}

var errProviderSecretChanged = errors.New("Integration Provider credential changed while the request was running")

type preparedProviderSecretUpdate struct {
	fieldKey, secretKey, ciphertext, fingerprint string
	expected                                     *providerSecretVersion
}

func (s *SecretResolver) applySecretUpdates(ctx context.Context, workspaceID string, references map[string]string, versions map[string]providerSecretVersion, updates map[string]string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" || s.cipher == nil {
		return fmt.Errorf("Integration Provider secret update storage is unavailable")
	}
	fields := make([]string, 0, len(updates))
	for fieldKey := range updates {
		fields = append(fields, fieldKey)
	}
	sort.Strings(fields)
	prepared := make([]preparedProviderSecretUpdate, 0, len(fields))
	for _, fieldKey := range fields {
		plaintext := updates[fieldKey]
		reference := strings.TrimSpace(references[fieldKey])
		if !strings.HasPrefix(reference, "secret:") {
			return fmt.Errorf("Integration Provider cannot update non-material secret reference %q", fieldKey)
		}
		secretKey := strings.TrimSpace(strings.TrimPrefix(reference, "secret:"))
		if secretKey == "" || s.cipher == nil {
			return fmt.Errorf("Integration Provider secret update %q has no writable material", fieldKey)
		}
		ciphertext, err := s.cipher.EncryptSecretMaterial(ctx, workspaceID, secretKey, plaintext)
		if err != nil {
			return fmt.Errorf("encrypt Integration Provider secret update %q: %w", fieldKey, err)
		}
		fingerprint := sha256.Sum256([]byte(plaintext))
		value := preparedProviderSecretUpdate{fieldKey: fieldKey, secretKey: secretKey, ciphertext: ciphertext, fingerprint: hex.EncodeToString(fingerprint[:])}
		if versions != nil {
			expected, ok := versions[fieldKey]
			if !ok || expected.SecretKey != secretKey {
				return fmt.Errorf("%w: %s", errProviderSecretChanged, fieldKey)
			}
			value.expected = &expected
		}
		prepared = append(prepared, value)
	}
	if len(prepared) == 0 {
		return nil
	}
	if s.transactions == nil {
		return s.applyPreparedSecretUpdates(ctx, workspaceID, prepared)
	}
	tx, err := s.transactions.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	scoped := *s
	scoped.database, scoped.transactions = tx, nil
	if err := scoped.applyPreparedSecretUpdates(ctx, workspaceID, prepared); err != nil {
		return err
	}
	return tx.Commit()
}

type currentProviderSecret struct {
	preparedProviderSecretUpdate
	materialID, fingerprint, updatedAt string
}

func (s *SecretResolver) applyPreparedSecretUpdates(ctx context.Context, workspaceID string, prepared []preparedProviderSecretUpdate) error {
	current := make([]currentProviderSecret, 0, len(prepared))
	for _, item := range prepared {
		statement, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("status", "value_ref", "fingerprint", "updated_at").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", item.secretKey))).Limit(1).Build()
		if err != nil {
			return err
		}
		var status, updatedAt string
		var valueRef, fingerprint sql.NullString
		if err := s.database.QueryRowContext(ctx, statement, args...).Scan(&status, &valueRef, &fingerprint, &updatedAt); err != nil {
			return fmt.Errorf("read Integration Provider secret update target %q: %w", item.secretKey, err)
		}
		if status != "active" || valueRef.String != "material:"+item.secretKey {
			return fmt.Errorf("%w: %s is not active material", errProviderSecretChanged, item.fieldKey)
		}
		if item.expected != nil && (item.expected.Fingerprint != fingerprint.String || item.expected.UpdatedAt != updatedAt) {
			return fmt.Errorf("%w: %s", errProviderSecretChanged, item.fieldKey)
		}
		materialStatement, materialArgs, err := query.NewSelectBuilder(s.dialect, "_integration_secret_materials").Columns("id").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", item.secretKey))).Limit(1).Build()
		if err != nil {
			return err
		}
		var materialID string
		if err := s.database.QueryRowContext(ctx, materialStatement, materialArgs...).Scan(&materialID); err != nil {
			return fmt.Errorf("read Integration Provider secret material %q: %w", item.secretKey, err)
		}
		current = append(current, currentProviderSecret{preparedProviderSecretUpdate: item, materialID: materialID, fingerprint: fingerprint.String, updatedAt: updatedAt})
	}
	now := time.Now().UTC()
	for _, item := range current {
		if now.Format(time.RFC3339Nano) == item.updatedAt {
			now = now.Add(time.Nanosecond)
		}
	}
	updatedAt := now.Format(time.RFC3339Nano)
	for _, item := range current {
		where := query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", item.secretKey), query.Equal("status", "active"), query.Equal("value_ref", "material:"+item.secretKey), query.Equal("fingerprint", item.fingerprint), query.Equal("updated_at", item.updatedAt))
		statement, args, err := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("fingerprint", item.preparedProviderSecretUpdate.fingerprint).Set("rotated_at", updatedAt).Set("updated_at", updatedAt).Where(query.And(subjectRowWriteAllowed("_integration_secrets"), where)).Build()
		if err != nil {
			return err
		}
		result, err := s.database.ExecContext(ctx, statement, args...)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return fmt.Errorf("%w: %s", errProviderSecretChanged, item.fieldKey)
		}
		statement, args, err = query.NewUpdateBuilder(s.dialect, "_integration_secret_materials").Set("ciphertext", item.ciphertext).Set("updated_at", updatedAt).Where(query.And(subjectRowWriteAllowed("_integration_secret_materials"), query.And(query.Equal("id", item.materialID), query.Equal("workspace_id", workspaceID), query.Equal("secret_key", item.secretKey)))).Build()
		if err != nil {
			return err
		}
		result, err = s.database.ExecContext(ctx, statement, args...)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return fmt.Errorf("Integration Provider secret material %q changed", item.secretKey)
		}
	}
	return nil
}

func NewSecretResolver(database modulehost.Database, dialect modulehost.Dialect, cipher modulehost.SecretMaterialCipher) *SecretResolver {
	return &SecretResolver{database: database, transactions: database, dialect: dialect, cipher: cipher}
}

func (s *SecretResolver) ResolveSecretReferences(ctx context.Context, workspaceID string, references map[string]string) (map[string]string, error) {
	resolved, err := s.ResolveSecretReferencesSnapshot(ctx, workspaceID, references)
	return resolved.Values, err
}

func (s *SecretResolver) ResolveSecretReferencesSnapshot(ctx context.Context, workspaceID string, references map[string]string) (resolvedProviderSecrets, error) {
	resolved := resolvedProviderSecrets{Values: make(map[string]string, len(references)), Versions: make(map[string]providerSecretVersion, len(references))}
	for name, reference := range references {
		if err := ctx.Err(); err != nil {
			return resolvedProviderSecrets{}, err
		}
		value, version, err := s.resolveReferenceSnapshot(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(reference))
		if err != nil {
			return resolvedProviderSecrets{}, fmt.Errorf("resolve Integration provider secret %q: %w", name, err)
		}
		resolved.Values[name] = value
		if version != nil {
			resolved.Versions[name] = *version
		}
	}
	return resolved, nil
}

func (s *SecretResolver) resolveReference(ctx context.Context, workspaceID, reference string) (string, error) {
	value, _, err := s.resolveReferenceSnapshot(ctx, workspaceID, reference)
	return value, err
}

func (s *SecretResolver) resolveReferenceSnapshot(ctx context.Context, workspaceID, reference string) (string, *providerSecretVersion, error) {
	if strings.HasPrefix(reference, "env:") {
		value, err := environmentSecret(reference)
		return value, nil, err
	}
	if !strings.HasPrefix(reference, "secret:") {
		return "", nil, fmt.Errorf("secret reference is invalid")
	}
	secretKey := strings.TrimSpace(strings.TrimPrefix(reference, "secret:"))
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("status", "value_ref", "expires_at", "fingerprint", "updated_at").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", secretKey))).Build()
	if err != nil {
		return "", nil, err
	}
	var status, updatedAt string
	var valueRef, expiresAt, fingerprint sql.NullString
	if err := s.database.QueryRowContext(ctx, queryValue, args...).Scan(&status, &valueRef, &expiresAt, &fingerprint, &updatedAt); err != nil {
		return "", nil, err
	}
	if status != "active" {
		return "", nil, fmt.Errorf("secret %q is not active", secretKey)
	}
	if expiresAt.String != "" {
		expires, parseErr := time.Parse(time.RFC3339, expiresAt.String)
		if parseErr != nil || !expires.After(time.Now().UTC()) {
			return "", nil, fmt.Errorf("secret %q is expired", secretKey)
		}
	}
	if strings.HasPrefix(valueRef.String, "env:") {
		value, err := environmentSecret(valueRef.String)
		return value, nil, err
	}
	if !strings.HasPrefix(valueRef.String, "material:") {
		return "", nil, fmt.Errorf("secret %q has no material", secretKey)
	}
	materialQuery, materialArgs, err := query.NewSelectBuilder(s.dialect, "_integration_secret_materials").Columns("ciphertext").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", secretKey))).Build()
	if err != nil {
		return "", nil, err
	}
	var ciphertext string
	if err := s.database.QueryRowContext(ctx, materialQuery, materialArgs...).Scan(&ciphertext); err != nil {
		return "", nil, err
	}
	value, err := s.cipher.DecryptSecretMaterial(ctx, workspaceID, secretKey, ciphertext)
	if err != nil {
		return "", nil, err
	}
	return value, &providerSecretVersion{SecretKey: secretKey, Fingerprint: fingerprint.String, UpdatedAt: updatedAt}, nil
}

func environmentSecret(reference string) (string, error) {
	key := strings.TrimSpace(strings.TrimPrefix(reference, "env:"))
	value := strings.TrimSpace(os.Getenv(key))
	if key == "" || value == "" {
		return "", fmt.Errorf("environment secret is unavailable")
	}
	return value, nil
}

var _ SecretReferenceResolver = (*SecretResolver)(nil)
var _ SecretSnapshotResolver = (*SecretResolver)(nil)
var _ SecretUpdateWriter = (*SecretResolver)(nil)
var _ ConditionalSecretUpdateWriter = (*SecretResolver)(nil)
