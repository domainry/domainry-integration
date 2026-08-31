package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
)

type SecretResolver struct {
	database modulehost.Database
	dialect  modulehost.Dialect
	cipher   modulehost.SecretMaterialCipher
}

func (s *SecretResolver) ApplySecretUpdates(ctx context.Context, workspaceID string, references, updates map[string]string) error {
	for fieldKey, plaintext := range updates {
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
		now := time.Now().UTC().Format(time.RFC3339Nano)
		lookup, args, err := query.NewSelectBuilder(s.dialect, "_integration_secret_materials").Columns("id").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", secretKey))).Build()
		if err != nil {
			return err
		}
		var id string
		lookupErr := s.database.QueryRowContext(ctx, lookup, args...).Scan(&id)
		switch lookupErr {
		case nil:
			statement, values, buildErr := query.NewUpdateBuilder(s.dialect, "_integration_secret_materials").Set("ciphertext", ciphertext).Set("updated_at", now).Where(query.Equal("id", id)).Build()
			if buildErr != nil {
				return buildErr
			}
			if _, err := s.database.ExecContext(ctx, statement, values...); err != nil {
				return err
			}
		case sql.ErrNoRows:
			digest := sha256.Sum256([]byte(workspaceID + "\x00" + secretKey))
			statement, values, buildErr := query.NewInsertBuilder(s.dialect, "_integration_secret_materials").Columns("id", "workspace_id", "secret_key", "ciphertext", "created_at", "updated_at").Values("secret_material_"+hex.EncodeToString(digest[:16]), workspaceID, secretKey, ciphertext, now, now).Build()
			if buildErr != nil {
				return buildErr
			}
			if _, err := s.database.ExecContext(ctx, statement, values...); err != nil {
				return err
			}
		default:
			return lookupErr
		}
		fingerprint := sha256.Sum256([]byte(plaintext))
		statement, values, err := query.NewUpdateBuilder(s.dialect, "_integration_secrets").Set("status", "active").Set("value_ref", "material:"+secretKey).Set("fingerprint", hex.EncodeToString(fingerprint[:])).Set("rotated_at", now).Set("updated_at", now).Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", secretKey))).Build()
		if err != nil {
			return err
		}
		result, err := s.database.ExecContext(ctx, statement, values...)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return fmt.Errorf("Integration Provider secret update target %q was not found", secretKey)
		}
	}
	return nil
}

func NewSecretResolver(database modulehost.Database, dialect modulehost.Dialect, cipher modulehost.SecretMaterialCipher) *SecretResolver {
	return &SecretResolver{database: database, dialect: dialect, cipher: cipher}
}

func (s *SecretResolver) ResolveSecretReferences(ctx context.Context, workspaceID string, references map[string]string) (map[string]string, error) {
	resolved := make(map[string]string, len(references))
	for name, reference := range references {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := s.resolveReference(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(reference))
		if err != nil {
			return nil, fmt.Errorf("resolve Integration provider secret %q: %w", name, err)
		}
		resolved[name] = value
	}
	return resolved, nil
}

func (s *SecretResolver) resolveReference(ctx context.Context, workspaceID, reference string) (string, error) {
	if strings.HasPrefix(reference, "env:") {
		return environmentSecret(reference)
	}
	if !strings.HasPrefix(reference, "secret:") {
		return "", fmt.Errorf("secret reference is invalid")
	}
	secretKey := strings.TrimSpace(strings.TrimPrefix(reference, "secret:"))
	queryValue, args, err := query.NewSelectBuilder(s.dialect, "_integration_secrets").Columns("status", "value_ref", "expires_at").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", secretKey))).Build()
	if err != nil {
		return "", err
	}
	var status string
	var valueRef, expiresAt sql.NullString
	if err := s.database.QueryRowContext(ctx, queryValue, args...).Scan(&status, &valueRef, &expiresAt); err != nil {
		return "", err
	}
	if status != "active" {
		return "", fmt.Errorf("secret %q is not active", secretKey)
	}
	if expiresAt.String != "" {
		expires, parseErr := time.Parse(time.RFC3339, expiresAt.String)
		if parseErr != nil || !expires.After(time.Now().UTC()) {
			return "", fmt.Errorf("secret %q is expired", secretKey)
		}
	}
	if strings.HasPrefix(valueRef.String, "env:") {
		return environmentSecret(valueRef.String)
	}
	if !strings.HasPrefix(valueRef.String, "material:") {
		return "", fmt.Errorf("secret %q has no material", secretKey)
	}
	materialQuery, materialArgs, err := query.NewSelectBuilder(s.dialect, "_integration_secret_materials").Columns("ciphertext").Where(query.And(query.Equal("workspace_id", workspaceID), query.Equal("secret_key", secretKey))).Build()
	if err != nil {
		return "", err
	}
	var ciphertext string
	if err := s.database.QueryRowContext(ctx, materialQuery, materialArgs...).Scan(&ciphertext); err != nil {
		return "", err
	}
	return s.cipher.DecryptSecretMaterial(ctx, workspaceID, secretKey, ciphertext)
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
