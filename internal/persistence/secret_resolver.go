package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormbuilder "github.com/domainry/domainry-orm/builder"
)

type SecretResolver struct {
	database modulehost.Database
	dialect  modulehost.Dialect
	cipher   modulehost.SecretMaterialCipher
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
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "integration_secrets").Columns("status", "value_ref", "expires_at").Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspaceID), ormbuilder.Equal("secret_key", secretKey))).Build()
	if err != nil {
		return "", err
	}
	var status string
	var valueRef, expiresAt sql.NullString
	if err := s.database.QueryRowContext(ctx, query, args...).Scan(&status, &valueRef, &expiresAt); err != nil {
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
	materialQuery, materialArgs, err := ormbuilder.NewSelectBuilder(s.dialect, "integration_secret_materials").Columns("ciphertext").Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspaceID), ormbuilder.Equal("secret_key", secretKey))).Build()
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
