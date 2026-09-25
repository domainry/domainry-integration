package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"testing"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type transactionTestCipher struct{}

func (transactionTestCipher) EncryptSecretMaterial(_ context.Context, _, _, plaintext string) (string, error) {
	return "encrypted:" + plaintext, nil
}

func TestAPIKeyUsesTypedCredentialRowWithoutSecretProjection(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "integration-api-key-credential")
	store := NewManagementStore(database, dialect, transactionTestCipher{}, nil)
	ctx := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.api_keys.manage", ActorID: "admin-a", Unrestricted: true,
	})
	created, err := store.CreateAPIKey(ctx, "workspace-a", "ignored", integrationsdk.APIKeyInput{
		Key: "automation", Name: "Automation", ActorID: "actor-a", RoleKey: "operator", Scopes: []string{"integration.read"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Token == "" || created.APIKey.TokenPrefix == "" || created.APIKey.Key != "automation" {
		t.Fatalf("incomplete API key credential: %#v", created)
	}
	secrets, err := store.ListSecrets(ctx, "workspace-a")
	if err != nil || len(secrets) != 0 {
		t.Fatalf("API key leaked into provider secrets: values=%#v err=%v", secrets, err)
	}
	keys, err := store.ListAPIKeys(ctx, "workspace-a")
	if err != nil || len(keys) != 1 || keys[0].ActorID != "actor-a" || keys[0].RoleKey != "operator" {
		t.Fatalf("typed API key projection=%#v err=%v", keys, err)
	}
	digest := sha256.Sum256([]byte(created.Token))
	var credentialType, kind, lookupHash, displayPrefix string
	var valueRef sql.NullString
	if err := database.QueryRowContext(ctx, `SELECT credential_type,kind,lookup_hash,display_prefix,value_ref FROM _integration_secrets WHERE workspace_id=? AND secret_key=?`, "workspace-a", "automation").Scan(&credentialType, &kind, &lookupHash, &displayPrefix, &valueRef); err != nil {
		t.Fatal(err)
	}
	if credentialType != "api_key" || kind != "api_key" || lookupHash != hex.EncodeToString(digest[:]) || displayPrefix != created.APIKey.TokenPrefix || valueRef.Valid {
		t.Fatalf("unexpected typed credential row: type=%q kind=%q hash=%q prefix=%q value_ref=%#v", credentialType, kind, lookupHash, displayPrefix, valueRef)
	}
	rotated, err := store.RotateAPIKey(ctx, "workspace-a", "automation", "ignored")
	if err != nil || rotated.Token == "" || rotated.Token == created.Token || rotated.APIKey.Status != "active" {
		t.Fatalf("rotated API key=%#v err=%v", rotated, err)
	}
	disabled, err := store.DisableAPIKey(ctx, "workspace-a", "automation", "ignored")
	if err != nil || disabled.Status != "disabled" || disabled.DisabledAt == "" {
		t.Fatalf("disabled API key=%#v err=%v", disabled, err)
	}
}

func (transactionTestCipher) DecryptSecretMaterial(_ context.Context, _, _, ciphertext string) (string, error) {
	return ciphertext, nil
}

func TestSecretRotationRollsBackMaterialAndMetadataTogether(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "integration-secret-rotation-transaction")
	store := NewManagementStore(database, dialect, transactionTestCipher{}, nil)
	allContext := integrationmodel.WithAccessScope(context.Background(), integrationmodel.AccessScope{
		WorkspaceID: "workspace-a", PermissionKey: "integration.secrets.rotate", ActorID: "admin-a", Unrestricted: true,
	})
	if _, err := store.UpsertSecret(allContext, "workspace-a", "token", "untrusted-actor", integrationsdk.SecretInput{Kind: "token", Value: "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(t.Context(), `CREATE TRIGGER fail_rotation_transition BEFORE UPDATE ON _integration_secrets WHEN NEW.rotated_at <> 0 BEGIN SELECT RAISE(ABORT, 'forced rotation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RotateSecret(allContext, "workspace-a", "token", "untrusted-actor", integrationsdk.SecretInput{Kind: "token", Value: "new"}); err == nil {
		t.Fatal("expected rotation to fail")
	}

	var ciphertext, fingerprint, createdBy string
	var rotatedAt int64
	if err := database.QueryRowContext(t.Context(), `SELECT m.ciphertext, s.fingerprint, s.rotated_at, s.created_by FROM _integration_secrets s JOIN _integration_secret_materials m ON m.workspace_id = s.workspace_id AND m.secret_key = s.secret_key WHERE s.workspace_id = ? AND s.secret_key = ?`, "workspace-a", "token").Scan(&ciphertext, &fingerprint, &rotatedAt, &createdBy); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("old"))
	if ciphertext != "encrypted:old" || fingerprint != hex.EncodeToString(digest[:]) || rotatedAt != 0 || createdBy != "admin-a" {
		t.Fatalf("rotation was partial: ciphertext=%q fingerprint=%q rotated_at=%d created_by=%q", ciphertext, fingerprint, rotatedAt, createdBy)
	}
}
