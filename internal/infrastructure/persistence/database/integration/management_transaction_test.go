package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type transactionTestCipher struct{}

func (transactionTestCipher) EncryptSecretMaterial(_ context.Context, _, _, plaintext string) (string, error) {
	return "encrypted:" + plaintext, nil
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
	if _, err := database.ExecContext(t.Context(), `CREATE TRIGGER fail_rotation_transition BEFORE UPDATE ON _integration_secrets WHEN NEW.rotated_at <> '' BEGIN SELECT RAISE(ABORT, 'forced rotation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RotateSecret(allContext, "workspace-a", "token", "untrusted-actor", integrationsdk.SecretInput{Kind: "token", Value: "new"}); err == nil {
		t.Fatal("expected rotation to fail")
	}

	var ciphertext, fingerprint, rotatedAt, createdBy string
	if err := database.QueryRowContext(t.Context(), `SELECT m.ciphertext, s.fingerprint, s.rotated_at, s.created_by FROM _integration_secrets s JOIN _integration_secret_materials m ON m.workspace_id = s.workspace_id AND m.secret_key = s.secret_key WHERE s.workspace_id = ? AND s.secret_key = ?`, "workspace-a", "token").Scan(&ciphertext, &fingerprint, &rotatedAt, &createdBy); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("old"))
	if ciphertext != "encrypted:old" || fingerprint != hex.EncodeToString(digest[:]) || rotatedAt != "" || createdBy != "admin-a" {
		t.Fatalf("rotation was partial: ciphertext=%q fingerprint=%q rotated_at=%q created_by=%q", ciphertext, fingerprint, rotatedAt, createdBy)
	}
}
