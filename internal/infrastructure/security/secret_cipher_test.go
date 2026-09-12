package security

import (
	"context"
	"strings"
	"testing"
)

func TestSecretCipherRestartAndIdentityBinding(t *testing.T) {
	key := []byte(strings.Repeat("a", 32))
	first, _ := NewSecretCipher(key)
	second, _ := NewSecretCipher(key)
	ctx := context.Background()
	sealed, err := first.EncryptSecretMaterial(ctx, "workspace", "secret", "private-material")
	if err != nil || strings.Contains(sealed, "private-material") {
		t.Fatalf("seal: %v", err)
	}
	other, _ := first.EncryptSecretMaterial(ctx, "workspace", "secret", "private-material")
	if sealed == other {
		t.Fatal("nonce reused")
	}
	plain, err := second.DecryptSecretMaterial(ctx, "workspace", "secret", sealed)
	if err != nil || plain != "private-material" {
		t.Fatalf("restart decrypt: %v", err)
	}
	for _, identity := range [][2]string{{"other", "secret"}, {"workspace", "other"}, {"", "secret"}} {
		if _, err := second.DecryptSecretMaterial(ctx, identity[0], identity[1], sealed); err == nil {
			t.Fatal("identity binding bypassed")
		}
	}
	wrongKey, _ := NewSecretCipher([]byte(strings.Repeat("b", 32)))
	if _, err := wrongKey.DecryptSecretMaterial(ctx, "workspace", "secret", sealed); err == nil {
		t.Fatal("wrong key accepted")
	}
	for _, invalid := range []string{"", "private-material", "v2." + sealed[3:], sealed[:len(sealed)-4]} {
		if _, err := second.DecryptSecretMaterial(ctx, "workspace", "secret", invalid); err == nil {
			t.Fatal("invalid ciphertext accepted")
		}
	}
	if _, err := NewSecretCipher([]byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := first.EncryptSecretMaterial(cancelled, "workspace", "secret", "private-material"); err == nil {
		t.Fatal("cancelled encryption accepted")
	}
}
