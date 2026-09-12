// Package security implements host-owned protection for standalone Integration.
package security

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

type SecretCipher struct{ aead cipher.AEAD }

// NewSecretCipher requires a deployment-owned AES-256 key. Ciphertexts bind to
// both workspace and material key and remain readable after a process restart.
func NewSecretCipher(masterKey []byte) (*SecretCipher, error) {
	if len(masterKey) != 32 {
		return nil, errors.New("Integration master key must contain 32 bytes")
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &SecretCipher{aead: aead}, nil
}

func secretAAD(workspace, key string) ([]byte, error) {
	if workspace == "" || key == "" {
		return nil, errors.New("Integration secret identity is required")
	}
	return json.Marshal([]string{"integration.secret.v1", workspace, key})
}

func (s *SecretCipher) EncryptSecretMaterial(ctx context.Context, workspace, key, plaintext string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	aad, err := secretAAD(workspace, key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", errors.New("Integration secret nonce generation failed")
	}
	sealed := s.aead.Seal(nonce, nonce, []byte(plaintext), aad)
	return "v1." + base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (s *SecretCipher) DecryptSecretMaterial(ctx context.Context, workspace, key, ciphertext string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	aad, err := secretAAD(workspace, key)
	if err != nil {
		return "", err
	}
	invalid := errors.New("Integration secret ciphertext is invalid")
	if !strings.HasPrefix(ciphertext, "v1.") {
		return "", invalid
	}
	sealed, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(ciphertext, "v1."))
	if err != nil || len(sealed) < s.aead.NonceSize()+s.aead.Overhead() {
		return "", invalid
	}
	plain, err := s.aead.Open(nil, sealed[:s.aead.NonceSize()], sealed[s.aead.NonceSize():], aad)
	if err != nil {
		return "", invalid
	}
	return string(plain), nil
}
