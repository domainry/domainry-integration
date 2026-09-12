package integration

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const providerSecretUpdateTimeout = 5 * time.Second

func (s *DeliveryStore) resolveProviderSecrets(ctx context.Context, workspaceID string, references map[string]string) (resolvedProviderSecrets, error) {
	if resolver, ok := s.secrets.(SecretSnapshotResolver); ok {
		return resolver.ResolveSecretReferencesSnapshot(ctx, workspaceID, references)
	}
	values, err := s.secrets.ResolveSecretReferences(ctx, workspaceID, references)
	return resolvedProviderSecrets{Values: values}, err
}

// A provider may have rotated a credential before its subsequent business
// request failed. Persist that completed rotation independently, while keeping
// both the provider error and any storage error available to the caller.
func (s *DeliveryStore) persistProviderSecretUpdates(ctx context.Context, workspaceID string, references map[string]string, versions map[string]providerSecretVersion, updates map[string]string, providerErr error) error {
	if len(updates) == 0 {
		return providerErr
	}
	write, cancel := context.WithTimeout(context.WithoutCancel(ctx), providerSecretUpdateTimeout)
	defer cancel()
	if versions != nil {
		writer, ok := s.secrets.(ConditionalSecretUpdateWriter)
		if !ok {
			return errors.Join(providerErr, fmt.Errorf("Integration conditional secret update writer is unavailable"))
		}
		if err := writer.ApplySecretUpdatesIfCurrent(write, workspaceID, references, versions, updates); err != nil {
			return errors.Join(providerErr, err)
		}
		return providerErr
	}
	writer, ok := s.secrets.(SecretUpdateWriter)
	if !ok {
		return errors.Join(providerErr, fmt.Errorf("Integration secret update writer is unavailable"))
	}
	if err := writer.ApplySecretUpdates(write, workspaceID, references, updates); err != nil {
		return errors.Join(providerErr, err)
	}
	return providerErr
}
