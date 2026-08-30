package integration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	ormbuilder "github.com/domainry/domainry-orm/builder"
)

// RequirementsStore materializes application declarations in Integration-owned
// storage. It deliberately preserves configuration, secret references and
// status already managed through the Integration control plane.
type RequirementsStore struct {
	database  modulehost.Database
	dialect   modulehost.Dialect
	providers modulehost.ProviderRegistry
}

func NewRequirementsStore(database modulehost.Database, dialect modulehost.Dialect, providers modulehost.ProviderRegistry) *RequirementsStore {
	return &RequirementsStore{database: database, dialect: dialect, providers: providers}
}

func (s *RequirementsStore) SynchronizeConnections(ctx context.Context, requirements []integrationsdk.ConnectionRequirement) error {
	for _, requirement := range requirements {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := requirement.Validate(); err != nil {
			return err
		}
		provider, found := s.providers.Provider(strings.TrimSpace(requirement.ConnectorKey), strings.TrimSpace(requirement.ProviderKey))
		var config map[string]any
		if err := json.Unmarshal(requirement.Config, &config); err != nil {
			return fmt.Errorf("decode Integration connection %q config: %w", requirement.Key, err)
		}
		if found {
			withDefaults, err := connector.ApplyConfigDefaults(provider.Descriptor().ConfigFields, config)
			if err != nil {
				return fmt.Errorf("apply Integration connection %q defaults: %w", requirement.Key, err)
			}
			config = withDefaults
		}
		if err := s.synchronizeConnection(ctx, requirement, provider, found, config); err != nil {
			return err
		}
	}
	return nil
}

func (s *RequirementsStore) synchronizeConnection(ctx context.Context, requirement integrationsdk.ConnectionRequirement, provider connector.Adapter, providerAvailable bool, config map[string]any) error {
	query, args, err := ormbuilder.NewSelectBuilder(s.dialect, "integration_connections").
		Columns("id", "name", "status", "config_json", "secret_refs_json", "created_by").
		Where(ormbuilder.And(ormbuilder.Equal("workspace_id", requirement.WorkspaceID), ormbuilder.Equal("connection_key", requirement.Key))).Limit(1).Build()
	if err != nil {
		return fmt.Errorf("build Integration connection requirement lookup: %w", err)
	}
	var id, name, status, configJSON, secretRefsJSON, createdBy string
	lookupErr := s.database.QueryRowContext(ctx, query, args...).Scan(&id, &name, &status, &configJSON, &secretRefsJSON, &createdBy)
	if lookupErr != nil && lookupErr != sql.ErrNoRows {
		return fmt.Errorf("lookup Integration connection requirement: %w", lookupErr)
	}
	if lookupErr == nil {
		var managed map[string]any
		if json.Unmarshal([]byte(configJSON), &managed) == nil {
			for key, value := range managed {
				config[key] = value
			}
		}
		if strings.TrimSpace(name) != "" && strings.TrimSpace(requirement.Name) == "" {
			requirement.Name = name
		}
		if strings.TrimSpace(status) != "" {
			requirement.Status = status
		}
	}
	if strings.TrimSpace(requirement.Status) == "" {
		requirement.Status = "configured"
		if providerAvailable {
			requirement.Status = defaultConnectionStatus(provider.Descriptor(), config)
		}
	}
	if !validConnectionStatus(requirement.Status) {
		return fmt.Errorf("Integration connection %q has invalid status %q", requirement.Key, requirement.Status)
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode Integration connection %q config: %w", requirement.Key, err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if lookupErr == nil {
		update, updateArgs, buildErr := ormbuilder.NewUpdateBuilder(s.dialect, "integration_connections").
			Set("connector_key", requirement.ConnectorKey).Set("provider_key", requirement.ProviderKey).
			Set("name", requirement.Name).Set("status", requirement.Status).Set("config_json", string(payload)).Set("updated_at", now).
			Where(ormbuilder.Equal("id", id)).Build()
		if buildErr != nil {
			return fmt.Errorf("build Integration connection requirement update: %w", buildErr)
		}
		if _, err := s.database.ExecContext(ctx, update, updateArgs...); err != nil {
			return fmt.Errorf("update Integration connection requirement: %w", err)
		}
		return nil
	}
	hash := sha256.Sum256([]byte(requirement.WorkspaceID + "\x00" + requirement.Key))
	id = "manifest_" + hex.EncodeToString(hash[:16])
	insert, insertArgs, err := ormbuilder.NewInsertBuilder(s.dialect, "integration_connections").
		Columns("id", "connection_key", "workspace_id", "connector_key", "provider_key", "name", "status", "config_json", "secret_refs_json", "created_by", "created_at", "updated_at").
		Values(id, requirement.Key, requirement.WorkspaceID, requirement.ConnectorKey, requirement.ProviderKey, requirement.Name, requirement.Status, string(payload), `{}`, "manifest", now, now).Build()
	if err != nil {
		return fmt.Errorf("build Integration connection requirement insert: %w", err)
	}
	if _, err := s.database.ExecContext(ctx, insert, insertArgs...); err != nil {
		return fmt.Errorf("insert Integration connection requirement: %w", err)
	}
	return nil
}

func validConnectionStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "configured", "active", "inactive", "error", "revoked":
		return true
	default:
		return false
	}
}

func defaultConnectionStatus(descriptor connector.ProviderDescriptor, config map[string]any) string {
	if descriptor.StartupActivation != connector.StartupActivationDefaultSafe || len(descriptor.SecretFields) > 0 {
		return "configured"
	}
	for _, field := range descriptor.ConfigFields {
		if field.Required {
			value, present := config[field.Key]
			if !present || value == nil || strings.TrimSpace(fmt.Sprint(value)) == "" {
				return "configured"
			}
		}
	}
	return "active"
}

var _ integrationsdk.Requirements = (*RequirementsStore)(nil)
