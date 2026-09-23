package definitionfixture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

type Store struct {
	mu       sync.Mutex
	values   map[string]metadatasdk.Definition
	versions map[string]metadatasdk.DefinitionVersion
	revision uint64
}

func NewStore() *Store {
	return &Store{values: map[string]metadatasdk.Definition{}, versions: map[string]metadatasdk.DefinitionVersion{}}
}

func definitionKey(owner, kind, key string) string {
	return strings.TrimSpace(owner) + "\x00" + strings.TrimSpace(kind) + "\x00" + strings.TrimSpace(key)
}

func (s *Store) ReplaceSourceSnapshot(_ context.Context, snapshot metadatasdk.ProjectionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	owner, sourceID := strings.TrimSpace(snapshot.Owner), strings.TrimSpace(snapshot.SourceID)
	if owner == "" || sourceID == "" {
		return fmt.Errorf("definition snapshot owner and source are required")
	}
	desired := map[string]bool{}
	for _, definition := range snapshot.Definitions {
		definition.Owner = owner
		if definition.SchemaVersion == "" {
			definition.SchemaVersion = snapshot.SchemaVersion
		}
		if definition.SourceKind == "" {
			definition.SourceKind = snapshot.SourceKind
		}
		definition.SourceID = sourceID
		key := definitionKey(definition.Owner, definition.ResourceType, definition.ResourceKey)
		desired[key] = true
		s.publishLocked(definition)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for key, definition := range s.values {
		if definition.Owner == owner && definition.SourceID == sourceID && !desired[key] && definition.Status == "active" {
			definition.Status, definition.DisabledAt, definition.UpdatedAt = "disabled", now, now
			s.values[key] = definition
		}
	}
	return nil
}

func (s *Store) Publish(_ context.Context, command metadatasdk.DefinitionPublishCommand) (metadatasdk.DefinitionPublishResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := definitionKey(command.Owner, command.ResourceType, command.ResourceKey)
	current, found := s.values[key]
	expected := strings.TrimSpace(command.ExpectedCurrentVersionID)
	if !found && expected != metadatasdk.DefinitionNoCurrentVersion || found && expected != current.CurrentVersionID {
		return metadatasdk.DefinitionPublishResult{}, fmt.Errorf("definition revision conflict")
	}
	definition := metadatasdk.Definition{
		Owner: command.Owner, ResourceType: command.ResourceType, ResourceKey: command.ResourceKey, ObjectKey: command.ObjectKey,
		Name: command.Name, Payload: append([]byte(nil), command.Payload...), SchemaVersion: command.SchemaVersion, SchemaHash: command.SchemaHash,
		SourceKind: command.SourceKind, SourceID: command.SourceID, PublishedBy: command.PublishedBy,
	}
	s.publishLocked(definition)
	current = s.values[key]
	return metadatasdk.DefinitionPublishResult{Definition: cloneDefinition(current), CurrentVersionID: current.CurrentVersionID}, nil
}

func (s *Store) publishLocked(definition metadatasdk.Definition) {
	s.revision++
	now := time.Now().UTC().Format(time.RFC3339Nano)
	digest := sha256.Sum256(append(append([]byte(definition.ResourceKey), definition.Payload...), byte(s.revision)))
	versionID := "version-" + hex.EncodeToString(digest[:16])
	key := definitionKey(definition.Owner, definition.ResourceType, definition.ResourceKey)
	if current, found := s.values[key]; found {
		definition.CreatedAt = current.CreatedAt
	}
	if definition.CreatedAt == "" {
		definition.CreatedAt = now
	}
	definition.CurrentVersionID, definition.Status, definition.PublishedAt, definition.UpdatedAt = versionID, "active", now, now
	definition.DisabledAt, definition.DisabledBy = "", ""
	definition.Payload = append([]byte(nil), definition.Payload...)
	s.values[key] = definition
	s.versions[versionID] = metadatasdk.DefinitionVersion{
		ID: versionID, Owner: definition.Owner, ResourceType: definition.ResourceType, ResourceKey: definition.ResourceKey,
		SchemaVersion: definition.SchemaVersion, SchemaHash: definition.SchemaHash, Payload: append([]byte(nil), definition.Payload...), CreatedAt: now,
	}
}

func (s *Store) Disable(_ context.Context, command metadatasdk.DefinitionDisableCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := definitionKey(command.Owner, command.ResourceType, command.ResourceKey)
	definition, found := s.values[key]
	if !found || definition.CurrentVersionID != command.ExpectedCurrentVersionID {
		return fmt.Errorf("definition revision conflict")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	definition.Status, definition.DisabledAt, definition.DisabledBy, definition.UpdatedAt = "disabled", now, command.DisabledBy, now
	s.values[key] = definition
	return nil
}

func (s *Store) List(_ context.Context, query metadatasdk.DefinitionQuery) ([]metadatasdk.Definition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []metadatasdk.Definition{}
	for _, definition := range s.values {
		if definition.Status != "active" || query.Owner != "" && definition.Owner != query.Owner ||
			query.ResourceType != "" && definition.ResourceType != query.ResourceType || query.SourceID != "" && definition.SourceID != query.SourceID {
			continue
		}
		result = append(result, cloneDefinition(definition))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Owner != result[j].Owner {
			return result[i].Owner < result[j].Owner
		}
		if result[i].ResourceType != result[j].ResourceType {
			return result[i].ResourceType < result[j].ResourceType
		}
		return result[i].ResourceKey < result[j].ResourceKey
	})
	return result, nil
}

func (s *Store) Get(_ context.Context, owner, kind, key string) (metadatasdk.Definition, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	definition, found := s.values[definitionKey(owner, kind, key)]
	if !found || definition.Status != "active" {
		return metadatasdk.Definition{}, false, nil
	}
	return cloneDefinition(definition), true, nil
}

func (s *Store) Snapshot(ctx context.Context, query metadatasdk.DefinitionQuery) (metadatasdk.DefinitionSnapshot, error) {
	values, err := s.List(ctx, query)
	return metadatasdk.DefinitionSnapshot{Definitions: values}, err
}

func (s *Store) GetVersion(_ context.Context, query metadatasdk.DefinitionVersionQuery) (metadatasdk.DefinitionVersion, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	version, found := s.versions[query.VersionID]
	if !found || version.Owner != query.Owner || version.ResourceType != query.ResourceType || version.ResourceKey != query.ResourceKey {
		return metadatasdk.DefinitionVersion{}, false, nil
	}
	version.Payload = append([]byte(nil), version.Payload...)
	return version, true, nil
}

func cloneDefinition(value metadatasdk.Definition) metadatasdk.Definition {
	value.Payload = append([]byte(nil), value.Payload...)
	return value
}

var _ metadatasdk.DefinitionStore = (*Store)(nil)
