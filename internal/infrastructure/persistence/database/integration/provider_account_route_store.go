package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-orm/query"
)

const providerAccountRouteTable = "_integration_provider_account_routes"

type providerAccountProbe struct {
	Subject string                 `json:"subject"`
	Routes  []providerAccountRoute `json:"routes"`
}

type providerAccountRoute struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

func decodeProviderAccountProbe(raw json.RawMessage) (*providerAccountProbe, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var envelope struct {
		ProviderAccount json.RawMessage `json:"provider_account"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("decode Integration provider account probe: %w", err)
	}
	if len(envelope.ProviderAccount) == 0 || string(envelope.ProviderAccount) == "null" {
		return nil, nil
	}
	var probe providerAccountProbe
	decoder := json.NewDecoder(strings.NewReader(string(envelope.ProviderAccount)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&probe); err != nil {
		return nil, fmt.Errorf("decode Integration provider account identity: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("Integration provider account identity contains multiple values")
	}
	probe.Subject = strings.TrimSpace(probe.Subject)
	if probe.Subject == "" || len(probe.Subject) > 1024 {
		return nil, fmt.Errorf("Integration provider account subject is invalid")
	}
	if len(probe.Routes) == 0 || len(probe.Routes) > 10 {
		return nil, fmt.Errorf("Integration provider account routes are invalid")
	}
	seen := map[string]bool{}
	for index := range probe.Routes {
		route := &probe.Routes[index]
		route.Kind = strings.TrimSpace(route.Kind)
		route.Value = normalizeProviderAccountRoute(route.Kind, route.Value)
		if !canonicalProviderAccountRouteKind(route.Kind) || route.Value == "" || len(route.Value) > 2048 {
			return nil, fmt.Errorf("Integration provider account route is invalid")
		}
		key := route.Kind + "\x00" + route.Value
		if seen[key] {
			return nil, fmt.Errorf("Integration provider account route is duplicated")
		}
		seen[key] = true
	}
	return &probe, nil
}

func canonicalProviderAccountRouteKind(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_') {
			return false
		}
	}
	return true
}

func normalizeProviderAccountRoute(kind, value string) string {
	value = strings.TrimSpace(value)
	if kind == "email" {
		value = strings.ToLower(value)
		if strings.Count(value, "@") != 1 || strings.IndexByte(value, '@') == 0 || strings.LastIndexByte(value, '@') == len(value)-1 || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
			return ""
		}
	}
	return value
}

func providerAccountRouteDigest(connectorKey, providerKey, kind, value string) [32]byte {
	return sha256.Sum256([]byte(strings.TrimSpace(connectorKey) + "\x00" + strings.TrimSpace(providerKey) + "\x00" + kind + "\x00" + value))
}

func providerAccountSubjectHash(providerKey, subject string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(providerKey) + "\x00" + strings.TrimSpace(subject)))
	return hex.EncodeToString(digest[:])
}

// providerAccountConnectionKey reuses a connection already bound to the same
// upstream account. For a first authorization the stable provider subject,
// rather than the one-time OAuth session, owns the key. The connections table's
// workspace/key uniqueness then closes concurrent duplicate authorizations.
func (s *ManagementStore) providerAccountConnectionKey(ctx context.Context, workspaceID, connectorKey, providerKey string, probe *providerAccountProbe) (string, error) {
	if probe == nil || strings.TrimSpace(probe.Subject) == "" {
		return "", fmt.Errorf("Integration provider account identity is unavailable")
	}
	subjectHash := providerAccountSubjectHash(providerKey, probe.Subject)
	statement, args, err := query.NewSelectBuilder(s.dialect, providerAccountRouteTable).
		Columns("connection_key").
		Where(query.And(
			query.Equal("workspace_id", strings.TrimSpace(workspaceID)),
			query.Equal("connector_key", strings.TrimSpace(connectorKey)),
			query.Equal("provider_key", strings.TrimSpace(providerKey)),
			query.Equal("subject_hash", subjectHash),
		)).Build()
	if err != nil {
		return "", err
	}
	rows, err := s.database.QueryContext(ctx, statement, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	keys := map[string]bool{}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return "", err
		}
		key = strings.TrimSpace(key)
		if key != "" {
			keys[key] = true
		}
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if len(keys) > 1 {
		return "", fmt.Errorf("Integration provider account is already bound to multiple connections")
	}
	for key := range keys {
		return key, nil
	}
	identity := strings.TrimSpace(connectorKey) + "\x00" + strings.TrimSpace(providerKey) + "\x00" + strings.TrimSpace(probe.Subject)
	return ownerID("account_oauth_", workspaceID, identity), nil
}

func (s *ManagementStore) replaceProviderAccountRoutes(ctx context.Context, tested integrationsdk.Connection, raw json.RawMessage) error {
	probe, err := decodeProviderAccountProbe(raw)
	if err != nil || probe == nil {
		return err
	}
	if s.transactions != nil {
		return s.withTransaction(ctx, func(store *ManagementStore) error {
			return store.replaceProviderAccountRoutes(ctx, tested, raw)
		})
	}
	current, err := s.GetConnection(ctx, tested.WorkspaceID, tested.Key)
	if err != nil {
		return err
	}
	if current.ConnectorKey != tested.ConnectorKey || current.ProviderKey != tested.ProviderKey || current.UpdatedAt != tested.UpdatedAt || current.Status != tested.Status {
		return fmt.Errorf("Integration connection changed while resolving its provider account")
	}
	if err = guardSubjectWrite(ctx, s.database, s.dialect, s.subjectLifecycle, current.WorkspaceID, subjectFenceReference{"connection", "", current.Key}); err != nil {
		return err
	}
	where := query.And(query.Equal("workspace_id", current.WorkspaceID), query.Equal("connection_key", current.Key))
	statement, args, err := query.NewDeleteBuilder(s.dialect, providerAccountRouteTable).Where(where).Build()
	if err != nil {
		return err
	}
	if _, err = s.database.ExecContext(ctx, statement, args...); err != nil {
		return err
	}
	now := timestampMillis(ownerNow())
	subjectHash := providerAccountSubjectHash(current.ProviderKey, probe.Subject)
	for _, route := range probe.Routes {
		digest := providerAccountRouteDigest(current.ConnectorKey, current.ProviderKey, route.Kind, route.Value)
		hash := hex.EncodeToString(digest[:])
		id := ownerID("provider_account_route_", current.WorkspaceID, current.Key+"\x00"+route.Kind+"\x00"+hash)
		insert, values, buildErr := query.NewInsertBuilder(s.dialect, providerAccountRouteTable).
			Columns("id", "workspace_id", "connection_key", "connector_key", "provider_key", "subject_hash", "route_kind", "route_hash", "route_bucket", "status", "created_at", "updated_at").
			Values(id, current.WorkspaceID, current.Key, current.ConnectorKey, current.ProviderKey, subjectHash, route.Kind, hash, int(digest[0]), providerAccountRouteStatus(current.Status), now, now).Build()
		if buildErr != nil {
			return buildErr
		}
		if _, err = s.database.ExecContext(ctx, insert, values...); err != nil {
			return fmt.Errorf("persist Integration provider account route: %w", err)
		}
	}
	return nil
}

func providerAccountRouteStatus(connectionStatus string) string {
	if connectionStatus == "active" {
		return "active"
	}
	return "inactive"
}

func (s *ManagementStore) syncProviderAccountRouteStatus(ctx context.Context, connection integrationsdk.Connection) error {
	where := query.And(query.Equal("workspace_id", connection.WorkspaceID), query.Equal("connection_key", connection.Key))
	remove, args, err := query.NewDeleteBuilder(s.dialect, providerAccountRouteTable).Where(query.And(where, query.Or(query.NotEqual("connector_key", connection.ConnectorKey), query.NotEqual("provider_key", connection.ProviderKey)))).Build()
	if err != nil {
		return err
	}
	if _, err = s.database.ExecContext(ctx, remove, args...); err != nil {
		return err
	}
	update, args, err := query.NewUpdateBuilder(s.dialect, providerAccountRouteTable).
		Set("status", providerAccountRouteStatus(connection.Status)).Set("updated_at", timestampMillis(ownerNow())).
		Where(query.And(where, query.Equal("connector_key", connection.ConnectorKey), query.Equal("provider_key", connection.ProviderKey))).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, update, args...)
	return err
}

func (s *ManagementStore) setProviderAccountRoutesInactive(ctx context.Context, workspaceID, connectionKey string) error {
	statement, args, err := query.NewUpdateBuilder(s.dialect, providerAccountRouteTable).
		Set("status", "inactive").Set("updated_at", timestampMillis(ownerNow())).
		Where(query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("connection_key", strings.TrimSpace(connectionKey)), query.NotEqual("status", "inactive"))).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, statement, args...)
	return err
}

func (s *ManagementStore) deleteProviderAccountRoutes(ctx context.Context, workspaceID, connectionKey string) error {
	statement, args, err := query.NewDeleteBuilder(s.dialect, providerAccountRouteTable).
		Where(query.And(query.Equal("workspace_id", strings.TrimSpace(workspaceID)), query.Equal("connection_key", strings.TrimSpace(connectionKey)))).Build()
	if err != nil {
		return err
	}
	_, err = s.database.ExecContext(ctx, statement, args...)
	return err
}
