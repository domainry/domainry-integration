package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	"github.com/domainry/domainry-orm/query"
)

const providerInboxTable = "_integration_provider_inbox"

type RoutedWebhookStore struct {
	database         modulehost.Database
	dialect          modulehost.Dialect
	delivery         *DeliveryStore
	subjectLifecycle *SubjectLifecyclePersistence
}

func NewRoutedWebhookStore(database modulehost.Database, dialect modulehost.Dialect, delivery *DeliveryStore, subjectLifecycle ...*SubjectLifecyclePersistence) *RoutedWebhookStore {
	return &RoutedWebhookStore{database: database, dialect: dialect, delivery: delivery, subjectLifecycle: subjectLifecyclePersistence(subjectLifecycle)}
}

type routedWebhookEnvelope struct {
	Routes    []providerAccountRoute      `json:"routes"`
	WakeTasks []string                    `json:"wake_tasks"`
	Events    []connector.BackgroundEvent `json:"events,omitempty"`
	Data      json.RawMessage             `json:"data"`
}

func decodeRoutedWebhookEnvelope(raw json.RawMessage) (routedWebhookEnvelope, error) {
	var value routedWebhookEnvelope
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode Integration routed webhook: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return value, fmt.Errorf("Integration routed webhook contains multiple values")
	}
	if len(value.Routes) == 0 || len(value.Routes) > 100 {
		return value, fmt.Errorf("Integration routed webhook account routes are invalid")
	}
	routeSeen := map[string]bool{}
	routes := make([]providerAccountRoute, 0, len(value.Routes))
	for _, route := range value.Routes {
		route.Kind = strings.TrimSpace(route.Kind)
		route.Value = normalizeProviderAccountRoute(route.Kind, route.Value)
		identity := route.Kind + "\x00" + route.Value
		if !canonicalProviderAccountRouteKind(route.Kind) || route.Value == "" || routeSeen[identity] {
			return value, fmt.Errorf("Integration routed webhook account routes are invalid")
		}
		routeSeen[identity] = true
		routes = append(routes, route)
	}
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Kind != routes[j].Kind {
			return routes[i].Kind < routes[j].Kind
		}
		return routes[i].Value < routes[j].Value
	})
	value.Routes = routes
	seen := map[string]bool{}
	tasks := make([]string, 0, len(value.WakeTasks))
	for _, task := range value.WakeTasks {
		task = strings.TrimSpace(task)
		if task == "" || seen[task] {
			return value, fmt.Errorf("Integration routed webhook wake tasks are invalid")
		}
		seen[task] = true
		tasks = append(tasks, task)
	}
	if len(tasks) == 0 || len(tasks) > 10 {
		return value, fmt.Errorf("Integration routed webhook wake tasks are invalid")
	}
	value.WakeTasks = tasks
	if len(value.Events) > 20 {
		return value, fmt.Errorf("Integration routed webhook events are invalid")
	}
	for _, event := range value.Events {
		if len(strings.TrimSpace(event.ExternalID)) > 1024 || len(strings.TrimSpace(event.EventType)) > 512 || event.Validate() != nil {
			return value, fmt.Errorf("Integration routed webhook events are invalid")
		}
		var object map[string]any
		if json.Unmarshal(event.Payload, &object) != nil || object == nil {
			return value, fmt.Errorf("Integration routed webhook event payload must be a JSON object")
		}
	}
	if len(value.Data) == 0 {
		value.Data = json.RawMessage(`{}`)
	}
	if len(value.Data) > 64<<10 || !json.Valid(value.Data) {
		return value, fmt.Errorf("Integration routed webhook data is invalid")
	}
	return value, nil
}

type routedWebhookTarget struct {
	workspaceID, connectionKey string
}

func (s *RoutedWebhookStore) AcceptRoutedWebhook(ctx context.Context, request integrationsdk.WebhookRequest) (integrationsdk.RoutedWebhookReceipt, error) {
	if err := request.Validate(); err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	connection, err := s.delivery.connection(ctx, integrationmodel.DeliveryRequest{WorkspaceID: request.WorkspaceID, ConnectorKey: request.ConnectorKey, ConnectionKey: request.ConnectionKey})
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	provider, ok := s.delivery.providers.Provider(connection.ConnectorKey, connection.ProviderKey)
	if !ok {
		return integrationsdk.RoutedWebhookReceipt{}, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	verifier, ok := provider.(connector.WebhookVerifier)
	if !ok {
		return integrationsdk.RoutedWebhookReceipt{}, fmt.Errorf("Integration provider %s/%s does not support routed webhooks", connection.ConnectorKey, connection.ProviderKey)
	}
	secrets, err := s.delivery.secrets.ResolveSecretReferences(ctx, request.WorkspaceID, connection.SecretRefs)
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	verified, err := verifier.VerifyWebhook(ctx, connector.VerifyWebhookRequest{
		ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey,
		Connection: connector.Connection{Key: connection.Key, WorkspaceID: connection.WorkspaceID, ConnectorKey: connection.ConnectorKey, ProviderKey: connection.ProviderKey, Status: connection.Status, Config: connection.Config, SecretRefs: connection.SecretRefs},
		Headers:    request.Headers, Query: request.Query, Secrets: secrets, Body: append([]byte(nil), request.Body...), ReceivedAt: request.ReceivedAt,
	})
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, fmt.Errorf("verify Integration routed webhook: %w", err)
	}
	if verified.Security == nil || !verified.Security.SignatureVerified {
		return integrationsdk.RoutedWebhookReceipt{}, fmt.Errorf("Integration routed webhook signature is not verified")
	}
	if verified.Challenge != "" {
		return integrationsdk.RoutedWebhookReceipt{ExternalID: strings.TrimSpace(verified.ExternalID), Challenge: verified.Challenge, Format: verified.ChallengeFormat}, nil
	}
	externalID := strings.TrimSpace(verified.ExternalID)
	if externalID == "" || len(externalID) > 1024 {
		return integrationsdk.RoutedWebhookReceipt{}, fmt.Errorf("Integration routed webhook external identity is invalid")
	}
	envelope, err := decodeRoutedWebhookEnvelope(verified.Payload)
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	routeKind, routeHash, routeBucket := routedWebhookRouteIdentity(connection.ConnectorKey, connection.ProviderKey, envelope.Routes)
	inboxDigest := sha256.Sum256([]byte(connection.WorkspaceID + "\x00" + connection.ProviderKey + "\x00" + connection.Key + "\x00" + externalID))
	inboxID := "provider_inbox_" + hex.EncodeToString(inboxDigest[:16])
	now := request.ReceivedAt.UTC()
	publishedAt := verifiedGooglePushPublishedAt(connection.ConnectorKey, connection.ProviderKey, verified.Security, now)

	tx, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	defer func() { _ = tx.Rollback() }()
	insert, args, err := query.NewInsertBuilder(s.dialect, providerInboxTable).
		Columns("id", "workspace_id", "connector_key", "provider_key", "ingress_connection_key", "external_id", "route_kind", "route_hash", "route_bucket", "payload_json", "status", "routed_count", "received_at", "published_at", "updated_at").
		Values(inboxID, connection.WorkspaceID, connection.ConnectorKey, connection.ProviderKey, connection.Key, externalID, routeKind, routeHash, routeBucket, string(envelope.Data), "received", 0, now.UnixMilli(), publishedAt, now.UnixMilli()).
		OnConflictDoNothing("workspace_id", "provider_key", "ingress_connection_key", "external_id").Build()
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	result, err := tx.ExecContext(ctx, insert, args...)
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return integrationsdk.RoutedWebhookReceipt{ExternalID: externalID, Duplicate: true}, nil
	}

	targetsByIdentity := map[string]routedWebhookTarget{}
	for _, route := range envelope.Routes {
		digest := providerAccountRouteDigest(connection.ConnectorKey, connection.ProviderKey, route.Kind, route.Value)
		currentHash := hex.EncodeToString(digest[:])
		routeQuery, routeArgs, buildErr := query.NewSelectBuilder(s.dialect, providerAccountRouteTable).
			Columns("workspace_id", "connection_key").
			Where(query.And(
				query.Equal("connector_key", connection.ConnectorKey), query.Equal("provider_key", connection.ProviderKey),
				query.Equal("route_kind", route.Kind), query.Equal("route_hash", currentHash), query.Equal("route_bucket", int(digest[0])), query.Equal("status", "active"),
			)).OrderBy(query.Ascending("workspace_id"), query.Ascending("connection_key")).Limit(1001).Build()
		if buildErr != nil {
			return integrationsdk.RoutedWebhookReceipt{}, buildErr
		}
		rows, queryErr := tx.QueryContext(ctx, routeQuery, routeArgs...)
		if queryErr != nil {
			return integrationsdk.RoutedWebhookReceipt{}, queryErr
		}
		for rows.Next() {
			var target routedWebhookTarget
			if err = rows.Scan(&target.workspaceID, &target.connectionKey); err != nil {
				_ = rows.Close()
				return integrationsdk.RoutedWebhookReceipt{}, err
			}
			targetsByIdentity[target.workspaceID+"\x00"+target.connectionKey] = target
			if len(targetsByIdentity) > 1000 {
				_ = rows.Close()
				return integrationsdk.RoutedWebhookReceipt{}, fmt.Errorf("Integration routed webhook target limit exceeded")
			}
		}
		if err = rows.Err(); err != nil {
			_ = rows.Close()
			return integrationsdk.RoutedWebhookReceipt{}, err
		}
		_ = rows.Close()
	}
	targets := make([]routedWebhookTarget, 0, len(targetsByIdentity))
	for _, target := range targetsByIdentity {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].workspaceID != targets[j].workspaceID {
			return targets[i].workspaceID < targets[j].workspaceID
		}
		return targets[i].connectionKey < targets[j].connectionKey
	})
	if len(targets) > 1000 {
		return integrationsdk.RoutedWebhookReceipt{}, fmt.Errorf("Integration routed webhook target limit exceeded")
	}
	routed := 0
	for _, target := range targets {
		ownerUserID, ownerErr := connectionOwnerUserID(ctx, tx, s.dialect, target.workspaceID, target.connectionKey)
		if ownerErr != nil {
			return integrationsdk.RoutedWebhookReceipt{}, ownerErr
		}
		for _, event := range envelope.Events {
			if _, eventErr := insertProviderEvent(ctx, tx, s.dialect, s.subjectLifecycle, target.workspaceID, connection.ConnectorKey, connection.ProviderKey, target.connectionKey, ownerUserID, event, true, now); eventErr != nil {
				return integrationsdk.RoutedWebhookReceipt{}, eventErr
			}
		}
		sequences, wakeErr := wakeProviderTasksWithSequences(ctx, tx, s.dialect, s.subjectLifecycle, target.workspaceID, target.connectionKey, envelope.WakeTasks, now)
		if wakeErr != nil {
			return integrationsdk.RoutedWebhookReceipt{}, wakeErr
		}
		if err := recordProviderWakeReceipts(ctx, tx, s.dialect, target.workspaceID, target.connectionKey, connection.ConnectorKey, connection.ProviderKey, inboxID, publishedAt, now, sequences); err != nil {
			return integrationsdk.RoutedWebhookReceipt{}, err
		}
		if len(sequences) > 0 {
			routed++
		}
	}
	status := "routed"
	if routed == 0 {
		status = "unmatched"
	}
	update, args, err := query.NewUpdateBuilder(s.dialect, providerInboxTable).
		Set("status", status).Set("routed_count", routed).Set("updated_at", now.UnixMilli()).
		Where(query.And(query.Equal("workspace_id", connection.WorkspaceID), query.Equal("id", inboxID), query.Equal("status", "received"))).Build()
	if err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	if _, err = tx.ExecContext(ctx, update, args...); err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	if err = tx.Commit(); err != nil {
		return integrationsdk.RoutedWebhookReceipt{}, err
	}
	return integrationsdk.RoutedWebhookReceipt{ExternalID: externalID, Routed: routed}, nil
}

func verifiedGooglePushPublishedAt(connectorKey, providerKey string, evidence *connector.WebhookSecurityEvidence, receivedAt time.Time) int64 {
	if connectorKey != "google_workspace" || providerKey != "google" || evidence == nil || evidence.EventTime.IsZero() {
		return 0
	}
	publishedAt := evidence.EventTime.UTC()
	if publishedAt.After(receivedAt) || publishedAt.Before(receivedAt.Add(-7*24*time.Hour)) {
		return 0
	}
	return publishedAt.UnixMilli()
}

func routedWebhookRouteIdentity(connectorKey, providerKey string, routes []providerAccountRoute) (string, string, int) {
	if len(routes) == 1 {
		digest := providerAccountRouteDigest(connectorKey, providerKey, routes[0].Kind, routes[0].Value)
		return routes[0].Kind, hex.EncodeToString(digest[:]), int(digest[0])
	}
	canonical := strings.Builder{}
	for _, route := range routes {
		canonical.WriteString(route.Kind)
		canonical.WriteByte('\x00')
		canonical.WriteString(route.Value)
		canonical.WriteByte('\x00')
	}
	digest := sha256.Sum256([]byte(connectorKey + "\x00" + providerKey + "\x00" + canonical.String()))
	return "multi", hex.EncodeToString(digest[:]), int(digest[0])
}

var _ integrationsdk.RoutedWebhooks = (*RoutedWebhookStore)(nil)
