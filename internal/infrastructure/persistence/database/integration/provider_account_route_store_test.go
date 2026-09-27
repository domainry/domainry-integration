package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type providerAccountRouteTestProvider struct {
	details json.RawMessage
}

func (*providerAccountRouteTestProvider) Descriptor() connector.ProviderDescriptor {
	return connector.ProviderDescriptor{ConnectorKey: "mail", ProviderKey: "route_probe", ProviderRevision: "1.0.0"}
}

func (*providerAccountRouteTestProvider) Call(context.Context, connector.CallRequest) (connector.CallResult, error) {
	return connector.CallResult{}, nil
}

func (p *providerAccountRouteTestProvider) TestConnection(context.Context, connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	return connector.TestConnectionResult{Connected: true, Details: append(json.RawMessage(nil), p.details...)}, nil
}

func TestConnectionProbePersistsHashedProviderAccountRoute(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-account-route")
	provider := &providerAccountRouteTestProvider{details: json.RawMessage(`{"provider_account":{"subject":"provider-subject-1","routes":[{"kind":"email","value":"Person@Example.Test"}]}}`)}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	connection, err := management.UpsertConnection(t.Context(), "workspace-a", "personal-mail", "user-a", integrationsdk.ConnectionInput{ConnectorKey: "mail", ProviderKey: "route_probe", Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = management.TestConnection(t.Context(), connection.WorkspaceID, connection.Key, integrationsdk.ConnectionTestRequest{}); err != nil {
		t.Fatal(err)
	}

	digest := providerAccountRouteDigest("mail", "route_probe", "email", "person@example.test")
	var subjectHash, routeHash, status string
	var bucket int
	if err = database.QueryRowContext(t.Context(), "SELECT subject_hash,route_hash,route_bucket,status FROM _integration_provider_account_routes WHERE workspace_id=? AND connection_key=?", "workspace-a", "personal-mail").Scan(&subjectHash, &routeHash, &bucket, &status); err != nil {
		t.Fatal(err)
	}
	if subjectHash == "provider-subject-1" || routeHash != hex.EncodeToString(digest[:]) || bucket != int(digest[0]) || status != "active" {
		t.Fatalf("subject_hash=%q route_hash=%q bucket=%d status=%q", subjectHash, routeHash, bucket, status)
	}
	var leaked int
	if err = database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_provider_account_routes WHERE subject_hash LIKE '%provider-subject%' OR route_hash LIKE '%example.test%'").Scan(&leaked); err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Fatal("provider account PII was stored in routing columns")
	}

	if _, err = management.SetConnectionStatus(t.Context(), "workspace-a", "personal-mail", "inactive", "user-a"); err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(t.Context(), "SELECT status FROM _integration_provider_account_routes WHERE workspace_id=? AND connection_key=?", "workspace-a", "personal-mail").Scan(&status); err != nil || status != "inactive" {
		t.Fatalf("status=%q err=%v", status, err)
	}
	if _, err = management.SetConnectionStatus(t.Context(), "workspace-a", "personal-mail", "active", "user-a"); err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(t.Context(), "SELECT status FROM _integration_provider_account_routes WHERE workspace_id=? AND connection_key=?", "workspace-a", "personal-mail").Scan(&status); err != nil || status != "active" {
		t.Fatalf("status=%q err=%v", status, err)
	}
	if err = management.DeleteConnection(t.Context(), "workspace-a", "personal-mail"); err != nil {
		t.Fatal(err)
	}
	var routes int
	if err = database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_provider_account_routes WHERE workspace_id=? AND connection_key=?", "workspace-a", "personal-mail").Scan(&routes); err != nil || routes != 0 {
		t.Fatalf("routes=%d err=%v", routes, err)
	}
}

func TestMalformedProviderAccountProbeCannotReplaceRoutes(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-account-route-invalid")
	provider := &providerAccountRouteTestProvider{details: json.RawMessage(`{"provider_account":{"subject":"provider-subject-1","routes":[{"kind":"email","value":"not-an-email"}]}}`)}
	delivery := NewDeliveryStore(database, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(database, dialect, nil, delivery)
	if _, err := management.UpsertConnection(t.Context(), "workspace-a", "personal-mail", "user-a", integrationsdk.ConnectionInput{ConnectorKey: "mail", ProviderKey: "route_probe", Status: "active"}); err != nil {
		t.Fatal(err)
	}
	if _, err := management.TestConnection(t.Context(), "workspace-a", "personal-mail", integrationsdk.ConnectionTestRequest{}); err == nil {
		t.Fatal("malformed provider account route was accepted")
	}
	var routes int
	if err := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM _integration_provider_account_routes").Scan(&routes); err != nil || routes != 0 {
		t.Fatalf("routes=%d err=%v", routes, err)
	}
}
