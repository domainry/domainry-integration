package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/requestcontext"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type connectionTestReceiptProvider struct {
	providerAccountRouteTestProvider
	err error
}

func (p *connectionTestReceiptProvider) TestConnection(context.Context, connector.TestConnectionRequest) (connector.TestConnectionResult, error) {
	return connector.TestConnectionResult{
		Connected: p.err == nil,
		Details:   json.RawMessage(`{"value":"provider-private-test-result"}`),
	}, p.err
}

func TestManagementConnectionTestRecordsSafeTenantScopedHistory(t *testing.T) {
	db, dialect := webPushTestDatabase(t, "connection-test-history")
	provider := &connectionTestReceiptProvider{}
	delivery := NewDeliveryStore(db, dialect, deliveryTestProviders{provider: provider}, emptyDeliveryTestSecrets{})
	management := NewManagementStore(db, dialect, nil, delivery)
	ctx := requestcontext.WithRequestID(t.Context(), "connection-test-request")
	connection, err := management.UpsertConnection(ctx, "tenant-a", "mail", "admin-a", integrationsdk.ConnectionInput{ConnectorKey: "mail", ProviderKey: "route_probe"})
	if err != nil {
		t.Fatal(err)
	}
	success, err := management.TestConnection(ctx, "tenant-a", "mail", integrationsdk.ConnectionTestRequest{})
	if err != nil {
		t.Fatal(err)
	}
	provider.err = connector.PermanentError("mail.authentication_failed", errors.New("credential in provider error"))
	failed, err := management.TestConnection(ctx, "tenant-a", "mail", integrationsdk.ConnectionTestRequest{})
	if err == nil || failed.Receipt.Status != integrationsdk.DeliveryStatusFailed {
		t.Fatal("expected failed probe", err)
	}
	operations := NewOperationsStore(db, dialect, delivery, nil, newTestDefinitionStore())
	query := integrationmodel.InvocationQuery{WorkspaceID: "tenant-a", ConnectionKey: "mail", Operation: connectionTestOperation}
	rows, err := operations.ListInvocations(ctx, query)
	if err != nil || len(rows) != 2 {
		t.Fatalf("read test history: result count=%d err=%v", len(rows), err)
	}
	if rows[0].InvocationID != failed.Receipt.MessageID || rows[1].InvocationID != success.Receipt.MessageID ||
		rows[0].Error != "mail.authentication_failed" || rows[0].Status != "failed" || rows[1].Status != "succeeded" ||
		rows[0].RequestRef != "connection-test-request" || rows[0].Metadata["connection_updated_at"] != connection.UpdatedAt ||
		rows[0].UpdatedAt == "" {
		t.Fatal("test outcomes are not tied to the tested configuration and request")
	}
	body, err := json.Marshal(rows)
	if err != nil || strings.Contains(string(body), "provider-private") || strings.Contains(string(body), "credential in provider error") {
		t.Fatal("probe material entered invocation history")
	}
	query.Limit = 1
	latest, err := operations.ListInvocations(ctx, query)
	if err != nil || len(latest) != 1 || latest[0].InvocationID != failed.Receipt.MessageID {
		t.Fatalf("latest test query returned %d rows: %v", len(latest), err)
	}
	if ordinary, err := operations.ListInvocations(ctx, integrationmodel.InvocationQuery{WorkspaceID: "tenant-a"}); err != nil || len(ordinary) != 0 {
		t.Fatalf("test receipts contaminated business invocation history: %d, %v", len(ordinary), err)
	}
	query.WorkspaceID = "tenant-b"
	if foreign, err := operations.ListInvocations(ctx, query); err != nil || len(foreign) != 0 {
		t.Fatalf("test receipts crossed workspace boundary: %d, %v", len(foreign), err)
	}
	worker := &WorkerStore{database: db, dialect: dialect}
	if candidates, err := worker.failedInvocationsAcrossWorkspaces(ctx, 10, time.Now()); err != nil || len(candidates) != 0 {
		t.Fatalf("management probe entered background reconciliation: %d, %v", len(candidates), err)
	}
}
