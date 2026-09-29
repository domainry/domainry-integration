package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

type providerQuotaObserverFixture struct {
	snapshot integrationsdk.ProviderQuotaSnapshot
	err      error
}

func (f providerQuotaObserverFixture) ObserveProviderQuota(context.Context, time.Time) (integrationsdk.ProviderQuotaSnapshot, error) {
	return f.snapshot, f.err
}

func TestProviderRunSnapshotSeparatesOfficialQuotaStatesFromInternalRateWindow(t *testing.T) {
	database, dialect := webPushTestDatabase(t, "provider-quota-monitoring")
	store := NewManagementStore(database, dialect, nil, nil)
	now := time.Date(2026, 9, 29, 2, 10, 0, 0, time.UTC)

	withoutConfiguration, err := store.providerRunSnapshot(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if withoutConfiguration.GoogleOfficialQuota.Status != integrationsdk.ProviderQuotaStatusNotConfigured || withoutConfiguration.GlobalRateLimit == 0 {
		t.Fatalf("snapshot=%+v", withoutConfiguration)
	}

	store.ConfigureProviderQuotaObserver(providerQuotaObserverFixture{err: errors.New("secret upstream body")})
	unavailable, err := store.providerRunSnapshot(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if unavailable.GoogleOfficialQuota.Status != integrationsdk.ProviderQuotaStatusUnavailable || unavailable.GoogleOfficialQuota.ErrorCode != "google.monitoring_unavailable" {
		t.Fatalf("snapshot=%+v", unavailable.GoogleOfficialQuota)
	}
	if value := unavailable.GoogleOfficialQuota.ErrorCode; value == "" || value == "secret upstream body" {
		t.Fatalf("unbounded upstream error leaked: %q", value)
	}

	percent := int64(82)
	store.ConfigureProviderQuotaObserver(providerQuotaObserverFixture{snapshot: integrationsdk.ProviderQuotaSnapshot{
		Source: "google_cloud_monitoring", Status: integrationsdk.ProviderQuotaStatusAvailable,
		ObservedAt: now.Format(time.RFC3339Nano), MaxUsedPercent: &percent,
	}})
	available, err := store.providerRunSnapshot(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if available.GoogleOfficialQuota.MaxUsedPercent == nil || *available.GoogleOfficialQuota.MaxUsedPercent != 82 {
		t.Fatalf("snapshot=%+v", available.GoogleOfficialQuota)
	}
}
