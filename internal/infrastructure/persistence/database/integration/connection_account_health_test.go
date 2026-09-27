package integration

import (
	"testing"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

func TestProviderTaskHealthSeparatesFirstRunDelayAndFailure(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, status string
		due          time.Time
		lastSuccess  string
		want         integrationsdk.ConnectionAccountHealthState
	}{
		{"newly scheduled", "ready", now, "", integrationsdk.ConnectionAccountHealthInitializing},
		{"previously succeeded", "ready", now, timestampString(now.Add(-time.Hour).UnixMilli()), integrationsdk.ConnectionAccountHealthHealthy},
		{"exact grace boundary", "ready", now.Add(-providerTaskHealthGrace), "", integrationsdk.ConnectionAccountHealthInitializing},
		{"queue delay", "ready", now.Add(-providerTaskHealthGrace - time.Millisecond), "", integrationsdk.ConnectionAccountHealthDelayed},
		{"stuck processing", "processing", now.Add(-providerTaskHealthGrace - time.Millisecond), "", integrationsdk.ConnectionAccountHealthDelayed},
		{"automatic retry failure", "failed", now.Add(time.Minute), "", integrationsdk.ConnectionAccountHealthFailing},
		{"dead letter", "dead_letter", time.Time{}, "", integrationsdk.ConnectionAccountHealthFailing},
		{"cancelled", "cancelled", time.Time{}, "", integrationsdk.ConnectionAccountHealthInactive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := integrationsdk.ConnectionAccountBackgroundTask{Status: tc.status, DueAt: timestampString(tc.due.UnixMilli()), LastSuccessAt: tc.lastSuccess}
			if tc.due.IsZero() {
				task.DueAt = ""
			}
			if got := providerTaskHealth(task, now); got != tc.want {
				t.Fatalf("task health=%s want=%s", got, tc.want)
			}
		})
	}
	if got := providerTaskHealth(integrationsdk.ConnectionAccountBackgroundTask{Status: "ready", DueAt: timestampString(now.Add(time.Minute).UnixMilli()), LastSuccessAt: timestampString(now.Add(-time.Hour).UnixMilli()), LastErrorCode: "previous.failure"}, now); got != integrationsdk.ConnectionAccountHealthFailing {
		t.Fatalf("scheduled retry hid the previous failure: %s", got)
	}
}

func TestConnectionHealthUsesWorstVisibleTaskWithoutCallingReadyHealthy(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	account := integrationsdk.ConnectionAccount{Status: "active", Background: []integrationsdk.ConnectionAccountBackgroundTask{
		{TaskKey: "calendar", Status: "ready", DueAt: timestampString(now.Add(time.Hour).UnixMilli()), LastSuccessAt: timestampString(now.Add(-time.Hour).UnixMilli())},
		{TaskKey: "mail", Status: "ready", DueAt: timestampString(now.UnixMilli())},
	}}
	summarizeConnectionAccountHealth(&account, now)
	if account.HealthState != integrationsdk.ConnectionAccountHealthInitializing || account.Background[0].HealthState != integrationsdk.ConnectionAccountHealthHealthy || account.Background[1].HealthState != integrationsdk.ConnectionAccountHealthInitializing {
		t.Fatalf("first run was mislabeled healthy: %#v", account)
	}
	account.Background[1].Status = "failed"
	summarizeConnectionAccountHealth(&account, now)
	if account.HealthState != integrationsdk.ConnectionAccountHealthFailing {
		t.Fatalf("failure did not dominate account health: %#v", account)
	}
	account.Status = "revoked"
	summarizeConnectionAccountHealth(&account, now)
	if account.HealthState != integrationsdk.ConnectionAccountHealthInactive {
		t.Fatalf("revoked account was not inactive: %#v", account)
	}
	account.Status, account.Background = "active", nil
	summarizeConnectionAccountHealth(&account, now)
	if account.HealthState != integrationsdk.ConnectionAccountHealthNotApplicable {
		t.Fatalf("account without background tasks was reported healthy: %#v", account)
	}
}
