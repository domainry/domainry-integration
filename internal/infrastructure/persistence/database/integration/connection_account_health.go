package integration

import (
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

// A run can legitimately hold a six-minute worker lease. Two lease windows
// without finishing an already-due run indicate queue/worker delay, not merely
// normal scheduling. This is a read-time signal; provider state stays owned by
// the durable run and is never rewritten by health projection.
const providerTaskHealthGrace = 2 * providerTaskLeaseDuration

func providerTaskHealth(task integrationsdk.ConnectionAccountBackgroundTask, now time.Time) integrationsdk.ConnectionAccountHealthState {
	switch task.Status {
	case "failed", "dead_letter":
		return integrationsdk.ConnectionAccountHealthFailing
	case "cancelled":
		return integrationsdk.ConnectionAccountHealthInactive
	case "ready", "processing":
		if task.LastErrorCode != "" {
			return integrationsdk.ConnectionAccountHealthFailing
		}
		if task.DueAt == "" {
			return integrationsdk.ConnectionAccountHealthUnknown
		}
		due, err := time.Parse(time.RFC3339Nano, task.DueAt)
		if err != nil {
			return integrationsdk.ConnectionAccountHealthUnknown
		}
		if now.Sub(due) > providerTaskHealthGrace {
			return integrationsdk.ConnectionAccountHealthDelayed
		}
		if task.LastSuccessAt == "" {
			return integrationsdk.ConnectionAccountHealthInitializing
		}
		return integrationsdk.ConnectionAccountHealthHealthy
	default:
		return integrationsdk.ConnectionAccountHealthUnknown
	}
}

func summarizeConnectionAccountHealth(account *integrationsdk.ConnectionAccount, now time.Time) {
	if len(account.Background) == 0 {
		account.HealthState = integrationsdk.ConnectionAccountHealthNotApplicable
	} else {
		account.HealthState = integrationsdk.ConnectionAccountHealthHealthy
		for i := range account.Background {
			task := &account.Background[i]
			task.HealthState = providerTaskHealth(*task, now)
			if connectionHealthPriority(task.HealthState) > connectionHealthPriority(account.HealthState) {
				account.HealthState = task.HealthState
			}
		}
	}
	if account.Status != "active" {
		account.HealthState = integrationsdk.ConnectionAccountHealthInactive
	}
}

func connectionHealthPriority(state integrationsdk.ConnectionAccountHealthState) int {
	switch state {
	case integrationsdk.ConnectionAccountHealthFailing:
		return 5
	case integrationsdk.ConnectionAccountHealthDelayed:
		return 4
	case integrationsdk.ConnectionAccountHealthUnknown:
		return 3
	case integrationsdk.ConnectionAccountHealthInitializing:
		return 2
	case integrationsdk.ConnectionAccountHealthHealthy:
		return 1
	default:
		return 0
	}
}
