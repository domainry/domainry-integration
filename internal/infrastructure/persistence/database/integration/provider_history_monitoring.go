package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

const (
	googleGmailHistoryMessagesAddedObservationKey = "google.gmail.history.messages_added"
	googleGmailHistoryCursorExpiredObservationKey = "google.gmail.history.cursor_expired"
)

// recordProviderHistoryObservations persists only exact Google Gmail facts.
// A history delta is evidence of a missed or late Push only when no verified
// Push receipt covers the task wake sequence claimed by this transition.
func recordProviderHistoryObservations(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, candidate providerTaskCandidate, observations []connector.BackgroundObservation, now time.Time) error {
	if candidate.connectorKey != "google_workspace" || candidate.providerKey != "google" || candidate.taskKey != "gmail_sync" || len(observations) == 0 {
		return nil
	}
	coveredByPush := false
	coverageRead := false
	for _, observation := range observations {
		metricKey := ""
		switch observation.Key {
		case googleGmailHistoryMessagesAddedObservationKey:
			if !coverageRead {
				var err error
				coveredByPush, err = hasCoveringGooglePushReceipt(ctx, database, dialect, candidate)
				if err != nil {
					return err
				}
				coverageRead = true
			}
			if coveredByPush {
				continue
			}
			metricKey = googleGmailHistoryMessagesWithoutPushMetricKey
		case googleGmailHistoryCursorExpiredObservationKey:
			metricKey = googleGmailHistoryCursorExpiredMetricKey
		default:
			continue
		}
		eventID := strconv.FormatInt(candidate.id, 10) + ":" + strconv.FormatInt(candidate.fencingToken+1, 10) + ":" + observation.Key
		if err := recordProviderMetricValue(ctx, database, dialect, metricKey, eventID, observation.Value, now); err != nil {
			return fmt.Errorf("record Integration Provider history observation %s: %w", observation.Key, err)
		}
	}
	return nil
}

func hasCoveringGooglePushReceipt(ctx context.Context, database sqlhost.DBTX, dialect modulehost.Dialect, candidate providerTaskCandidate) (bool, error) {
	statement, args, err := query.NewSelectBuilder(dialect, providerWakeReceiptTable).
		Columns("id").Where(query.And(
		query.Equal("workspace_id", candidate.workspaceID), query.Equal("connection_key", candidate.connectionKey), query.Equal("task_key", candidate.taskKey),
		query.Equal("connector_key", "google_workspace"), query.Equal("provider_key", "google"), query.Equal("status", "pending"),
		query.LessThanOrEqual("wake_sequence", candidate.wakeSequence),
	)).Limit(1).Build()
	if err != nil {
		return false, err
	}
	var id int64
	err = database.QueryRowContext(ctx, statement, args...).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read covering Google Push receipt: %w", err)
	}
	return true, nil
}
