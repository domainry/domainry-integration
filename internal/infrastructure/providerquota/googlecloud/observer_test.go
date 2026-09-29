package googlecloud

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObserverReadsOfficialProjectQuotaWithoutTreatingPerUserLimitAsAggregate(t *testing.T) {
	now := time.Date(2026, 9, 29, 2, 10, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v3/projects/project-a/timeSeries" || request.URL.Query().Get("view") != "FULL" {
			t.Fatalf("unexpected request %s", request.URL.String())
		}
		filter := request.URL.Query().Get("filter")
		groups := request.URL.Query()["aggregation.groupByFields"]
		if len(groups) < 2 || request.URL.Query().Get("aggregation.crossSeriesReducer") == "" {
			t.Fatalf("quota request did not bound provider time-series cardinality: %s", request.URL.RawQuery)
		}
		response.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(filter, "quota/limit"):
			_ = json.NewEncoder(response).Encode(map[string]any{"timeSeries": []any{
				timeSeriesFixture("gmail.googleapis.com/default", "requestsPerMinutePerProject", "global", "100", "2026-09-29T01:00:00Z"),
				timeSeriesFixture("gmail.googleapis.com/default", "requestsPerMinutePerUser", "global", "20", "2026-09-29T01:00:00Z"),
			}})
		case strings.Contains(filter, "quota/rate/net_usage"):
			if got := request.URL.Query().Get("interval.endTime"); got != "2026-09-29T02:05:00Z" {
				t.Fatalf("usage interval end=%q want latency-safe minute", got)
			}
			if got := request.URL.Query().Get("interval.startTime"); got != "2026-09-28T01:05:00Z" {
				t.Fatalf("usage interval start=%q want cutoff-relative bound", got)
			}
			_ = json.NewEncoder(response).Encode(map[string]any{"timeSeries": []any{
				usageSeriesFixture("gmail.googleapis.com/default", "global", []map[string]string{
					{"value": "40", "end": "2026-09-29T02:05:00Z"},
					{"value": "35", "end": "2026-09-29T02:04:30Z"},
					{"value": "90", "end": "2026-09-29T02:03:59Z"},
					{"value": "100", "end": "2026-09-29T02:06:00Z"},
				}),
			}})
		default:
			t.Fatalf("unexpected filter %q", filter)
		}
	}))
	defer server.Close()

	observer, err := New(t.Context(), Config{ProjectID: "project-a", Services: []string{"gmail.googleapis.com"}, BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := observer.ObserveProviderQuota(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "available" || snapshot.Source != "google_cloud_monitoring" || snapshot.MaxUsedPercent == nil || *snapshot.MaxUsedPercent != 75 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if len(snapshot.Usage) != 1 || snapshot.Usage[0].Usage != 75 || snapshot.Usage[0].Limit != 100 || snapshot.Usage[0].Window != "1m0s" {
		t.Fatalf("usage=%+v", snapshot.Usage)
	}
	if snapshot.Unmatched != 1 || snapshot.DataThroughAt != "2026-09-29T02:05:00Z" {
		t.Fatalf("unmatched=%d data_through_at=%q", snapshot.Unmatched, snapshot.DataThroughAt)
	}
}

func TestObserverUsesLatencySafeCutoffSoOldUsageCanRecover(t *testing.T) {
	now := time.Date(2026, 9, 29, 2, 10, 59, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		filter := request.URL.Query().Get("filter")
		switch {
		case strings.Contains(filter, "quota/limit"):
			_ = json.NewEncoder(response).Encode(map[string]any{"timeSeries": []any{
				timeSeriesFixture("gmail.googleapis.com/default", "Units per minute", "global", "1200000", "2026-09-29T01:00:00Z"),
			}})
		case strings.Contains(filter, "quota/rate/net_usage"):
			_ = json.NewEncoder(response).Encode(map[string]any{"timeSeries": []any{
				usageSeriesFixture("gmail.googleapis.com/default", "global", []map[string]string{
					{"value": "4080", "end": "2026-09-29T01:00:00Z"},
				}),
			}})
		default:
			t.Fatalf("unexpected filter %q", filter)
		}
	}))
	defer server.Close()

	observer, err := New(t.Context(), Config{ProjectID: "project-a", Services: []string{"gmail.googleapis.com"}, BaseURL: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := observer.ObserveProviderQuota(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Usage) != 1 || snapshot.Usage[0].Usage != 0 || snapshot.Usage[0].UsedPercent != 0 {
		t.Fatalf("old usage was retained as current: %+v", snapshot.Usage)
	}
	if snapshot.DataThroughAt != "2026-09-29T02:05:00Z" {
		t.Fatalf("data_through_at=%q", snapshot.DataThroughAt)
	}
}

func TestQuotaWindowAcceptsGoogleLimitNameShapes(t *testing.T) {
	for name, want := range map[string]time.Duration{
		"requestsPerMinutePerProject": time.Minute,
		"requests_per_100_seconds":    100 * time.Second,
		"Requests per day":            24 * time.Hour,
		"Units per minute":            time.Minute,
	} {
		got, ok := quotaWindow(name)
		if !ok || got != want {
			t.Fatalf("quotaWindow(%q)=(%s,%v) want %s", name, got, ok, want)
		}
	}
	if !isPerUserLimit("Units per minute per user") || isPerUserLimit("Units per minute") {
		t.Fatal("real Gmail project and per-user quota names were not distinguished")
	}
}

func timeSeriesFixture(metric, limit, location, value, end string) map[string]any {
	return map[string]any{
		"metric":   map[string]any{"labels": map[string]string{"quota_metric": metric, "limit_name": limit}},
		"resource": map[string]any{"labels": map[string]string{"location": location}},
		"points":   []any{map[string]any{"interval": map[string]string{"endTime": end}, "value": map[string]string{"int64Value": value}}},
	}
}

func usageSeriesFixture(metric, location string, points []map[string]string) map[string]any {
	result := make([]any, 0, len(points))
	for _, point := range points {
		result = append(result, map[string]any{"interval": map[string]string{"endTime": point["end"]}, "value": map[string]string{"int64Value": point["value"]}})
	}
	return map[string]any{
		"metric":   map[string]any{"labels": map[string]string{"quota_metric": metric, "method": "gmail.users.messages.list"}},
		"resource": map[string]any{"labels": map[string]string{"location": location}},
		"points":   result,
	}
}
