package googlecloud

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

const realQuotaAcceptanceEnv = "INTEGRATION_GOOGLE_QUOTA_REAL_ACCEPTANCE"

func TestObserverRealGoogleCloudQuotaAcceptance(t *testing.T) {
	if os.Getenv(realQuotaAcceptanceEnv) != "1" {
		t.Skip("set " + realQuotaAcceptanceEnv + "=1 to read an actual Google Cloud quota sample")
	}
	projectID := strings.TrimSpace(os.Getenv("INTEGRATION_GOOGLE_QUOTA_PROJECT_ID"))
	if projectID == "" {
		t.Fatal("INTEGRATION_GOOGLE_QUOTA_PROJECT_ID is required")
	}
	services := splitServices(os.Getenv("INTEGRATION_GOOGLE_QUOTA_SERVICES"))
	credentialsFile := strings.TrimSpace(os.Getenv("INTEGRATION_GOOGLE_QUOTA_CREDENTIALS_FILE"))
	now := time.Now().UTC()
	observer, err := New(t.Context(), Config{
		ProjectID:       projectID,
		Services:        services,
		CredentialsFile: credentialsFile,
		Timeout:         30 * time.Second,
	})
	if err != nil {
		t.Fatalf("create official quota observer: %v", err)
	}
	snapshot, err := observer.ObserveProviderQuota(t.Context(), now)
	if err != nil {
		t.Fatalf("read official quota sample: %v", err)
	}
	if snapshot.Status != "available" || snapshot.Source != "google_cloud_monitoring" {
		t.Fatalf("unexpected official quota source: status=%q source=%q", snapshot.Status, snapshot.Source)
	}
	if snapshot.DataThroughAt == "" || len(snapshot.Usage) == 0 || snapshot.MaxUsedPercent == nil {
		t.Fatalf("official quota sample is incomplete: data_through=%q observations=%d max_percent_present=%t unmatched=%d", snapshot.DataThroughAt, len(snapshot.Usage), snapshot.MaxUsedPercent != nil, snapshot.Unmatched)
	}
	dataThrough, err := time.Parse(time.RFC3339Nano, snapshot.DataThroughAt)
	if err != nil || dataThrough.After(now.Add(time.Minute)) || dataThrough.Before(now.Add(-25*time.Hour)) {
		t.Fatalf("official quota data-through time is outside the requested sample window: %q", snapshot.DataThroughAt)
	}
	maximum := int64(0)
	for _, sample := range snapshot.Usage {
		if sample.Service == "" || sample.QuotaMetric == "" || sample.LimitName == "" || sample.Window == "" || sample.Limit <= 0 || sample.Usage < 0 {
			t.Fatalf("invalid official quota observation: %+v", sample)
		}
		wantPercent := int64(math.Ceil(float64(sample.Usage) * 100 / float64(sample.Limit)))
		if sample.UsedPercent != wantPercent {
			t.Fatalf("official quota percentage mismatch: got=%d want=%d sample=%+v", sample.UsedPercent, wantPercent, sample)
		}
		if sample.UsedPercent > maximum {
			maximum = sample.UsedPercent
		}
	}
	if *snapshot.MaxUsedPercent != maximum {
		t.Fatalf("official quota maximum mismatch: got=%d want=%d", *snapshot.MaxUsedPercent, maximum)
	}
	t.Logf("official quota sample source=%s observations=%d max_used_percent=%d data_through=%s unmatched=%d truncated=%d", snapshot.Source, len(snapshot.Usage), maximum, snapshot.DataThroughAt, snapshot.Unmatched, snapshot.Truncated)
}

func splitServices(raw string) []string {
	values := []string{}
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}
