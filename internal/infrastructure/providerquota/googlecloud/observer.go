package googlecloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	defaultMonitoringBaseURL = "https://monitoring.googleapis.com"
	monitoringReadScope      = "https://www.googleapis.com/auth/monitoring.read"
	maxResponseBytes         = 16 << 20
	maxPages                 = 10
	maxObservations          = 100
	// Google documents a 60-second sampling period and up to 240 seconds before
	// quota/rate/net_usage is visible. Evaluate the last minute safely beyond both.
	quotaUsageVisibilityLag = 5 * time.Minute
)

var (
	servicePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*\.googleapis\.com$`)
	windowPattern  = regexp.MustCompile(`per_?([0-9]+)?_?(seconds?|minutes?|hours?|days?)`)
)

type Config struct {
	ProjectID       string
	Services        []string
	CredentialsFile string
	BaseURL         string
	Timeout         time.Duration
	HTTPClient      *http.Client
	TokenSource     oauth2.TokenSource
}

type Observer struct {
	projectID string
	services  []string
	baseURL   string
	client    *http.Client
}

func New(ctx context.Context, config Config) (*Observer, error) {
	projectID := strings.TrimSpace(config.ProjectID)
	if projectID == "" {
		return nil, fmt.Errorf("Google quota project ID is required")
	}
	services := make([]string, 0, len(config.Services))
	seen := map[string]bool{}
	for _, raw := range config.Services {
		service := strings.ToLower(strings.TrimSpace(raw))
		if !servicePattern.MatchString(service) {
			return nil, fmt.Errorf("Google quota service %q is invalid", raw)
		}
		if !seen[service] {
			seen[service], services = true, append(services, service)
		}
	}
	if len(services) == 0 {
		services = []string{"gmail.googleapis.com"}
	}
	sort.Strings(services)
	baseURL := strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	if baseURL == "" {
		baseURL = defaultMonitoringBaseURL
	}
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	client := config.HTTPClient
	if client == nil {
		tokenSource := config.TokenSource
		if tokenSource == nil {
			var credentials *google.Credentials
			var err error
			if path := strings.TrimSpace(config.CredentialsFile); path != "" {
				payload, readErr := os.ReadFile(path)
				if readErr != nil {
					return nil, fmt.Errorf("read Google quota credentials: %w", readErr)
				}
				credentials, err = google.CredentialsFromJSON(ctx, payload, monitoringReadScope)
			} else {
				credentials, err = google.FindDefaultCredentials(ctx, monitoringReadScope)
			}
			if err != nil {
				return nil, fmt.Errorf("load Google quota credentials: %w", err)
			}
			tokenSource = credentials.TokenSource
		}
		client = &http.Client{
			Timeout: timeout,
			Transport: &oauth2.Transport{
				Source: oauth2.ReuseTokenSource(nil, tokenSource), Base: http.DefaultTransport,
			},
		}
	} else if client.Timeout <= 0 {
		copy := *client
		copy.Timeout = timeout
		client = &copy
	}
	return &Observer{projectID: projectID, services: services, baseURL: baseURL, client: client}, nil
}

type listTimeSeriesResponse struct {
	TimeSeries    []timeSeries `json:"timeSeries"`
	NextPageToken string       `json:"nextPageToken"`
}

type timeSeries struct {
	Metric struct {
		Labels map[string]string `json:"labels"`
	} `json:"metric"`
	Resource struct {
		Labels map[string]string `json:"labels"`
	} `json:"resource"`
	Points []struct {
		Interval struct {
			StartTime string `json:"startTime"`
			EndTime   string `json:"endTime"`
		} `json:"interval"`
		Value struct {
			Int64Value  string   `json:"int64Value"`
			DoubleValue *float64 `json:"doubleValue"`
		} `json:"value"`
	} `json:"points"`
}

type seriesPoint struct {
	end   time.Time
	value int64
}

type usageKey struct {
	service, metric, location string
}

type limitKey struct {
	usageKey
	name string
}

type limitValue struct {
	value int64
	end   time.Time
}

func (o *Observer) ObserveProviderQuota(ctx context.Context, now time.Time) (integrationsdk.ProviderQuotaSnapshot, error) {
	now = now.UTC()
	usageDataThrough := now.Truncate(time.Minute).Add(-quotaUsageVisibilityLag)
	result := integrationsdk.ProviderQuotaSnapshot{
		Source: "google_cloud_monitoring", Status: integrationsdk.ProviderQuotaStatusAvailable,
		ObservedAt: now.Format(time.RFC3339Nano), Usage: []integrationsdk.ProviderQuotaUsage{},
	}
	limits := map[limitKey]limitValue{}
	usage := map[usageKey][]seriesPoint{}
	hasPairedUsage := false
	for _, service := range o.services {
		limitSeries, err := o.listTimeSeries(ctx, service, "serviceruntime.googleapis.com/quota/limit", now.Add(-49*time.Hour), now)
		if err != nil {
			return integrationsdk.ProviderQuotaSnapshot{}, err
		}
		// Anchor Google's ALIGN_SUM buckets to the same latency-safe minute used
		// below. Cloud Monitoring aligns rate buckets to the request interval end;
		// using wall-clock seconds here produces points such as 16:00:42-16:01:42,
		// which are then (correctly) rejected by a 16:01:00 safe cutoff even though
		// the underlying 16:00 minute is complete. Querying through the cutoff keeps
		// the official 60-second bucket and the evaluation window identical.
		usageSeries, err := o.listTimeSeries(ctx, service, "serviceruntime.googleapis.com/quota/rate/net_usage", usageDataThrough.Add(-25*time.Hour), usageDataThrough)
		if err != nil {
			return integrationsdk.ProviderQuotaSnapshot{}, err
		}
		collectLimits(service, limitSeries, limits)
		collectUsage(service, usageSeries, usage)
	}
	for key, limit := range limits {
		window, ok := quotaWindow(key.name)
		if !ok || isPerUserLimit(key.name) || limit.value <= 0 {
			result.Unmatched++
			continue
		}
		points := usage[key.usageKey]
		if len(points) == 0 {
			result.Unmatched++
			continue
		}
		from := usageDataThrough.Add(-window)
		var consumed int64
		for _, point := range points {
			if point.end.After(from) && !point.end.After(usageDataThrough) && point.value > 0 {
				consumed += point.value
			}
		}
		percent := int64(math.Ceil(float64(consumed) * 100 / float64(limit.value)))
		result.Usage = append(result.Usage, integrationsdk.ProviderQuotaUsage{
			Service: key.service, QuotaMetric: key.metric, LimitName: key.name, Location: key.location,
			Window: window.String(), Usage: consumed, Limit: limit.value, UsedPercent: percent,
		})
		hasPairedUsage = true
	}
	if hasPairedUsage {
		result.DataThroughAt = usageDataThrough.Format(time.RFC3339Nano)
	}
	sort.Slice(result.Usage, func(i, j int) bool {
		left, right := result.Usage[i], result.Usage[j]
		if left.UsedPercent != right.UsedPercent {
			return left.UsedPercent > right.UsedPercent
		}
		if left.Service != right.Service {
			return left.Service < right.Service
		}
		if left.QuotaMetric != right.QuotaMetric {
			return left.QuotaMetric < right.QuotaMetric
		}
		return left.LimitName < right.LimitName
	})
	if len(result.Usage) > maxObservations {
		result.Truncated = int64(len(result.Usage) - maxObservations)
		result.Usage = result.Usage[:maxObservations]
	}
	if len(result.Usage) > 0 {
		value := result.Usage[0].UsedPercent
		result.MaxUsedPercent = &value
	}
	return result, nil
}

func collectLimits(service string, values []timeSeries, target map[limitKey]limitValue) {
	for _, value := range values {
		metric, name := strings.TrimSpace(value.Metric.Labels["quota_metric"]), strings.TrimSpace(value.Metric.Labels["limit_name"])
		if metric == "" || name == "" {
			continue
		}
		key := limitKey{usageKey: usageKey{service: service, metric: metric, location: strings.TrimSpace(value.Resource.Labels["location"])}, name: name}
		for _, point := range value.Points {
			end, amount, ok := parsePoint(point.Interval.EndTime, point.Value.Int64Value, point.Value.DoubleValue)
			if !ok || amount <= 0 {
				continue
			}
			current, found := target[key]
			if !found || end.After(current.end) || (end.Equal(current.end) && amount > current.value) {
				target[key] = limitValue{value: amount, end: end}
			}
		}
	}
}

func collectUsage(service string, values []timeSeries, target map[usageKey][]seriesPoint) {
	for _, value := range values {
		metric := strings.TrimSpace(value.Metric.Labels["quota_metric"])
		if metric == "" {
			continue
		}
		key := usageKey{service: service, metric: metric, location: strings.TrimSpace(value.Resource.Labels["location"])}
		for _, point := range value.Points {
			end, amount, ok := parsePoint(point.Interval.EndTime, point.Value.Int64Value, point.Value.DoubleValue)
			if ok {
				target[key] = append(target[key], seriesPoint{end: end, value: amount})
			}
		}
	}
}

func parsePoint(rawTime, rawInt string, rawDouble *float64) (time.Time, int64, bool) {
	end, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(rawTime))
	if err != nil {
		return time.Time{}, 0, false
	}
	if strings.TrimSpace(rawInt) != "" {
		value, parseErr := strconv.ParseInt(strings.TrimSpace(rawInt), 10, 64)
		return end.UTC(), value, parseErr == nil
	}
	if rawDouble != nil && !math.IsNaN(*rawDouble) && !math.IsInf(*rawDouble, 0) {
		return end.UTC(), int64(math.Round(*rawDouble)), true
	}
	return time.Time{}, 0, false
}

func quotaWindow(raw string) (time.Duration, bool) {
	value := normalizeLimitName(raw)
	match := windowPattern.FindStringSubmatch(value)
	if len(match) == 0 {
		return 0, false
	}
	count := int64(1)
	if match[1] != "" {
		parsed, err := strconv.ParseInt(match[1], 10, 32)
		if err != nil || parsed <= 0 {
			return 0, false
		}
		count = parsed
	}
	unit := time.Second
	switch strings.TrimSuffix(match[2], "s") {
	case "minute":
		unit = time.Minute
	case "hour":
		unit = time.Hour
	case "day":
		unit = 24 * time.Hour
	}
	window := time.Duration(count) * unit
	return window, window > 0 && window <= 24*time.Hour
}

func normalizeLimitName(value string) string {
	var out strings.Builder
	for index, char := range strings.TrimSpace(value) {
		if char >= 'A' && char <= 'Z' {
			if index > 0 {
				out.WriteByte('_')
			}
			out.WriteRune(char + ('a' - 'A'))
			continue
		}
		if char == '-' || char == ' ' || char == '.' {
			char = '_'
		}
		out.WriteRune(char)
	}
	return strings.ToLower(out.String())
}

func isPerUserLimit(value string) bool {
	normalized := normalizeLimitName(value)
	return strings.Contains(normalized, "per_user") || strings.Contains(normalized, "peruser")
}

func (o *Observer) listTimeSeries(ctx context.Context, service, metricType string, start, end time.Time) ([]timeSeries, error) {
	endpoint := o.baseURL + "/v3/projects/" + url.PathEscape(o.projectID) + "/timeSeries"
	values := []timeSeries{}
	pageToken := ""
	for page := 0; page < maxPages; page++ {
		query := url.Values{}
		query.Set("filter", fmt.Sprintf(`metric.type=%q AND resource.type="consumer_quota" AND resource.label."service"=%q`, metricType, service))
		query.Set("interval.startTime", start.UTC().Format(time.RFC3339Nano))
		query.Set("interval.endTime", end.UTC().Format(time.RFC3339Nano))
		query.Set("view", "FULL")
		query.Set("pageSize", "100000")
		if metricType == "serviceruntime.googleapis.com/quota/limit" {
			query.Set("aggregation.alignmentPeriod", "86400s")
			query.Set("aggregation.perSeriesAligner", "ALIGN_NEXT_OLDER")
			query.Set("aggregation.crossSeriesReducer", "REDUCE_MAX")
			query.Add("aggregation.groupByFields", "metric.label.quota_metric")
			query.Add("aggregation.groupByFields", "metric.label.limit_name")
			query.Add("aggregation.groupByFields", "resource.label.location")
		} else {
			query.Set("aggregation.alignmentPeriod", "60s")
			query.Set("aggregation.perSeriesAligner", "ALIGN_SUM")
			query.Set("aggregation.crossSeriesReducer", "REDUCE_SUM")
			query.Add("aggregation.groupByFields", "metric.label.quota_metric")
			query.Add("aggregation.groupByFields", "resource.label.location")
		}
		if pageToken != "" {
			query.Set("pageToken", pageToken)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+query.Encode(), nil)
		if err != nil {
			return nil, fmt.Errorf("build Google Monitoring quota request: %w", err)
		}
		response, err := o.client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("read Google Monitoring quota metric: %w", err)
		}
		var payload listTimeSeriesResponse
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, maxResponseBytes)).Decode(&payload)
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("Google Monitoring quota metric returned HTTP %d", response.StatusCode)
		}
		if decodeErr != nil {
			return nil, fmt.Errorf("decode Google Monitoring quota metric: %w", decodeErr)
		}
		values = append(values, payload.TimeSeries...)
		pageToken = strings.TrimSpace(payload.NextPageToken)
		if pageToken == "" {
			return values, nil
		}
	}
	return nil, fmt.Errorf("Google Monitoring quota metric exceeded pagination bound")
}
