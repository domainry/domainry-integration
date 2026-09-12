package integration

import (
	"fmt"
	"time"
)

// RFC3339Nano removes trailing zeroes, so text ordering is not instant ordering
// within one second (".123Z" sorts after ".1231Z"). The SQL scan includes the
// whole current UTC second; each candidate is then checked as an actual instant.
func providerDeadlineScanEnd(now time.Time) string {
	return now.UTC().Truncate(time.Second).Add(time.Second).Format("2006-01-02T15:04:05")
}

func providerDeadlineReady(due, lease string, now time.Time) (bool, error) {
	t, err := time.Parse(time.RFC3339Nano, due)
	if err != nil {
		return false, fmt.Errorf("Integration provider due time is invalid")
	}
	if t.After(now) {
		return false, nil
	}
	if lease == "" {
		return true, nil
	}
	expiry, err := time.Parse(time.RFC3339Nano, lease)
	if err != nil {
		return false, fmt.Errorf("Integration provider lease time is invalid")
	}
	return !expiry.After(now), nil
}

func (s *WorkerStore) now() time.Time {
	if s.clock != nil {
		return s.clock().UTC()
	}
	return time.Now().UTC()
}
