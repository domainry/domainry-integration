package integration

import "time"

func providerDeadlineScanEnd(now time.Time) int64 {
	return now.UTC().UnixMilli()
}

func providerDeadlineReady(due, lease int64, now time.Time) (bool, error) {
	if due > now.UTC().UnixMilli() {
		return false, nil
	}
	if lease == 0 {
		return true, nil
	}
	return lease <= now.UTC().UnixMilli(), nil
}

func (s *WorkerStore) now() time.Time {
	if s.clock != nil {
		return s.clock().UTC()
	}
	return time.Now().UTC()
}
