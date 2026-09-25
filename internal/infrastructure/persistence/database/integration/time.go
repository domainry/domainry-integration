package integration

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func timestampMillis(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if millis, err := strconv.ParseInt(value, 10, 64); err == nil {
		return millis
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC().UnixMilli()
		}
	}
	return 0
}

func timestampString(value int64) string {
	if value == 0 {
		return ""
	}
	return time.UnixMilli(value).UTC().Format(time.RFC3339Nano)
}

func timestampValue(value any) int64 {
	switch typed := value.(type) {
	case time.Time:
		return typed.UTC().UnixMilli()
	case int64:
		return typed
	default:
		return timestampMillis(fmt.Sprint(value))
	}
}
