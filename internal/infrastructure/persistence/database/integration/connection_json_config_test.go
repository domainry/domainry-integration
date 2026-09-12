package integration

import (
	connector "github.com/domainry/domainry-connector-sdk"
	"testing"
)

func TestProviderJSONConfigAcceptsEquivalentModuleAndHTTPShapes(t *testing.T) {
	field := connector.ConfigField{Key: "sources", Type: connector.ConfigFieldJSON}
	for _, value := range []any{[]string{"example.com"}, []any{"example.com"}, map[string]string{"host": "example.com"}, map[string]any{"host": "example.com"}} {
		if err := validateProviderConfigValue(field, value); err != nil {
			t.Fatal(value, err)
		}
	}
	for _, value := range []any{nil, []string(nil), `["example.com"]`, 1, true, make(chan int)} {
		if err := validateProviderConfigValue(field, value); err == nil {
			t.Fatalf("invalid JSON config accepted: %T", value)
		}
	}
}
