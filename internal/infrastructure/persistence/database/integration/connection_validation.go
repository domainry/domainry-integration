package integration

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"

	connector "github.com/domainry/domainry-connector-sdk"
)

// ValidateProviderConnectionCandidate runs the same source-owned connection
// normalization and provider-specific validation used before persistence. It
// is exported only so the module capability facet cannot drift from execution.
func ValidateProviderConnectionCandidate(provider connector.Adapter, connection connector.Connection, requireReady bool) error {
	_, err := normalizeProviderConnection(provider, connection, requireReady)
	return err
}

func normalizeProviderConnection(provider connector.Adapter, connection connector.Connection, requireReady bool) (connector.Connection, error) {
	if provider == nil {
		return connection, fmt.Errorf("Integration provider %s/%s is unavailable", connection.ConnectorKey, connection.ProviderKey)
	}
	descriptor := provider.Descriptor()
	if descriptor.ConnectorKey != strings.TrimSpace(connection.ConnectorKey) || descriptor.ProviderKey != strings.TrimSpace(connection.ProviderKey) {
		return connection, fmt.Errorf("Integration connection Provider identity does not match %s/%s", descriptor.ConnectorKey, descriptor.ProviderKey)
	}
	config, err := connector.ApplyConfigDefaults(descriptor.ConfigFields, connection.Config)
	if err != nil {
		return connection, fmt.Errorf("apply Integration Provider config defaults: %w", err)
	}
	connection.Config = config
	declaredConfig := make(map[string]connector.ConfigField, len(descriptor.ConfigFields))
	for _, field := range descriptor.ConfigFields {
		declaredConfig[field.Key] = field
	}
	for key, value := range config {
		field, exists := declaredConfig[strings.TrimSpace(key)]
		if !exists {
			return connection, fmt.Errorf("Integration Provider config field %q is not declared", key)
		}
		if err := validateProviderConfigValue(field, value); err != nil {
			return connection, err
		}
	}
	for _, field := range descriptor.ConfigFields {
		value, present := config[field.Key]
		if requireReady && field.Required && (!present || integrationEmptyValue(value)) {
			return connection, fmt.Errorf("Integration Provider config field %q is required", field.Key)
		}
		if !present || integrationEmptyValue(value) {
			continue
		}
		for _, dependency := range field.RequiredWith {
			if integrationEmptyValue(config[dependency]) {
				return connection, fmt.Errorf("Integration Provider config field %q requires %q", field.Key, dependency)
			}
		}
	}
	declaredSecrets := make(map[string]connector.SecretField, len(descriptor.SecretFields))
	for _, field := range descriptor.SecretFields {
		declaredSecrets[field.Key] = field
	}
	for key, reference := range connection.SecretRefs {
		if _, exists := declaredSecrets[strings.TrimSpace(key)]; !exists {
			return connection, fmt.Errorf("Integration Provider secret field %q is not declared", key)
		}
		if strings.TrimSpace(reference) == "" {
			return connection, fmt.Errorf("Integration Provider secret reference %q is empty", key)
		}
	}
	if requireReady {
		for _, field := range descriptor.SecretFields {
			if field.Required && strings.TrimSpace(connection.SecretRefs[field.Key]) == "" {
				return connection, fmt.Errorf("Integration Provider secret reference %q is required", field.Key)
			}
		}
		if validator, ok := provider.(connector.ConfigValidator); ok {
			if err := validator.ValidateConfig(connection); err != nil {
				return connection, fmt.Errorf("validate Integration Provider config: %w", err)
			}
		}
	}
	return connection, nil
}

func validateProviderConfigValue(field connector.ConfigField, value any) error {
	valid := false
	switch field.Type {
	case connector.ConfigFieldText, connector.ConfigFieldEmail, connector.ConfigFieldSelect:
		_, valid = value.(string)
	case connector.ConfigFieldInteger:
		switch typed := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			valid = true
		case float64:
			valid = typed == math.Trunc(typed)
		case json.Number:
			_, err := typed.Int64()
			valid = err == nil
		}
	case connector.ConfigFieldDecimal:
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
			valid = true
		}
	case connector.ConfigFieldBoolean:
		_, valid = value.(bool)
	case connector.ConfigFieldJSON:
		_, object := value.(map[string]any)
		_, array := value.([]any)
		valid = object || array
	}
	if !valid {
		return fmt.Errorf("Integration Provider config field %q must be %s", field.Key, field.Type)
	}
	if text, ok := value.(string); ok {
		length := len([]rune(text))
		if field.Validation.MinLength > 0 && length < field.Validation.MinLength {
			return fmt.Errorf("Integration Provider config field %q is shorter than %d", field.Key, field.Validation.MinLength)
		}
		if field.Validation.MaxLength > 0 && length > field.Validation.MaxLength {
			return fmt.Errorf("Integration Provider config field %q is longer than %d", field.Key, field.Validation.MaxLength)
		}
		if pattern := strings.TrimSpace(field.Validation.Pattern); pattern != "" {
			expression, err := regexp.Compile(pattern)
			if err != nil || !expression.MatchString(text) {
				return fmt.Errorf("Integration Provider config field %q does not match its pattern", field.Key)
			}
		}
		if len(field.Validation.Options) != 0 {
			matched := false
			for _, option := range field.Validation.Options {
				matched = matched || text == option
			}
			if !matched {
				return fmt.Errorf("Integration Provider config field %q has an unsupported option", field.Key)
			}
		}
	}
	if field.Validation.Min != nil || field.Validation.Max != nil {
		number, ok := providerConfigNumber(value)
		if !ok || (field.Validation.Min != nil && number < *field.Validation.Min) || (field.Validation.Max != nil && number > *field.Validation.Max) {
			return fmt.Errorf("Integration Provider config field %q is outside its allowed range", field.Key)
		}
	}
	return nil
}

func providerConfigNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int8:
		return float64(typed), true
	case int16:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint8:
		return float64(typed), true
	case uint16:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		value, err := typed.Float64()
		return value, err == nil
	default:
		return 0, false
	}
}

func integrationEmptyValue(value any) bool {
	if value == nil {
		return true
	}
	text, ok := value.(string)
	return ok && strings.TrimSpace(text) == ""
}
