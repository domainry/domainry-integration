package module

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	"github.com/domainry/domainry-foundation/modulecapability"
	"github.com/domainry/domainry-foundation/modulehttp"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
)

const (
	integrationConnectionsCategory   = "integration.connections"
	integrationCredentialsCategory   = "integration.credentials"
	integrationOperationsCategory    = "integration.operations"
	integrationSubscriptionsCategory = "integration.subscriptions"
)

func NewCapabilityBinding(definitions []connectorscatalog.ConnectorSchema, validator modulecapability.Validator) (*modulecapability.StaticBinding, error) {
	definitions = append([]connectorscatalog.ConnectorSchema(nil), definitions...)
	sort.Slice(definitions, func(i, j int) bool { return definitions[i].Key < definitions[j].Key })
	operations := integrationsdk.IntegrationHTTPSurfaceContract().OpenAPI
	groups := map[string][]modulehttp.Route{}
	for _, route := range integrationRoutes() {
		key := integrationCapabilityCategory(route.Pattern)
		groups[key] = append(groups[key], route)
	}
	definitionsByCategory := []struct {
		key, name, description string
		chains, scopes         []string
	}{
		{integrationConnectionsCategory, "Connector connections", "Discover connector/provider contracts and manage validated connection resources and provider-operation probes.", []string{"connector_descriptor_to_integration_connection", "integration_connection_to_provider_operation", "secret_reference_to_connection_validation"}, []string{"integration.connection_requirement"}},
		{integrationCredentialsCategory, "Integration credentials", "Manage secret references, application API keys, and external identity mappings without exposing secret material.", []string{"identity_actor_to_integration_api_key", "provider_subject_to_external_identity"}, []string{}},
		{integrationSubscriptionsCategory, "Inbound and push subscriptions", "Manage webhook and browser push subscriptions and accept provider-authenticated inbound webhooks.", []string{"provider_webhook_to_integration_event", "integration_event_mapping_to_runtime_execution", "notification_delivery_to_web_push_provider"}, []string{"integration.event_mapping"}},
		{integrationOperationsCategory, "Invocation and event operations", "Inspect provider invocations and durable inbound events, and replay eligible event execution.", []string{"integration_provider_call_to_invocation", "integration_event_to_runtime_execution_receipt"}, []string{}},
	}
	documents := make([]modulecapability.CategoryDocument, 0, len(definitionsByCategory))
	for _, definition := range definitionsByCategory {
		document, err := modulecapability.CategoryFromHTTPRoutes(modulecapability.HTTPRouteCategory{
			Owner: "integration", Category: modulecapability.CategorySummary{Key: definition.key, Name: definition.name, Description: definition.description, AssemblyChains: definition.chains, ValidationScopes: definition.scopes},
			Routes: groups[definition.key], Operations: operations,
			Components: map[string]map[string]json.RawMessage{
				"securitySchemes": {"BearerAuth": json.RawMessage(`{"type":"http","scheme":"bearer","bearerFormat":"JWT"}`)},
			},
		})
		if err != nil {
			return nil, err
		}
		if definition.key == integrationConnectionsCategory {
			document.ValidationContracts = []modulecapability.ValidationScopeContract{{
				Kind: "integration.connection_requirement", Description: "Validate one project connection binding against the registered Connector/provider contract.", Coverage: modulecapability.ValidationCoverageAllCandidates, CandidateCollections: []string{"integrations.connections"},
			}}
		} else if definition.key == integrationSubscriptionsCategory {
			document.ValidationContracts = []modulecapability.ValidationScopeContract{{
				Kind:                  "integration.event_mapping",
				Description:           "Validate one project inbound-event mapping and its finite Runtime target references.",
				Coverage:              modulecapability.ValidationCoverageAllCandidates,
				CandidateCollections:  []string{"integrations.event_mappings"},
				ReferencedCollections: []string{"actions", "integrations.connections", "objects", "workflows"},
			}}
		}
		documents = append(documents, document)
	}
	connectorProjections, err := integrationConnectorProjections(definitions)
	if err != nil {
		return nil, err
	}
	for index, batch := range batchIntegrationConnectorProjections(connectorProjections) {
		documents = append(documents, modulecapability.CategoryDocument{
			Category: modulecapability.CategorySummary{
				Key: fmt.Sprintf("integration.connectors.%02d", index+1), Name: fmt.Sprintf("Connector contracts %d", index+1),
				Description:      "Bounded source-owned Connector definitions and the registered Provider descriptors that implement them.",
				AssemblyChains:   []string{"connector_descriptor_to_integration_connection", "integration_connection_to_provider_operation"},
				ValidationScopes: []string{},
			},
			OpenAPI:     modulecapability.OpenAPIFragment{OpenAPI: "3.1.0", Paths: map[string]map[string]json.RawMessage{}},
			Projections: batch,
		})
	}
	provided := []string{"integration.api_keys", "integration.connections", "integration.delivery", "integration.events", "integration.external_identities", "integration.invocations", "integration.secrets", "integration.web_push", "integration.webhook_subscriptions"}
	for _, definition := range definitions {
		provided = append(provided, "connector_definition."+definition.Key)
		for _, provider := range definition.Providers {
			provided = append(provided, "connector."+definition.Key+"."+provider.Key)
			for _, operation := range provider.OperationKeys {
				provided = append(provided, "connector."+definition.Key+"."+provider.Key+"."+operation)
			}
		}
	}
	provided = uniqueIntegrationStrings(provided)
	summary := modulecapability.ModuleSummary{
		Identity: modulecapability.ModuleIdentity{Key: "integration", SourceOwner: "integration", ModuleVersion: integrationsdk.ProtocolVersionV1, ValidationRevision: "integration-owner-validation-v1", SupportedDeploymentModes: []modulecapability.DeploymentMode{modulecapability.DeploymentModeModule, modulecapability.DeploymentModeSaaS}},
		Name:     "Integration", Description: "Connector catalog and provider registry, governed connections and credentials, provider operations, webhooks, external identities, and delivery evidence.",
		Scenarios: modulecapability.AdaptationScenarios{
			UseWhen:              []string{"A PRD requires calling an external provider, receiving verified webhooks, managing connector credentials, synchronizing external identities, or browser push delivery"},
			DoNotUseWhen:         []string{"The requirement is entirely internal domain behavior with no provider protocol, external credential, webhook, or delivery boundary"},
			RequirementSignals:   []string{"third-party API", "connector", "provider credentials", "webhook", "external identity", "web push", "outbound delivery", "provider reconciliation"},
			ProvidedCapabilities: provided, RequiredModules: []string{"identity"}, OptionalModules: []string{"audit", "notification", "scheduler"}, ConflictingModules: []string{},
			AssemblyChains: []string{
				"connector_descriptor_to_integration_connection", "integration_connection_to_provider_operation", "secret_reference_to_connection_validation",
				"identity_actor_to_integration_api_key", "provider_subject_to_external_identity",
				"provider_webhook_to_integration_event", "integration_event_mapping_to_runtime_execution", "notification_delivery_to_web_push_provider",
				"integration_provider_call_to_invocation", "integration_event_to_runtime_execution_receipt",
			},
			ValidationScopes:  []string{"integration.connection_requirement", "integration.event_mapping"},
			SelectionExamples: []modulecapability.ScenarioExample{{Requirement: "Send order updates to a configured CRM and ingest signed CRM webhooks into a workflow", Reason: "Integration owns provider connections, operation delivery, webhook verification, durable events, and Runtime trigger handoff"}},
			RejectionExamples: []modulecapability.ScenarioExample{{Requirement: "Notify an in-app user when an approval is completed", Reason: "Notification owns in-app user communication; Integration is selected only if an external delivery provider or webhook is also needed"}},
		},
	}
	return modulecapability.NewStaticBinding(summary, documents, validator)
}

func integrationConnectorProjections(definitions []connectorscatalog.ConnectorSchema) ([]modulecapability.SourceProjection, error) {
	result := make([]modulecapability.SourceProjection, 0, len(definitions))
	for _, definition := range definitions {
		payload, err := modulecapability.CanonicalJSON(trimConnectorProjection(definition))
		if err != nil {
			return nil, err
		}
		if len(payload) > modulecapability.MaxCategoryCanonicalBytes-(32<<10) {
			return nil, fmt.Errorf("Connector definition %q exceeds one capability batch", definition.Key)
		}
		result = append(result, modulecapability.SourceProjection{Kind: "integration.connector", Key: definition.Key, Payload: json.RawMessage(payload)})
	}
	return result, nil
}

type connectorProjection struct {
	Key                   string                `json:"key"`
	Name                  string                `json:"name,omitempty"`
	Description           string                `json:"description,omitempty"`
	Type                  string                `json:"type"`
	Provider              string                `json:"provider,omitempty"`
	Version               string                `json:"version,omitempty"`
	MinimumRuntimeVersion string                `json:"minimum_runtime_version,omitempty"`
	FeatureFlags          []string              `json:"feature_flags,omitempty"`
	Classification        string                `json:"classification,omitempty"`
	LifecycleStatus       string                `json:"lifecycle_status,omitempty"`
	ReplacementCapability string                `json:"replacement_capability,omitempty"`
	Capabilities          []string              `json:"capabilities,omitempty"`
	Providers             []providerProjection  `json:"providers,omitempty"`
	Operations            []operationProjection `json:"operations,omitempty"`
}

type providerProjection struct {
	Key              string            `json:"key"`
	ProviderRevision string            `json:"provider_revision,omitempty"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	ConfigFields     []fieldProjection `json:"config_fields,omitempty"`
	SecretFields     []fieldProjection `json:"secret_fields,omitempty"`
	OperationKeys    []string          `json:"operation_keys,omitempty"`
}

type operationProjection struct {
	Key                   string            `json:"key"`
	Name                  string            `json:"name,omitempty"`
	Description           string            `json:"description,omitempty"`
	Method                string            `json:"method,omitempty"`
	ExecutionMode         string            `json:"execution_mode,omitempty"`
	SideEffect            string            `json:"side_effect,omitempty"`
	Input                 []fieldProjection `json:"input,omitempty"`
	Output                []fieldProjection `json:"output,omitempty"`
	TimeoutDefaultSeconds int               `json:"timeout_default_seconds,omitempty"`
	TimeoutMaxSeconds     int               `json:"timeout_max_seconds,omitempty"`
	IdempotencySupported  bool              `json:"idempotency_supported,omitempty"`
	CompensationOperation string            `json:"compensation_operation,omitempty"`
	TestSupported         bool              `json:"test_supported,omitempty"`
	DryRunSupported       bool              `json:"dry_run_supported,omitempty"`
}

type fieldProjection struct {
	Key          string                            `json:"key"`
	Name         string                            `json:"name,omitempty"`
	Description  string                            `json:"description,omitempty"`
	Type         string                            `json:"type"`
	Config       map[string]any                    `json:"config,omitempty"`
	Validation   connectorscatalog.FieldValidation `json:"validation,omitempty"`
	Options      any                               `json:"options,omitempty"`
	Required     bool                              `json:"required"`
	Default      any                               `json:"default,omitempty"`
	DefaultValue any                               `json:"default_value,omitempty"`
}

func trimConnectorProjection(value connectorscatalog.ConnectorSchema) connectorProjection {
	providers := make([]providerProjection, len(value.Providers))
	for index, provider := range value.Providers {
		providers[index] = providerProjection{
			Key: provider.Key, ProviderRevision: provider.ProviderRevision, Name: provider.Name, Description: provider.Description,
			ConfigFields: trimConnectorFields(provider.ConfigFields), SecretFields: trimConnectorFields(provider.SecretFields), OperationKeys: append([]string(nil), provider.OperationKeys...),
		}
	}
	operations := make([]operationProjection, len(value.Operations))
	for index, operation := range value.Operations {
		operations[index] = operationProjection{
			Key: operation.Key, Name: operation.Name, Description: operation.Description, Method: operation.Method,
			ExecutionMode: operation.ExecutionMode, SideEffect: operation.SideEffect,
			Input: trimConnectorFields(operation.Input), Output: trimConnectorFields(operation.Output),
			TimeoutDefaultSeconds: operation.TimeoutDefaultSeconds, TimeoutMaxSeconds: operation.TimeoutMaxSeconds,
			IdempotencySupported: operation.IdempotencySupported, CompensationOperation: operation.CompensationOperation,
			TestSupported: operation.TestSupported, DryRunSupported: operation.DryRunSupported,
		}
	}
	return connectorProjection{
		Key: value.Key, Name: value.Name, Description: value.Description, Type: value.Type, Provider: value.Provider,
		Version: value.Version, MinimumRuntimeVersion: value.MinimumRuntimeVersion,
		FeatureFlags: append([]string(nil), value.FeatureFlags...), Classification: value.Classification,
		LifecycleStatus: value.LifecycleStatus, ReplacementCapability: value.ReplacementCapability,
		Capabilities: append([]string(nil), value.Capabilities...), Providers: providers, Operations: operations,
	}
}

func trimConnectorFields(values []connectorscatalog.FieldSchema) []fieldProjection {
	result := make([]fieldProjection, len(values))
	for index, value := range values {
		result[index] = fieldProjection{
			Key: value.Key, Name: value.Name, Description: value.Description, Type: value.Type,
			Config: value.Config, Validation: value.Validation, Options: value.Options, Required: value.Required,
			Default: value.Default, DefaultValue: value.DefaultValue,
		}
	}
	return result
}

func uniqueIntegrationStrings(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func batchIntegrationConnectorProjections(values []modulecapability.SourceProjection) [][]modulecapability.SourceProjection {
	const payloadBudget = 768 << 10
	result := [][]modulecapability.SourceProjection{}
	current, size := []modulecapability.SourceProjection{}, 0
	for _, value := range values {
		if len(current) != 0 && (len(current) == modulecapability.MaxCategoryProjections || size+len(value.Payload) > payloadBudget) {
			result = append(result, current)
			current, size = []modulecapability.SourceProjection{}, 0
		}
		current = append(current, value)
		size += len(value.Payload)
	}
	if len(current) != 0 {
		result = append(result, current)
	}
	return result
}

func integrationCapabilityCategory(pattern string) string {
	_, path, _ := strings.Cut(pattern, " ")
	switch {
	case strings.Contains(path, "/secrets") || strings.Contains(path, "/api-keys") || strings.Contains(path, "/external-identities"):
		return integrationCredentialsCategory
	case strings.Contains(path, "/webhook-subscriptions") || strings.Contains(path, "/web-push") || strings.HasPrefix(path, "/integrations/webhooks/"):
		return integrationSubscriptionsCategory
	case strings.Contains(path, "/invocations") || strings.Contains(path, "/events"):
		return integrationOperationsCategory
	default:
		return integrationConnectionsCategory
	}
}

type integrationConnectionAuthoringFragment struct {
	Key          string          `json:"key"`
	ModuleKey    string          `json:"module_key,omitempty"`
	ConnectorKey string          `json:"connector_key"`
	ProviderKey  string          `json:"provider_key"`
	Name         string          `json:"name,omitempty"`
	I18n         json.RawMessage `json:"i18n,omitempty"`
	Config       map[string]any  `json:"config,omitempty"`
}

type integrationEventMappingAuthoringFragment struct {
	Key              string                                            `json:"key"`
	Provider         string                                            `json:"provider"`
	EventType        string                                            `json:"event_type,omitempty"`
	CommandPrefix    string                                            `json:"command_prefix,omitempty"`
	TargetType       string                                            `json:"target_type"`
	WorkflowKey      string                                            `json:"workflow_key,omitempty"`
	ObjectKey        string                                            `json:"object_key,omitempty"`
	ObjectKeyPath    string                                            `json:"object_key_path,omitempty"`
	RecordID         string                                            `json:"record_id,omitempty"`
	RecordIDPath     string                                            `json:"record_id_path,omitempty"`
	ActionKey        string                                            `json:"action_key,omitempty"`
	ActionKeyPath    string                                            `json:"action_key_path,omitempty"`
	ActionInput      map[string]string                                 `json:"action_input,omitempty"`
	WorkflowInput    map[string]string                                 `json:"workflow_input,omitempty"`
	EventFields      []integrationsdk.EventFieldRequirement            `json:"event_fields,omitempty"`
	ExternalIdentity integrationsdk.ExternalIdentityMappingRequirement `json:"external_identity"`
	Payload          map[string]any                                    `json:"payload,omitempty"`
}

func ValidateCapabilityCandidate(ctx context.Context, request modulecapability.ValidationRequest, definitions []connectorscatalog.ConnectorSchema) (modulecapability.ValidationResult, error) {
	result := modulecapability.ValidationResult{Diagnostics: []modulecapability.Diagnostic{}}
	invalid := func(rule, field string, err error) (modulecapability.ValidationResult, error) {
		message := "Integration candidate is invalid"
		if err != nil && strings.TrimSpace(err.Error()) != "" {
			message = err.Error()
		}
		result.Diagnostics = append(result.Diagnostics, modulecapability.Diagnostic{Owner: "integration", RuleKey: rule, Severity: modulecapability.SeverityError, FieldPath: field, Message: message})
		return result, nil
	}
	switch request.Kind {
	case "integration.connection_requirement":
		var source integrationConnectionAuthoringFragment
		if err := modulecapability.DecodeKeyedAuthoringValue(request.Candidate, "key", &source); err != nil {
			return invalid("integration.connection_requirement.invalid_json", "$.candidate.value", err)
		}
		if source.Key != request.Candidate.Key {
			return invalid("integration.connection_requirement.key_mismatch", "$.candidate.value.key", fmt.Errorf("connection key %q differs from fragment key %q", source.Key, request.Candidate.Key))
		}
		config, err := json.Marshal(source.Config)
		if err != nil {
			return invalid("integration.connection_requirement.invalid", "$.candidate.value.config", err)
		}
		if source.Config == nil {
			config = []byte(`{}`)
		}
		value := integrationsdk.ConnectionRequirement{
			Key: request.Candidate.Key, WorkspaceID: "authoring-validation", ConnectorKey: source.ConnectorKey,
			ProviderKey: source.ProviderKey, Name: source.Name, Config: config,
		}
		if err := value.Validate(); err != nil {
			return invalid("integration.connection_requirement.invalid", "$.candidate.value", err)
		}
		definition, provider, err := integrationConnectorProvider(definitions, value.ConnectorKey, value.ProviderKey)
		if err != nil {
			return invalid("integration.connection_requirement.provider_not_found", "$.candidate.value.provider_key", err)
		}
		if err := validateIntegrationConnection(definition, provider, source.Config, nil, false); err != nil {
			return invalid("integration.connection_requirement.invalid", "$.candidate.value.config", err)
		}
	case "integration.event_mapping":
		var source integrationEventMappingAuthoringFragment
		if err := modulecapability.DecodeKeyedAuthoringValue(request.Candidate, "key", &source); err != nil {
			return invalid("integration.event_mapping.invalid_json", "$.candidate.value", err)
		}
		if source.Key != request.Candidate.Key {
			return invalid("integration.event_mapping.key_mismatch", "$.candidate.value.key", fmt.Errorf("event mapping key %q differs from fragment key %q", source.Key, request.Candidate.Key))
		}
		connectionKey, _ := source.Payload["connection_key"].(string)
		value := integrationsdk.EventMappingRequirement{
			Key: request.Candidate.Key, WorkspaceID: "authoring-validation", Provider: source.Provider,
			ConnectionKey: connectionKey, EventType: source.EventType, CommandPrefix: source.CommandPrefix,
			TargetType: source.TargetType, WorkflowKey: source.WorkflowKey, ObjectKey: source.ObjectKey,
			ObjectKeyPath: source.ObjectKeyPath, RecordID: source.RecordID, RecordIDPath: source.RecordIDPath,
			ActionKey: source.ActionKey, ActionKeyPath: source.ActionKeyPath, ActionInput: source.ActionInput,
			WorkflowInput: source.WorkflowInput, EventFields: source.EventFields, ExternalIdentity: source.ExternalIdentity,
			Payload: source.Payload, Enabled: true,
		}
		if err := value.Validate(); err != nil {
			return invalid("integration.event_mapping.invalid", "$.candidate.value", err)
		}
		if connectionKey != "" {
			fragment, found := modulecapability.FindReferencedFragment(request, "integrations.connections", connectionKey)
			if !found {
				return invalid("integration.event_mapping.connection_not_found", "$.candidate.value.payload.connection_key", fmt.Errorf("connection %q is not present in referenced context", connectionKey))
			}
			var connection integrationConnectionAuthoringFragment
			if err := modulecapability.DecodeKeyedAuthoringValue(fragment, "key", &connection); err != nil {
				return invalid("integration.event_mapping.connection_invalid", "$.referenced_context", err)
			}
			if connection.ProviderKey != source.Provider {
				return invalid("integration.event_mapping.connection_provider_mismatch", "$.candidate.value.payload.connection_key", fmt.Errorf("connection %q uses provider %q, not mapping provider %q", connectionKey, connection.ProviderKey, source.Provider))
			}
		}
	default:
		return modulecapability.ValidationResult{}, &modulecapability.Error{StatusCode: 400, Code: "module_capability.validation_scope_invalid"}
	}
	_ = ctx
	return result, nil
}

func validateIntegrationOperation(definitions []connectorscatalog.ConnectorSchema, connectorKey, providerKey, operationKey string) error {
	connectorKey, providerKey, operationKey = strings.TrimSpace(connectorKey), strings.TrimSpace(providerKey), strings.TrimSpace(operationKey)
	for _, definition := range definitions {
		if definition.Key != connectorKey {
			continue
		}
		if providerKey != "" {
			providerFound, operationFound := false, false
			for _, provider := range definition.Providers {
				if provider.Key != providerKey {
					continue
				}
				providerFound = true
				for _, key := range provider.OperationKeys {
					operationFound = operationFound || key == operationKey
				}
			}
			if !providerFound || !operationFound {
				break
			}
		}
		for _, operation := range definition.Operations {
			if operation.Key == strings.TrimSpace(operationKey) {
				return nil
			}
		}
		break
	}
	return fmt.Errorf("connector operation %s/%s/%s is not declared by the official catalog", connectorKey, providerKey, operationKey)
}

func integrationConnectorProvider(definitions []connectorscatalog.ConnectorSchema, connectorKey, providerKey string) (connectorscatalog.ConnectorSchema, connectorscatalog.ConnectorProviderSchema, error) {
	connectorKey, providerKey = strings.TrimSpace(connectorKey), strings.TrimSpace(providerKey)
	for _, definition := range definitions {
		if definition.Key != connectorKey {
			continue
		}
		for _, provider := range definition.Providers {
			if provider.Key == providerKey {
				return definition, provider, nil
			}
		}
		return connectorscatalog.ConnectorSchema{}, connectorscatalog.ConnectorProviderSchema{}, fmt.Errorf("provider %s/%s is not declared by the official catalog", connectorKey, providerKey)
	}
	return connectorscatalog.ConnectorSchema{}, connectorscatalog.ConnectorProviderSchema{}, fmt.Errorf("connector %s is not declared by the official catalog", connectorKey)
}

func validateIntegrationConnection(definition connectorscatalog.ConnectorSchema, provider connectorscatalog.ConnectorProviderSchema, config map[string]any, secretRefs map[string]string, requireSecrets bool) error {
	if config == nil {
		config = map[string]any{}
	}
	if err := connectorscatalog.ValidateProviderConfig(definition, provider.Key, config); err != nil {
		return err
	}
	declaredSecrets := make(map[string]connectorscatalog.FieldSchema, len(provider.SecretFields))
	for _, field := range provider.SecretFields {
		declaredSecrets[strings.TrimSpace(field.Key)] = field
	}
	for key, reference := range secretRefs {
		if _, found := declaredSecrets[strings.TrimSpace(key)]; !found {
			return fmt.Errorf("Integration Provider secret field %q is not declared", key)
		}
		if strings.TrimSpace(reference) == "" {
			return fmt.Errorf("Integration Provider secret reference %q is empty", key)
		}
	}
	if requireSecrets {
		for key, field := range declaredSecrets {
			if field.Required && strings.TrimSpace(secretRefs[key]) == "" {
				return fmt.Errorf("Integration Provider secret reference %q is required", key)
			}
		}
	}
	return nil
}
