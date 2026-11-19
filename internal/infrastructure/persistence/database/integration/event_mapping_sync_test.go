package integration

import (
	"strings"
	"testing"

	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

func TestEventMappingReplacementUsesContentRevisionAndRetainsHistory(t *testing.T) {
	definitions := newTestDefinitionStore()
	initial := []integrationmodel.EventMappingRequirement{{
		Key: "contact-change", WorkspaceID: "workspace-a", Provider: "probe", EventType: "contact.changed",
		TargetType: "action", ObjectKey: "contact", ActionKey: "sync",
		ActionInput: map[string]string{"name": "contact.name"},
		EventFields: []integrationmodel.EventFieldRequirement{{Path: "contact.name", Type: "text"}}, Enabled: true,
	}}
	if err := SynchronizeEventMappingDefinitions(t.Context(), definitions, initial); err != nil {
		t.Fatal(err)
	}
	key := integrationEventMappingDefinitionKey("workspace-a", "contact-change")
	old, found, err := definitions.Get(t.Context(), metadatasdk.DefinitionOwnerIntegration, integrationEventMappingDefinitionKind, key)
	if err != nil || !found || !strings.HasPrefix(old.SchemaVersion, integrationEventMappingSchemaVersion+"-") {
		t.Fatalf("initial event mapping Definition=%#v found=%v err=%v", old, found, err)
	}

	updated := append([]integrationmodel.EventMappingRequirement(nil), initial...)
	updated[0].ActionInput = map[string]string{"name": "contact.display_name"}
	updated[0].EventFields = []integrationmodel.EventFieldRequirement{{Path: "contact.display_name", Type: "text"}}
	if err := SynchronizeEventMappingDefinitions(t.Context(), definitions, updated); err != nil {
		t.Fatal(err)
	}
	current, found, err := definitions.Get(t.Context(), metadatasdk.DefinitionOwnerIntegration, integrationEventMappingDefinitionKind, key)
	if err != nil || !found || current.CurrentVersionID == old.CurrentVersionID || current.SchemaVersion == old.SchemaVersion {
		t.Fatalf("event mapping replacement failed: old=%#v current=%#v found=%v err=%v", old, current, found, err)
	}
	version, found, err := definitions.GetVersion(t.Context(), metadatasdk.DefinitionVersionQuery{
		Owner: metadatasdk.DefinitionOwnerIntegration, ResourceType: integrationEventMappingDefinitionKind,
		ResourceKey: key, VersionID: old.CurrentVersionID,
	})
	if err != nil || !found || !strings.Contains(string(version.Payload), `"contact.name"`) {
		t.Fatalf("immutable event mapping history missing: %#v found=%v err=%v", version, found, err)
	}
}
