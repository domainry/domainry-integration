package integrationmodel

import "testing"

func TestEventMappingAllowsObjectLevelActionWithoutRecordIdentity(t *testing.T) {
	mapping := EventMappingRequirement{
		Key: "meeting-event", WorkspaceID: "workspace-a", Provider: "calendar", EventType: "calendar.changed",
		TargetType: "action", ObjectKey: "meeting", ActionKey: "meeting.ingest", Enabled: true,
	}
	if err := mapping.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestEventMappingClosesAutomationRuleAndMappedInput(t *testing.T) {
	mapping := EventMappingRequirement{
		Key: "contact-webhook", WorkspaceID: "workspace-a", Provider: "crm", EventType: "contact.changed",
		TargetType: "automation", AutomationRuleKey: "contact.webhook",
		AutomationInput: map[string]string{"name": "contact.name"},
		EventFields:     []EventFieldRequirement{{Path: "contact.name", Type: "text"}}, Enabled: true,
	}
	if err := mapping.Validate(); err != nil {
		t.Fatal(err)
	}
	missingRule := mapping
	missingRule.AutomationRuleKey = ""
	if err := missingRule.Validate(); err == nil {
		t.Fatal("Automation mapping without a finite rule accepted")
	}
	undeclaredInput := mapping
	undeclaredInput.AutomationInput = map[string]string{"name": "contact.missing"}
	if err := undeclaredInput.Validate(); err == nil {
		t.Fatal("Automation mapping with undeclared input accepted")
	}
}
