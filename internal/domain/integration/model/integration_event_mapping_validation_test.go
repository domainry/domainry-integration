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
