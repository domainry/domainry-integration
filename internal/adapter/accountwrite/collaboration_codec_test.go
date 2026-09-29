package accountwrite

import (
	"encoding/json"
	"testing"

	"github.com/domainry/domainry-connector-sdk/collaborationwrite"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

func collaborationWriteClaim(payload json.RawMessage) model.AccountWriteClaim {
	return model.AccountWriteClaim{
		InvocationID: "account-write:1",
		Request: model.ConnectionAccountWriteRequest{
			ExpectedSource: model.ConnectionAccountWriteSource{Operation: collaborationwrite.SendOperationKey, ContractSHA256: collaborationwrite.OperationSHA256(collaborationwrite.SendOperationKey)},
			Payload:        payload,
		},
	}
}

func TestCollaborationWriteCodecKeepsOneRecipientAndBoundReceipt(t *testing.T) {
	codec := Codec{}
	op := model.ConnectionAccountWriteOperation{Operation: collaborationwrite.SendOperationKey, ContractSHA256: collaborationwrite.OperationSHA256(collaborationwrite.SendOperationKey)}
	payload, err := codec.CanonicalPayload(op, json.RawMessage(`{"text":"已确认。","recipient":"buyer@example.test"}`))
	if err != nil || string(payload) != `{"recipient":"buyer@example.test","text":"已确认。"}` {
		t.Fatalf("payload=%s err=%v", payload, err)
	}
	receipt, err := codec.Receipt(collaborationWriteClaim(payload), json.RawMessage(`{"request_ref":"account-write:1","status":"accepted","message_id":"message-1","accepted_at":"2026-09-28T12:00:00+08:00","delivery":"unknown"}`))
	if err != nil || string(receipt) != `{"request_ref":"account-write:1","status":"accepted","message_id":"message-1","accepted_at":"2026-09-28T12:00:00+08:00","delivery":"unknown"}` {
		t.Fatalf("receipt=%s err=%v", receipt, err)
	}
}

func TestCollaborationWriteCodecRejectsForeignFieldsAndReceipts(t *testing.T) {
	codec := Codec{}
	op := model.ConnectionAccountWriteOperation{Operation: collaborationwrite.SendOperationKey, ContractSHA256: collaborationwrite.OperationSHA256(collaborationwrite.SendOperationKey)}
	for _, raw := range []string{
		`{"recipient":"buyer@example.test","text":"ok","access_token":"secret"}`,
		`{"recipient":"one@example.test,two@example.test","text":"ok"}`,
	} {
		if _, err := codec.CanonicalPayload(op, json.RawMessage(raw)); err == nil {
			t.Fatalf("invalid payload accepted: %s", raw)
		}
	}
	claim := collaborationWriteClaim(json.RawMessage(`{"recipient":"buyer@example.test","text":"ok"}`))
	for _, raw := range []string{
		`{"request_ref":"other","status":"accepted","message_id":"message-1","accepted_at":"2026-09-28T12:00:00Z","delivery":"unknown"}`,
		`{"request_ref":"account-write:1","status":"accepted","message_id":"message-1","accepted_at":"2026-09-28T12:00:00Z","delivery":"delivered"}`,
		`{"request_ref":"account-write:1","status":"accepted","message_id":"message-1","accepted_at":"2026-09-28T12:00:00Z","delivery":"unknown","native":{}}`,
	} {
		if _, err := codec.Receipt(claim, json.RawMessage(raw)); err == nil {
			t.Fatalf("invalid receipt accepted: %s", raw)
		}
	}
}
