package accountwrite

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/domainry/domainry-connector-sdk/mcptool"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

func mcpWriteClaim(t *testing.T, payload string) model.AccountWriteClaim {
	t.Helper()
	return model.AccountWriteClaim{Request: model.ConnectionAccountWriteRequest{
		ExpectedSource: model.ConnectionAccountWriteSource{Operation: mcptool.CallToolOperationKey, ContractSHA256: mcptool.CallToolOperationSHA256},
		Payload:        json.RawMessage(payload),
	}}
}

func TestMCPWriteCodecCanonicalizesApprovedCallAndBoundedReceipt(t *testing.T) {
	codec := Codec{}
	op := model.ConnectionAccountWriteOperation{Operation: mcptool.CallToolOperationKey, ContractSHA256: mcptool.CallToolOperationSHA256}
	if err := codec.ValidateOperation(op); err != nil {
		t.Fatal(err)
	}
	payload, err := codec.CanonicalPayload(op, json.RawMessage(`{"tool_name":"lookup","arguments":{"id":7},"approved":true}`))
	if err != nil || string(payload) != `{"tool_name":"lookup","arguments":{"id":7},"approved":true}` {
		t.Fatalf("payload=%s err=%v", payload, err)
	}
	receipt, err := codec.Receipt(mcpWriteClaim(t, string(payload)), json.RawMessage(`{"tool_name":"lookup","content":[{"type":"text","text":"ok"}],"structuredContent":{"id":7},"isError":false,"_meta":{"private":"drop"},"vendor_extra":"drop"}`))
	if err != nil || string(receipt) != `{"content":[{"type":"text","text":"ok"}],"structuredContent":{"id":7},"tool_name":"lookup"}` {
		t.Fatalf("receipt=%s err=%v", receipt, err)
	}
}

func TestMCPWriteCodecRejectsUnapprovedOrUnboundResults(t *testing.T) {
	codec := Codec{}
	op := model.ConnectionAccountWriteOperation{Operation: mcptool.CallToolOperationKey, ContractSHA256: mcptool.CallToolOperationSHA256}
	for _, raw := range []string{
		`{"tool_name":"lookup","arguments":{}}`,
		`{"tool_name":" lookup","arguments":{},"approved":true}`,
		`{"tool_name":"lookup","arguments":{},"approved":true,"authority":"model"}`,
	} {
		if _, err := codec.CanonicalPayload(op, json.RawMessage(raw)); err == nil {
			t.Fatalf("invalid payload accepted: %s", raw)
		}
	}
	claim := mcpWriteClaim(t, `{"tool_name":"lookup","arguments":{},"approved":true}`)
	for _, raw := range []string{
		`{"tool_name":"other","content":[]}`,
		`{"tool_name":"lookup","isError":true,"content":[]}`,
		`{"tool_name":"lookup"}`,
		`{"tool_name":"lookup","content":{}}`,
		`{"tool_name":"lookup","content":[]}` + strings.Repeat(" ", 33<<10),
	} {
		if _, err := codec.Receipt(claim, json.RawMessage(raw)); err == nil {
			t.Fatalf("invalid receipt accepted: %.80s", raw)
		}
	}
}
