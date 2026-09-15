// Package accountwrite adapts the closed public write contracts to owner ports.
// It imports no Provider, account database, transport or other business owner.
package accountwrite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/domainry/domainry-connector-sdk/calendarwrite"
	"github.com/domainry/domainry-connector-sdk/mailwrite"
	"github.com/domainry/domainry-connector-sdk/mcptool"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type Codec struct{}

func (Codec) ValidateOperation(op model.ConnectionAccountWriteOperation) error {
	var hash string
	switch op.Operation {
	case calendarwrite.CreateOperationKey, calendarwrite.UpdateOperationKey:
		hash = calendarwrite.OperationSHA256(op.Operation)
	case mailwrite.SendOperationKey, mailwrite.ReplyOperationKey:
		hash = mailwrite.OperationSHA256(op.Operation)
	case mcptool.CallToolOperationKey:
		hash = mcptool.OperationSHA256(op.Operation)
	}
	if hash == "" || op.ContractSHA256 != hash {
		return fmt.Errorf("Integration account write contract is unsupported")
	}
	return nil
}

func (c Codec) CanonicalPayload(op model.ConnectionAccountWriteOperation, raw json.RawMessage) (json.RawMessage, error) {
	if err := c.ValidateOperation(op); err != nil {
		return nil, err
	}
	var value any
	switch op.Operation {
	case calendarwrite.CreateOperationKey:
		value = &calendarwrite.CreateRequest{}
	case calendarwrite.UpdateOperationKey:
		value = &calendarwrite.UpdateRequest{}
	case mailwrite.SendOperationKey:
		value = &mailwrite.SendRequest{}
	case mailwrite.ReplyOperationKey:
		value = &mailwrite.ReplyRequest{}
	case mcptool.CallToolOperationKey:
		var request mcptool.CallToolRequest
		if err := strictReceipt(raw, &request); err != nil || request.Validate() != nil || !request.Approved {
			return nil, fmt.Errorf("MCP account write payload is invalid")
		}
		return json.Marshal(request)
	}
	if err := json.Unmarshal(raw, value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func (Codec) Receipt(claim model.AccountWriteClaim, raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > 32<<10 {
		return nil, fmt.Errorf("Integration account write receipt size is invalid")
	}
	source := claim.Request.ExpectedSource
	switch source.Operation {
	case calendarwrite.CreateOperationKey, calendarwrite.UpdateOperationKey:
		var result calendarwrite.Result
		if err := strictReceipt(raw, &result); err != nil {
			return nil, err
		}
		calendarID, eventID := "", ""
		if source.Operation == calendarwrite.CreateOperationKey {
			var request calendarwrite.CreateRequest
			if err := json.Unmarshal(claim.Request.Payload, &request); err != nil {
				return nil, err
			}
			calendarID = request.CalendarID
		} else {
			var request calendarwrite.UpdateRequest
			if err := json.Unmarshal(claim.Request.Payload, &request); err != nil {
				return nil, err
			}
			calendarID, eventID = request.CalendarID, request.EventID
		}
		if err := result.Validate(source.Operation, claim.InvocationID, calendarID, eventID); err != nil {
			return nil, err
		}
		return json.Marshal(result)
	case mailwrite.SendOperationKey, mailwrite.ReplyOperationKey:
		var result mailwrite.Result
		if err := strictReceipt(raw, &result); err != nil {
			return nil, err
		}
		original := ""
		if source.Operation == mailwrite.ReplyOperationKey {
			var request mailwrite.ReplyRequest
			if err := json.Unmarshal(claim.Request.Payload, &request); err != nil {
				return nil, err
			}
			original = request.MessageID
		}
		if err := result.Validate(source.Operation, claim.InvocationID, original); err != nil {
			return nil, err
		}
		return json.Marshal(result)
	case mcptool.CallToolOperationKey:
		var request mcptool.CallToolRequest
		if err := json.Unmarshal(claim.Request.Payload, &request); err != nil || request.Validate() != nil || !request.Approved {
			return nil, fmt.Errorf("MCP account write request is invalid")
		}
		var result map[string]json.RawMessage
		if err := strictReceipt(raw, &result); err != nil {
			return nil, err
		}
		var toolName string
		if json.Unmarshal(result["tool_name"], &toolName) != nil || toolName != request.ToolName {
			return nil, fmt.Errorf("MCP account write receipt tool is invalid")
		}
		var isError bool
		if value := result["isError"]; len(value) > 0 && json.Unmarshal(value, &isError) != nil || isError {
			return nil, fmt.Errorf("MCP account write receipt reports an error")
		}
		content, structured := result["content"], result["structuredContent"]
		if len(content) == 0 && len(structured) == 0 {
			return nil, fmt.Errorf("MCP account write receipt has no result")
		}
		receipt := map[string]json.RawMessage{"tool_name": result["tool_name"]}
		if len(content) > 0 {
			var blocks []json.RawMessage
			if json.Unmarshal(content, &blocks) != nil {
				return nil, fmt.Errorf("MCP account write content is invalid")
			}
			receipt["content"] = content
		}
		if len(structured) > 0 {
			if !json.Valid(structured) {
				return nil, fmt.Errorf("MCP account write structured result is invalid")
			}
			receipt["structuredContent"] = structured
		}
		return json.Marshal(receipt)
	}
	return nil, fmt.Errorf("Integration account write receipt contract is unsupported")
}

func strictReceipt(raw []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("Integration account write receipt has trailing data")
	}
	return nil
}
