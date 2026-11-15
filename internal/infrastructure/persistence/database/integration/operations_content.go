package integration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	connector "github.com/domainry/domainry-connector-sdk"
	integrationmodel "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

const guardedCallIdentityKey = "guarded_call_sha256"

func freezeProviderContentRequest(r integrationmodel.ProviderCallRequest) integrationmodel.ProviderCallRequest {
	if r.ExpectedConnection != nil {
		copy := *r.ExpectedConnection
		r.ExpectedConnection = &copy
		r.Payload = append([]byte(nil), r.Payload...)
	}
	if r.Content != nil {
		copy := *r.Content
		copy.Data = append([]byte(nil), r.Content.Data...)
		r.Content = &copy
	}
	return r
}

func sameGuardedProviderRequest(r integrationmodel.ProviderCallRequest, invocation integrationmodel.Invocation) bool {
	previous, _ := invocation.Metadata[guardedCallIdentityKey].(string)
	current := r.GuardedCallSHA256()
	return previous == current
}

func providerTargetMatches(expected *integrationmodel.ProviderCallTarget, connection deliveryConnection, operation connector.OperationDescriptor) bool {
	return expected == nil || expected.ConnectionUpdatedAt == connection.UpdatedAt && expected.ConnectionSHA256 == providerConnectionSHA256(connection) && expected.ProviderKey == connection.ProviderKey && expected.ContractSHA256 == operation.ContractSHA256
}

func providerConnectionSHA256(c deliveryConnection) string {
	config, refs := c.Config, c.SecretRefs
	if len(config) == 0 {
		config = nil
	}
	if len(refs) == 0 {
		refs = nil
	}
	raw, err := json.Marshal([]any{c.WorkspaceID, c.Key, c.ConnectorKey, c.ProviderKey, c.Status, c.UpdatedAt, config, refs})
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
