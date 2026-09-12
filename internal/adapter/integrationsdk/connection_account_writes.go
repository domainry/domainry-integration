package integrationsdkadapter

import (
	"context"
	sdk "github.com/domainry/domainry-integration-sdk"
	application "github.com/domainry/domainry-integration/internal/application/integration"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type accountWritesBinding struct {
	service *application.AccountWriteService
}

func (b accountWritesBinding) AuthorizeConnectionAccountWrite(ctx context.Context, subject sdk.ConnectionAccountSubject, key string, request sdk.ConnectionAccountWriteOperation) (sdk.ConnectionAccountWriteAccess, error) {
	s, err := convert[model.ConnectionAccountSubject](subject, nil)
	if err != nil {
		return sdk.ConnectionAccountWriteAccess{}, err
	}
	r, err := convert[model.ConnectionAccountWriteOperation](request, nil)
	if err != nil {
		return sdk.ConnectionAccountWriteAccess{}, err
	}
	value, err := b.service.AuthorizeConnectionAccountWrite(ctx, s, key, r)
	return convert[sdk.ConnectionAccountWriteAccess](value, err)
}

func (b accountWritesBinding) WriteConnectionAccount(ctx context.Context, subject sdk.ConnectionAccountSubject, key string, request sdk.ConnectionAccountWriteRequest) (sdk.ConnectionAccountWriteResult, error) {
	s, err := convert[model.ConnectionAccountSubject](subject, nil)
	if err != nil {
		return sdk.ConnectionAccountWriteResult{}, err
	}
	r, err := convert[model.ConnectionAccountWriteRequest](request, nil)
	if err != nil {
		return sdk.ConnectionAccountWriteResult{}, err
	}
	value, err := b.service.WriteConnectionAccount(ctx, s, key, r)
	return convert[sdk.ConnectionAccountWriteResult](value, err)
}

func (b accountWritesBinding) ReadConnectionAccountWriteReceipt(ctx context.Context, subject sdk.ConnectionAccountSubject, key string, request sdk.ConnectionAccountWriteRequest) (sdk.ConnectionAccountWriteResult, error) {
	s, err := convert[model.ConnectionAccountSubject](subject, nil)
	if err != nil {
		return sdk.ConnectionAccountWriteResult{}, err
	}
	r, err := convert[model.ConnectionAccountWriteRequest](request, nil)
	if err != nil {
		return sdk.ConnectionAccountWriteResult{}, err
	}
	value, err := b.service.ReadConnectionAccountWriteReceipt(ctx, s, key, r)
	return convert[sdk.ConnectionAccountWriteResult](value, err)
}

var _ sdk.ConnectionAccountWrites = accountWritesBinding{}
