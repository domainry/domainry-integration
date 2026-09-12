package integrationsdkadapter

import (
	"context"
	sdk "github.com/domainry/domainry-integration-sdk"
	application "github.com/domainry/domainry-integration/internal/application/integration"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type accountReadsBinding struct {
	service *application.AccountReadService
}

func (b accountReadsBinding) AuthorizeConnectionAccountRead(ctx context.Context, subject sdk.ConnectionAccountSubject, key string, request sdk.ConnectionAccountReadOperation) (sdk.ConnectionAccountReadAccess, error) {
	s, err := convert[model.ConnectionAccountSubject](subject, nil)
	if err != nil {
		return sdk.ConnectionAccountReadAccess{}, err
	}
	r, err := convert[model.ConnectionAccountReadOperation](request, nil)
	if err != nil {
		return sdk.ConnectionAccountReadAccess{}, err
	}
	value, err := b.service.AuthorizeConnectionAccountRead(ctx, s, key, r)
	return convert[sdk.ConnectionAccountReadAccess](value, err)
}

func (b accountReadsBinding) ReadConnectionAccount(ctx context.Context, subject sdk.ConnectionAccountSubject, key string, request sdk.ConnectionAccountReadRequest) (sdk.ConnectionAccountReadResult, error) {
	s, err := convert[model.ConnectionAccountSubject](subject, nil)
	if err != nil {
		return sdk.ConnectionAccountReadResult{}, err
	}
	r, err := convert[model.ConnectionAccountReadRequest](request, nil)
	if err != nil {
		return sdk.ConnectionAccountReadResult{}, err
	}
	value, err := b.service.ReadConnectionAccount(ctx, s, key, r)
	return convert[sdk.ConnectionAccountReadResult](value, err)
}

var _ sdk.ConnectionAccountReads = accountReadsBinding{}
