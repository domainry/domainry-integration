package integrationsdkadapter

import (
	"context"
	"encoding/json"
	sdk "github.com/domainry/domainry-integration-sdk"
	application "github.com/domainry/domainry-integration/internal/application/integration"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type subjectLifecycleBinding struct {
	service *application.SubjectLifecycleService
}

func NewSubjectLifecycleBinding(service *application.SubjectLifecycleService) sdk.SubjectLifecycle {
	return subjectLifecycleBinding{service}
}
func (b subjectLifecycleBinding) PreviewSubject(ctx context.Context, r sdk.SubjectErasureRequest) (json.RawMessage, error) {
	v, err := convert[model.SubjectErasureRequest](r, nil)
	if err != nil {
		return nil, err
	}
	return b.service.PreviewSubject(ctx, v)
}
func (b subjectLifecycleBinding) ExportSubject(ctx context.Context, r sdk.SubjectErasureRequest) (json.RawMessage, error) {
	v, err := convert[model.SubjectErasureRequest](r, nil)
	if err != nil {
		return nil, err
	}
	return b.service.ExportSubject(ctx, v)
}
func (b subjectLifecycleBinding) PrepareSubjectErasure(ctx context.Context, r sdk.SubjectErasureRequest) (json.RawMessage, error) {
	v, err := convert[model.SubjectErasureRequest](r, nil)
	if err != nil {
		return nil, err
	}
	return b.service.PrepareSubjectErasure(ctx, v)
}
func (b subjectLifecycleBinding) ErasePreparedSubject(ctx context.Context, r sdk.SubjectErasureRequest, p json.RawMessage) (json.RawMessage, error) {
	v, err := convert[model.SubjectErasureRequest](r, nil)
	if err != nil {
		return nil, err
	}
	return b.service.ErasePreparedSubject(ctx, v, p)
}

var _ sdk.SubjectLifecycle = subjectLifecycleBinding{}
