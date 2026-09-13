package integrationapplication

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/domainry/domainry-foundation/requestcontext"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	repository "github.com/domainry/domainry-integration/internal/domain/integration/repository"
)

type SubjectLifecycleService struct {
	store repository.SubjectLifecycleRepository
}

func NewSubjectLifecycleService(store repository.SubjectLifecycleRepository) *SubjectLifecycleService {
	return &SubjectLifecycleService{store: store}
}
func (s *SubjectLifecycleService) validate(ctx context.Context, r model.SubjectErasureRequest, erase bool) error {
	if err := r.Validate(erase); err != nil {
		return err
	}
	if s.store == nil || requestcontext.WorkspaceID(ctx) != r.WorkspaceID {
		return fmt.Errorf("Integration subject lifecycle scope mismatch")
	}
	return nil
}
func (s *SubjectLifecycleService) PreviewSubject(ctx context.Context, r model.SubjectErasureRequest) (json.RawMessage, error) {
	if err := s.validate(ctx, r, false); err != nil {
		return nil, err
	}
	return s.store.PreviewSubject(ctx, r)
}
func (s *SubjectLifecycleService) ExportSubject(ctx context.Context, r model.SubjectErasureRequest) (json.RawMessage, error) {
	return s.PreviewSubject(ctx, r)
}
func (s *SubjectLifecycleService) PrepareSubjectErasure(ctx context.Context, r model.SubjectErasureRequest) (json.RawMessage, error) {
	if err := s.validate(ctx, r, true); err != nil {
		return nil, err
	}
	return s.store.PrepareSubjectErasure(ctx, r)
}
func (s *SubjectLifecycleService) ErasePreparedSubject(ctx context.Context, r model.SubjectErasureRequest, p json.RawMessage) (json.RawMessage, error) {
	if err := s.validate(ctx, r, true); err != nil {
		return nil, err
	}
	return s.store.ErasePreparedSubject(ctx, r, p)
}
