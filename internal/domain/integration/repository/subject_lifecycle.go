package integrationrepository

import (
	"context"
	"encoding/json"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
)

type SubjectLifecycleRepository interface {
	PreviewSubject(context.Context, model.SubjectErasureRequest) (json.RawMessage, error)
	PrepareSubjectErasure(context.Context, model.SubjectErasureRequest) (json.RawMessage, error)
	ErasePreparedSubject(context.Context, model.SubjectErasureRequest, json.RawMessage) (json.RawMessage, error)
}
