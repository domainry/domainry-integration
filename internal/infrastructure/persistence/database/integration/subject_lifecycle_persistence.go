package integration

import "sync/atomic"

// SubjectLifecyclePersistence is shared by every owner-side store assembled
// for one Integration binding. Ordinary standalone Integration use does not
// query Lifecycle-owned tables until the host explicitly binds them.
type SubjectLifecyclePersistence struct {
	bound atomic.Bool
}

func NewSubjectLifecyclePersistence() *SubjectLifecyclePersistence {
	return &SubjectLifecyclePersistence{}
}

func (p *SubjectLifecyclePersistence) Bind() {
	if p != nil {
		p.bound.Store(true)
	}
}

func (p *SubjectLifecyclePersistence) Bound() bool {
	return p != nil && p.bound.Load()
}

func subjectLifecyclePersistence(values []*SubjectLifecyclePersistence) *SubjectLifecyclePersistence {
	if len(values) != 0 && values[0] != nil {
		return values[0]
	}
	return NewSubjectLifecyclePersistence()
}
