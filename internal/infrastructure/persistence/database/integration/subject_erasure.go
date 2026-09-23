package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/domainry/domainry-integration-sdk/modulehost"
	model "github.com/domainry/domainry-integration/internal/domain/integration/model"
	lifecyclemodel "github.com/domainry/domainry-lifecycle-sdk/model"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
	"github.com/domainry/domainry-orm/query"
)

type SubjectLifecycleStore struct {
	db               modulehost.Database
	dialect          modulehost.Dialect
	subjectLifecycle *SubjectLifecyclePersistence
	definitions      metadatasdk.DefinitionStore
}

func NewSubjectLifecycleStore(db modulehost.Database, dialect modulehost.Dialect, definitions metadatasdk.DefinitionStore, subjectLifecycle ...*SubjectLifecyclePersistence) *SubjectLifecycleStore {
	return &SubjectLifecycleStore{db: db, dialect: dialect, definitions: definitions, subjectLifecycle: subjectLifecyclePersistence(subjectLifecycle)}
}

func (s *SubjectLifecycleStore) BindSubjectLifecyclePersistence(ctx context.Context) error {
	for table, columns := range map[string][]string{
		"_subject_requests": {"workspace_id", "id", "request_type", "kind", "resolved_identity"},
		"_subject_steps":    {"workspace_id", "request_id", "owner", "operation", "payload_json", "completed_at"},
	} {
		statement, args, err := query.NewSelectBuilder(s.dialect, table).Columns(columns...).Where(query.AlwaysFalse()).Limit(1).Build()
		if err != nil {
			return err
		}
		rows, err := s.db.QueryContext(ctx, statement, args...)
		if err != nil {
			return fmt.Errorf("Integration shared subject lifecycle table %s is unavailable: %w", table, err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	s.subjectLifecycle.Bind()
	return nil
}

func (s *SubjectLifecycleStore) SubjectLifecyclePersistenceBound() bool {
	return s != nil && s.subjectLifecycle.Bound()
}

type subjectRow struct {
	Table string `json:"table"`
	ID    string `json:"id"`
}
type subjectPlan struct {
	Request        model.SubjectErasureRequest `json:"request"`
	ConnectionKeys []string                    `json:"connection_keys"`
	SecretKeys     []string                    `json:"secret_keys"`
	ExternalFences []subjectExternalFence      `json:"external_fences"`
	Rows           []subjectRow                `json:"rows"`
	Fences         []subjectFenceReference     `json:"fences"`
}

type subjectStepQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type subjectStepExecutor interface {
	subjectStepQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}
type subjectSpec struct {
	table, status string
	busy          []string
	erase         bool
	token         bool
	values        map[string]any
}

var subjectSpecs = []subjectSpec{
	{table: "_integration_connection_accounts", values: map[string]any{"created_by": "anonymous"}},
	{table: "_integration_connections", status: "status", values: map[string]any{"name": "", "config_json": "{}", "secret_refs_json": "{}", "granted_scopes_json": nil, "created_by": "anonymous", "status": "revoked"}},
	{table: "_integration_provider_runs", status: "status", busy: []string{"processing", "running"}, token: true, values: map[string]any{"payload_json": "{}", "status": "cancelled", "lease_owner": "", "lease_expires_at": "", "due_at": "", "last_error_code": "integration.subject_erased"}},
	{table: "_integration_webhook_subscriptions", status: "status", values: map[string]any{"name": "", "description": "", "event_types_json": "[]", "created_by": "anonymous", "status": "disabled"}},
	{table: "_integration_oauth_sessions", status: "status", busy: []string{"exchanging"}, erase: true},
	{table: "_integration_web_push_subscriptions", status: "status", erase: true},
	{table: "_integration_external_identities", status: "status", erase: true},
	{table: "_integration_secrets", status: "status", erase: true},
	{table: "_integration_secret_materials", erase: true},
	{table: "_integration_invocations", status: "status", busy: []string{"running", "processing", "reconciling"}, values: map[string]any{"metadata_json": "{}", "request_ref": "", "response_ref": "", "error": "", "status": "failed"}},
	{table: "_integration_events", status: "status", busy: []string{"processing", "running"}, token: true, values: map[string]any{"payload_json": "{}", "mapping_key": "", "target_type": "", "execution_json": "{}", "error": "", "status": "cancelled", "next_retry_at": "", "lease_owner": "", "lease_expires_at": ""}},
}

func subjectIn(column string, ids []string) query.Predicate {
	if len(ids) == 0 {
		return query.AlwaysFalse()
	}
	values := make([]any, len(ids))
	for i, v := range ids {
		values[i] = v
	}
	return query.In(column, values...)
}
func subjectResources(refs []model.SubjectRecordReference) query.Predicate {
	ps := []query.Predicate{query.AlwaysFalse()}
	for _, r := range refs {
		ps = append(ps, query.And(query.Equal("object_key", r.ObjectKey), query.Equal("record_id", r.RecordID)))
	}
	return query.Or(ps...)
}
func normalizeSubjectRequest(r model.SubjectErasureRequest) model.SubjectErasureRequest {
	r.LegalHolds = nil
	r.Resources = slices.Clone(r.Resources)
	sort.Slice(r.Resources, func(i, j int) bool {
		if r.Resources[i].ObjectKey != r.Resources[j].ObjectKey {
			return r.Resources[i].ObjectKey < r.Resources[j].ObjectKey
		}
		return r.Resources[i].RecordID < r.Resources[j].RecordID
	})
	r.Resources = slices.Compact(r.Resources)
	r.PublicationMessageIDs = slices.Clone(r.PublicationMessageIDs)
	sort.Strings(r.PublicationMessageIDs)
	r.PublicationMessageIDs = slices.Compact(r.PublicationMessageIDs)
	r.EventIDs = slices.Clone(r.EventIDs)
	sort.Strings(r.EventIDs)
	r.EventIDs = slices.Compact(r.EventIDs)
	return r
}
func (s *SubjectLifecycleStore) sharedStep(ctx context.Context, db subjectStepQueryer, r model.SubjectErasureRequest, operation string) (json.RawMessage, bool, error) {
	stmt, args, err := query.NewWorkspaceSelectBuilder(s.dialect, sharedSubjectExecutionStepsTable, r.WorkspaceID).Columns("payload_json").Where(query.And(
		query.Equal("request_id", r.RequestID),
		query.Equal("owner", integrationSubjectOwner),
		query.Equal("operation", operation),
	)).Build()
	if err != nil {
		return nil, false, err
	}
	var raw string
	if err = db.QueryRowContext(ctx, stmt, args...).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	var step lifecyclemodel.SubjectExecutionStep
	if json.Unmarshal([]byte(raw), &step) != nil || step.WorkspaceID != r.WorkspaceID || step.RequestID != r.RequestID || step.Owner != integrationSubjectOwner || step.Operation != operation || !json.Valid(step.Payload) {
		return nil, false, fmt.Errorf("Integration shared subject execution step invalid")
	}
	return append(json.RawMessage(nil), step.Payload...), true, nil
}

func (s *SubjectLifecycleStore) saveSharedStep(ctx context.Context, db subjectStepExecutor, r model.SubjectErasureRequest, operation string, payload json.RawMessage) error {
	if !json.Valid(payload) {
		return fmt.Errorf("Integration shared subject execution payload invalid")
	}
	if previous, found, err := s.sharedStep(ctx, db, r, operation); err != nil {
		return err
	} else if found {
		if !bytes.Equal(previous, payload) {
			return fmt.Errorf("Integration shared subject execution step payload conflict")
		}
		return nil
	}
	completedAt := time.Now().UTC()
	step := lifecyclemodel.SubjectExecutionStep{WorkspaceID: r.WorkspaceID, RequestID: r.RequestID, Owner: integrationSubjectOwner, Operation: operation, Payload: append(json.RawMessage(nil), payload...), CompletedAt: completedAt}
	raw, err := json.Marshal(step)
	if err != nil {
		return err
	}
	stmt, args, err := query.NewWorkspaceInsertBuilder(s.dialect, sharedSubjectExecutionStepsTable, r.WorkspaceID).
		Columns("request_id", "owner", "operation", "payload_json", "completed_at").
		Values(r.RequestID, integrationSubjectOwner, operation, string(raw), completedAt.Format(time.RFC3339Nano)).Build()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, stmt, args...)
	return err
}

func (s *SubjectLifecycleStore) requireSharedFence(ctx context.Context, db subjectStepQueryer, r model.SubjectErasureRequest) error {
	stmt, args, err := sharedSubjectFenceRequests(s.dialect, r.WorkspaceID, r.SubjectID, r.RequestID).Build()
	if err != nil {
		return err
	}
	var requestID string
	if err = db.QueryRowContext(ctx, stmt, args...).Scan(&requestID); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("Integration erasure requires Lifecycle fence")
	} else if err != nil {
		return err
	}
	return nil
}

func subjectReceiptPlan(r model.SubjectErasureRequest, saved json.RawMessage) (json.RawMessage, error) {
	var p subjectPlan
	if json.Unmarshal(saved, &p) != nil {
		return nil, fmt.Errorf("Integration shared erasure plan invalid")
	}
	expected, _ := json.Marshal(normalizeSubjectRequest(r))
	actual, _ := json.Marshal(p.Request)
	if !bytes.Equal(expected, actual) {
		return nil, fmt.Errorf("Integration erasure request provenance conflicts")
	}
	return append(json.RawMessage(nil), saved...), nil
}
func (s *SubjectLifecycleStore) selectIDs(ctx context.Context, tx *sql.Tx, r model.SubjectErasureRequest, sp subjectSpec, predicate query.Predicate, prepare bool) ([]subjectRow, error) {
	cols := []string{"id"}
	if sp.status != "" {
		cols = append(cols, sp.status)
	}
	stmt, args, err := query.NewWorkspaceSelectBuilder(s.dialect, sp.table, r.WorkspaceID).Columns(cols...).Where(predicate).OrderBy(query.Ascending("id")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []subjectRow{}
	for rows.Next() {
		var id string
		var status sql.NullString
		dest := []any{&id}
		if sp.status != "" {
			dest = append(dest, &status)
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		if prepare && slices.Contains(sp.busy, status.String) {
			return nil, fmt.Errorf("integration.subject_busy: %s", sp.table)
		}
		result = append(result, subjectRow{sp.table, id})
		if len(result) > 10000 {
			return nil, fmt.Errorf("Integration subject inventory exceeds limit")
		}
	}
	return result, rows.Err()
}
func (s *SubjectLifecycleStore) collect(ctx context.Context, tx *sql.Tx, r model.SubjectErasureRequest, prepare bool) (subjectPlan, error) {
	p := subjectPlan{Request: normalizeSubjectRequest(r), ConnectionKeys: []string{}, SecretKeys: []string{}, Rows: []subjectRow{}}
	stmt, args, err := query.NewWorkspaceSelectBuilder(s.dialect, "_integration_connection_accounts", r.WorkspaceID).Columns("connection_key").Where(query.And(query.Equal("scope", "personal"), query.Equal("owner_user_id", r.SubjectID))).OrderBy(query.Ascending("connection_key")).Build()
	if err != nil {
		return p, err
	}
	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return p, err
	}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return p, err
		}
		p.ConnectionKeys = append(p.ConnectionKeys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, err
	}
	// Inspect typed secret references in all scoped connections. A credential
	// shared by another account or workspace connection is never deleted.
	stmt, args, err = query.NewWorkspaceSelectBuilder(s.dialect, "_integration_connections", r.WorkspaceID).Columns("connection_key", "secret_refs_json").Build()
	if err != nil {
		return p, err
	}
	rows, err = tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return p, err
	}
	owned, shared := map[string]bool{}, map[string]bool{}
	for rows.Next() {
		var key, raw string
		if err = rows.Scan(&key, &raw); err != nil {
			rows.Close()
			return p, err
		}
		var refs map[string]string
		if err = json.Unmarshal([]byte(raw), &refs); err != nil {
			rows.Close()
			return p, fmt.Errorf("Integration connection secret references invalid")
		}
		for _, ref := range refs {
			if strings.HasPrefix(ref, "secret:") {
				secret := strings.TrimPrefix(ref, "secret:")
				if slices.Contains(p.ConnectionKeys, key) {
					owned[secret] = true
				} else {
					shared[secret] = true
				}
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, err
	}
	stmt, args, err = query.NewWorkspaceSelectBuilder(s.dialect, "_integration_secrets", r.WorkspaceID).Columns("secret_key").Where(query.And(query.Equal("credential_type", "secret"), subjectIn("connection_key", p.ConnectionKeys))).Build()
	if err != nil {
		return p, err
	}
	rows, err = tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return p, err
	}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return p, err
		}
		owned[key] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, err
	}
	for key := range owned {
		if !shared[key] {
			p.SecretKeys = append(p.SecretKeys, key)
		}
	}
	sort.Strings(p.SecretKeys)
	eventIDs, externalFences, err := s.subjectEvents(ctx, tx, r, p.ConnectionKeys)
	if err != nil {
		return p, err
	}
	p.ExternalFences = externalFences
	// Account-write receipts have a documented typed actor. Ordinary operation
	// evidence uses operation-v1 metadata; no arbitrary payload text matching.
	actorInvocations := []string{}
	stmt, args, err = query.NewWorkspaceSelectBuilder(s.dialect, "_integration_invocations", r.WorkspaceID).Columns("id", "metadata_json").Build()
	if err != nil {
		return p, err
	}
	rows, err = tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return p, err
	}
	for rows.Next() {
		var id, raw string
		if err = rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return p, err
		}
		var m struct {
			Kind    string                 `json:"kind"`
			ActorID string                 `json:"actor_id"`
			Source  model.InvocationSource `json:"source"`
		}
		if json.Unmarshal([]byte(raw), &m) == nil && (m.Kind == "account-write-v1" || m.Kind == "operation-v1") {
			covered := m.ActorID == r.SubjectID
			if m.Kind == "operation-v1" {
				for _, ref := range r.Resources {
					covered = covered || m.Source.ObjectKey == ref.ObjectKey && m.Source.RecordID == ref.RecordID
				}
			}
			if covered {
				actorInvocations = append(actorInvocations, id)
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, err
	}
	for _, sp := range subjectSpecs {
		predicate := subjectIn("connection_key", p.ConnectionKeys)
		switch sp.table {
		case "_integration_oauth_sessions", "_integration_web_push_subscriptions":
			predicate = query.Equal("user_id", r.SubjectID)
		case "_integration_external_identities":
			predicate = query.Equal("actor_id", r.SubjectID)
		case "_integration_secrets":
			predicate = query.Or(subjectIn("secret_key", p.SecretKeys), query.And(query.Equal("credential_type", "api_key"), query.Equal("actor_id", r.SubjectID)))
		case "_integration_secret_materials":
			predicate = subjectIn("secret_key", p.SecretKeys)
		case "_integration_invocations":
			predicate = query.Or(predicate, subjectIn("id", actorInvocations), subjectIn("request_ref", r.PublicationMessageIDs), subjectIn("event_id", eventIDs), subjectResources(r.Resources))
		case "_integration_events":
			predicate = subjectIn("id", eventIDs)
		}
		found, e := s.selectIDs(ctx, tx, r, sp, predicate, prepare)
		if e != nil {
			return p, e
		}
		p.Rows = append(p.Rows, found...)
		if len(p.Rows) > 10000 {
			return p, fmt.Errorf("Integration subject inventory exceeds limit")
		}
	}
	return p, nil
}
func (s *SubjectLifecycleStore) PreviewSubject(ctx context.Context, r model.SubjectErasureRequest) (json.RawMessage, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	p, err := s.collect(ctx, tx, r, false)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"owner": "integration", "records": len(p.Rows), "personal_connections": len(p.ConnectionKeys), "private_credentials": len(p.SecretKeys)})
}

func subjectPlanFences(p subjectPlan) []subjectFenceReference {
	refs := make([]subjectFenceReference, 0, len(p.ConnectionKeys)+len(p.SecretKeys)+len(p.Request.PublicationMessageIDs)+len(p.Request.Resources)+len(p.ExternalFences)+len(p.Rows))
	for _, key := range p.ConnectionKeys {
		refs = append(refs, subjectFenceReference{Kind: "connection", ID: key})
	}
	for _, key := range p.SecretKeys {
		refs = append(refs, subjectFenceReference{Kind: "secret", ID: key})
	}
	for _, id := range p.Request.PublicationMessageIDs {
		refs = append(refs, subjectFenceReference{Kind: "message", ID: id})
	}
	for _, ref := range p.Request.Resources {
		refs = append(refs, subjectFenceReference{Kind: "resource", Object: ref.ObjectKey, ID: ref.RecordID})
	}
	for _, ref := range p.ExternalFences {
		refs = append(refs, subjectFenceReference{Kind: "external", Object: ref.Provider, ID: ref.SubjectSHA256})
	}
	for _, row := range p.Rows {
		refs = append(refs, subjectFenceReference{Kind: "row", Object: row.Table, ID: row.ID})
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		if refs[i].Object != refs[j].Object {
			return refs[i].Object < refs[j].Object
		}
		return refs[i].ID < refs[j].ID
	})
	return slices.Compact(refs)
}

func (s *SubjectLifecycleStore) PrepareSubjectErasure(ctx context.Context, r model.SubjectErasureRequest) (json.RawMessage, error) {
	if !s.SubjectLifecyclePersistenceBound() {
		return nil, fmt.Errorf("Integration shared subject lifecycle persistence is not bound")
	}
	if saved, found, err := s.sharedStep(ctx, s.db, r, subjectErasePlanOperation); err != nil {
		return nil, err
	} else if found {
		return subjectReceiptPlan(r, saved)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if saved, found, err := s.sharedStep(ctx, tx, r, subjectErasePlanOperation); err != nil {
		return nil, err
	} else if found {
		return subjectReceiptPlan(r, saved)
	}
	if err = s.requireSharedFence(ctx, tx, r); err != nil {
		return nil, err
	}
	p, err := s.collect(ctx, tx, r, true)
	if err != nil {
		return nil, err
	}
	p.Fences = subjectPlanFences(p)
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	for _, row := range p.Rows {
		for _, sp := range subjectSpecs {
			if sp.table != row.Table || sp.status == "" {
				continue
			}
			b := query.NewWorkspaceUpdateBuilder(s.dialect, sp.table, r.WorkspaceID).Set(sp.status, "erasing").Where(query.Equal("id", row.ID))
			if sp.token {
				b.SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).Set("lease_owner", "").Set("lease_expires_at", "")
			}
			stmt, args, e := b.Build()
			if e != nil {
				return nil, e
			}
			if _, e = tx.ExecContext(ctx, stmt, args...); e != nil {
				return nil, e
			}
		}
	}
	if err = s.saveSharedStep(ctx, tx, r, subjectErasePlanOperation, raw); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return raw, nil
}
func (s *SubjectLifecycleStore) ErasePreparedSubject(ctx context.Context, r model.SubjectErasureRequest, raw json.RawMessage) (json.RawMessage, error) {
	if !s.SubjectLifecyclePersistenceBound() {
		return nil, fmt.Errorf("Integration shared subject lifecycle persistence is not bound")
	}
	var p subjectPlan
	if json.Unmarshal(raw, &p) != nil {
		return nil, fmt.Errorf("Integration erasure plan invalid")
	}
	normalized := normalizeSubjectRequest(r)
	if p.Request.WorkspaceID != r.WorkspaceID || p.Request.SubjectID != r.SubjectID || p.Request.RequestID != r.RequestID {
		return nil, fmt.Errorf("Integration erasure plan scope mismatch")
	}
	expected, _ := json.Marshal(normalized)
	actual, _ := json.Marshal(p.Request)
	if !bytes.Equal(expected, actual) {
		return nil, fmt.Errorf("Integration erasure provenance mismatch")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	saved, found, err := s.sharedStep(ctx, tx, r, subjectErasePlanOperation)
	if err != nil {
		return nil, err
	}
	if !found || !bytes.Equal(saved, raw) {
		return nil, fmt.Errorf("Integration erasure plan differs from shared Lifecycle step")
	}
	if result, completed, err := s.sharedStep(ctx, tx, r, subjectEraseOperation); err != nil {
		return nil, err
	} else if completed {
		return result, nil
	}
	for _, row := range p.Rows {
		var sp *subjectSpec
		for i := range subjectSpecs {
			if subjectSpecs[i].table == row.Table {
				sp = &subjectSpecs[i]
				break
			}
		}
		if sp == nil {
			return nil, fmt.Errorf("Integration erasure table invalid")
		}
		var stmt string
		var args []any
		if sp.erase {
			stmt, args, err = query.NewWorkspaceDeleteBuilder(s.dialect, row.Table, r.WorkspaceID).Where(query.Equal("id", row.ID)).Build()
		} else {
			b := query.NewWorkspaceUpdateBuilder(s.dialect, row.Table, r.WorkspaceID).Set("updated_at", ownerNow()).Where(query.Equal("id", row.ID))
			keys := []string{}
			for key := range sp.values {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				b.Set(key, sp.values[key])
			}
			stmt, args, err = b.Build()
		}
		if err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, stmt, args...); err != nil {
			return nil, err
		}
	}
	out, err := json.Marshal(map[string]any{"owner": "integration", "erased": true, "records": len(p.Rows)})
	if err != nil {
		return nil, err
	}
	if err = s.saveSharedStep(ctx, tx, r, subjectEraseOperation, out); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
