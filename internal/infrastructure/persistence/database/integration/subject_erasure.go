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
	"github.com/domainry/domainry-orm/query"
	"github.com/domainry/domainry-orm/sqlhost"
)

type SubjectLifecycleStore struct {
	db      modulehost.Database
	dialect modulehost.Dialect
}

func NewSubjectLifecycleStore(db modulehost.Database, dialect modulehost.Dialect) *SubjectLifecycleStore {
	return &SubjectLifecycleStore{db: db, dialect: dialect}
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
	{table: "_integration_connections", status: "status", values: map[string]any{"name": "", "config_json": "{}", "secret_refs_json": "{}", "created_by": "anonymous", "status": "revoked"}},
	{table: "_integration_connection_account_secrets", erase: true},
	{table: "_integration_connection_grants", erase: true},
	{table: "_integration_connector_provider_states", status: "status", busy: []string{"processing", "running"}, token: true, values: map[string]any{"payload_json": "{}", "status": "cancelled", "lease_owner": "", "lease_expires_at": "", "due_at": "", "last_error_code": "integration.subject_erased"}},
	{table: "_integration_connector_provider_commits", status: "status", busy: []string{"processing", "running"}, token: true, values: map[string]any{"payload_json": "{}", "status": "cancelled", "lease_owner": "", "lease_expires_at": "", "due_at": ""}},
	{table: "_integration_credential_refresh_leases", status: "lease_expires_at", erase: true},
	{table: "_integration_webhook_subscriptions", status: "status", values: map[string]any{"name": "", "description": "", "event_types_json": "[]", "created_by": "anonymous", "status": "disabled"}},
	{table: "_integration_oauth_sessions", status: "status", busy: []string{"exchanging"}, erase: true},
	{table: "_integration_web_push_subscriptions", status: "status", erase: true},
	{table: "_integration_api_keys", status: "status", erase: true},
	{table: "_integration_external_identities", status: "status", erase: true},
	{table: "_integration_secrets", status: "status", erase: true},
	{table: "_integration_secret_materials", erase: true},
	{table: "_integration_invocations", status: "status", busy: []string{"running", "processing", "reconciling"}, values: map[string]any{"metadata_json": "{}", "request_ref": "", "response_ref": "", "error": "", "status": "failed"}},
	{table: "_integration_events", status: "status", busy: []string{"processing", "running"}, token: true, values: map[string]any{"payload_json": "{}", "error": "", "status": "cancelled", "next_retry_at": "", "lease_owner": "", "lease_expires_at": ""}},
	{table: "_integration_event_mapping_intents", status: "status", values: map[string]any{"payload_json": "{}", "status": "cancelled"}},
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
func (s *SubjectLifecycleStore) receipt(ctx context.Context, db sqlhost.Queryer, r model.SubjectErasureRequest) (string, string, error) {
	stmt, args, err := query.NewWorkspaceSelectBuilder(s.dialect, "_integration_subject_erasure_receipts", r.WorkspaceID).Columns("subject_id", "plan_json", "result_json").Where(query.Equal("request_id", r.RequestID)).Build()
	if err != nil {
		return "", "", err
	}
	var subject, p, result string
	err = db.QueryRowContext(ctx, stmt, args...).Scan(&subject, &p, &result)
	if err == nil && subject != r.SubjectID {
		return "", "", fmt.Errorf("Integration erasure receipt subject mismatch")
	}
	return p, result, err
}

func subjectReceiptPlan(r model.SubjectErasureRequest, saved string) (json.RawMessage, error) {
	var p subjectPlan
	if json.Unmarshal([]byte(saved), &p) != nil {
		return nil, fmt.Errorf("Integration source receipt invalid")
	}
	expected, _ := json.Marshal(normalizeSubjectRequest(r))
	actual, _ := json.Marshal(p.Request)
	if !bytes.Equal(expected, actual) {
		return nil, fmt.Errorf("Integration erasure request provenance conflicts")
	}
	return json.RawMessage(saved), nil
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
		if prepare && sp.table == "_integration_credential_refresh_leases" && status.String != "" {
			deadline, e := time.Parse(time.RFC3339Nano, status.String)
			if e != nil || deadline.After(time.Now().UTC()) {
				return nil, fmt.Errorf("integration.subject_busy: credential refresh")
			}
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
	stmt, args, err = query.NewWorkspaceSelectBuilder(s.dialect, "_integration_connection_account_secrets", r.WorkspaceID).Columns("secret_key").Where(subjectIn("connection_key", p.ConnectionKeys)).Build()
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
		case "_integration_api_keys", "_integration_external_identities":
			predicate = query.Equal("actor_id", r.SubjectID)
		case "_integration_secrets", "_integration_secret_materials":
			predicate = subjectIn("secret_key", p.SecretKeys)
		case "_integration_invocations":
			predicate = query.Or(predicate, subjectIn("id", actorInvocations), subjectIn("request_ref", r.PublicationMessageIDs), subjectIn("event_id", eventIDs), subjectResources(r.Resources))
		case "_integration_events":
			predicate = subjectIn("id", eventIDs)
		case "_integration_event_mapping_intents":
			predicate = subjectIn("event_id", eventIDs)
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
func (s *SubjectLifecycleStore) fence(ctx context.Context, tx *sql.Tx, r model.SubjectErasureRequest, kind, object, id string) error {
	stmt, args, err := query.NewWorkspaceSelectBuilder(s.dialect, "_integration_subject_erasure_fences", r.WorkspaceID).Columns("id").Where(query.And(query.Equal("kind", kind), query.Equal("object_key", object), query.Equal("resource_id", id))).Build()
	if err != nil {
		return err
	}
	var found string
	err = tx.QueryRowContext(ctx, stmt, args...).Scan(&found)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	stmt, args, err = query.NewWorkspaceInsertBuilder(s.dialect, "_integration_subject_erasure_fences", r.WorkspaceID).Columns("id", "kind", "object_key", "resource_id", "request_id").Values(ownerID("erasure-fence:", r.WorkspaceID, kind+"\x00"+object+"\x00"+id), kind, object, id, r.RequestID).Build()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, stmt, args...)
	return err
}
func (s *SubjectLifecycleStore) PrepareSubjectErasure(ctx context.Context, r model.SubjectErasureRequest) (json.RawMessage, error) {
	if saved, _, err := s.receipt(ctx, s.db, r); err == nil {
		return subjectReceiptPlan(r, saved)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if saved, _, err := s.receipt(ctx, tx, r); err == nil {
		return subjectReceiptPlan(r, saved)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	p, err := s.collect(ctx, tx, r, true)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if err = s.fence(ctx, tx, r, "subject", "", r.SubjectID); err != nil {
		return nil, err
	}
	for _, key := range p.ConnectionKeys {
		if err = s.fence(ctx, tx, r, "connection", "", key); err != nil {
			return nil, err
		}
	}
	for _, key := range p.SecretKeys {
		if err = s.fence(ctx, tx, r, "secret", "", key); err != nil {
			return nil, err
		}
	}
	for _, id := range r.PublicationMessageIDs {
		if err = s.fence(ctx, tx, r, "message", "", id); err != nil {
			return nil, err
		}
	}
	for _, ref := range r.Resources {
		if err = s.fence(ctx, tx, r, "resource", ref.ObjectKey, ref.RecordID); err != nil {
			return nil, err
		}
	}
	for _, ref := range p.ExternalFences {
		if err = s.fence(ctx, tx, r, "external", ref.Provider, ref.SubjectSHA256); err != nil {
			return nil, err
		}
	}
	for _, row := range p.Rows {
		if err = s.fence(ctx, tx, r, "row", row.Table, row.ID); err != nil {
			return nil, err
		}
		for _, sp := range subjectSpecs {
			if sp.table != row.Table || sp.status == "" {
				continue
			}
			if sp.table == "_integration_credential_refresh_leases" {
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
	stmt, args, err := query.NewWorkspaceInsertBuilder(s.dialect, "_integration_subject_erasure_receipts", r.WorkspaceID).Columns("id", "request_id", "subject_id", "plan_json", "result_json").Values(ownerID("erasure-receipt:", r.WorkspaceID, r.RequestID), r.RequestID, r.SubjectID, string(raw), "").Build()
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, stmt, args...); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return raw, nil
}
func (s *SubjectLifecycleStore) ErasePreparedSubject(ctx context.Context, r model.SubjectErasureRequest, raw json.RawMessage) (json.RawMessage, error) {
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
	saved, result, err := s.receipt(ctx, tx, r)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte(saved), raw) {
		return nil, fmt.Errorf("Integration erasure plan differs from source receipt")
	}
	if result != "" {
		return json.RawMessage(result), nil
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
	stmt, args, err := query.NewWorkspaceUpdateBuilder(s.dialect, "_integration_subject_erasure_receipts", r.WorkspaceID).Set("result_json", string(out)).Where(query.Equal("request_id", r.RequestID)).Build()
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, stmt, args...); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
