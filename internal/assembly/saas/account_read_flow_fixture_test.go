package saas

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	"github.com/domainry/domainry-foundation/modulehttp"
	identity "github.com/domainry/domainry-identity-sdk"
	sdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/remote"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

type accountReadFlowFixture struct {
	t                *testing.T
	mode, path       string
	upstream         *httptest.Server
	provider         func(connector.Transport) (connector.Adapter, error)
	allowRead        atomic.Bool
	db               *sql.DB
	binding          sdk.Binding
	owner            *Service
	backend, product *httptest.Server
}

func (f *accountReadFlowFixture) open() {
	f.t.Helper()
	var err error
	f.db, err = sql.Open("sqlite", f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	f.db.SetMaxOpenConns(1)
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	p, err := f.provider(accountReadFlowTransport{base: f.upstream.URL, client: f.upstream.Client()})
	if err != nil {
		f.t.Fatal(err)
	}
	r := connector.NewRegistry()
	if err = r.Register(p); err != nil {
		f.t.Fatal(err)
	}
	r.Freeze()
	host := oauthFlowHost{accountSaaSHost: accountSaaSHost{newTestHost(f.db, dialect.WithSchema(""))}, registry: r}
	if f.mode == "module" {
		f.binding, err = moduleassembly.NewFactory().OpenModule(f.t.Context(), sdk.ApplicationRef{RuntimeID: "mail-module"}, host)
	} else {
		f.owner, err = Open(f.t.Context(), sdk.ApplicationRef{RuntimeID: "mail-service"}, host, "test-service-token")
		if err != nil {
			f.t.Fatal(err)
		}
		f.backend = httptest.NewServer(f.owner.Handler)
		f.binding, err = NewFactory(remote.NewFactory(remote.Options{BaseURL: f.backend.URL, Token: "test-service-token", HTTPClient: f.backend.Client()})).OpenSaaS(f.t.Context(), sdk.ApplicationRef{RuntimeID: "mail-product"}, nil)
	}
	if err != nil {
		f.t.Fatal(err)
	}
	adapter := f.binding.(modulehttp.Provider).HTTPAdapters()[0]
	f.product = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		p := saasAccountPrincipal("workspace-a", user, false)
		if f.allowRead.Load() {
			for _, action := range []string{"read_access", "read"} {
				p.AccessBundle.FunctionGrants = append(p.AccessBundle.FunctionGrants, identity.FunctionGrant{Resource: "integration.connection_accounts", Action: identity.Action(action), Effect: identity.EffectAllow})
				p.AccessBundle.DataPolicies = append(p.AccessBundle.DataPolicies, identity.DataPolicy{Key: "integration.connection_accounts." + action, Resource: "integration.connection_accounts", Action: identity.Action(action), Effect: identity.EffectAllow, DataScopes: []identity.DataScope{identity.DataScopeOwner}, Predicate: identity.Predicate{Fact: "owner_user_id", Operator: identity.OperatorEqual, Value: "$subject.id"}})
			}
		}
		adapter.Handler().ServeHTTP(w, r.WithContext(identity.WithRequestIdentity(r.Context(), identity.RequestIdentity{Principal: p})))
	}))
}
func (f *accountReadFlowFixture) close() {
	if f.product != nil {
		f.product.Close()
		f.product = nil
	}
	if f.backend != nil {
		f.backend.Close()
		f.backend = nil
	}
	if f.owner != nil {
		_ = f.owner.Close(context.Background())
		f.owner = nil
	}
	if f.binding != nil {
		_ = f.binding.Close(context.Background())
		f.binding = nil
	}
	if f.db != nil {
		_ = f.db.Close()
		f.db = nil
	}
}
func (f *accountReadFlowFixture) request(user, account, path string, in any, status int) []byte {
	f.t.Helper()
	raw, _ := json.Marshal(in)
	r, err := http.NewRequest("POST", f.product.URL+"/integration/connection-accounts/"+url.PathEscape(account)+path, bytes.NewReader(raw))
	if err != nil {
		f.t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+user)
	r.Header.Set("Content-Type", "application/json")
	response, err := f.product.Client().Do(r)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	b, err := io.ReadAll(response.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	if response.StatusCode != status {
		f.t.Fatalf("%s status %d want %d: %s", path, response.StatusCode, status, b)
	}
	if status == 200 && response.Header.Get("Cache-Control") != "no-store" {
		f.t.Fatal("mail response cacheable")
	}
	if strings.Contains(string(b), "private-") || status != 200 && strings.Contains(string(b), "SENSITIVE-MAIL") {
		f.t.Fatal("mail response disclosed credentials or unauthorized content")
	}
	return b
}
