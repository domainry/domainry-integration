// Package module exposes the stable in-process Integration module factory.
// Implementation details remain under internal/assembly.
package module

import (
	"fmt"

	"github.com/domainry/domainry-foundation/schemaownership"
	identitysdk "github.com/domainry/domainry-identity-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationapplication "github.com/domainry/domainry-integration/internal/application/integration"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	saasassembly "github.com/domainry/domainry-integration/internal/assembly/saas"
	databaseschema "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/schema"
)

// ConnectionAccountSubjectForPrincipal compiles one current Identity action
// into the narrow personal/workspace account scope accepted by Integration's
// public account ports. Models and browser payloads cannot construct this value.
func ConnectionAccountSubjectForPrincipal(principal identitysdk.Principal, permissionKey string) (integrationsdk.ConnectionAccountSubject, error) {
	scope, err := integrationapplication.AuthorizeDataAccess(principal, permissionKey)
	if err != nil {
		return integrationsdk.ConnectionAccountSubject{}, err
	}
	personal, workspace := scope.ConnectionAccountAccess()
	if !personal && !workspace {
		return integrationsdk.ConnectionAccountSubject{}, fmt.Errorf("Integration connection account scope is denied")
	}
	return integrationsdk.ConnectionAccountSubject{
		WorkspaceID: principal.WorkspaceID,
		UserID:      principal.UserID,
		Access:      integrationsdk.ConnectionAccountAccess{Personal: personal, Workspace: workspace},
	}, nil
}

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory

const MigrationOwner = databaseschema.MigrationOwner

func OptionsFromEnvironment() Options        { return moduleassembly.OptionsFromEnvironment() }
func NewFactory(options ...Options) *Factory { return moduleassembly.NewFactory(options...) }
func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return moduleassembly.SchemaMigrations(driver, schema)
}
func SchemaOwnership() []schemaownership.Table { return databaseschema.SchemaOwnership() }
func OwnedTables() []string                    { return schemaownership.Names(SchemaOwnership()) }

// NewSaaSFactory keeps SaaS product HTTP ownership in Integration while the
// supplied SDK Factory remains responsible for remote service calls.
func NewSaaSFactory(remote integrationsdk.Factory) integrationsdk.Factory {
	return saasassembly.NewFactory(remote)
}
