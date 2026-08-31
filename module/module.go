// Package module exposes the stable in-process Integration module factory.
// Implementation details remain under internal/assembly.
package module

import (
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
	saasassembly "github.com/domainry/domainry-integration/internal/assembly/saas"
)

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory

func OptionsFromEnvironment() Options        { return moduleassembly.OptionsFromEnvironment() }
func NewFactory(options ...Options) *Factory { return moduleassembly.NewFactory(options...) }
func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return moduleassembly.SchemaMigrations(driver, schema)
}

// NewSaaSFactory keeps SaaS product HTTP ownership in Integration while the
// supplied SDK Factory remains responsible for remote service calls.
func NewSaaSFactory(remote integrationsdk.Factory) integrationsdk.Factory {
	return saasassembly.NewFactory(remote)
}
