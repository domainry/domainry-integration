// Package module exposes the stable in-process Integration module factory.
// Implementation details remain under internal/assembly.
package module

import (
	"github.com/domainry/domainry-integration-sdk/modulehost"
	moduleassembly "github.com/domainry/domainry-integration/internal/assembly/module"
)

type Options = moduleassembly.Options
type Factory = moduleassembly.Factory

func OptionsFromEnvironment() Options        { return moduleassembly.OptionsFromEnvironment() }
func NewFactory(options ...Options) *Factory { return moduleassembly.NewFactory(options...) }
func SchemaMigrations(driver, schema string) ([]modulehost.SchemaMigration, error) {
	return moduleassembly.SchemaMigrations(driver, schema)
}
