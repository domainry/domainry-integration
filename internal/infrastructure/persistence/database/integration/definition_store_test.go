package integration

import (
	"github.com/domainry/domainry-integration/internal/testsupport/definitionfixture"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

func newTestDefinitionStore() metadatasdk.DefinitionStore {
	return definitionfixture.NewStore()
}
