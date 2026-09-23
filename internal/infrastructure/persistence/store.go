package persistence

import (
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationdatabase "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/integration"
	metadatasdk "github.com/domainry/domainry-metadata-sdk"
)

// Stores groups Integration repositories created over one host-owned database.
type Stores struct {
	Catalog      *integrationdatabase.CatalogStore
	Requirements *integrationdatabase.RequirementsStore
	Delivery     *integrationdatabase.DeliveryStore
	WebPush      *integrationdatabase.WebPushSubscriptionStore
}

func NewStores(database modulehost.Database, dialect modulehost.Dialect, providers modulehost.ProviderRegistry, cipher modulehost.SecretMaterialCipher, definitions metadatasdk.DefinitionStore) Stores {
	webPush := integrationdatabase.NewWebPushSubscriptionStore(database, dialect)
	secrets := integrationdatabase.NewSecretResolver(database, dialect, cipher)
	return Stores{Catalog: integrationdatabase.NewCatalogStore(definitions), Requirements: integrationdatabase.NewRequirementsStore(database, dialect, providers, definitions), Delivery: integrationdatabase.NewDeliveryStore(database, dialect, providers, secrets, webPush), WebPush: webPush}
}
