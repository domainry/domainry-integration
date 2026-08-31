package persistence

import (
	"github.com/domainry/domainry-integration-sdk/modulehost"
	integrationdatabase "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/integration"
)

// Stores groups Integration repositories created over one host-owned database.
type Stores struct {
	Catalog      *integrationdatabase.CatalogStore
	Requirements *integrationdatabase.RequirementsStore
	Delivery     *integrationdatabase.DeliveryStore
	WebPush      *integrationdatabase.WebPushSubscriptionStore
}

func NewStores(database modulehost.Database, dialect modulehost.Dialect, providers modulehost.ProviderRegistry, cipher modulehost.SecretMaterialCipher) Stores {
	webPush := integrationdatabase.NewWebPushSubscriptionStore(database, dialect)
	secrets := integrationdatabase.NewSecretResolver(database, dialect, cipher)
	return Stores{Catalog: integrationdatabase.NewCatalogStore(database, dialect), Requirements: integrationdatabase.NewRequirementsStore(database, dialect, providers), Delivery: integrationdatabase.NewDeliveryStore(database, dialect, providers, secrets, webPush), WebPush: webPush}
}
