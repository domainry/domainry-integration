package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	saasassembly "github.com/domainry/domainry-integration/internal/assembly/saas"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormmigration "github.com/domainry/domainry-orm/migration"
	_ "modernc.org/sqlite"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	application := integrationsdk.ApplicationRef{RuntimeID: strings.TrimSpace(os.Getenv("INTEGRATION_RUNTIME_ID"))}
	token := strings.TrimSpace(os.Getenv("INTEGRATION_SERVICE_TOKEN"))
	if err := application.Validate(); err != nil {
		return err
	}
	if token == "" {
		return errors.New("INTEGRATION_SERVICE_TOKEN is required")
	}
	path := strings.TrimSpace(os.Getenv("INTEGRATION_SQLITE_PATH"))
	if path == "" {
		path = "integration.db"
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open Integration database: %w", err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	host := &standaloneHost{database: database, dialect: dialect.WithSchema("")}
	service, err := saasassembly.Open(context.Background(), application, host, token)
	if err != nil {
		return err
	}
	defer service.Close(context.Background())
	address := strings.TrimSpace(os.Getenv("INTEGRATION_HTTP_ADDRESS"))
	if address == "" {
		address = ":8080"
	}
	server := &http.Server{Addr: address, Handler: service.Handler, ReadHeaderTimeout: 5 * time.Second}
	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	workerDone := service.StartWorkers(stop, time.Second, 25)
	defer func() {
		cancel()
		<-workerDone
	}()
	go func() {
		<-stop.Done()
		ctx, release := context.WithTimeout(context.Background(), 10*time.Second)
		defer release()
		_ = server.Shutdown(ctx)
	}()
	log.Printf("Integration SaaS server listening on %s", address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type standaloneHost struct {
	database    *sql.DB
	dialect     ormdialect.Renderer
	migrationMu sync.Mutex
}

func (h *standaloneHost) Database() modulehost.Database               { return h.database }
func (h *standaloneHost) Dialect() modulehost.Dialect                 { return h.dialect }
func (h *standaloneHost) Migrations() modulehost.MigrationRegistrar   { return h }
func (*standaloneHost) Providers() modulehost.ProviderRegistry        { return emptyProviders{} }
func (*standaloneHost) SecretCipher() modulehost.SecretMaterialCipher { return unavailableCipher{} }
func (*standaloneHost) RuntimeTriggers() integrationsdk.TriggerSink   { return unavailableTrigger{} }
func (*standaloneHost) Driver() string                                { return "sqlite" }
func (*standaloneHost) Schema() string                                { return "" }
func (h *standaloneHost) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []modulehost.SchemaMigration) error {
	if owner != "integration" {
		return fmt.Errorf("unsupported migration owner %q", owner)
	}
	h.migrationMu.Lock()
	defer h.migrationMu.Unlock()
	runner, err := ormmigration.NewRunner(h.database, h.dialect, ormmigration.Options{LedgerTable: "_schema_migrations"})
	if err != nil {
		return err
	}
	return runner.Apply(ctx, migrations)
}

type emptyProviders struct{}

func (emptyProviders) Provider(string, string) (connector.Adapter, bool) { return nil, false }
func (emptyProviders) Descriptors() []connector.ProviderDescriptor       { return nil }

type unavailableCipher struct{}

type unavailableTrigger struct{}

func (unavailableTrigger) Trigger(context.Context, integrationsdk.TriggerRequest) (integrationsdk.RuntimeExecutionReceipt, error) {
	return integrationsdk.RuntimeExecutionReceipt{}, errors.New("standalone Integration Runtime TriggerSink is not configured")
}

func (unavailableCipher) EncryptSecretMaterial(context.Context, string, string, string) (string, error) {
	return "", errors.New("standalone Integration secret cipher is not configured")
}
func (unavailableCipher) DecryptSecretMaterial(context.Context, string, string, string) (string, error) {
	return "", errors.New("standalone Integration secret cipher is not configured")
}
