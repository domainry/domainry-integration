package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	connector "github.com/domainry/domainry-connector-sdk"
	connectorscatalog "github.com/domainry/domainry-connectors/catalog"
	connectormodule "github.com/domainry/domainry-connectors/module"
	shareddefinition "github.com/domainry/domainry-foundation/definition"
	"github.com/domainry/domainry-foundation/logging"
	"github.com/domainry/domainry-foundation/processhost"
	integrationsdk "github.com/domainry/domainry-integration-sdk"
	"github.com/domainry/domainry-integration-sdk/modulehost"
	saasassembly "github.com/domainry/domainry-integration/internal/assembly/saas"
	"github.com/domainry/domainry-integration/internal/infrastructure/connectortransport"
	integrationmigration "github.com/domainry/domainry-integration/internal/infrastructure/persistence/database/migration"
	"github.com/domainry/domainry-integration/internal/infrastructure/security"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	_ "modernc.org/sqlite"
)

func main() {
	if run() != nil {
		os.Exit(1)
	}
}

func run() (runErr error) {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	observability, err := processhost.Open(ctx, processhost.Config{ServiceName: "domainry-integration"})
	if err != nil {
		return err
	}
	started := false
	startupFailure := logging.Failure{Stage: "configuration_validate"}
	defer func() {
		if !started && runErr != nil {
			observability.StartFailed(context.Background(), runErr, startupFailure)
			runErr = errors.Join(runErr, observability.Close(context.Background(), nil))
			return
		}
		runErr = observability.Finish(context.Background(), nil, runErr)
	}()
	application := integrationsdk.ApplicationRef{RuntimeID: strings.TrimSpace(os.Getenv("INTEGRATION_RUNTIME_ID"))}
	token := strings.TrimSpace(os.Getenv("INTEGRATION_SERVICE_TOKEN"))
	if err := application.Validate(); err != nil {
		return err
	}
	if token == "" {
		return errors.New("INTEGRATION_SERVICE_TOKEN is required")
	}
	cipher, err := configuredCipher(os.Getenv("INTEGRATION_MASTER_KEY"))
	if err != nil {
		return err
	}
	providers, err := configuredProviders(strings.TrimSpace(os.Getenv("INTEGRATION_WEB_PROXY_ORIGIN")))
	if err != nil {
		return err
	}
	registry, err := connectormodule.NewFactory(connectormodule.Options{Providers: providers}).Registry()
	if err != nil {
		return err
	}
	path := strings.TrimSpace(os.Getenv("INTEGRATION_SQLITE_PATH"))
	if path == "" {
		path = "integration.db"
	}
	startupFailure = logging.Failure{Stage: "database_initialize", Dependency: "integration_database"}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open Integration database: %w", err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	dialect, _ := ormdialect.New(ormdialect.SQLite)
	host := &standaloneHost{database: database, dialect: dialect.WithSchema(""), providers: registry, cipher: cipher}
	startupFailure = logging.Failure{Stage: "module_assembly"}
	service, err := saasassembly.Open(ctx, application, host, token, saasassembly.Options{ConnectorCatalog: connectorscatalog.Definitions})
	if err != nil {
		return err
	}
	defer service.Close(context.Background())
	switch persistence := strings.TrimSpace(os.Getenv("INTEGRATION_SUBJECT_LIFECYCLE_PERSISTENCE")); persistence {
	case "":
		// Subject lifecycle endpoints remain fail-closed until a deployment that
		// shares Lifecycle's schema opts in explicitly.
	case "shared":
		if err := service.BindSubjectLifecyclePersistence(context.Background()); err != nil {
			return fmt.Errorf("bind shared Subject Lifecycle persistence: %w", err)
		}
	default:
		return fmt.Errorf("INTEGRATION_SUBJECT_LIFECYCLE_PERSISTENCE must be empty or shared")
	}
	address := strings.TrimSpace(os.Getenv("INTEGRATION_HTTP_ADDRESS"))
	if address == "" {
		address = ":8080"
	}
	server := &http.Server{Addr: address, Handler: observability.Handler("public", service.Handler), ReadHeaderTimeout: 5 * time.Second}
	startupFailure = logging.Failure{Stage: "listener_bind", Dependency: "public_listener"}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	workerDone := service.StartWorkers(ctx, time.Second, 25)
	defer func() {
		cancel()
		<-workerDone
	}()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	started = true
	observability.Started(ctx, map[string]any{"listener_group": "public"})
	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		observability.Stopping(context.Background(), map[string]any{"reason": "signal"})
		shutdownCtx, release := context.WithTimeout(context.Background(), 10*time.Second)
		defer release()
		return server.Shutdown(shutdownCtx)
	}
}

type standaloneHost struct {
	database    *sql.DB
	dialect     ormdialect.Renderer
	migrationMu sync.Mutex
	providers   modulehost.ProviderRegistry
	cipher      modulehost.SecretMaterialCipher
}

func (h *standaloneHost) Database() modulehost.Database                 { return h.database }
func (h *standaloneHost) Dialect() modulehost.Dialect                   { return h.dialect }
func (h *standaloneHost) Migrations() modulehost.MigrationRegistrar     { return h }
func (h *standaloneHost) Providers() modulehost.ProviderRegistry        { return h.providers }
func (h *standaloneHost) SecretCipher() modulehost.SecretMaterialCipher { return h.cipher }
func (*standaloneHost) RuntimeTriggers() integrationsdk.TriggerSink     { return unavailableTrigger{} }
func (*standaloneHost) Driver() string                                  { return "sqlite" }
func (*standaloneHost) Schema() string                                  { return "" }
func (h *standaloneHost) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []modulehost.SchemaMigration) error {
	if owner != "integration" && owner != shareddefinition.MigrationOwner {
		return fmt.Errorf("unsupported migration owner %q", owner)
	}
	h.migrationMu.Lock()
	defer h.migrationMu.Unlock()
	return integrationmigration.ApplyOwnedMigrations(ctx, h.database, h.dialect, owner, migrations)
}

type unavailableTrigger struct{}

func (unavailableTrigger) Trigger(context.Context, integrationsdk.TriggerRequest) (integrationsdk.RuntimeExecutionReceipt, error) {
	return integrationsdk.RuntimeExecutionReceipt{}, errors.New("standalone Integration Runtime TriggerSink is not configured")
}

func configuredCipher(encoded string) (*security.SecretCipher, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(key) != 32 {
		return nil, errors.New("INTEGRATION_MASTER_KEY is required: base64-encoded 32-byte AES key")
	}
	return security.NewSecretCipher(key)
}

// Provider families receive separate, host-owned network policies. Registering
// a web connection cannot expand the Google/Microsoft transport allowlist.
func configuredProviders(webOrigin string) (connector.ProviderSet, error) {
	providers, err := connectormodule.WorkAccountProviders(connectortransport.NewWorkAccounts())
	if err != nil || webOrigin == "" {
		return providers, err
	}
	transport, err := connectortransport.NewPublicWeb(webOrigin)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	webProviders, err := connectormodule.PublicWebProviders(transport)
	if err != nil {
		return connector.ProviderSet{}, err
	}
	providers.Providers = append(providers.Providers, webProviders.Providers...)
	return providers, nil
}
