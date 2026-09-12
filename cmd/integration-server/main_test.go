package main

import (
	"encoding/base64"
	"strings"
	"testing"

	connector "github.com/domainry/domainry-connector-sdk"
	connectormodule "github.com/domainry/domainry-connectors/module"
)

func TestStandaloneConfigurationRequiresKeyAndPublishesOAuthProviders(t *testing.T) {
	for _, invalid := range []string{"", "invalid", base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, err := configuredCipher(invalid); err == nil {
			t.Fatal("invalid key accepted")
		}
	}
	if _, err := configuredCipher(base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))); err != nil {
		t.Fatal(err)
	}
	providers, err := configuredProviders("")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := connectormodule.NewFactory(connectormodule.Options{Providers: providers}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.Descriptors()) != 2 {
		t.Fatal("unexpected standalone catalog")
	}
	for _, keys := range [][2]string{{"google_workspace", "google"}, {"microsoft_365", "microsoft"}} {
		provider, ok := registry.Provider(keys[0], keys[1])
		if !ok {
			t.Fatal("provider missing")
		}
		if _, ok := connector.ResolveOAuthAuthorizer(provider); !ok {
			t.Fatal("OAuth capability lost at public composition boundary")
		}
	}
}

func TestStandaloneWebIsExplicitAndHasSeparateTransport(t *testing.T) {
	if _, err := configuredProviders("http://public.example.com"); err == nil {
		t.Fatal("insecure remote origin accepted")
	}
	providers, err := configuredProviders("https://proxy.example.com")
	if err != nil || len(providers.Providers) != 3 {
		t.Fatal("web opt-in failed", err)
	}
	registry, err := connectormodule.NewFactory(connectormodule.Options{Providers: providers}).Registry()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Provider("web", "llm_proxy"); !ok {
		t.Fatal("web missing")
	}
}
