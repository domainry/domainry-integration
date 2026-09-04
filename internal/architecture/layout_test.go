package architecture

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestIntegrationUsesInternalLayeredLayout(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	for _, required := range []string{
		"cmd/integration-server",
		"internal/application/integration",
		"internal/domain/integration/model",
		"internal/domain/integration/repository",
		"internal/domain/integration/service",
		"internal/adapter/integrationsdk",
		"internal/assembly/module",
		"internal/assembly/saas",
		"internal/transport/http/module",
		"internal/transport/http/saas",
		"internal/infrastructure/persistence/database/integration",
		"internal/infrastructure/persistence/database/migration",
		"internal/infrastructure/persistence/database/schema",
		"internal/infrastructure/persistence/sqlite",
		"internal/infrastructure/persistence/mysql",
		"internal/infrastructure/persistence/postgres",
		"module",
	} {
		if info, err := os.Stat(filepath.Join(root, required)); err != nil || !info.IsDir() {
			t.Errorf("required Integration boundary %q is missing", required)
		}
	}
	for _, forbidden := range []string{"application", "domain", "infrastructure", "server"} {
		if _, err := os.Stat(filepath.Join(root, forbidden)); !os.IsNotExist(err) {
			t.Errorf("Integration implementation package %q must remain internal", forbidden)
		}
	}
}

func TestPublicModuleIsThinFacade(t *testing.T) {
	entries, err := os.ReadDir("../../module")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || entry.Name() == "module.go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		t.Errorf("public module package contains implementation file %q", entry.Name())
	}
}

func TestModuleUsesTaggedDependencies(t *testing.T) {
	content, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(content), "replace ") || strings.Contains(string(content), "../domainry-") {
		t.Fatal("Integration must consume released module tags, not local directory replacements")
	}
}

func TestDomainAndApplicationDoNotDependOnSDKOrInfrastructure(t *testing.T) {
	for _, root := range []string{"../domain", "../application"} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || filepath.Ext(path) != ".go" {
				return walkErr
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			text := string(content)
			for _, forbidden := range []string{"domainry-integration-sdk", "/internal/infrastructure/", "/internal/adapter/", "/internal/assembly/", "/internal/transport/"} {
				if strings.Contains(text, forbidden) {
					t.Errorf("%s crosses internal layer boundary with %q", path, forbidden)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestIntegrationDoesNotOwnConcreteProvidersOrRuntimeOutbox(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	forbiddenImports := []string{
		"github.com/domainry/domainry-connectors/providers",
		"github.com/domainry/domainry-runtime",
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), "_publication_outbox") {
			t.Errorf("%s moves the Runtime-owned publication outbox into Integration", path)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, content, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, declaration := range parsed.Imports {
			importPath, err := strconv.Unquote(declaration.Path.Value)
			if err != nil {
				return err
			}
			for _, prefix := range forbiddenImports {
				if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") {
					t.Errorf("%s imports forbidden implementation package %s", path, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
