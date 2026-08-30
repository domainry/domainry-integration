package architecture

import (
	"os"
	"strings"
	"testing"
)

func TestPersistenceImplementationUsesCanonicalInternalLayout(t *testing.T) {
	if _, err := os.Stat("../persistence"); !os.IsNotExist(err) {
		t.Fatal("Integration must keep persistence under internal/infrastructure/persistence/database/integration")
	}
	if info, err := os.Stat("../infrastructure/persistence/database/integration"); err != nil || !info.IsDir() {
		t.Fatal("Integration canonical persistence package is missing")
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
