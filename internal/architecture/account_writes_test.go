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

func TestAccountWriteApplicationUsesOwnerPortsAndProtocolAdapter(t *testing.T) {
	for _, root := range []string{"../application/integration", "../domain/integration/model", "../adapter/accountwrite"} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			if !strings.Contains(path, "account_write") && !strings.Contains(path, "accountwrite") {
				return nil
			}
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, imp := range f.Imports {
				name, err := strconv.Unquote(imp.Path.Value)
				if err != nil {
					return err
				}
				for _, forbidden := range []string{"/internal/infrastructure/", "/internal/assembly/", "domainry-connectors", "domainry-tools", "domainry-agent", "domainry-runtime"} {
					if strings.Contains(name, forbidden) {
						t.Errorf("%s imports implementation %s", path, name)
					}
				}
				if !strings.Contains(path, "/adapter/") && strings.Contains(name, "domainry-connector-sdk") {
					t.Errorf("%s bypasses write codec port with %s", path, name)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
