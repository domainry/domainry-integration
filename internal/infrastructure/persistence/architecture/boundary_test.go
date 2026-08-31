package architecture_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestIntegrationBusinessPersistenceStaysClassifiedAndUsesORM(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve Integration persistence root")
	}
	databaseRoot := filepath.Join(filepath.Dir(filepath.Dir(source)), "database")
	entries, err := os.ReadDir(databaseRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
			t.Errorf("Integration database root contains unclassified source %s", entry.Name())
		}
	}
	err = filepath.WalkDir(filepath.Join(databaseRoot, "integration"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return walkErr
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			upper := strings.ToUpper(strings.TrimSpace(value))
			if (strings.HasPrefix(upper, "SELECT ") && strings.Contains(upper, " FROM ")) || strings.HasPrefix(upper, "INSERT INTO ") || (strings.HasPrefix(upper, "UPDATE ") && strings.Contains(upper, " SET ")) || strings.HasPrefix(upper, "DELETE FROM ") {
				t.Errorf("%s contains hand-written business SQL", path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
