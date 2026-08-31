package persistence

import (
	"fmt"

	ormdialect "github.com/domainry/domainry-orm/dialect"
)

type Engine struct{ name ormdialect.Name }

func NewEngine(driver string) (Engine, error) {
	parsed, err := ormdialect.Parse(driver)
	if err != nil {
		return Engine{}, fmt.Errorf("Integration database driver %q is unsupported: %w", driver, err)
	}
	return Engine{name: parsed.Name()}, nil
}
func (e Engine) Name() ormdialect.Name { return e.name }
