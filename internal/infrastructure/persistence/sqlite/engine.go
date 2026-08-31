package sqlite

import ormdialect "github.com/domainry/domainry-orm/dialect"

func Driver() ormdialect.Name { return ormdialect.SQLite }
