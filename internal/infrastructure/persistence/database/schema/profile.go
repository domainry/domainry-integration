package schema

import ormdialect "github.com/domainry/domainry-orm/dialect"

type Profile struct{ Driver ormdialect.Name }

func New(driver ormdialect.Name) Profile { return Profile{Driver: driver} }
