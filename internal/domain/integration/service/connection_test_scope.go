package integrationservice

// ConnectionTestScopeState evaluates only the fixed probe's declared OAuth
// requirements. It never decides business-tool access or changes a grant.
func ConnectionTestScopeState(granted []string, verified bool, alternatives [][]string) string {
	return OAuthScopeState(granted, verified, alternatives)
}

// OAuthScopeState evaluates exact declared grants. The caller separately owns
// current-user authorization, account ownership and operation effect checks.
func OAuthScopeState(granted []string, verified bool, alternatives [][]string) string {
	if len(alternatives) == 0 || len(alternatives) > 64 {
		return "requirements_invalid"
	}
	for _, alternative := range alternatives {
		if len(alternative) > 64 {
			return "requirements_invalid"
		}
		for _, scope := range alternative {
			if len(scope) == 0 || len(scope) > 2048 {
				return "requirements_invalid"
			}
			for _, ch := range scope {
				if ch < 0x21 || ch > 0x7e || ch == '"' || ch == '\\' {
					return "requirements_invalid"
				}
			}
		}
	}
	for _, alternative := range alternatives {
		if len(alternative) == 0 {
			return "ready"
		}
	}
	if !verified {
		return "scope_unverified"
	}
	have := make(map[string]bool, len(granted))
	for _, scope := range granted {
		have[scope] = true
	}
	for _, alternative := range alternatives {
		matches := true
		for _, scope := range alternative {
			matches = matches && have[scope]
		}
		if matches {
			return "ready"
		}
	}
	return "scope_required"
}
