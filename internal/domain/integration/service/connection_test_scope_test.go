package integrationservice

import "testing"

func TestConnectionTestScopePolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		grants       []string
		verified     bool
		alternatives [][]string
		want         string
	}{
		{"no declaration", nil, true, nil, "requirements_invalid"},
		{"bad member after match", []string{"read"}, true, [][]string{{"read"}, {"bad scope"}}, "requirements_invalid"},
		{"no scope required", nil, false, [][]string{{}}, "ready"},
		{"legacy lacks evidence", []string{"read"}, false, [][]string{{"read"}}, "scope_unverified"},
		{"empty known grant", nil, true, [][]string{{"read"}}, "scope_required"},
		{"all members required", []string{"read"}, true, [][]string{{"read", "profile"}}, "scope_required"},
		{"second alternative", []string{"email"}, true, [][]string{{"read", "profile"}, {"email"}}, "ready"},
		{"all members", []string{"profile", "read"}, true, [][]string{{"read", "profile"}}, "ready"},
		{"case is significant", []string{"READ"}, true, [][]string{{"read"}}, "scope_required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ConnectionTestScopeState(tc.grants, tc.verified, tc.alternatives); got != tc.want {
				t.Fatalf("state=%s want=%s", got, tc.want)
			}
		})
	}
}
