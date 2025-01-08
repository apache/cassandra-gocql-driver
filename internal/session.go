package internal

import "strings"

// TODO: move to internal
func IsUseStatement(stmt string) bool {
	if len(stmt) < 3 {
		return false
	}

	return strings.EqualFold(stmt[0:3], "use")
}
