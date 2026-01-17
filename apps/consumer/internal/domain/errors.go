package domain

import (
	sharedDomain "github.com/mabumusa1/football-simulator/pkg/domain"
)

// Re-export types from shared domain package
type ValidationError = sharedDomain.ValidationError

// Re-export functions from shared domain package
var (
	NewValidationError = sharedDomain.NewValidationError
	IsValidationError  = sharedDomain.IsValidationError
	AsValidationError  = sharedDomain.AsValidationError
)
