package domain

import (
	"errors"
	"testing"
)

func TestValidationError_Error(t *testing.T) {
	ve := &ValidationError{
		Field:   "testField",
		Message: "is required",
	}

	expected := "validation error: field 'testField' is required"
	if ve.Error() != expected {
		t.Errorf("expected %q, got %q", expected, ve.Error())
	}
}

func TestNewValidationError(t *testing.T) {
	ve := NewValidationError("email", "must be a valid email address")

	if ve.Field != "email" {
		t.Errorf("expected field 'email', got %s", ve.Field)
	}
	if ve.Message != "must be a valid email address" {
		t.Errorf("expected message 'must be a valid email address', got %s", ve.Message)
	}
}

func TestIsValidationError_True(t *testing.T) {
	ve := NewValidationError("field", "message")

	if !IsValidationError(ve) {
		t.Error("expected IsValidationError to return true for ValidationError")
	}
}

func TestIsValidationError_False(t *testing.T) {
	err := errors.New("regular error")

	if IsValidationError(err) {
		t.Error("expected IsValidationError to return false for regular error")
	}
}

func TestIsValidationError_Nil(t *testing.T) {
	if IsValidationError(nil) {
		t.Error("expected IsValidationError to return false for nil")
	}
}

func TestAsValidationError_Success(t *testing.T) {
	ve := NewValidationError("username", "too short")

	result := AsValidationError(ve)
	if result == nil {
		t.Fatal("expected non-nil ValidationError")
	}
	if result.Field != "username" {
		t.Errorf("expected field 'username', got %s", result.Field)
	}
	if result.Message != "too short" {
		t.Errorf("expected message 'too short', got %s", result.Message)
	}
}

func TestAsValidationError_RegularError(t *testing.T) {
	err := errors.New("regular error")

	result := AsValidationError(err)
	if result != nil {
		t.Error("expected nil for regular error")
	}
}

func TestAsValidationError_Nil(t *testing.T) {
	result := AsValidationError(nil)
	if result != nil {
		t.Error("expected nil for nil input")
	}
}

func TestValidationError_ImplementsErrorInterface(t *testing.T) {
	var err error = NewValidationError("field", "message")

	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestValidationError_DifferentFieldsAndMessages(t *testing.T) {
	testCases := []struct {
		field    string
		message  string
		expected string
	}{
		{"eventId", "must be a valid UUID", "validation error: field 'eventId' must be a valid UUID"},
		{"matchId", "is required", "validation error: field 'matchId' is required"},
		{"timestamp", "must be RFC3339 format", "validation error: field 'timestamp' must be RFC3339 format"},
		{"teamId", "must be 1 or 2", "validation error: field 'teamId' must be 1 or 2"},
	}

	for _, tc := range testCases {
		t.Run(tc.field, func(t *testing.T) {
			ve := NewValidationError(tc.field, tc.message)
			if ve.Error() != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, ve.Error())
			}
		})
	}
}
