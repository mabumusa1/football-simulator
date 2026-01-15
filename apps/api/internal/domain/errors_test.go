package domain

import (
	"errors"
	"testing"
)

func TestValidationError_Error(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		message  string
		expected string
	}{
		{
			name:     "standard validation error",
			field:    "eventId",
			message:  "must be a valid UUID",
			expected: "validation error: field 'eventId' must be a valid UUID",
		},
		{
			name:     "required field error",
			field:    "matchId",
			message:  "is required",
			expected: "validation error: field 'matchId' is required",
		},
		{
			name:     "empty field name",
			field:    "",
			message:  "some error",
			expected: "validation error: field '' some error",
		},
		{
			name:     "empty message",
			field:    "field",
			message:  "",
			expected: "validation error: field 'field' ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ve := &ValidationError{
				Field:   tt.field,
				Message: tt.message,
			}
			if got := ve.Error(); got != tt.expected {
				t.Errorf("ValidationError.Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestNewValidationError(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		message string
	}{
		{
			name:    "creates validation error with field and message",
			field:   "eventType",
			message: "must be a valid event type",
		},
		{
			name:    "creates validation error with empty values",
			field:   "",
			message: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ve := NewValidationError(tt.field, tt.message)
			if ve == nil {
				t.Fatal("NewValidationError() returned nil")
			}
			if ve.Field != tt.field {
				t.Errorf("NewValidationError().Field = %q, want %q", ve.Field, tt.field)
			}
			if ve.Message != tt.message {
				t.Errorf("NewValidationError().Message = %q, want %q", ve.Message, tt.message)
			}
		})
	}
}

func TestIsValidationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "validation error returns true",
			err:  &ValidationError{Field: "test", Message: "error"},
			want: true,
		},
		{
			name: "validation error from NewValidationError returns true",
			err:  NewValidationError("field", "message"),
			want: true,
		},
		{
			name: "standard error returns false",
			err:  errors.New("standard error"),
			want: false,
		},
		{
			name: "nil error returns false",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidationError(tt.err); got != tt.want {
				t.Errorf("IsValidationError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAsValidationError(t *testing.T) {
	validationErr := &ValidationError{Field: "testField", Message: "test message"}
	standardErr := errors.New("standard error")

	tests := []struct {
		name    string
		err     error
		wantNil bool
		field   string
		message string
	}{
		{
			name:    "validation error returns pointer",
			err:     validationErr,
			wantNil: false,
			field:   "testField",
			message: "test message",
		},
		{
			name:    "standard error returns nil",
			err:     standardErr,
			wantNil: true,
		},
		{
			name:    "nil error returns nil",
			err:     nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AsValidationError(tt.err)
			if tt.wantNil {
				if got != nil {
					t.Errorf("AsValidationError() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("AsValidationError() = nil, want non-nil")
			}
			if got.Field != tt.field {
				t.Errorf("AsValidationError().Field = %q, want %q", got.Field, tt.field)
			}
			if got.Message != tt.message {
				t.Errorf("AsValidationError().Message = %q, want %q", got.Message, tt.message)
			}
		})
	}
}

func TestValidationError_ImplementsError(t *testing.T) {
	var _ error = &ValidationError{}
	var _ error = NewValidationError("field", "message")
}
