package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRespondJSON_SetsContentType(t *testing.T) {
	rr := httptest.NewRecorder()

	respondJSON(rr, http.StatusOK, map[string]string{"key": "value"})

	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("respondJSON Content-Type = %q, want %q", contentType, "application/json")
	}
}

func TestRespondJSON_SetsStatusCode(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
	}{
		{"200 OK", http.StatusOK},
		{"201 Created", http.StatusCreated},
		{"202 Accepted", http.StatusAccepted},
		{"400 Bad Request", http.StatusBadRequest},
		{"401 Unauthorized", http.StatusUnauthorized},
		{"404 Not Found", http.StatusNotFound},
		{"500 Internal Server Error", http.StatusInternalServerError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			respondJSON(rr, tc.statusCode, nil)

			if rr.Code != tc.statusCode {
				t.Errorf("respondJSON status = %d, want %d", rr.Code, tc.statusCode)
			}
		})
	}
}

func TestRespondJSON_EncodesData(t *testing.T) {
	testCases := []struct {
		name     string
		data     interface{}
		expected string
	}{
		{
			name:     "map",
			data:     map[string]string{"message": "hello"},
			expected: `{"message":"hello"}`,
		},
		{
			name:     "struct",
			data:     struct{ Name string }{"test"},
			expected: `{"Name":"test"}`,
		},
		{
			name: "slice",
			data: []int{1, 2, 3},
			expected: `[1,2,3]`,
		},
		{
			name:     "string",
			data:     "hello",
			expected: `"hello"`,
		},
		{
			name:     "number",
			data:     42,
			expected: `42`,
		},
		{
			name:     "boolean",
			data:     true,
			expected: `true`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			respondJSON(rr, http.StatusOK, tc.data)

			// Trim newline from encoded JSON
			body := rr.Body.String()
			if len(body) > 0 && body[len(body)-1] == '\n' {
				body = body[:len(body)-1]
			}

			if body != tc.expected {
				t.Errorf("respondJSON body = %q, want %q", body, tc.expected)
			}
		})
	}
}

func TestRespondJSON_NilData(t *testing.T) {
	rr := httptest.NewRecorder()
	respondJSON(rr, http.StatusNoContent, nil)

	if rr.Code != http.StatusNoContent {
		t.Errorf("respondJSON with nil data: status = %d, want %d", rr.Code, http.StatusNoContent)
	}

	// Body should be empty for nil data
	if rr.Body.Len() != 0 {
		t.Errorf("respondJSON with nil data: body should be empty, got %q", rr.Body.String())
	}
}

func TestRespondJSON_ComplexStruct(t *testing.T) {
	type Inner struct {
		Value int `json:"value"`
	}
	type Outer struct {
		Name  string `json:"name"`
		Inner Inner  `json:"inner"`
	}

	data := Outer{
		Name:  "test",
		Inner: Inner{Value: 42},
	}

	rr := httptest.NewRecorder()
	respondJSON(rr, http.StatusOK, data)

	var result Outer
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("respondJSON complex struct: failed to parse response: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("respondJSON complex struct: Name = %q, want %q", result.Name, "test")
	}
	if result.Inner.Value != 42 {
		t.Errorf("respondJSON complex struct: Inner.Value = %d, want %d", result.Inner.Value, 42)
	}
}

func TestRespondError_SetsContentType(t *testing.T) {
	rr := httptest.NewRecorder()
	respondError(rr, http.StatusBadRequest, "test message", "")

	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("respondError Content-Type = %q, want %q", contentType, "application/json")
	}
}

func TestRespondError_SetsStatusCode(t *testing.T) {
	testCases := []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	}

	for _, statusCode := range testCases {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			rr := httptest.NewRecorder()
			respondError(rr, statusCode, "message", "")

			if rr.Code != statusCode {
				t.Errorf("respondError status = %d, want %d", rr.Code, statusCode)
			}
		})
	}
}

func TestRespondError_FormatsResponse(t *testing.T) {
	rr := httptest.NewRecorder()
	respondError(rr, http.StatusBadRequest, "validation failed", "")

	var response ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("respondError: failed to parse response: %v", err)
	}

	if response.Error != "Bad Request" {
		t.Errorf("respondError Error = %q, want %q", response.Error, "Bad Request")
	}
	if response.Message != "validation failed" {
		t.Errorf("respondError Message = %q, want %q", response.Message, "validation failed")
	}
}

func TestRespondError_IncludesDetail(t *testing.T) {
	rr := httptest.NewRecorder()
	respondError(rr, http.StatusBadRequest, "validation failed", "field_name")

	var response ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("respondError: failed to parse response: %v", err)
	}

	if response.Field != "field_name" {
		t.Errorf("respondError Field = %q, want %q", response.Field, "field_name")
	}
}

func TestRespondError_OmitsEmptyDetail(t *testing.T) {
	rr := httptest.NewRecorder()
	respondError(rr, http.StatusBadRequest, "message", "")

	var response ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("respondError: failed to parse response: %v", err)
	}

	if response.Field != "" {
		t.Errorf("respondError Field should be empty for empty detail, got %q", response.Field)
	}
}

func TestRespondError_DifferentStatusTexts(t *testing.T) {
	testCases := []struct {
		statusCode   int
		expectedText string
	}{
		{http.StatusBadRequest, "Bad Request"},
		{http.StatusUnauthorized, "Unauthorized"},
		{http.StatusForbidden, "Forbidden"},
		{http.StatusNotFound, "Not Found"},
		{http.StatusConflict, "Conflict"},
		{http.StatusInternalServerError, "Internal Server Error"},
		{http.StatusServiceUnavailable, "Service Unavailable"},
	}

	for _, tc := range testCases {
		t.Run(tc.expectedText, func(t *testing.T) {
			rr := httptest.NewRecorder()
			respondError(rr, tc.statusCode, "message", "")

			var response ErrorResponse
			if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
				t.Fatalf("respondError: failed to parse response: %v", err)
			}

			if response.Error != tc.expectedText {
				t.Errorf("respondError Error = %q, want %q", response.Error, tc.expectedText)
			}
		})
	}
}

func TestRespondErrorWithField_SetsContentType(t *testing.T) {
	rr := httptest.NewRecorder()
	respondErrorWithField(rr, http.StatusBadRequest, "message", "fieldName")

	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("respondErrorWithField Content-Type = %q, want %q", contentType, "application/json")
	}
}

func TestRespondErrorWithField_SetsStatusCode(t *testing.T) {
	rr := httptest.NewRecorder()
	respondErrorWithField(rr, http.StatusUnprocessableEntity, "message", "field")

	if rr.Code != http.StatusUnprocessableEntity {
		t.Errorf("respondErrorWithField status = %d, want %d", rr.Code, http.StatusUnprocessableEntity)
	}
}

func TestRespondErrorWithField_IncludesField(t *testing.T) {
	rr := httptest.NewRecorder()
	respondErrorWithField(rr, http.StatusBadRequest, "invalid value", "username")

	var response ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("respondErrorWithField: failed to parse response: %v", err)
	}

	if response.Field != "username" {
		t.Errorf("respondErrorWithField Field = %q, want %q", response.Field, "username")
	}
	if response.Message != "invalid value" {
		t.Errorf("respondErrorWithField Message = %q, want %q", response.Message, "invalid value")
	}
}

func TestRespondErrorWithField_EmptyField(t *testing.T) {
	rr := httptest.NewRecorder()
	respondErrorWithField(rr, http.StatusBadRequest, "message", "")

	var response ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("respondErrorWithField: failed to parse response: %v", err)
	}

	// Field should be empty string (omitted from JSON if using omitempty)
	if response.Field != "" {
		t.Errorf("respondErrorWithField with empty field: Field = %q, want empty", response.Field)
	}
}

func TestErrorResponse_Struct(t *testing.T) {
	resp := ErrorResponse{
		Error:   "Bad Request",
		Message: "validation failed",
		Field:   "email",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("ErrorResponse marshal error: %v", err)
	}

	var parsed ErrorResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("ErrorResponse unmarshal error: %v", err)
	}

	if parsed.Error != resp.Error {
		t.Errorf("ErrorResponse Error = %q, want %q", parsed.Error, resp.Error)
	}
	if parsed.Message != resp.Message {
		t.Errorf("ErrorResponse Message = %q, want %q", parsed.Message, resp.Message)
	}
	if parsed.Field != resp.Field {
		t.Errorf("ErrorResponse Field = %q, want %q", parsed.Field, resp.Field)
	}
}

func TestErrorResponse_OmitsEmptyField(t *testing.T) {
	resp := ErrorResponse{
		Error:   "Bad Request",
		Message: "error message",
		// Field is empty
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("ErrorResponse marshal error: %v", err)
	}

	// Check that field is omitted from JSON
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("ErrorResponse unmarshal to map error: %v", err)
	}

	if _, exists := raw["field"]; exists {
		t.Error("ErrorResponse should omit empty field from JSON")
	}
}
