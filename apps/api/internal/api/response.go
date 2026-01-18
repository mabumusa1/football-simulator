package api

import (
	"encoding/json"
	"net/http"
)

// ErrorResponse represents a standardized error response.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// respondJSON writes a JSON response with the given status code and data.
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if data != nil {
		// Encode response; if it fails, we've already written the header,
		// so we can only rely on caller's middleware for error handling
		_ = json.NewEncoder(w).Encode(data)
	}
}

// respondError creates an ErrorResponse and sends it as JSON.
func respondError(w http.ResponseWriter, status int, message, detail string) {
	resp := ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
	}
	if detail != "" {
		resp.Field = detail
	}
	respondJSON(w, status, resp)
}

// respondErrorWithField creates an ErrorResponse with a field name and sends it as JSON.
func respondErrorWithField(w http.ResponseWriter, status int, message, field string) {
	resp := ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
		Field:   field,
	}
	respondJSON(w, status, resp)
}
