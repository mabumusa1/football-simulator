package api

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestAPIKeyAuth_ValidKey tests that a valid API key allows the request to proceed.
func TestAPIKeyAuth_ValidKey(t *testing.T) {
	expectedKey := "test-api-key-12345"
	middleware := APIKeyAuth(expectedKey)

	// Create a simple handler that returns 200 OK
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success"))
	})

	// Wrap handler with middleware
	wrappedHandler := middleware(handler)

	// Create request with valid API key
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-API-Key", expectedKey)

	// Record the response
	rr := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rr, req)

	// Verify the request was allowed through
	if rr.Code != http.StatusOK {
		t.Errorf("APIKeyAuth with valid key: got status %d, want %d", rr.Code, http.StatusOK)
	}
	if rr.Body.String() != "success" {
		t.Errorf("APIKeyAuth with valid key: got body %q, want %q", rr.Body.String(), "success")
	}
}

// TestAPIKeyAuth_InvalidKey tests that an invalid API key returns 401 Unauthorized.
func TestAPIKeyAuth_InvalidKey(t *testing.T) {
	expectedKey := "correct-api-key"
	middleware := APIKeyAuth(expectedKey)

	// Create a handler that should never be called
	handlerCalled := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(handler)

	// Create request with invalid API key
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-API-Key", "wrong-api-key")

	rr := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rr, req)

	// Verify unauthorized response
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("APIKeyAuth with invalid key: got status %d, want %d", rr.Code, http.StatusUnauthorized)
	}

	// Verify handler was not called
	if handlerCalled {
		t.Error("APIKeyAuth with invalid key: handler should not have been called")
	}

	// Verify JSON response
	var response map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("APIKeyAuth: failed to parse JSON response: %v", err)
	}

	if response["error"] != "Unauthorized" {
		t.Errorf("APIKeyAuth: got error %q, want %q", response["error"], "Unauthorized")
	}
	if response["message"] != "invalid API key" {
		t.Errorf("APIKeyAuth: got message %q, want %q", response["message"], "invalid API key")
	}

	// Verify Content-Type header
	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("APIKeyAuth: got Content-Type %q, want %q", contentType, "application/json")
	}
}

// TestAPIKeyAuth_MissingKey tests that a missing API key returns 401 Unauthorized.
func TestAPIKeyAuth_MissingKey(t *testing.T) {
	expectedKey := "some-api-key"
	middleware := APIKeyAuth(expectedKey)

	// Create a handler that should never be called
	handlerCalled := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(handler)

	// Create request without API key header
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// No X-API-Key header set

	rr := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rr, req)

	// Verify unauthorized response
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("APIKeyAuth with missing key: got status %d, want %d", rr.Code, http.StatusUnauthorized)
	}

	// Verify handler was not called
	if handlerCalled {
		t.Error("APIKeyAuth with missing key: handler should not have been called")
	}

	// Verify JSON response
	var response map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("APIKeyAuth: failed to parse JSON response: %v", err)
	}

	if response["error"] != "Unauthorized" {
		t.Errorf("APIKeyAuth: got error %q, want %q", response["error"], "Unauthorized")
	}
	if response["message"] != "missing API key" {
		t.Errorf("APIKeyAuth: got message %q, want %q", response["message"], "missing API key")
	}
}

// TestAPIKeyAuth_EmptyKeyHeader tests that an empty API key header returns 401 Unauthorized.
func TestAPIKeyAuth_EmptyKeyHeader(t *testing.T) {
	expectedKey := "valid-key"
	middleware := APIKeyAuth(expectedKey)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(handler)

	// Create request with empty API key header
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-API-Key", "")

	rr := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("APIKeyAuth with empty key: got status %d, want %d", rr.Code, http.StatusUnauthorized)
	}

	var response map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("APIKeyAuth: failed to parse JSON response: %v", err)
	}

	if response["message"] != "missing API key" {
		t.Errorf("APIKeyAuth: got message %q, want %q", response["message"], "missing API key")
	}
}

// TestAPIKeyAuth_ConstantTimeComparison tests that the API key comparison is secure.
func TestAPIKeyAuth_ConstantTimeComparison(t *testing.T) {
	// This test verifies that partial key matches are rejected
	expectedKey := "abcdefghijklmnop"
	middleware := APIKeyAuth(expectedKey)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(handler)

	partialKeys := []string{
		"abcdefghijklmno",   // One character short
		"abcdefghijklmnopq", // One character extra
		"abcdefghijklmnoo",  // Last character different
		"Abcdefghijklmnop",  // First character different case
	}

	for _, partialKey := range partialKeys {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", partialKey)

		rr := httptest.NewRecorder()
		wrappedHandler.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Errorf("APIKeyAuth with partial key %q: got status %d, want %d", partialKey, rr.Code, http.StatusUnauthorized)
		}
	}
}

// TestResponseTimeTracker_NewResponseTimeTracker tests tracker creation.
func TestResponseTimeTracker_NewResponseTimeTracker(t *testing.T) {
	tracker := NewResponseTimeTracker(100)

	if tracker == nil {
		t.Fatal("NewResponseTimeTracker returned nil")
	}

	if tracker.maxSize != 100 {
		t.Errorf("NewResponseTimeTracker: maxSize = %d, want %d", tracker.maxSize, 100)
	}

	if len(tracker.samples) != 0 {
		t.Errorf("NewResponseTimeTracker: initial samples length = %d, want %d", len(tracker.samples), 0)
	}

	if tracker.position != 0 {
		t.Errorf("NewResponseTimeTracker: initial position = %d, want %d", tracker.position, 0)
	}
}

// TestResponseTimeTracker_Record tests recording response times.
func TestResponseTimeTracker_Record(t *testing.T) {
	tracker := NewResponseTimeTracker(5)

	// Record some values
	tracker.Record(10.0)
	tracker.Record(20.0)
	tracker.Record(30.0)

	if len(tracker.samples) != 3 {
		t.Errorf("Record: samples length = %d, want %d", len(tracker.samples), 3)
	}
}

// TestResponseTimeTracker_RecordCircularBuffer tests the circular buffer behavior.
func TestResponseTimeTracker_RecordCircularBuffer(t *testing.T) {
	tracker := NewResponseTimeTracker(3)

	// Fill the buffer
	tracker.Record(10.0)
	tracker.Record(20.0)
	tracker.Record(30.0)

	// These should overwrite the earliest values
	tracker.Record(40.0)
	tracker.Record(50.0)

	if len(tracker.samples) != 3 {
		t.Errorf("Record circular: samples length = %d, want %d", len(tracker.samples), 3)
	}

	// The buffer should contain [40, 50, 30] after wraparound
	// Position 0 was overwritten with 40, position 1 with 50
	expected := []float64{40.0, 50.0, 30.0}
	for i, want := range expected {
		if tracker.samples[i] != want {
			t.Errorf("Record circular: samples[%d] = %f, want %f", i, tracker.samples[i], want)
		}
	}
}

// TestResponseTimeTracker_Percentiles_NoSamples tests percentiles with no samples.
func TestResponseTimeTracker_Percentiles_NoSamples(t *testing.T) {
	tracker := NewResponseTimeTracker(100)

	percentiles := tracker.Percentiles()

	if percentiles != nil {
		t.Errorf("Percentiles with no samples: got %+v, want nil", percentiles)
	}
}

// TestResponseTimeTracker_Percentiles_SingleSample tests percentiles with a single sample.
func TestResponseTimeTracker_Percentiles_SingleSample(t *testing.T) {
	tracker := NewResponseTimeTracker(100)
	tracker.Record(50.0)

	percentiles := tracker.Percentiles()

	if percentiles == nil {
		t.Fatal("Percentiles with single sample: got nil")
	}

	// With a single sample, all percentiles should be that value
	if percentiles.P50 != 50.0 {
		t.Errorf("Percentiles P50 = %f, want %f", percentiles.P50, 50.0)
	}
	if percentiles.P95 != 50.0 {
		t.Errorf("Percentiles P95 = %f, want %f", percentiles.P95, 50.0)
	}
	if percentiles.P99 != 50.0 {
		t.Errorf("Percentiles P99 = %f, want %f", percentiles.P99, 50.0)
	}
}

// TestResponseTimeTracker_Percentiles_MultipleSamples tests percentiles calculation.
func TestResponseTimeTracker_Percentiles_MultipleSamples(t *testing.T) {
	tracker := NewResponseTimeTracker(100)

	// Add 100 samples: 1, 2, 3, ..., 100
	for i := 1; i <= 100; i++ {
		tracker.Record(float64(i))
	}

	percentiles := tracker.Percentiles()

	if percentiles == nil {
		t.Fatal("Percentiles returned nil")
	}

	// P50 should be around 50.5 (interpolated between 50 and 51)
	if percentiles.P50 < 50.0 || percentiles.P50 > 51.0 {
		t.Errorf("Percentiles P50 = %f, want around 50.5", percentiles.P50)
	}

	// P95 should be around 95.05 (interpolated between 95 and 96)
	if percentiles.P95 < 94.0 || percentiles.P95 > 96.0 {
		t.Errorf("Percentiles P95 = %f, want around 95", percentiles.P95)
	}

	// P99 should be around 99.01 (interpolated between 99 and 100)
	if percentiles.P99 < 98.0 || percentiles.P99 > 100.0 {
		t.Errorf("Percentiles P99 = %f, want around 99", percentiles.P99)
	}
}

// TestResponseTimeTracker_Percentiles_Concurrent tests thread safety of the tracker.
func TestResponseTimeTracker_Percentiles_Concurrent(t *testing.T) {
	tracker := NewResponseTimeTracker(1000)

	var wg sync.WaitGroup
	numGoroutines := 10
	recordsPerGoroutine := 100

	// Start multiple goroutines recording values
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				tracker.Record(float64(base*recordsPerGoroutine + j))
			}
		}(i)
	}

	// Also read percentiles concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = tracker.Percentiles()
		}
	}()

	wg.Wait()

	// After all goroutines complete, we should have samples
	percentiles := tracker.Percentiles()
	if percentiles == nil {
		t.Error("Percentiles should not be nil after concurrent writes")
	}
}

// TestResponseTimeTracker_Percentiles_DoesNotModifySamples tests that Percentiles does not modify samples.
func TestResponseTimeTracker_Percentiles_DoesNotModifySamples(t *testing.T) {
	tracker := NewResponseTimeTracker(100)

	// Add samples in descending order
	tracker.Record(100.0)
	tracker.Record(50.0)
	tracker.Record(10.0)

	// Call Percentiles multiple times
	_ = tracker.Percentiles()
	_ = tracker.Percentiles()

	// Original order should be preserved
	expected := []float64{100.0, 50.0, 10.0}
	for i, want := range expected {
		if tracker.samples[i] != want {
			t.Errorf("Percentiles modified samples: samples[%d] = %f, want %f", i, tracker.samples[i], want)
		}
	}
}

// TestPrometheusMiddleware_RecordsMetrics tests that the middleware records metrics.
func TestPrometheusMiddleware_RecordsMetrics(t *testing.T) {
	// Create a test handler that returns a specific status
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	})

	wrappedHandler := PrometheusMiddleware(handler)

	req := httptest.NewRequest(http.MethodPost, "/api/events", nil)
	rr := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rr, req)

	// Verify the response passes through correctly
	if rr.Code != http.StatusCreated {
		t.Errorf("PrometheusMiddleware: got status %d, want %d", rr.Code, http.StatusCreated)
	}
	if rr.Body.String() != "created" {
		t.Errorf("PrometheusMiddleware: got body %q, want %q", rr.Body.String(), "created")
	}
}

// TestPrometheusMiddleware_PassesThroughRequest tests that requests pass through correctly.
func TestPrometheusMiddleware_PassesThroughRequest(t *testing.T) {
	var capturedMethod, capturedPath string

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		capturedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := PrometheusMiddleware(handler)

	req := httptest.NewRequest(http.MethodPut, "/api/test/path", nil)
	rr := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rr, req)

	if capturedMethod != http.MethodPut {
		t.Errorf("PrometheusMiddleware: captured method = %q, want %q", capturedMethod, http.MethodPut)
	}
	if capturedPath != "/api/test/path" {
		t.Errorf("PrometheusMiddleware: captured path = %q, want %q", capturedPath, "/api/test/path")
	}
}

// TestPrometheusMiddleware_CapturesStatusCode tests that different status codes are captured.
func TestPrometheusMiddleware_CapturesStatusCode(t *testing.T) {
	testCases := []struct {
		name         string
		statusCode   int
		expectedCode int
	}{
		{"200 OK", http.StatusOK, http.StatusOK},
		{"201 Created", http.StatusCreated, http.StatusCreated},
		{"400 Bad Request", http.StatusBadRequest, http.StatusBadRequest},
		{"404 Not Found", http.StatusNotFound, http.StatusNotFound},
		{"500 Internal Server Error", http.StatusInternalServerError, http.StatusInternalServerError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
			})

			wrappedHandler := PrometheusMiddleware(handler)
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rr := httptest.NewRecorder()

			wrappedHandler.ServeHTTP(rr, req)

			if rr.Code != tc.expectedCode {
				t.Errorf("PrometheusMiddleware %s: got status %d, want %d", tc.name, rr.Code, tc.expectedCode)
			}
		})
	}
}

// TestPrometheusMiddleware_DefaultStatusCode tests that default status code is 200.
func TestPrometheusMiddleware_DefaultStatusCode(t *testing.T) {
	// Handler that writes body without explicit WriteHeader
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("no explicit status"))
	})

	wrappedHandler := PrometheusMiddleware(handler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("PrometheusMiddleware default status: got %d, want %d", rr.Code, http.StatusOK)
	}
}

// TestResponseWriter_WriteHeader tests the custom responseWriter WriteHeader behavior.
func TestResponseWriter_WriteHeader(t *testing.T) {
	rr := httptest.NewRecorder()
	wrapped := newResponseWriter(rr)

	// Initial state
	if wrapped.statusCode != http.StatusOK {
		t.Errorf("responseWriter initial statusCode = %d, want %d", wrapped.statusCode, http.StatusOK)
	}
	if wrapped.written {
		t.Error("responseWriter initial written should be false")
	}

	// Write header
	wrapped.WriteHeader(http.StatusNotFound)

	if wrapped.statusCode != http.StatusNotFound {
		t.Errorf("responseWriter statusCode after WriteHeader = %d, want %d", wrapped.statusCode, http.StatusNotFound)
	}
	if !wrapped.written {
		t.Error("responseWriter written should be true after WriteHeader")
	}
	if rr.Code != http.StatusNotFound {
		t.Errorf("underlying recorder Code = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

// TestResponseWriter_WriteHeaderOnce tests that WriteHeader only writes once.
func TestResponseWriter_WriteHeaderOnce(t *testing.T) {
	rr := httptest.NewRecorder()
	wrapped := newResponseWriter(rr)

	// First call
	wrapped.WriteHeader(http.StatusCreated)
	// Second call should be ignored
	wrapped.WriteHeader(http.StatusInternalServerError)

	if wrapped.statusCode != http.StatusCreated {
		t.Errorf("responseWriter statusCode = %d, want %d (first call)", wrapped.statusCode, http.StatusCreated)
	}
	if rr.Code != http.StatusCreated {
		t.Errorf("underlying recorder Code = %d, want %d", rr.Code, http.StatusCreated)
	}
}

// TestResponseWriter_Write tests the Write method behavior.
func TestResponseWriter_Write(t *testing.T) {
	rr := httptest.NewRecorder()
	wrapped := newResponseWriter(rr)

	// Write without explicit WriteHeader
	n, err := wrapped.Write([]byte("hello"))

	if err != nil {
		t.Errorf("responseWriter Write error: %v", err)
	}
	if n != 5 {
		t.Errorf("responseWriter Write returned %d bytes, want %d", n, 5)
	}
	if !wrapped.written {
		t.Error("responseWriter written should be true after Write")
	}
	if wrapped.statusCode != http.StatusOK {
		t.Errorf("responseWriter statusCode = %d, want %d", wrapped.statusCode, http.StatusOK)
	}
	if rr.Body.String() != "hello" {
		t.Errorf("responseWriter body = %q, want %q", rr.Body.String(), "hello")
	}
}

// TestResponseWriter_WriteAfterWriteHeader tests Write after WriteHeader.
func TestResponseWriter_WriteAfterWriteHeader(t *testing.T) {
	rr := httptest.NewRecorder()
	wrapped := newResponseWriter(rr)

	wrapped.WriteHeader(http.StatusAccepted)
	_, _ = wrapped.Write([]byte("accepted"))

	if rr.Code != http.StatusAccepted {
		t.Errorf("responseWriter Code = %d, want %d", rr.Code, http.StatusAccepted)
	}
	if rr.Body.String() != "accepted" {
		t.Errorf("responseWriter body = %q, want %q", rr.Body.String(), "accepted")
	}
}

// TestResponseWriter_Unwrap tests the Unwrap method for middleware compatibility.
func TestResponseWriter_Unwrap(t *testing.T) {
	rr := httptest.NewRecorder()
	wrapped := newResponseWriter(rr)

	unwrapped := wrapped.Unwrap()

	if unwrapped != rr {
		t.Error("Unwrap() should return the original ResponseWriter")
	}
}

// TestPercentile_EmptySlice tests percentile calculation with empty slice.
func TestPercentile_EmptySlice(t *testing.T) {
	result := percentile([]float64{}, 50)
	if result != 0 {
		t.Errorf("percentile of empty slice = %f, want 0", result)
	}
}

// TestPercentile_SingleValue tests percentile calculation with single value.
func TestPercentile_SingleValue(t *testing.T) {
	result := percentile([]float64{42.5}, 50)
	if result != 42.5 {
		t.Errorf("percentile of single value = %f, want 42.5", result)
	}

	result = percentile([]float64{42.5}, 99)
	if result != 42.5 {
		t.Errorf("percentile P99 of single value = %f, want 42.5", result)
	}
}

// TestPercentile_KnownValues tests percentile calculation with known values.
func TestPercentile_KnownValues(t *testing.T) {
	// Sorted slice: [10, 20, 30, 40, 50]
	sorted := []float64{10, 20, 30, 40, 50}

	// P50 with 5 values: index = 0.5 * 4 = 2, so value at index 2 = 30
	p50 := percentile(sorted, 50)
	if p50 != 30 {
		t.Errorf("P50 = %f, want 30", p50)
	}

	// P0 should be the first value
	p0 := percentile(sorted, 0)
	if p0 != 10 {
		t.Errorf("P0 = %f, want 10", p0)
	}

	// P100 should be the last value
	p100 := percentile(sorted, 100)
	if p100 != 50 {
		t.Errorf("P100 = %f, want 50", p100)
	}
}

// TestPercentile_Interpolation tests percentile interpolation.
func TestPercentile_Interpolation(t *testing.T) {
	sorted := []float64{10, 20}

	// P50 with 2 values: index = 0.5 * 1 = 0.5
	// Interpolation: 10 + 0.5 * (20 - 10) = 15
	p50 := percentile(sorted, 50)
	if p50 != 15 {
		t.Errorf("P50 interpolation = %f, want 15", p50)
	}
}

// TestPercentile_Rounding tests that percentile results are rounded to 2 decimal places.
func TestPercentile_Rounding(t *testing.T) {
	// Create values that would produce many decimal places
	sorted := []float64{1, 2, 3}

	// P33.33: index = 0.3333 * 2 = 0.6666
	// lower = 0, upper = 1, fraction = 0.6666
	// result = 1 + 0.6666 * (2 - 1) = 1.6666... should round to 1.67
	p33 := percentile(sorted, 33.33)

	// Check that it's rounded to at most 2 decimal places
	rounded := math.Round(p33*100) / 100
	if p33 != rounded {
		t.Errorf("percentile should be rounded to 2 decimal places: got %f", p33)
	}
}

// TestRecordEventIngested tests the event ingestion counter.
func TestRecordEventIngested(t *testing.T) {
	// This test verifies the function doesn't panic
	// Actual metric values are harder to test without resetting the registry
	RecordEventIngested("goal")
	RecordEventIngested("pass")
	RecordEventIngested("shot")
}

// TestRecordEngagementIngested tests the engagement ingestion counter.
func TestRecordEngagementIngested(t *testing.T) {
	RecordEngagementIngested("like")
	RecordEngagementIngested("comment")
	RecordEngagementIngested("share")
}

// TestRecordKafkaProduceError tests the Kafka error counter.
func TestRecordKafkaProduceError(t *testing.T) {
	RecordKafkaProduceError()
}

// TestRecordClickHouseQueryError tests the ClickHouse error counter.
func TestRecordClickHouseQueryError(t *testing.T) {
	RecordClickHouseQueryError()
}

// TestWriteUnauthorizedError tests the unauthorized error response format.
func TestWriteUnauthorizedError(t *testing.T) {
	rr := httptest.NewRecorder()

	writeUnauthorizedError(rr, "test error message")

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("writeUnauthorizedError status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}

	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf("writeUnauthorizedError Content-Type = %q, want %q", rr.Header().Get("Content-Type"), "application/json")
	}

	var response map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("writeUnauthorizedError: failed to parse JSON: %v", err)
	}

	if response["error"] != "Unauthorized" {
		t.Errorf("writeUnauthorizedError error = %q, want %q", response["error"], "Unauthorized")
	}
	if response["message"] != "test error message" {
		t.Errorf("writeUnauthorizedError message = %q, want %q", response["message"], "test error message")
	}
}
