package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/mabumusa1/football-simulator/apps/api/internal/kafka"
	"github.com/mabumusa1/football-simulator/apps/api/internal/repository"
)

// Test infrastructure helpers
func getKafkaBroker() string {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}
	return broker
}

func getClickHouseHost() string {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "clickhouse"
	}
	return host
}

func getClickHousePort() string {
	port := os.Getenv("CLICKHOUSE_PORT")
	if port == "" {
		port = "9000"
	}
	return port
}

func setupTestProducers(t *testing.T) (*kafka.EventProducer, *kafka.EngagementProducer, func()) {
	broker := getKafkaBroker()
	eventTopic := "test-events-" + uuid.New().String()[:8]
	engagementTopic := "test-engagements-" + uuid.New().String()[:8]

	eventWriter := kafka.NewWriter([]string{broker}, eventTopic)
	engagementWriter := kafka.NewEngagementWriter([]string{broker}, engagementTopic)

	eventProducer := kafka.NewEventProducer(eventWriter, nil)
	engagementProducer := kafka.NewEngagementProducer(engagementWriter, nil)

	cleanup := func() {
		_ = eventProducer.Close()
		_ = engagementProducer.Close()
	}

	return eventProducer, engagementProducer, cleanup
}

func setupTestRepository(t *testing.T) (*repository.ClickHouseRepository, func()) {
	host := getClickHouseHost()
	port := getClickHousePort()

	opts := &clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{
			Database: "football_simulator",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 10 * time.Second,
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("failed to open ClickHouse connection: %v", err)
	}

	repo := repository.NewClickHouseRepository(conn, nil)

	cleanup := func() {
		_ = conn.Close()
	}

	return repo, cleanup
}

// Helper function to create a valid event request JSON
func validEventRequestJSON() string {
	return `{
		"eventId": "` + uuid.New().String() + `",
		"matchId": "match-123",
		"eventType": "goal",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
		"teamId": 1,
		"playerId": "player-456",
		"metadata": {"assistBy": "player-789"}
	}`
}

// Helper function to create a valid engagement batch request JSON
func validEngagementBatchJSON() string {
	return `{
		"events": [
			{
				"event_id": "` + uuid.New().String() + `",
				"match_id": "match-123",
				"user_id": "user-001",
				"session_id": "session-001",
				"engagement_type": "reaction",
				"engagement_subtype": "cheer",
				"game_minute": 45,
				"device_type": "mobile",
				"platform": "ios",
				"country_code": "US",
				"content": "",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			}
		]
	}`
}

// =============================================================================
// IngestEvent Handler Tests
// =============================================================================

func TestIngestEvent_ValidEvent(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := validEventRequestJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	var result IngestEventResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Status != "accepted" {
		t.Errorf("expected status 'accepted', got '%s'", result.Status)
	}

	if result.EventID == "" {
		t.Error("expected eventId to be set")
	}
}

func TestIngestEvent_InvalidJSON(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{invalid json}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "invalid JSON body" {
		t.Errorf("expected message 'invalid JSON body', got '%s'", result.Message)
	}
}

func TestIngestEvent_ValidationError_InvalidEventID(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{
		"eventId": "not-a-uuid",
		"matchId": "match-123",
		"eventType": "goal",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
		"teamId": 1,
		"playerId": "player-456"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Field != "eventId" {
		t.Errorf("expected field 'eventId', got '%s'", result.Field)
	}
}

func TestIngestEvent_ValidationError_MissingMatchID(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{
		"eventId": "` + uuid.New().String() + `",
		"matchId": "",
		"eventType": "goal",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
		"teamId": 1,
		"playerId": "player-456"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Field != "matchId" {
		t.Errorf("expected field 'matchId', got '%s'", result.Field)
	}
}

func TestIngestEvent_ValidationError_InvalidEventType(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{
		"eventId": "` + uuid.New().String() + `",
		"matchId": "match-123",
		"eventType": "invalid_type",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
		"teamId": 1,
		"playerId": "player-456"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Field != "eventType" {
		t.Errorf("expected field 'eventType', got '%s'", result.Field)
	}
}

func TestIngestEvent_ValidationError_InvalidTimestamp(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{
		"eventId": "` + uuid.New().String() + `",
		"matchId": "match-123",
		"eventType": "goal",
		"timestamp": "not-a-timestamp",
		"teamId": 1,
		"playerId": "player-456"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Field != "timestamp" {
		t.Errorf("expected field 'timestamp', got '%s'", result.Field)
	}
}

func TestIngestEvent_ValidationError_InvalidTeamID(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{
		"eventId": "` + uuid.New().String() + `",
		"matchId": "match-123",
		"eventType": "goal",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
		"teamId": 3,
		"playerId": "player-456"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Field != "teamId" {
		t.Errorf("expected field 'teamId', got '%s'", result.Field)
	}
}

func TestIngestEvent_AllEventTypes(t *testing.T) {
	eventTypes := []string{
		"pass", "shot", "goal", "foul", "yellow_card", "red_card",
		"substitution", "offside", "corner", "free_kick", "interception",
	}

	for _, eventType := range eventTypes {
		t.Run(eventType, func(t *testing.T) {
			eventProducer, _, cleanupProducers := setupTestProducers(t)
			defer cleanupProducers()

			repo, cleanupRepo := setupTestRepository(t)
			defer cleanupRepo()

			handler := NewHandler(eventProducer, repo)

			reqBody := `{
				"eventId": "` + uuid.New().String() + `",
				"matchId": "match-123",
				"eventType": "` + eventType + `",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
				"teamId": 1,
				"playerId": "player-456"
			}`
			req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler.IngestEvent(w, req)

			resp := w.Result()
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusAccepted {
				t.Errorf("expected status %d, got %d for event type %s", http.StatusAccepted, resp.StatusCode, eventType)
			}
		})
	}
}

func TestIngestEvent_EmptyBody(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}
}

// =============================================================================
// IngestEngagements Handler Tests
// =============================================================================

func TestIngestEngagements_ValidBatch(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

	reqBody := validEngagementBatchJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	var result IngestEngagementsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Accepted != 1 {
		t.Errorf("expected 1 accepted, got %d", result.Accepted)
	}

	if result.Rejected != 0 {
		t.Errorf("expected 0 rejected, got %d", result.Rejected)
	}

	if result.Status != "accepted" {
		t.Errorf("expected status 'accepted', got '%s'", result.Status)
	}
}

func TestIngestEngagements_PartialFailures(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

	// Batch with one valid and one invalid event (missing user_id)
	reqBody := `{
		"events": [
			{
				"event_id": "` + uuid.New().String() + `",
				"match_id": "match-123",
				"user_id": "user-001",
				"session_id": "session-001",
				"engagement_type": "reaction",
				"game_minute": 45,
				"device_type": "mobile",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			},
			{
				"event_id": "` + uuid.New().String() + `",
				"match_id": "match-123",
				"user_id": "",
				"session_id": "session-002",
				"engagement_type": "reaction",
				"game_minute": 46,
				"device_type": "mobile",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	var result IngestEngagementsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Accepted != 1 {
		t.Errorf("expected 1 accepted, got %d", result.Accepted)
	}

	if result.Rejected != 1 {
		t.Errorf("expected 1 rejected, got %d", result.Rejected)
	}
}

func TestIngestEngagements_EmptyEvents(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

	reqBody := `{"events": []}`
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "no events provided" {
		t.Errorf("expected message 'no events provided', got '%s'", result.Message)
	}
}

func TestIngestEngagements_InvalidJSON(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

	reqBody := `{invalid json}`
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "invalid JSON body" {
		t.Errorf("expected message 'invalid JSON body', got '%s'", result.Message)
	}
}

func TestIngestEngagements_NotConfigured(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	// Create handler without engagement producer
	handler := NewHandler(eventProducer, repo)

	reqBody := validEngagementBatchJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "engagement ingestion not configured" {
		t.Errorf("expected message 'engagement ingestion not configured', got '%s'", result.Message)
	}
}

func TestIngestEngagements_MultipleBatchEvents(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

	// Create a batch with 3 valid events
	reqBody := `{
		"events": [
			{
				"event_id": "` + uuid.New().String() + `",
				"match_id": "match-123",
				"user_id": "user-001",
				"session_id": "session-001",
				"engagement_type": "reaction",
				"game_minute": 45,
				"device_type": "mobile",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			},
			{
				"event_id": "` + uuid.New().String() + `",
				"match_id": "match-123",
				"user_id": "user-002",
				"session_id": "session-002",
				"engagement_type": "comment",
				"game_minute": 46,
				"device_type": "desktop",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			},
			{
				"event_id": "` + uuid.New().String() + `",
				"match_id": "match-123",
				"user_id": "user-003",
				"session_id": "session-003",
				"engagement_type": "share",
				"game_minute": 47,
				"device_type": "tablet",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	var result IngestEngagementsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Accepted != 3 {
		t.Errorf("expected 3 accepted, got %d", result.Accepted)
	}
}

// =============================================================================
// GetMatchMetrics Handler Tests
// =============================================================================

func TestGetMatchMetrics_NonExistentMatch(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/nonexistent-match-"+uuid.New().String()+"/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "match not found" {
		t.Errorf("expected message 'match not found', got '%s'", result.Message)
	}
}

func TestGetMatchMetrics_MissingMatchID(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	// Test without chi router to simulate missing matchId
	req := httptest.NewRequest(http.MethodGet, "/api/matches//metrics", nil)
	w := httptest.NewRecorder()

	handler.GetMatchMetrics(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "matchId is required" {
		t.Errorf("expected message 'matchId is required', got '%s'", result.Message)
	}
}

// =============================================================================
// HealthCheck Handler Tests
// =============================================================================

func TestHealthCheck_ReturnsHealthy(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler.HealthCheck(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); contentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got '%s'", contentType)
	}

	var result HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Status != "healthy" {
		t.Errorf("expected status 'healthy', got '%s'", result.Status)
	}

	if result.Timestamp.IsZero() {
		t.Error("expected timestamp to be set")
	}
}

func TestHealthCheck_TimestampIsUTC(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler.HealthCheck(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	var result HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Timestamp.Location() != time.UTC {
		t.Errorf("expected timestamp in UTC, got %v", result.Timestamp.Location())
	}
}

// =============================================================================
// ReadinessCheck Handler Tests
// =============================================================================

func TestReadinessCheck_AllHealthy(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	handler.ReadinessCheck(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var result ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Status != "ready" {
		t.Errorf("expected status 'ready', got '%s'", result.Status)
	}

	if result.Checks["clickhouse"] != "healthy" {
		t.Errorf("expected clickhouse check 'healthy', got '%s'", result.Checks["clickhouse"])
	}

	if result.Checks["kafka"] != "healthy" {
		t.Errorf("expected kafka check 'healthy', got '%s'", result.Checks["kafka"])
	}
}

func TestReadinessCheck_TimestampIsSet(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	handler.ReadinessCheck(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	var result ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Timestamp.IsZero() {
		t.Error("expected timestamp to be set")
	}

	if result.Timestamp.Location() != time.UTC {
		t.Errorf("expected timestamp in UTC, got %v", result.Timestamp.Location())
	}
}

// =============================================================================
// Handler Constructor Tests
// =============================================================================

func TestNewHandler(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	if handler.producer == nil {
		t.Error("expected producer to be set")
	}

	if handler.repository == nil {
		t.Error("expected repository to be set")
	}

	if handler.engagementProducer != nil {
		t.Error("expected engagementProducer to be nil")
	}
}

func TestNewHandlerWithEngagement(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

	if handler.producer == nil {
		t.Error("expected producer to be set")
	}

	if handler.repository == nil {
		t.Error("expected repository to be set")
	}

	if handler.engagementProducer == nil {
		t.Error("expected engagementProducer to be set")
	}
}

// =============================================================================
// SwaggerUI Handler Tests
// =============================================================================

func TestSwaggerUI(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	SwaggerUI(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); contentType != "text/html; charset=utf-8" {
		t.Errorf("expected Content-Type 'text/html; charset=utf-8', got '%s'", contentType)
	}

	// Verify response contains expected HTML content
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	body := buf.String()

	if !strings.Contains(body, "swagger-ui") {
		t.Error("expected response to contain 'swagger-ui'")
	}

	if !strings.Contains(body, "Football Simulator Events API") {
		t.Error("expected response to contain 'Football Simulator Events API'")
	}
}

// =============================================================================
// ServeOpenAPISpec Handler Tests
// =============================================================================

func TestServeOpenAPISpec(t *testing.T) {
	spec := []byte(`openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"`)

	handler := ServeOpenAPISpec(spec)

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); contentType != "application/x-yaml" {
		t.Errorf("expected Content-Type 'application/x-yaml', got '%s'", contentType)
	}

	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	body := buf.String()

	if !strings.Contains(body, "openapi:") {
		t.Error("expected response to contain 'openapi:'")
	}

	if !strings.Contains(body, "Test API") {
		t.Error("expected response to contain 'Test API'")
	}
}

// =============================================================================
// Edge Cases and Additional Tests
// =============================================================================

func TestIngestEvent_WithMetadata(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{
		"eventId": "` + uuid.New().String() + `",
		"matchId": "match-123",
		"eventType": "goal",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
		"teamId": 1,
		"playerId": "player-456",
		"metadata": {
			"assistBy": "player-789",
			"goalType": "header",
			"distance": 12.5
		}
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}
}

func TestIngestEvent_TeamID2(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandler(eventProducer, repo)

	reqBody := `{
		"eventId": "` + uuid.New().String() + `",
		"matchId": "match-123",
		"eventType": "goal",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `",
		"teamId": 2,
		"playerId": "player-456"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}
}

func TestIngestEngagements_AllInvalid(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

	// All events are invalid (missing required fields)
	reqBody := `{
		"events": [
			{
				"event_id": "invalid-uuid",
				"match_id": "match-123",
				"user_id": "user-001",
				"session_id": "session-001",
				"engagement_type": "reaction",
				"game_minute": 45,
				"device_type": "mobile",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			},
			{
				"event_id": "` + uuid.New().String() + `",
				"match_id": "",
				"user_id": "user-002",
				"session_id": "session-002",
				"engagement_type": "reaction",
				"game_minute": 45,
				"device_type": "mobile",
				"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
			}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	var result IngestEngagementsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Accepted != 0 {
		t.Errorf("expected 0 accepted, got %d", result.Accepted)
	}

	if result.Rejected != 2 {
		t.Errorf("expected 2 rejected, got %d", result.Rejected)
	}
}

func TestIngestEngagements_AllEngagementTypes(t *testing.T) {
	engagementTypes := []string{
		"reaction", "comment", "video_action", "share", "prediction", "click", "session",
	}

	for _, engagementType := range engagementTypes {
		t.Run(engagementType, func(t *testing.T) {
			eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
			defer cleanupProducers()

			repo, cleanupRepo := setupTestRepository(t)
			defer cleanupRepo()

			handler := NewHandlerWithEngagement(eventProducer, engagementProducer, repo)

			reqBody := `{
				"events": [
					{
						"event_id": "` + uuid.New().String() + `",
						"match_id": "match-123",
						"user_id": "user-001",
						"session_id": "session-001",
						"engagement_type": "` + engagementType + `",
						"game_minute": 45,
						"device_type": "mobile",
						"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
					}
				]
			}`
			req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler.IngestEngagements(w, req)

			resp := w.Result()
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusAccepted {
				t.Errorf("expected status %d, got %d for engagement type %s", http.StatusAccepted, resp.StatusCode, engagementType)
			}
		})
	}
}

func TestPingContext(t *testing.T) {
	eventProducer, _, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify Kafka ping
	err := eventProducer.Ping(ctx)
	if err != nil {
		t.Errorf("Kafka ping failed: %v", err)
	}

	// Verify ClickHouse ping
	err = repo.Ping(ctx)
	if err != nil {
		t.Errorf("ClickHouse ping failed: %v", err)
	}
}
