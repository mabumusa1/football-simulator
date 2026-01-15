package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/mabumusa1/football-simulator/apps/api/internal/domain"
)

// MockEventProducer is a mock implementation of the EventProducer interface.
type MockEventProducer struct {
	ProduceFunc func(ctx context.Context, event *domain.Event) error
	PingFunc    func(ctx context.Context) error
}

func (m *MockEventProducer) Produce(ctx context.Context, event *domain.Event) error {
	if m.ProduceFunc != nil {
		return m.ProduceFunc(ctx, event)
	}
	return nil
}

func (m *MockEventProducer) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

// MockEngagementProducer is a mock implementation of the EngagementProducer interface.
type MockEngagementProducer struct {
	ProduceFunc      func(ctx context.Context, event *domain.EngagementEvent) error
	ProduceBatchFunc func(ctx context.Context, events []*domain.EngagementEvent) error
	PingFunc         func(ctx context.Context) error
}

func (m *MockEngagementProducer) Produce(ctx context.Context, event *domain.EngagementEvent) error {
	if m.ProduceFunc != nil {
		return m.ProduceFunc(ctx, event)
	}
	return nil
}

func (m *MockEngagementProducer) ProduceBatch(ctx context.Context, events []*domain.EngagementEvent) error {
	if m.ProduceBatchFunc != nil {
		return m.ProduceBatchFunc(ctx, events)
	}
	return nil
}

func (m *MockEngagementProducer) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

// MockMetricsRepository is a mock implementation of the MetricsRepository interface.
type MockMetricsRepository struct {
	GetMatchMetricsFunc    func(ctx context.Context, matchID string) (*domain.MatchMetrics, error)
	GetEventsPerMinuteFunc func(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error)
	PingFunc               func(ctx context.Context) error
}

func (m *MockMetricsRepository) GetMatchMetrics(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
	if m.GetMatchMetricsFunc != nil {
		return m.GetMatchMetricsFunc(ctx, matchID)
	}
	return nil, nil
}

func (m *MockMetricsRepository) GetEventsPerMinute(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error) {
	if m.GetEventsPerMinuteFunc != nil {
		return m.GetEventsPerMinuteFunc(ctx, matchID)
	}
	return nil, nil
}

func (m *MockMetricsRepository) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
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
	mockProducer := &MockEventProducer{
		ProduceFunc: func(ctx context.Context, event *domain.Event) error {
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

	reqBody := validEventRequestJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

	reqBody := `{invalid json}`
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	defer resp.Body.Close()

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

func TestIngestEvent_KafkaError(t *testing.T) {
	mockProducer := &MockEventProducer{
		ProduceFunc: func(ctx context.Context, event *domain.Event) error {
			return errors.New("kafka connection failed")
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

	reqBody := validEventRequestJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "failed to queue event" {
		t.Errorf("expected message 'failed to queue event', got '%s'", result.Message)
	}
}

func TestIngestEvent_AllEventTypes(t *testing.T) {
	eventTypes := []string{
		"pass", "shot", "goal", "foul", "yellow_card", "red_card",
		"substitution", "offside", "corner", "free_kick", "interception",
	}

	for _, eventType := range eventTypes {
		t.Run(eventType, func(t *testing.T) {
			mockProducer := &MockEventProducer{
				ProduceFunc: func(ctx context.Context, event *domain.Event) error {
					if string(event.EventType) != eventType {
						t.Errorf("expected event type '%s', got '%s'", eventType, event.EventType)
					}
					return nil
				},
			}
			mockRepo := &MockMetricsRepository{}

			handler := NewHandler(mockProducer, mockRepo)

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
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Errorf("expected status %d, got %d for event type %s", http.StatusAccepted, resp.StatusCode, eventType)
			}
		})
	}
}

func TestIngestEvent_EmptyBody(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodPost, "/api/events", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEvent(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
	}
}

// =============================================================================
// IngestEngagements Handler Tests
// =============================================================================

func TestIngestEngagements_ValidBatch(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{
		ProduceBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			if len(events) != 1 {
				t.Errorf("expected 1 event, got %d", len(events))
			}
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

	reqBody := validEngagementBatchJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{
		ProduceBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

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
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

	reqBody := `{"events": []}`
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

	reqBody := `{invalid json}`
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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

func TestIngestEngagements_KafkaError(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{
		ProduceBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			return errors.New("kafka batch produce failed")
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

	reqBody := validEngagementBatchJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "failed to queue engagements" {
		t.Errorf("expected message 'failed to queue engagements', got '%s'", result.Message)
	}
}

func TestIngestEngagements_NotConfigured(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	// Create handler without engagement producer
	handler := NewHandler(mockProducer, mockRepo)

	reqBody := validEngagementBatchJSON()
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.IngestEngagements(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	eventCount := 0
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{
		ProduceBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			eventCount = len(events)
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	if eventCount != 3 {
		t.Errorf("expected 3 events in batch, got %d", eventCount)
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

func TestGetMatchMetrics_Success(t *testing.T) {
	now := time.Now().UTC()
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{
		GetMatchMetricsFunc: func(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
			return &domain.MatchMetrics{
				MatchID:     matchID,
				TotalEvents: 100,
				EventsByType: map[string]int64{
					"goal":        2,
					"pass":        50,
					"shot":        10,
					"yellow_card": 3,
				},
				Goals:        2,
				YellowCards:  3,
				RedCards:     0,
				FirstEventAt: &now,
				LastEventAt:  &now,
			}, nil
		},
		GetEventsPerMinuteFunc: func(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error) {
			return []domain.EventsPerMinute{
				{Minute: now, EventType: "pass", EventCount: 10},
				{Minute: now.Add(time.Minute), EventType: "pass", EventCount: 15},
			}, nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	// Use chi router to properly set URL parameters
	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/match-123/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var result domain.MatchMetrics
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.MatchID != "match-123" {
		t.Errorf("expected matchId 'match-123', got '%s'", result.MatchID)
	}

	if result.TotalEvents != 100 {
		t.Errorf("expected 100 total events, got %d", result.TotalEvents)
	}

	if result.Goals != 2 {
		t.Errorf("expected 2 goals, got %d", result.Goals)
	}
}

func TestGetMatchMetrics_NotFound(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{
		GetMatchMetricsFunc: func(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
			return &domain.MatchMetrics{
				MatchID:     matchID,
				TotalEvents: 0,
			}, nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/nonexistent-match/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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

func TestGetMatchMetrics_NilMetrics(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{
		GetMatchMetricsFunc: func(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
			return nil, nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/nonexistent-match/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, resp.StatusCode)
	}
}

func TestGetMatchMetrics_RepositoryError(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{
		GetMatchMetricsFunc: func(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
			return nil, errors.New("database connection failed")
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/match-123/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, resp.StatusCode)
	}

	var result ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Message != "failed to fetch metrics" {
		t.Errorf("expected message 'failed to fetch metrics', got '%s'", result.Message)
	}
}

func TestGetMatchMetrics_EventsPerMinuteError(t *testing.T) {
	now := time.Now().UTC()
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{
		GetMatchMetricsFunc: func(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
			return &domain.MatchMetrics{
				MatchID:     matchID,
				TotalEvents: 100,
				Goals:       2,
			}, nil
		},
		GetEventsPerMinuteFunc: func(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error) {
			return nil, errors.New("query failed")
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/match-123/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	// Should still return 200 but without peak engagement data
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var result domain.MatchMetrics
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// PeakMinute should be nil when events per minute query fails
	if result.PeakMinute != nil {
		t.Errorf("expected PeakMinute to be nil, got %v", result.PeakMinute)
	}

	_ = now // Unused in this test
}

func TestGetMatchMetrics_MissingMatchID(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

	// Test without chi router to simulate missing matchId
	req := httptest.NewRequest(http.MethodGet, "/api/matches//metrics", nil)
	w := httptest.NewRecorder()

	handler.GetMatchMetrics(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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

func TestGetMatchMetrics_WithPeakEngagement(t *testing.T) {
	peakTime := time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC)
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{
		GetMatchMetricsFunc: func(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
			return &domain.MatchMetrics{
				MatchID:     matchID,
				TotalEvents: 100,
			}, nil
		},
		GetEventsPerMinuteFunc: func(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error) {
			return []domain.EventsPerMinute{
				{Minute: peakTime, EventType: "pass", EventCount: 10},
				{Minute: peakTime, EventType: "shot", EventCount: 5},
				{Minute: peakTime.Add(time.Minute), EventType: "pass", EventCount: 8},
			}, nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/match-123/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var result domain.MatchMetrics
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.PeakMinute == nil {
		t.Fatal("expected PeakMinute to be set")
	}

	// Peak should be at peakTime with count 15 (10 + 5)
	if result.PeakMinute.EventCount != 15 {
		t.Errorf("expected peak event count 15, got %d", result.PeakMinute.EventCount)
	}
}

// =============================================================================
// HealthCheck Handler Tests
// =============================================================================

func TestHealthCheck_ReturnsHealthy(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler.HealthCheck(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler.HealthCheck(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{
		PingFunc: func(ctx context.Context) error {
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{
		PingFunc: func(ctx context.Context) error {
			return nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	handler.ReadinessCheck(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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

func TestReadinessCheck_ClickHouseUnhealthy(t *testing.T) {
	mockProducer := &MockEventProducer{
		PingFunc: func(ctx context.Context) error {
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{
		PingFunc: func(ctx context.Context) error {
			return errors.New("connection refused")
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	handler.ReadinessCheck(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}

	var result ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Status != "not ready" {
		t.Errorf("expected status 'not ready', got '%s'", result.Status)
	}

	if !strings.Contains(result.Checks["clickhouse"], "unhealthy") {
		t.Errorf("expected clickhouse check to contain 'unhealthy', got '%s'", result.Checks["clickhouse"])
	}

	if !strings.Contains(result.Checks["clickhouse"], "connection refused") {
		t.Errorf("expected clickhouse check to contain error message, got '%s'", result.Checks["clickhouse"])
	}
}

func TestReadinessCheck_KafkaUnhealthy(t *testing.T) {
	mockProducer := &MockEventProducer{
		PingFunc: func(ctx context.Context) error {
			return errors.New("broker not available")
		},
	}
	mockRepo := &MockMetricsRepository{
		PingFunc: func(ctx context.Context) error {
			return nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	handler.ReadinessCheck(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}

	var result ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Status != "not ready" {
		t.Errorf("expected status 'not ready', got '%s'", result.Status)
	}

	if !strings.Contains(result.Checks["kafka"], "unhealthy") {
		t.Errorf("expected kafka check to contain 'unhealthy', got '%s'", result.Checks["kafka"])
	}

	if !strings.Contains(result.Checks["kafka"], "broker not available") {
		t.Errorf("expected kafka check to contain error message, got '%s'", result.Checks["kafka"])
	}

	// ClickHouse should still be healthy
	if result.Checks["clickhouse"] != "healthy" {
		t.Errorf("expected clickhouse check 'healthy', got '%s'", result.Checks["clickhouse"])
	}
}

func TestReadinessCheck_BothUnhealthy(t *testing.T) {
	mockProducer := &MockEventProducer{
		PingFunc: func(ctx context.Context) error {
			return errors.New("kafka error")
		},
	}
	mockRepo := &MockMetricsRepository{
		PingFunc: func(ctx context.Context) error {
			return errors.New("clickhouse error")
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	handler.ReadinessCheck(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}

	var result ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.Status != "not ready" {
		t.Errorf("expected status 'not ready', got '%s'", result.Status)
	}

	if !strings.Contains(result.Checks["clickhouse"], "unhealthy") {
		t.Errorf("expected clickhouse check to contain 'unhealthy', got '%s'", result.Checks["clickhouse"])
	}

	if !strings.Contains(result.Checks["kafka"], "unhealthy") {
		t.Errorf("expected kafka check to contain 'unhealthy', got '%s'", result.Checks["kafka"])
	}
}

func TestReadinessCheck_TimestampIsSet(t *testing.T) {
	mockProducer := &MockEventProducer{
		PingFunc: func(ctx context.Context) error {
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{
		PingFunc: func(ctx context.Context) error {
			return nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	handler.ReadinessCheck(w, req)

	resp := w.Result()
	defer resp.Body.Close()

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
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); contentType != "text/html; charset=utf-8" {
		t.Errorf("expected Content-Type 'text/html; charset=utf-8', got '%s'", contentType)
	}

	// Verify response contains expected HTML content
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	if contentType := resp.Header.Get("Content-Type"); contentType != "application/x-yaml" {
		t.Errorf("expected Content-Type 'application/x-yaml', got '%s'", contentType)
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
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
	var capturedEvent *domain.Event
	mockProducer := &MockEventProducer{
		ProduceFunc: func(ctx context.Context, event *domain.Event) error {
			capturedEvent = event
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	if capturedEvent == nil {
		t.Fatal("expected event to be captured")
	}

	if capturedEvent.Metadata == nil {
		t.Fatal("expected metadata to be set")
	}

	if capturedEvent.Metadata["assistBy"] != "player-789" {
		t.Errorf("expected assistBy 'player-789', got '%v'", capturedEvent.Metadata["assistBy"])
	}
}

func TestIngestEvent_TeamID2(t *testing.T) {
	var capturedEvent *domain.Event
	mockProducer := &MockEventProducer{
		ProduceFunc: func(ctx context.Context, event *domain.Event) error {
			capturedEvent = event
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandler(mockProducer, mockRepo)

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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}

	if capturedEvent == nil {
		t.Fatal("expected event to be captured")
	}

	if capturedEvent.TeamID != 2 {
		t.Errorf("expected teamId 2, got %d", capturedEvent.TeamID)
	}
}

func TestIngestEngagements_AllInvalid(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockEngagementProducer := &MockEngagementProducer{
		ProduceBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			return nil
		},
	}
	mockRepo := &MockMetricsRepository{}

	handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

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
	defer resp.Body.Close()

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
			mockProducer := &MockEventProducer{}
			mockEngagementProducer := &MockEngagementProducer{
				ProduceBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
					if len(events) != 1 {
						t.Errorf("expected 1 event, got %d", len(events))
					}
					if string(events[0].EngagementType) != engagementType {
						t.Errorf("expected engagement type '%s', got '%s'", engagementType, events[0].EngagementType)
					}
					return nil
				},
			}
			mockRepo := &MockMetricsRepository{}

			handler := NewHandlerWithEngagement(mockProducer, mockEngagementProducer, mockRepo)

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
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Errorf("expected status %d, got %d for engagement type %s", http.StatusAccepted, resp.StatusCode, engagementType)
			}
		})
	}
}

func TestGetMatchMetrics_EmptyEventsPerMinute(t *testing.T) {
	mockProducer := &MockEventProducer{}
	mockRepo := &MockMetricsRepository{
		GetMatchMetricsFunc: func(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
			return &domain.MatchMetrics{
				MatchID:     matchID,
				TotalEvents: 100,
			}, nil
		},
		GetEventsPerMinuteFunc: func(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error) {
			return []domain.EventsPerMinute{}, nil
		},
	}

	handler := NewHandler(mockProducer, mockRepo)

	r := chi.NewRouter()
	r.Get("/api/matches/{matchId}/metrics", handler.GetMatchMetrics)

	req := httptest.NewRequest(http.MethodGet, "/api/matches/match-123/metrics", nil)
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	var result domain.MatchMetrics
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// PeakMinute should be nil when events per minute is empty
	if result.PeakMinute != nil {
		t.Errorf("expected PeakMinute to be nil, got %v", result.PeakMinute)
	}
}
