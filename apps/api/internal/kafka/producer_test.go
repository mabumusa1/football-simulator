package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/api/internal/domain"
)

// mockWriter implements a mock kafka.Writer for testing purposes.
// Since kafka.Writer is a concrete struct, we use a wrapper approach.
type mockWriter struct {
	topic        string
	writeErr     error
	writtenMsgs  []kafka.Message
	mu           sync.Mutex
	writeCounter int
	// failOnNthWrite allows simulating partial failures
	failOnNthWrite int
}

func newMockWriter(topic string) *mockWriter {
	return &mockWriter{
		topic:       topic,
		writtenMsgs: make([]kafka.Message, 0),
	}
}

func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeCounter++

	if m.failOnNthWrite > 0 && m.writeCounter == m.failOnNthWrite {
		return m.writeErr
	}

	if m.writeErr != nil && m.failOnNthWrite == 0 {
		return m.writeErr
	}

	m.writtenMsgs = append(m.writtenMsgs, msgs...)
	return nil
}

func (m *mockWriter) getWrittenMessages() []kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writtenMsgs
}

// testableEventProducer wraps EventProducer with a mock writer interface
type testableEventProducer struct {
	mock   *mockWriter
	logger *slog.Logger
}

func newTestableEventProducer(mock *mockWriter, logger *slog.Logger) *testableEventProducer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &testableEventProducer{
		mock:   mock,
		logger: logger,
	}
}

func (p *testableEventProducer) Produce(ctx context.Context, event *domain.Event) error {
	if event == nil {
		return errors.New("event cannot be nil")
	}

	value, err := event.ToKafkaMessage()
	if err != nil {
		return errors.New("failed to serialize event: " + err.Error())
	}

	msg := kafka.Message{
		Key:   []byte(event.MatchID),
		Value: value,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(string(event.EventType))},
			{Key: "event_id", Value: []byte(event.EventID.String())},
		},
		Time: event.Timestamp,
	}

	err = p.mock.WriteMessages(ctx, msg)
	if err != nil {
		return errors.New("failed to produce message: " + err.Error())
	}

	return nil
}

func (p *testableEventProducer) ProduceBatch(ctx context.Context, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(events))

	for _, event := range events {
		if event == nil {
			continue
		}

		value, err := event.ToKafkaMessage()
		if err != nil {
			// Skip events that fail serialization
			continue
		}

		msg := kafka.Message{
			Key:   []byte(event.MatchID),
			Value: value,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(string(event.EventType))},
				{Key: "event_id", Value: []byte(event.EventID.String())},
			},
			Time: event.Timestamp,
		}

		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil
	}

	err := p.mock.WriteMessages(ctx, messages...)
	if err != nil {
		return errors.New("failed to produce batch: " + err.Error())
	}

	return nil
}

// testableEngagementProducer wraps EngagementProducer with a mock writer interface
type testableEngagementProducer struct {
	mock   *mockWriter
	logger *slog.Logger
}

func newTestableEngagementProducer(mock *mockWriter, logger *slog.Logger) *testableEngagementProducer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &testableEngagementProducer{
		mock:   mock,
		logger: logger,
	}
}

func (p *testableEngagementProducer) Produce(ctx context.Context, event *domain.EngagementEvent) error {
	if event == nil {
		return errors.New("engagement event cannot be nil")
	}

	value, err := event.ToKafkaMessage()
	if err != nil {
		return errors.New("failed to serialize engagement: " + err.Error())
	}

	msg := kafka.Message{
		Key:   []byte(event.MatchID + ":" + event.UserID),
		Value: value,
		Headers: []kafka.Header{
			{Key: "engagement_type", Value: []byte(string(event.EngagementType))},
			{Key: "event_id", Value: []byte(event.EventID.String())},
			{Key: "user_id", Value: []byte(event.UserID)},
		},
		Time: event.Timestamp,
	}

	err = p.mock.WriteMessages(ctx, msg)
	if err != nil {
		return errors.New("failed to produce engagement: " + err.Error())
	}

	return nil
}

func (p *testableEngagementProducer) ProduceBatch(ctx context.Context, events []*domain.EngagementEvent) error {
	if len(events) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(events))

	for _, event := range events {
		if event == nil {
			continue
		}

		value, err := event.ToKafkaMessage()
		if err != nil {
			// Skip events that fail serialization
			continue
		}

		msg := kafka.Message{
			Key:   []byte(event.MatchID + ":" + event.UserID),
			Value: value,
			Headers: []kafka.Header{
				{Key: "engagement_type", Value: []byte(string(event.EngagementType))},
				{Key: "event_id", Value: []byte(event.EventID.String())},
				{Key: "user_id", Value: []byte(event.UserID)},
			},
			Time: event.Timestamp,
		}

		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil
	}

	err := p.mock.WriteMessages(ctx, messages...)
	if err != nil {
		return errors.New("failed to produce engagement batch: " + err.Error())
	}

	return nil
}

// Helper functions to create test events
func createTestEvent(matchID string, eventType domain.EventType) *domain.Event {
	return &domain.Event{
		EventID:   uuid.New(),
		MatchID:   matchID,
		EventType: eventType,
		Timestamp: time.Now().UTC(),
		TeamID:    1,
		PlayerID:  "player-1",
		Metadata:  map[string]interface{}{"test": true},
	}
}

func createTestEngagementEvent(matchID, userID string, engagementType domain.EngagementType) *domain.EngagementEvent {
	return &domain.EngagementEvent{
		EventID:           uuid.New(),
		MatchID:           matchID,
		UserID:            userID,
		SessionID:         "session-123",
		EngagementType:    engagementType,
		EngagementSubtype: "cheer",
		GameMinute:        45,
		DeviceType:        domain.DeviceMobile,
		Platform:          "ios",
		CountryCode:       "US",
		Content:           "test content",
		Metadata:          map[string]interface{}{"test": true},
		Timestamp:         time.Now().UTC(),
	}
}

// ============================================
// EventProducer.Produce Tests
// ============================================

func TestEventProducer_Produce_Success(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	event := createTestEvent("match-123", domain.EventTypeGoal)
	ctx := context.Background()

	err := producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	msg := msgs[0]
	if string(msg.Key) != event.MatchID {
		t.Errorf("Message key = %q, want %q", string(msg.Key), event.MatchID)
	}

	// Verify headers
	if len(msg.Headers) != 2 {
		t.Errorf("Expected 2 headers, got %d", len(msg.Headers))
	}

	headerMap := make(map[string]string)
	for _, h := range msg.Headers {
		headerMap[h.Key] = string(h.Value)
	}

	if headerMap["event_type"] != string(event.EventType) {
		t.Errorf("Header event_type = %q, want %q", headerMap["event_type"], event.EventType)
	}
	if headerMap["event_id"] != event.EventID.String() {
		t.Errorf("Header event_id = %q, want %q", headerMap["event_id"], event.EventID.String())
	}
}

func TestEventProducer_Produce_NilEvent(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	err := producer.Produce(context.Background(), nil)
	if err == nil {
		t.Error("Produce() expected error for nil event, got nil")
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages for nil event, got %d", len(msgs))
	}
}

func TestEventProducer_Produce_WriteError(t *testing.T) {
	mock := newMockWriter("events")
	mock.writeErr = errors.New("kafka connection failed")
	producer := newTestableEventProducer(mock, nil)

	event := createTestEvent("match-123", domain.EventTypePass)
	err := producer.Produce(context.Background(), event)

	if err == nil {
		t.Error("Produce() expected error for write failure, got nil")
	}
	if !errors.Is(err, mock.writeErr) && err.Error() != "failed to produce message: kafka connection failed" {
		t.Errorf("Produce() error = %v, want to contain 'kafka connection failed'", err)
	}
}

func TestEventProducer_Produce_AllEventTypes(t *testing.T) {
	eventTypes := []domain.EventType{
		domain.EventTypePass,
		domain.EventTypeShot,
		domain.EventTypeGoal,
		domain.EventTypeFoul,
		domain.EventTypeYellowCard,
		domain.EventTypeRedCard,
		domain.EventTypeSubstitution,
		domain.EventTypeOffside,
		domain.EventTypeCorner,
		domain.EventTypeFreeKick,
		domain.EventTypeInterception,
	}

	for _, eventType := range eventTypes {
		t.Run(string(eventType), func(t *testing.T) {
			mock := newMockWriter("events")
			producer := newTestableEventProducer(mock, nil)

			event := createTestEvent("match-test", eventType)
			err := producer.Produce(context.Background(), event)

			if err != nil {
				t.Errorf("Produce() error for %s: %v", eventType, err)
			}

			msgs := mock.getWrittenMessages()
			if len(msgs) != 1 {
				t.Errorf("Expected 1 message for %s, got %d", eventType, len(msgs))
			}
		})
	}
}

func TestEventProducer_Produce_WithMetadata(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-with-metadata",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now().UTC(),
		TeamID:    2,
		PlayerID:  "player-10",
		Metadata: map[string]interface{}{
			"minute":        90,
			"assisted_by":   "player-7",
			"goal_type":     "header",
			"is_own_goal":   false,
			"distance_feet": 18.5,
		},
	}

	err := producer.Produce(context.Background(), event)
	if err != nil {
		t.Fatalf("Produce() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	// Verify the message value contains the serialized event
	if len(msgs[0].Value) == 0 {
		t.Error("Message value should not be empty")
	}
}

func TestEventProducer_Produce_ContextCancellation(t *testing.T) {
	mock := newMockWriter("events")
	mock.writeErr = context.Canceled
	producer := newTestableEventProducer(mock, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	event := createTestEvent("match-123", domain.EventTypePass)
	err := producer.Produce(ctx, event)

	if err == nil {
		t.Error("Produce() expected error for canceled context")
	}
}

// ============================================
// EventProducer.ProduceBatch Tests
// ============================================

func TestEventProducer_ProduceBatch_Success(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	events := []*domain.Event{
		createTestEvent("match-1", domain.EventTypePass),
		createTestEvent("match-1", domain.EventTypeShot),
		createTestEvent("match-1", domain.EventTypeGoal),
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(msgs))
	}
}

func TestEventProducer_ProduceBatch_EmptySlice(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	err := producer.ProduceBatch(context.Background(), []*domain.Event{})
	if err != nil {
		t.Errorf("ProduceBatch() unexpected error for empty slice: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages for empty slice, got %d", len(msgs))
	}
}

func TestEventProducer_ProduceBatch_NilSlice(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	err := producer.ProduceBatch(context.Background(), nil)
	if err != nil {
		t.Errorf("ProduceBatch() unexpected error for nil slice: %v", err)
	}
}

func TestEventProducer_ProduceBatch_WithNilEvents(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	events := []*domain.Event{
		createTestEvent("match-1", domain.EventTypePass),
		nil, // Should be skipped
		createTestEvent("match-1", domain.EventTypeGoal),
		nil, // Should be skipped
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 2 {
		t.Errorf("Expected 2 messages (skipping nil events), got %d", len(msgs))
	}
}

func TestEventProducer_ProduceBatch_AllNilEvents(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	events := []*domain.Event{nil, nil, nil}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Errorf("ProduceBatch() unexpected error for all nil events: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages for all nil events, got %d", len(msgs))
	}
}

func TestEventProducer_ProduceBatch_WriteError(t *testing.T) {
	mock := newMockWriter("events")
	mock.writeErr = errors.New("kafka batch write failed")
	producer := newTestableEventProducer(mock, nil)

	events := []*domain.Event{
		createTestEvent("match-1", domain.EventTypePass),
		createTestEvent("match-1", domain.EventTypeShot),
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err == nil {
		t.Error("ProduceBatch() expected error for write failure, got nil")
	}
}

func TestEventProducer_ProduceBatch_LargeBatch(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	// Create a large batch of events
	events := make([]*domain.Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = createTestEvent("match-large-batch", domain.EventTypePass)
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error for large batch: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 100 {
		t.Errorf("Expected 100 messages, got %d", len(msgs))
	}
}

func TestEventProducer_ProduceBatch_MultipleMatches(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	events := []*domain.Event{
		createTestEvent("match-A", domain.EventTypePass),
		createTestEvent("match-B", domain.EventTypeShot),
		createTestEvent("match-A", domain.EventTypeGoal),
		createTestEvent("match-C", domain.EventTypeFoul),
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 4 {
		t.Errorf("Expected 4 messages, got %d", len(msgs))
	}

	// Verify keys are correct
	expectedKeys := []string{"match-A", "match-B", "match-A", "match-C"}
	for i, msg := range msgs {
		if string(msg.Key) != expectedKeys[i] {
			t.Errorf("Message %d key = %q, want %q", i, string(msg.Key), expectedKeys[i])
		}
	}
}

// ============================================
// EngagementProducer.Produce Tests
// ============================================

func TestEngagementProducer_Produce_Success(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	event := createTestEngagementEvent("match-123", "user-456", domain.EngagementTypeReaction)
	ctx := context.Background()

	err := producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	msg := msgs[0]
	expectedKey := event.MatchID + ":" + event.UserID
	if string(msg.Key) != expectedKey {
		t.Errorf("Message key = %q, want %q", string(msg.Key), expectedKey)
	}

	// Verify headers
	if len(msg.Headers) != 3 {
		t.Errorf("Expected 3 headers, got %d", len(msg.Headers))
	}

	headerMap := make(map[string]string)
	for _, h := range msg.Headers {
		headerMap[h.Key] = string(h.Value)
	}

	if headerMap["engagement_type"] != string(event.EngagementType) {
		t.Errorf("Header engagement_type = %q, want %q", headerMap["engagement_type"], event.EngagementType)
	}
	if headerMap["user_id"] != event.UserID {
		t.Errorf("Header user_id = %q, want %q", headerMap["user_id"], event.UserID)
	}
	if headerMap["event_id"] != event.EventID.String() {
		t.Errorf("Header event_id = %q, want %q", headerMap["event_id"], event.EventID.String())
	}
}

func TestEngagementProducer_Produce_NilEvent(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	err := producer.Produce(context.Background(), nil)
	if err == nil {
		t.Error("Produce() expected error for nil event, got nil")
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages for nil event, got %d", len(msgs))
	}
}

func TestEngagementProducer_Produce_WriteError(t *testing.T) {
	mock := newMockWriter("engagements")
	mock.writeErr = errors.New("kafka connection failed")
	producer := newTestableEngagementProducer(mock, nil)

	event := createTestEngagementEvent("match-123", "user-456", domain.EngagementTypeComment)
	err := producer.Produce(context.Background(), event)

	if err == nil {
		t.Error("Produce() expected error for write failure, got nil")
	}
}

func TestEngagementProducer_Produce_AllEngagementTypes(t *testing.T) {
	engagementTypes := []domain.EngagementType{
		domain.EngagementTypeReaction,
		domain.EngagementTypeComment,
		domain.EngagementTypeVideoAction,
		domain.EngagementTypeShare,
		domain.EngagementTypePrediction,
		domain.EngagementTypeClick,
		domain.EngagementTypeSession,
	}

	for _, engagementType := range engagementTypes {
		t.Run(string(engagementType), func(t *testing.T) {
			mock := newMockWriter("engagements")
			producer := newTestableEngagementProducer(mock, nil)

			event := createTestEngagementEvent("match-test", "user-test", engagementType)
			err := producer.Produce(context.Background(), event)

			if err != nil {
				t.Errorf("Produce() error for %s: %v", engagementType, err)
			}

			msgs := mock.getWrittenMessages()
			if len(msgs) != 1 {
				t.Errorf("Expected 1 message for %s, got %d", engagementType, len(msgs))
			}
		})
	}
}

func TestEngagementProducer_Produce_WithRelatedGameEvent(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	relatedEventID := uuid.New()
	event := &domain.EngagementEvent{
		EventID:            uuid.New(),
		MatchID:            "match-with-related",
		UserID:             "user-123",
		SessionID:          "session-456",
		EngagementType:     domain.EngagementTypeReaction,
		EngagementSubtype:  "emoji_goal",
		RelatedGameEventID: &relatedEventID,
		GameMinute:         75,
		DeviceType:         domain.DeviceDesktop,
		Platform:           "web",
		CountryCode:        "UK",
		Content:            "",
		Metadata:           nil,
		Timestamp:          time.Now().UTC(),
	}

	err := producer.Produce(context.Background(), event)
	if err != nil {
		t.Fatalf("Produce() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
}

func TestEngagementProducer_Produce_AllDeviceTypes(t *testing.T) {
	deviceTypes := []domain.DeviceType{
		domain.DeviceMobile,
		domain.DeviceDesktop,
		domain.DeviceTablet,
		domain.DeviceTV,
		domain.DeviceUnknown,
	}

	for _, deviceType := range deviceTypes {
		t.Run(string(deviceType), func(t *testing.T) {
			mock := newMockWriter("engagements")
			producer := newTestableEngagementProducer(mock, nil)

			event := &domain.EngagementEvent{
				EventID:           uuid.New(),
				MatchID:           "match-device-test",
				UserID:            "user-device-test",
				SessionID:         "session-device",
				EngagementType:    domain.EngagementTypeSession,
				EngagementSubtype: "join",
				GameMinute:        0,
				DeviceType:        deviceType,
				Platform:          "test",
				CountryCode:       "US",
				Timestamp:         time.Now().UTC(),
			}

			err := producer.Produce(context.Background(), event)
			if err != nil {
				t.Errorf("Produce() error for device %s: %v", deviceType, err)
			}
		})
	}
}

// ============================================
// EngagementProducer.ProduceBatch Tests
// ============================================

func TestEngagementProducer_ProduceBatch_Success(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	events := []*domain.EngagementEvent{
		createTestEngagementEvent("match-1", "user-1", domain.EngagementTypeReaction),
		createTestEngagementEvent("match-1", "user-2", domain.EngagementTypeComment),
		createTestEngagementEvent("match-1", "user-3", domain.EngagementTypeShare),
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(msgs))
	}
}

func TestEngagementProducer_ProduceBatch_EmptySlice(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	err := producer.ProduceBatch(context.Background(), []*domain.EngagementEvent{})
	if err != nil {
		t.Errorf("ProduceBatch() unexpected error for empty slice: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages for empty slice, got %d", len(msgs))
	}
}

func TestEngagementProducer_ProduceBatch_NilSlice(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	err := producer.ProduceBatch(context.Background(), nil)
	if err != nil {
		t.Errorf("ProduceBatch() unexpected error for nil slice: %v", err)
	}
}

func TestEngagementProducer_ProduceBatch_WithNilEvents(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	events := []*domain.EngagementEvent{
		createTestEngagementEvent("match-1", "user-1", domain.EngagementTypeReaction),
		nil,
		createTestEngagementEvent("match-1", "user-2", domain.EngagementTypeComment),
		nil,
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 2 {
		t.Errorf("Expected 2 messages (skipping nil events), got %d", len(msgs))
	}
}

func TestEngagementProducer_ProduceBatch_AllNilEvents(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	events := []*domain.EngagementEvent{nil, nil, nil}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Errorf("ProduceBatch() unexpected error for all nil events: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 0 {
		t.Errorf("Expected 0 messages for all nil events, got %d", len(msgs))
	}
}

func TestEngagementProducer_ProduceBatch_WriteError(t *testing.T) {
	mock := newMockWriter("engagements")
	mock.writeErr = errors.New("kafka batch write failed")
	producer := newTestableEngagementProducer(mock, nil)

	events := []*domain.EngagementEvent{
		createTestEngagementEvent("match-1", "user-1", domain.EngagementTypeReaction),
		createTestEngagementEvent("match-1", "user-2", domain.EngagementTypeComment),
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err == nil {
		t.Error("ProduceBatch() expected error for write failure, got nil")
	}
}

func TestEngagementProducer_ProduceBatch_LargeBatch(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	// Create a large batch of engagement events
	events := make([]*domain.EngagementEvent, 500)
	for i := 0; i < 500; i++ {
		events[i] = createTestEngagementEvent("match-large-batch", "user-"+string(rune(i%26+'A')), domain.EngagementTypeReaction)
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error for large batch: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 500 {
		t.Errorf("Expected 500 messages, got %d", len(msgs))
	}
}

func TestEngagementProducer_ProduceBatch_MultipleUsersAndMatches(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	events := []*domain.EngagementEvent{
		createTestEngagementEvent("match-A", "user-1", domain.EngagementTypeReaction),
		createTestEngagementEvent("match-B", "user-2", domain.EngagementTypeComment),
		createTestEngagementEvent("match-A", "user-1", domain.EngagementTypeShare),
		createTestEngagementEvent("match-A", "user-3", domain.EngagementTypePrediction),
	}

	err := producer.ProduceBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("ProduceBatch() unexpected error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 4 {
		t.Errorf("Expected 4 messages, got %d", len(msgs))
	}

	// Verify keys are correct (matchID:userID format)
	expectedKeys := []string{"match-A:user-1", "match-B:user-2", "match-A:user-1", "match-A:user-3"}
	for i, msg := range msgs {
		if string(msg.Key) != expectedKeys[i] {
			t.Errorf("Message %d key = %q, want %q", i, string(msg.Key), expectedKeys[i])
		}
	}
}

// ============================================
// Factory Function Tests
// ============================================

func TestNewEventProducer(t *testing.T) {
	writer := NewWriter([]string{"localhost:9092"}, "test-events")
	defer writer.Close()

	// Test with nil logger (should use default)
	producer := NewEventProducer(writer, nil)
	if producer == nil {
		t.Fatal("NewEventProducer() returned nil")
	}
	if producer.writer != writer {
		t.Error("NewEventProducer() writer not set correctly")
	}

	// Test with custom logger
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	producer2 := NewEventProducer(writer, logger)
	if producer2 == nil {
		t.Fatal("NewEventProducer() with logger returned nil")
	}
}

func TestNewEngagementProducer(t *testing.T) {
	writer := NewEngagementWriter([]string{"localhost:9092"}, "test-engagements")
	defer writer.Close()

	// Test with nil logger (should use default)
	producer := NewEngagementProducer(writer, nil)
	if producer == nil {
		t.Fatal("NewEngagementProducer() returned nil")
	}
	if producer.writer != writer {
		t.Error("NewEngagementProducer() writer not set correctly")
	}

	// Test with custom logger
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	producer2 := NewEngagementProducer(writer, logger)
	if producer2 == nil {
		t.Fatal("NewEngagementProducer() with logger returned nil")
	}
}

func TestNewWriter(t *testing.T) {
	brokers := []string{"broker1:9092", "broker2:9092"}
	topic := "test-topic"

	writer := NewWriter(brokers, topic)
	defer writer.Close()

	if writer == nil {
		t.Fatal("NewWriter() returned nil")
	}
	if writer.Topic != topic {
		t.Errorf("Writer.Topic = %q, want %q", writer.Topic, topic)
	}
	if writer.BatchSize != 100 {
		t.Errorf("Writer.BatchSize = %d, want %d", writer.BatchSize, 100)
	}
	if writer.RequiredAcks != kafka.RequireAll {
		t.Errorf("Writer.RequiredAcks = %d, want %d", writer.RequiredAcks, kafka.RequireAll)
	}
	if writer.Async != false {
		t.Error("Writer.Async should be false")
	}
}

func TestNewEngagementWriter(t *testing.T) {
	brokers := []string{"broker1:9092", "broker2:9092"}
	topic := "test-engagements"

	writer := NewEngagementWriter(brokers, topic)
	defer writer.Close()

	if writer == nil {
		t.Fatal("NewEngagementWriter() returned nil")
	}
	if writer.Topic != topic {
		t.Errorf("Writer.Topic = %q, want %q", writer.Topic, topic)
	}
	if writer.BatchSize != 500 {
		t.Errorf("Writer.BatchSize = %d, want %d (higher for engagement volume)", writer.BatchSize, 500)
	}
	if writer.RequiredAcks != kafka.RequireOne {
		t.Errorf("Writer.RequiredAcks = %d, want %d (lower durability for engagement)", writer.RequiredAcks, kafka.RequireOne)
	}
}

func TestNewWriterWithConfig(t *testing.T) {
	cfg := WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "custom-topic",
		BatchSize:    50,
		BatchTimeout: 100 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		MaxAttempts:  5,
		Async:        true,
	}

	writer := NewWriterWithConfig(cfg)
	defer writer.Close()

	if writer == nil {
		t.Fatal("NewWriterWithConfig() returned nil")
	}
	if writer.Topic != cfg.Topic {
		t.Errorf("Writer.Topic = %q, want %q", writer.Topic, cfg.Topic)
	}
	if writer.BatchSize != cfg.BatchSize {
		t.Errorf("Writer.BatchSize = %d, want %d", writer.BatchSize, cfg.BatchSize)
	}
	if writer.BatchTimeout != cfg.BatchTimeout {
		t.Errorf("Writer.BatchTimeout = %v, want %v", writer.BatchTimeout, cfg.BatchTimeout)
	}
	if writer.WriteTimeout != cfg.WriteTimeout {
		t.Errorf("Writer.WriteTimeout = %v, want %v", writer.WriteTimeout, cfg.WriteTimeout)
	}
	if writer.MaxAttempts != cfg.MaxAttempts {
		t.Errorf("Writer.MaxAttempts = %d, want %d", writer.MaxAttempts, cfg.MaxAttempts)
	}
	if writer.Async != cfg.Async {
		t.Errorf("Writer.Async = %v, want %v", writer.Async, cfg.Async)
	}
}

func TestDefaultWriterConfig(t *testing.T) {
	brokers := []string{"localhost:9092", "localhost:9093"}
	topic := "default-topic"

	cfg := DefaultWriterConfig(brokers, topic)

	if len(cfg.Brokers) != 2 {
		t.Errorf("DefaultWriterConfig().Brokers length = %d, want %d", len(cfg.Brokers), 2)
	}
	if cfg.Topic != topic {
		t.Errorf("DefaultWriterConfig().Topic = %q, want %q", cfg.Topic, topic)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("DefaultWriterConfig().BatchSize = %d, want %d", cfg.BatchSize, 100)
	}
	if cfg.BatchTimeout != 10*time.Millisecond {
		t.Errorf("DefaultWriterConfig().BatchTimeout = %v, want %v", cfg.BatchTimeout, 10*time.Millisecond)
	}
	if cfg.WriteTimeout != 10*time.Second {
		t.Errorf("DefaultWriterConfig().WriteTimeout = %v, want %v", cfg.WriteTimeout, 10*time.Second)
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("DefaultWriterConfig().MaxAttempts = %d, want %d", cfg.MaxAttempts, 3)
	}
	if cfg.Async != false {
		t.Error("DefaultWriterConfig().Async should be false")
	}
}

// ============================================
// Close Method Tests
// ============================================

func TestEventProducer_Close(t *testing.T) {
	writer := NewWriter([]string{"localhost:9092"}, "test-events")
	producer := NewEventProducer(writer, nil)

	err := producer.Close()
	if err != nil {
		t.Errorf("Close() unexpected error: %v", err)
	}
}

func TestEventProducer_Close_NilWriter(t *testing.T) {
	producer := &EventProducer{
		writer: nil,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err := producer.Close()
	if err != nil {
		t.Errorf("Close() with nil writer should return nil, got: %v", err)
	}
}

func TestEngagementProducer_Close(t *testing.T) {
	writer := NewEngagementWriter([]string{"localhost:9092"}, "test-engagements")
	producer := NewEngagementProducer(writer, nil)

	err := producer.Close()
	if err != nil {
		t.Errorf("Close() unexpected error: %v", err)
	}
}

func TestEngagementProducer_Close_NilWriter(t *testing.T) {
	producer := &EngagementProducer{
		writer: nil,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err := producer.Close()
	if err != nil {
		t.Errorf("Close() with nil writer should return nil, got: %v", err)
	}
}

// ============================================
// Message Key and Header Verification Tests
// ============================================

func TestEventProducer_MessageKey_PartitionOrdering(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	// All events from same match should have same key for partition ordering
	matchID := "match-partition-test"
	events := []*domain.Event{
		createTestEvent(matchID, domain.EventTypePass),
		createTestEvent(matchID, domain.EventTypeShot),
		createTestEvent(matchID, domain.EventTypeGoal),
	}

	for _, event := range events {
		err := producer.Produce(context.Background(), event)
		if err != nil {
			t.Fatalf("Produce() error: %v", err)
		}
	}

	msgs := mock.getWrittenMessages()
	for i, msg := range msgs {
		if string(msg.Key) != matchID {
			t.Errorf("Message %d key = %q, want %q (for partition ordering)", i, string(msg.Key), matchID)
		}
	}
}

func TestEngagementProducer_MessageKey_UserPartitioning(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	// Key should be matchID:userID for user-based partitioning
	matchID := "match-user-test"
	userID := "user-specific"

	event := createTestEngagementEvent(matchID, userID, domain.EngagementTypeReaction)
	err := producer.Produce(context.Background(), event)
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	expectedKey := matchID + ":" + userID
	if string(msgs[0].Key) != expectedKey {
		t.Errorf("Message key = %q, want %q", string(msgs[0].Key), expectedKey)
	}
}

// ============================================
// Timestamp Preservation Tests
// ============================================

func TestEventProducer_TimestampPreservation(t *testing.T) {
	mock := newMockWriter("events")
	producer := newTestableEventProducer(mock, nil)

	originalTimestamp := time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC)
	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-timestamp-test",
		EventType: domain.EventTypeGoal,
		Timestamp: originalTimestamp,
		TeamID:    1,
		PlayerID:  "player-1",
	}

	err := producer.Produce(context.Background(), event)
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	if !msgs[0].Time.Equal(originalTimestamp) {
		t.Errorf("Message timestamp = %v, want %v", msgs[0].Time, originalTimestamp)
	}
}

func TestEngagementProducer_TimestampPreservation(t *testing.T) {
	mock := newMockWriter("engagements")
	producer := newTestableEngagementProducer(mock, nil)

	originalTimestamp := time.Date(2024, 6, 15, 14, 30, 45, 123456789, time.UTC)
	event := &domain.EngagementEvent{
		EventID:           uuid.New(),
		MatchID:           "match-timestamp-test",
		UserID:            "user-timestamp",
		SessionID:         "session-123",
		EngagementType:    domain.EngagementTypeReaction,
		EngagementSubtype: "cheer",
		GameMinute:        45,
		DeviceType:        domain.DeviceMobile,
		Platform:          "ios",
		CountryCode:       "US",
		Timestamp:         originalTimestamp,
	}

	err := producer.Produce(context.Background(), event)
	if err != nil {
		t.Fatalf("Produce() error: %v", err)
	}

	msgs := mock.getWrittenMessages()
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	if !msgs[0].Time.Equal(originalTimestamp) {
		t.Errorf("Message timestamp = %v, want %v", msgs[0].Time, originalTimestamp)
	}
}
