package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/api/internal/domain"
)

// Test infrastructure helpers
func getKafkaBroker() string {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}
	return broker
}

func createTestEvent() *domain.Event {
	return &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "test-match-" + uuid.New().String()[:8],
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
		PlayerID:  "test-player-" + uuid.New().String()[:8],
		Metadata:  map[string]interface{}{"minute": 45},
	}
}

func createTestEngagementEvent() *domain.EngagementEvent {
	return &domain.EngagementEvent{
		EventID:           uuid.New(),
		MatchID:           "test-match-" + uuid.New().String()[:8],
		UserID:            "test-user-" + uuid.New().String()[:8],
		SessionID:         "test-session-" + uuid.New().String()[:8],
		EngagementType:    domain.EngagementTypeReaction,
		EngagementSubtype: "cheer",
		GameMinute:        45,
		DeviceType:        "mobile",
		Platform:          "ios",
		CountryCode:       "US",
		Timestamp:         time.Now(),
		Metadata:          map[string]interface{}{"source": "test"},
	}
}

// =============================================================================
// NewWriter Tests
// =============================================================================

func TestNewWriter(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-topic-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	defer func() { _ = writer.Close() }()

	if writer == nil {
		t.Fatal("expected non-nil writer")
	}

	if writer.Topic != topic {
		t.Errorf("expected topic '%s', got '%s'", topic, writer.Topic)
	}

	if writer.BatchSize != 100 {
		t.Errorf("expected BatchSize 100, got %d", writer.BatchSize)
	}

	if writer.BatchTimeout != 10*time.Millisecond {
		t.Errorf("expected BatchTimeout 10ms, got %v", writer.BatchTimeout)
	}

	if writer.WriteTimeout != 10*time.Second {
		t.Errorf("expected WriteTimeout 10s, got %v", writer.WriteTimeout)
	}
}

func TestNewWriterWithConfig(t *testing.T) {
	cfg := WriterConfig{
		Brokers:      []string{getKafkaBroker()},
		Topic:        "custom-topic-" + uuid.New().String()[:8],
		BatchSize:    200,
		BatchTimeout: 20 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		MaxAttempts:  5,
		Async:        false,
	}

	writer := NewWriterWithConfig(cfg)
	defer func() { _ = writer.Close() }()

	if writer == nil {
		t.Fatal("expected non-nil writer")
	}

	if writer.Topic != cfg.Topic {
		t.Errorf("expected topic '%s', got '%s'", cfg.Topic, writer.Topic)
	}

	if writer.BatchSize != cfg.BatchSize {
		t.Errorf("expected BatchSize %d, got %d", cfg.BatchSize, writer.BatchSize)
	}

	if writer.BatchTimeout != cfg.BatchTimeout {
		t.Errorf("expected BatchTimeout %v, got %v", cfg.BatchTimeout, writer.BatchTimeout)
	}
}

func TestDefaultWriterConfig(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "default-topic-" + uuid.New().String()[:8]

	cfg := DefaultWriterConfig(brokers, topic)

	if len(cfg.Brokers) != 1 {
		t.Errorf("expected 1 broker, got %d", len(cfg.Brokers))
	}

	if cfg.Topic != topic {
		t.Errorf("expected topic '%s', got '%s'", topic, cfg.Topic)
	}

	if cfg.BatchSize != 100 {
		t.Errorf("expected BatchSize 100, got %d", cfg.BatchSize)
	}

	if cfg.BatchTimeout != 10*time.Millisecond {
		t.Errorf("expected BatchTimeout 10ms, got %v", cfg.BatchTimeout)
	}

	if cfg.MaxAttempts != 3 {
		t.Errorf("expected MaxAttempts 3, got %d", cfg.MaxAttempts)
	}
}

// =============================================================================
// NewEngagementWriter Tests
// =============================================================================

func TestNewEngagementWriter(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "engagement-topic-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	defer func() { _ = writer.Close() }()

	if writer == nil {
		t.Fatal("expected non-nil writer")
	}

	if writer.Topic != topic {
		t.Errorf("expected topic '%s', got '%s'", topic, writer.Topic)
	}

	if writer.BatchSize != 500 {
		t.Errorf("expected BatchSize 500, got %d", writer.BatchSize)
	}

	if writer.BatchTimeout != 50*time.Millisecond {
		t.Errorf("expected BatchTimeout 50ms, got %v", writer.BatchTimeout)
	}
}

// =============================================================================
// EventProducer Tests
// =============================================================================

func TestNewEventProducer(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	if producer == nil {
		t.Fatal("expected non-nil producer")
	}

	if producer.writer == nil {
		t.Error("expected writer to be set")
	}

	if producer.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestEventProducer_Produce(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	event := createTestEvent()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
}

func TestEventProducer_Produce_NilEvent(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Produce(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil event, got nil")
	}

	if err.Error() != "event cannot be nil" {
		t.Errorf("expected 'event cannot be nil', got '%s'", err.Error())
	}
}

func TestEventProducer_ProduceBatch(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	events := []*domain.Event{
		createTestEvent(),
		createTestEvent(),
		createTestEvent(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed: %v", err)
	}
}

func TestEventProducer_ProduceBatch_EmptySlice(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, []*domain.Event{})
	if err != nil {
		t.Fatalf("expected no error for empty slice, got: %v", err)
	}
}

func TestEventProducer_ProduceBatch_WithNilEvents(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	events := []*domain.Event{
		createTestEvent(),
		nil, // This should be skipped
		createTestEvent(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed: %v", err)
	}
}

func TestEventProducer_ProduceBatch_AllNil(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	events := []*domain.Event{nil, nil, nil}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("expected no error for all nil events, got: %v", err)
	}
}

func TestEventProducer_ProduceBatch_LargeBatch(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	events := make([]*domain.Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = createTestEvent()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed for large batch: %v", err)
	}
}

func TestEventProducer_Ping(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestEventProducer_Close(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)

	err := producer.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestEventProducer_Close_NilWriter(t *testing.T) {
	producer := &EventProducer{
		writer: nil,
		logger: nil,
	}

	err := producer.Close()
	if err != nil {
		t.Fatalf("expected no error for nil writer, got: %v", err)
	}
}

func TestEventProducer_AllEventTypes(t *testing.T) {
	eventTypes := []domain.EventType{
		domain.EventTypeGoal,
		domain.EventTypePass,
		domain.EventTypeShot,
		domain.EventTypeShotOnTarget,
		domain.EventTypeTackle,
		domain.EventTypeFoul,
		domain.EventTypeYellowCard,
		domain.EventTypeRedCard,
		domain.EventTypeCorner,
		domain.EventTypeFreeKick,
		domain.EventTypePenalty,
		domain.EventTypeSubstitution,
		domain.EventTypeInjury,
		domain.EventTypeOffside,
		domain.EventTypeKickoff,
		domain.EventTypeHalfTime,
		domain.EventTypeFullTime,
	}

	for _, eventType := range eventTypes {
		t.Run(string(eventType), func(t *testing.T) {
			brokers := []string{getKafkaBroker()}
			topic := "test-events-" + uuid.New().String()[:8]

			writer := NewWriter(brokers, topic)
			producer := NewEventProducer(writer, nil)
			defer func() { _ = producer.Close() }()

			event := &domain.Event{
				EventID:   uuid.New(),
				MatchID:   "match-123",
				EventType: eventType,
				Timestamp: time.Now(),
				TeamID:    1,
				PlayerID:  "player-456",
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			err := producer.Produce(ctx, event)
			if err != nil {
				t.Fatalf("Produce failed for event type %s: %v", eventType, err)
			}
		})
	}
}

// =============================================================================
// EngagementProducer Tests
// =============================================================================

func TestNewEngagementProducer(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	if producer == nil {
		t.Fatal("expected non-nil producer")
	}

	if producer.writer == nil {
		t.Error("expected writer to be set")
	}

	if producer.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestEngagementProducer_Produce(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	event := createTestEngagementEvent()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
}

func TestEngagementProducer_Produce_NilEvent(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Produce(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil event, got nil")
	}

	if err.Error() != "engagement event cannot be nil" {
		t.Errorf("expected 'engagement event cannot be nil', got '%s'", err.Error())
	}
}

func TestEngagementProducer_ProduceBatch(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	events := []*domain.EngagementEvent{
		createTestEngagementEvent(),
		createTestEngagementEvent(),
		createTestEngagementEvent(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed: %v", err)
	}
}

func TestEngagementProducer_ProduceBatch_EmptySlice(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, []*domain.EngagementEvent{})
	if err != nil {
		t.Fatalf("expected no error for empty slice, got: %v", err)
	}
}

func TestEngagementProducer_ProduceBatch_WithNilEvents(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	events := []*domain.EngagementEvent{
		createTestEngagementEvent(),
		nil, // This should be skipped
		createTestEngagementEvent(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed: %v", err)
	}
}

func TestEngagementProducer_ProduceBatch_LargeBatch(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	events := make([]*domain.EngagementEvent, 500)
	for i := 0; i < 500; i++ {
		events[i] = createTestEngagementEvent()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed for large batch: %v", err)
	}
}

func TestEngagementProducer_Ping(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestEngagementProducer_Close(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)

	err := producer.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestEngagementProducer_AllEngagementTypes(t *testing.T) {
	engagementTypes := []domain.EngagementType{
		domain.EngagementTypeReaction,
		domain.EngagementTypeComment,
		domain.EngagementTypeShare,
	}

	for _, engagementType := range engagementTypes {
		t.Run(string(engagementType), func(t *testing.T) {
			brokers := []string{getKafkaBroker()}
			topic := "test-engagements-" + uuid.New().String()[:8]

			writer := NewEngagementWriter(brokers, topic)
			producer := NewEngagementProducer(writer, nil)
			defer func() { _ = producer.Close() }()

			event := &domain.EngagementEvent{
				EventID:        uuid.New(),
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: engagementType,
				Timestamp:      time.Now(),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			err := producer.Produce(ctx, event)
			if err != nil {
				t.Fatalf("Produce failed for engagement type %s: %v", engagementType, err)
			}
		})
	}
}

// =============================================================================
// Message Validation Tests
// =============================================================================

func TestEventProducer_MessageContent(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	event := createTestEvent()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	// Create a consumer to read the message back
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "test-consumer-" + uuid.New().String()[:8],
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        5 * time.Second,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})
	defer func() { _ = reader.Close() }()

	readCtx, readCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer readCancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("Failed to read message: %v", err)
	}

	// Verify message key is the matchID
	if string(msg.Key) != event.MatchID {
		t.Errorf("expected key '%s', got '%s'", event.MatchID, string(msg.Key))
	}

	// Verify headers
	var eventTypeHeader, eventIDHeader string
	for _, h := range msg.Headers {
		switch h.Key {
		case "event_type":
			eventTypeHeader = string(h.Value)
		case "event_id":
			eventIDHeader = string(h.Value)
		}
	}

	if eventTypeHeader != string(event.EventType) {
		t.Errorf("expected event_type header '%s', got '%s'", event.EventType, eventTypeHeader)
	}

	if eventIDHeader != event.EventID.String() {
		t.Errorf("expected event_id header '%s', got '%s'", event.EventID.String(), eventIDHeader)
	}
}

func TestEngagementProducer_MessageContent(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	event := createTestEngagementEvent()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	// Create a consumer to read the message back
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "test-consumer-" + uuid.New().String()[:8],
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        5 * time.Second,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})
	defer func() { _ = reader.Close() }()

	readCtx, readCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer readCancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("Failed to read message: %v", err)
	}

	// Verify message key is matchID:userID
	expectedKey := event.MatchID + ":" + event.UserID
	if string(msg.Key) != expectedKey {
		t.Errorf("expected key '%s', got '%s'", expectedKey, string(msg.Key))
	}

	// Verify headers
	var engagementTypeHeader, eventIDHeader, userIDHeader string
	for _, h := range msg.Headers {
		switch h.Key {
		case "engagement_type":
			engagementTypeHeader = string(h.Value)
		case "event_id":
			eventIDHeader = string(h.Value)
		case "user_id":
			userIDHeader = string(h.Value)
		}
	}

	if engagementTypeHeader != string(event.EngagementType) {
		t.Errorf("expected engagement_type header '%s', got '%s'", event.EngagementType, engagementTypeHeader)
	}

	if eventIDHeader != event.EventID.String() {
		t.Errorf("expected event_id header '%s', got '%s'", event.EventID.String(), eventIDHeader)
	}

	if userIDHeader != event.UserID {
		t.Errorf("expected user_id header '%s', got '%s'", event.UserID, userIDHeader)
	}
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func TestEventProducer_ConcurrentProduce(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter(brokers, topic)
	producer := NewEventProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Produce events concurrently
	errChan := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			event := createTestEvent()
			errChan <- producer.Produce(ctx, event)
		}()
	}

	// Collect results
	for i := 0; i < 10; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent produce failed: %v", err)
		}
	}
}

func TestEngagementProducer_ConcurrentProduce(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter(brokers, topic)
	producer := NewEngagementProducer(writer, nil)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Produce events concurrently
	errChan := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			event := createTestEngagementEvent()
			errChan <- producer.Produce(ctx, event)
		}()
	}

	// Collect results
	for i := 0; i < 10; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent produce failed: %v", err)
		}
	}
}
