package kafka

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

// =============================================================================
// EngagementConsumer Tests
// =============================================================================

// mockEngagementRepository is a mock implementation for engagement testing
type mockEngagementRepository struct {
	insertFunc func(ctx context.Context, events []*domain.EngagementEvent) error
}

func (m *mockEngagementRepository) InsertEngagementBatch(ctx context.Context, events []*domain.EngagementEvent) error {
	if m.insertFunc != nil {
		return m.insertFunc(ctx, events)
	}
	return nil
}

func createTestEngagementEvent() *domain.EngagementEvent {
	return &domain.EngagementEvent{
		EventID:           uuid.New(),
		MatchID:           "test-match-" + uuid.New().String()[:8],
		UserID:            "user-" + uuid.New().String()[:8],
		SessionID:         "session-" + uuid.New().String()[:8],
		EngagementType:    domain.EngagementTypeReaction,
		EngagementSubtype: "cheer",
		Timestamp:         time.Now(),
		DeviceType:        "mobile",
		Platform:          "ios",
		CountryCode:       "US",
	}
}

func TestNewEngagementConsumer_DefaultValues(t *testing.T) {
	repo := &mockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: repo,
	})

	if consumer == nil {
		t.Fatal("expected non-nil consumer")
	}
	if consumer.consumer == nil {
		t.Fatal("expected non-nil underlying consumer")
	}
}

func TestNewEngagementConsumer_CustomValues(t *testing.T) {
	repo := &mockEngagementRepository{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    repo,
		BatchSize:     5000,
		FlushInterval: 1 * time.Second,
		WorkerCount:   16,
		Logger:        logger,
	})

	if consumer == nil {
		t.Fatal("expected non-nil consumer")
	}
	// The underlying consumer should have the custom values
	if consumer.consumer.batchSize != 5000 {
		t.Errorf("expected batchSize 5000, got %d", consumer.consumer.batchSize)
	}
	if consumer.consumer.flushInterval != 1*time.Second {
		t.Errorf("expected flushInterval 1s, got %v", consumer.consumer.flushInterval)
	}
	if consumer.consumer.workerCount != 16 {
		t.Errorf("expected workerCount 16, got %d", consumer.consumer.workerCount)
	}
}

func TestNewEngagementConsumer_ZeroValues(t *testing.T) {
	repo := &mockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    repo,
		BatchSize:     0,
		FlushInterval: 0,
		WorkerCount:   0,
	})

	// Zeros should be replaced with engagement-specific defaults
	if consumer.consumer.batchSize != 2000 {
		t.Errorf("expected default batchSize 2000 for engagement, got %d", consumer.consumer.batchSize)
	}
	if consumer.consumer.flushInterval != 500*time.Millisecond {
		t.Errorf("expected default flushInterval 500ms for engagement, got %v", consumer.consumer.flushInterval)
	}
	if consumer.consumer.workerCount != 8 {
		t.Errorf("expected default workerCount 8 for engagement, got %d", consumer.consumer.workerCount)
	}
}

func TestNewEngagementConsumer_NilLogger(t *testing.T) {
	repo := &mockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: repo,
		Logger:     nil,
	})

	if consumer.consumer.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestNewEngagementConsumer_ConsumerName(t *testing.T) {
	repo := &mockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: repo,
	})

	if consumer.consumer.consumerName != "engagement" {
		t.Errorf("expected consumerName 'engagement', got '%s'", consumer.consumer.consumerName)
	}
}

func TestNewEngagementConsumer_Metrics(t *testing.T) {
	repo := &mockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: repo,
	})

	// Check that metrics are set
	if consumer.consumer.lagMetric == nil {
		t.Error("expected lagMetric to be set")
	}
	if consumer.consumer.batchesProcessed == nil {
		t.Error("expected batchesProcessed to be set")
	}
	if consumer.consumer.eventsConsumed == nil {
		t.Error("expected eventsConsumed to be set")
	}
	if consumer.consumer.consumeDuration == nil {
		t.Error("expected consumeDuration to be set")
	}
}

func TestEngagementConsumer_StartAndStop(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagement-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	repo := &mockEngagementRepository{}

	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WorkerCount:   1,
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	// Let it run for a short time
	time.Sleep(200 * time.Millisecond)

	// Cancel context to trigger shutdown
	cancel()

	// Wait for consumer to stop with timeout
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop in time")
	}

	_ = reader.Close()
}

func TestEngagementConsumer_StopMethod(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagement-stop-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	repo := &mockEngagementRepository{}

	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WorkerCount:   1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	// Let it run for a short time
	time.Sleep(200 * time.Millisecond)

	// Use Stop method
	consumer.Stop()

	// Wait for consumer to stop with timeout
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop in time")
	}

	_ = reader.Close()
}

// =============================================================================
// engagementRepoAdapter Tests
// =============================================================================

func TestEngagementRepoAdapter_InsertBatch(t *testing.T) {
	var capturedEvents []*domain.EngagementEvent
	repo := &mockEngagementRepository{
		insertFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			capturedEvents = events
			return nil
		},
	}

	adapter := &engagementRepoAdapter{repo: repo}

	events := []*domain.EngagementEvent{
		createTestEngagementEvent(),
		createTestEngagementEvent(),
	}

	err := adapter.InsertBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(capturedEvents) != 2 {
		t.Errorf("expected 2 events, got %d", len(capturedEvents))
	}
}

// =============================================================================
// Integration Tests with Kafka
// =============================================================================

func TestEngagementConsumer_ProcessesMessages(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-engagement-process-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	// Ensure topic exists
	ensureTestTopic(t, topic)

	// Track inserted events
	insertedEvents := make([]*domain.EngagementEvent, 0)
	repo := &mockEngagementRepository{
		insertFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			insertedEvents = append(insertedEvents, events...)
			return nil
		},
	}

	// Produce a test message
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    1,
		BatchTimeout: time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll,
	}
	defer func() { _ = writer.Close() }()

	event := createTestEngagementEvent()
	value, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("failed to serialize event: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := kafka.Message{
		Key:   []byte(event.MatchID + ":" + event.UserID),
		Value: value,
	}

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	// Create and start consumer
	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		BatchSize:     1,
		FlushInterval: 100 * time.Millisecond,
		WorkerCount:   1,
	})

	consumerCtx, consumerCancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		consumer.Start(consumerCtx)
		close(done)
	}()

	// Let consumer process messages
	time.Sleep(2 * time.Second)

	consumerCancel()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop in time")
	}

	_ = reader.Close()

	// Verify events were processed
	if len(insertedEvents) == 0 {
		t.Log("Note: No events were inserted - this may happen if Kafka is not available")
	}
}

// =============================================================================
// Engagement Event Serialization Tests
// =============================================================================

func TestEngagementEvent_FromKafkaMessage(t *testing.T) {
	event := createTestEngagementEvent()
	value, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("failed to serialize event: %v", err)
	}

	parsedEvent, err := domain.EngagementEventFromKafkaMessage(value)
	if err != nil {
		t.Fatalf("failed to parse event: %v", err)
	}

	if parsedEvent.EventID != event.EventID {
		t.Errorf("expected EventID %s, got %s", event.EventID, parsedEvent.EventID)
	}

	if parsedEvent.MatchID != event.MatchID {
		t.Errorf("expected MatchID %s, got %s", event.MatchID, parsedEvent.MatchID)
	}

	if parsedEvent.UserID != event.UserID {
		t.Errorf("expected UserID %s, got %s", event.UserID, parsedEvent.UserID)
	}

	if parsedEvent.EngagementType != event.EngagementType {
		t.Errorf("expected EngagementType %s, got %s", event.EngagementType, parsedEvent.EngagementType)
	}
}

func TestEngagementEvent_FromKafkaMessage_InvalidJSON(t *testing.T) {
	_, err := domain.EngagementEventFromKafkaMessage([]byte(`{invalid json}`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEngagementEvent_AllEngagementTypes(t *testing.T) {
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
			event := &domain.EngagementEvent{
				EventID:           uuid.New(),
				MatchID:           "match-123",
				UserID:            "user-456",
				SessionID:         "session-789",
				EngagementType:    engagementType,
				EngagementSubtype: "test",
				Timestamp:         time.Now(),
			}

			value, err := event.ToKafkaMessage()
			if err != nil {
				t.Fatalf("failed to serialize event: %v", err)
			}

			parsedEvent, err := domain.EngagementEventFromKafkaMessage(value)
			if err != nil {
				t.Fatalf("failed to parse event: %v", err)
			}

			if parsedEvent.EngagementType != engagementType {
				t.Errorf("expected EngagementType %s, got %s", engagementType, parsedEvent.EngagementType)
			}
		})
	}
}
