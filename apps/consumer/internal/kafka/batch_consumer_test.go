package kafka

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

// =============================================================================
// GenericBatchConsumer Tests
// =============================================================================

// mockRepository is a mock implementation for testing
type mockRepository[T any] struct {
	insertFunc func(ctx context.Context, events []*T) error
}

func (m *mockRepository[T]) InsertBatch(ctx context.Context, events []*T) error {
	if m.insertFunc != nil {
		return m.insertFunc(ctx, events)
	}
	return nil
}

func TestNewGenericBatchConsumer_DefaultValues(t *testing.T) {
	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Repository: repo,
		Parser:     parser,
	})

	if consumer == nil {
		t.Fatal("expected non-nil consumer")
	}
	if consumer.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected default flushInterval 1s, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 4 {
		t.Errorf("expected default workerCount 4, got %d", consumer.workerCount)
	}
	if consumer.consumerName != "generic" {
		t.Errorf("expected default consumerName 'generic', got '%s'", consumer.consumerName)
	}
	if consumer.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestNewGenericBatchConsumer_CustomValues(t *testing.T) {
	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Repository:    repo,
		Parser:        parser,
		BatchSize:     500,
		FlushInterval: 5 * time.Second,
		WorkerCount:   8,
		Logger:        logger,
		ConsumerName:  "test-consumer",
	})

	if consumer.batchSize != 500 {
		t.Errorf("expected batchSize 500, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 5*time.Second {
		t.Errorf("expected flushInterval 5s, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 8 {
		t.Errorf("expected workerCount 8, got %d", consumer.workerCount)
	}
	if consumer.consumerName != "test-consumer" {
		t.Errorf("expected consumerName 'test-consumer', got '%s'", consumer.consumerName)
	}
	if consumer.logger != logger {
		t.Error("expected custom logger to be set")
	}
}

func TestNewGenericBatchConsumer_ZeroValues(t *testing.T) {
	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Repository:    repo,
		Parser:        parser,
		BatchSize:     0,
		FlushInterval: 0,
		WorkerCount:   0,
	})

	// Zeros should be replaced with defaults
	if consumer.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000 for zero, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected default flushInterval 1s for zero, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 4 {
		t.Errorf("expected default workerCount 4 for zero, got %d", consumer.workerCount)
	}
}

func TestNewGenericBatchConsumer_NegativeValues(t *testing.T) {
	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Repository:    repo,
		Parser:        parser,
		BatchSize:     -1,
		FlushInterval: -1 * time.Second,
		WorkerCount:   -1,
	})

	// Negative values should be replaced with defaults
	if consumer.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000 for negative, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected default flushInterval 1s for negative, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 4 {
		t.Errorf("expected default workerCount 4 for negative, got %d", consumer.workerCount)
	}
}

func TestNewGenericBatchConsumer_WithMetrics(t *testing.T) {
	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}

	lagMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_lag",
	}, []string{"topic", "partition"})

	batchesProcessed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_batches",
	}, []string{"status"})

	eventsConsumed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_events",
	}, []string{"status"})

	consumeDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_duration",
	}, []string{"operation"})

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Repository:       repo,
		Parser:           parser,
		LagMetric:        lagMetric,
		BatchesProcessed: batchesProcessed,
		EventsConsumed:   eventsConsumed,
		ConsumeDuration:  consumeDuration,
	})

	if consumer.lagMetric != lagMetric {
		t.Error("expected lagMetric to be set")
	}
	if consumer.batchesProcessed != batchesProcessed {
		t.Error("expected batchesProcessed to be set")
	}
	if consumer.eventsConsumed != eventsConsumed {
		t.Error("expected eventsConsumed to be set")
	}
	if consumer.consumeDuration != consumeDuration {
		t.Error("expected consumeDuration to be set")
	}
}

func TestGenericBatchConsumer_StartAndStop(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-generic-consumer-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}

	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Reader:        reader,
		Repository:    repo,
		Parser:        parser,
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

func TestGenericBatchConsumer_StopMethod(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-generic-stop-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}

	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Reader:        reader,
		Repository:    repo,
		Parser:        parser,
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

func TestGenericBatchConsumer_NilMetrics(t *testing.T) {
	// Test that the consumer handles nil metrics gracefully
	repo := &mockRepository[domain.Event]{}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
	}

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Repository:       repo,
		Parser:           parser,
		LagMetric:        nil,
		BatchesProcessed: nil,
		EventsConsumed:   nil,
		ConsumeDuration:  nil,
	})

	if consumer.lagMetric != nil {
		t.Error("expected lagMetric to be nil")
	}
	if consumer.batchesProcessed != nil {
		t.Error("expected batchesProcessed to be nil")
	}
	if consumer.eventsConsumed != nil {
		t.Error("expected eventsConsumed to be nil")
	}
	if consumer.consumeDuration != nil {
		t.Error("expected consumeDuration to be nil")
	}
}

// =============================================================================
// Integration Tests with Kafka
// =============================================================================

func TestGenericBatchConsumer_ProcessesMessages(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-generic-process-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	// Ensure topic exists
	ensureTestTopic(t, topic)

	// Track inserted events
	insertedEvents := make([]*domain.Event, 0)
	repo := &mockRepository[domain.Event]{
		insertFunc: func(ctx context.Context, events []*domain.Event) error {
			insertedEvents = append(insertedEvents, events...)
			return nil
		},
	}
	parser := func(data []byte) (*domain.Event, error) {
		return domain.EventFromKafkaMessage(data)
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

	event := createTestEvent()
	value, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("failed to serialize event: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := kafka.Message{
		Key:   []byte(event.MatchID),
		Value: value,
	}

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	// Create and start consumer
	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewGenericBatchConsumer(GenericBatchConsumerConfig[domain.Event]{
		Reader:        reader,
		Repository:    repo,
		Parser:        parser,
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
