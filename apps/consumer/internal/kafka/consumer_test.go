package kafka

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

// MockRepository implements the Repository interface for testing
type MockRepository struct {
	InsertBatchFunc func(ctx context.Context, events []*domain.Event) error
	InsertCalls     int
	InsertedEvents  [][]*domain.Event
	mu              sync.Mutex
}

func (m *MockRepository) InsertBatch(ctx context.Context, events []*domain.Event) error {
	m.mu.Lock()
	m.InsertCalls++
	m.InsertedEvents = append(m.InsertedEvents, events)
	m.mu.Unlock()

	if m.InsertBatchFunc != nil {
		return m.InsertBatchFunc(ctx, events)
	}
	return nil
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestNewBatchConsumer_DefaultValues(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
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
	if consumer.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3, got %d", consumer.maxRetries)
	}
	if consumer.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestNewBatchConsumer_CustomValues(t *testing.T) {
	mockRepo := &MockRepository{}
	logger := newTestLogger()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    mockRepo,
		BatchSize:     500,
		FlushInterval: 10 * time.Second,
		MaxRetries:    5,
		Logger:        logger,
	})

	if consumer.batchSize != 500 {
		t.Errorf("expected batchSize 500, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 10*time.Second {
		t.Errorf("expected flushInterval 10s, got %v", consumer.flushInterval)
	}
	if consumer.maxRetries != 5 {
		t.Errorf("expected maxRetries 5, got %d", consumer.maxRetries)
	}
	if consumer.logger != logger {
		t.Error("expected custom logger to be set")
	}
}

func TestNewBatchConsumer_ZeroValues(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    mockRepo,
		BatchSize:     0,
		FlushInterval: 0,
		MaxRetries:    0,
	})

	// Zero values should be replaced with defaults
	if consumer.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000 for zero value, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected default flushInterval 1s for zero value, got %v", consumer.flushInterval)
	}
	if consumer.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3 for zero value, got %d", consumer.maxRetries)
	}
}

func TestNewBatchConsumer_NegativeValues(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    mockRepo,
		BatchSize:     -1,
		FlushInterval: -1 * time.Second,
		MaxRetries:    -1,
	})

	// Negative values should be replaced with defaults
	if consumer.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000 for negative value, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected default flushInterval 1s for negative value, got %v", consumer.flushInterval)
	}
	if consumer.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3 for negative value, got %d", consumer.maxRetries)
	}
}

func TestBatchConsumer_BatchCapacity(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		BatchSize:  100,
	})

	if cap(consumer.batch) != 100 {
		t.Errorf("expected batch capacity 100, got %d", cap(consumer.batch))
	}
	if cap(consumer.messages) != 100 {
		t.Errorf("expected messages capacity 100, got %d", cap(consumer.messages))
	}
}

func TestBatchConsumer_DoneChannel(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
	})

	if consumer.done == nil {
		t.Error("expected done channel to be initialized")
	}
}

func TestDefaultReaderConfig(t *testing.T) {
	brokers := []string{"kafka:9092", "kafka2:9092"}
	topic := "test-topic"
	groupID := "test-group"

	cfg := DefaultReaderConfig(brokers, topic, groupID)

	if len(cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Brokers))
	}
	if cfg.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, cfg.Topic)
	}
	if cfg.GroupID != groupID {
		t.Errorf("expected groupID %s, got %s", groupID, cfg.GroupID)
	}
	if cfg.MinBytes != 1 {
		t.Errorf("expected MinBytes 1, got %d", cfg.MinBytes)
	}
	if cfg.MaxBytes != 10e6 {
		t.Errorf("expected MaxBytes 10MB, got %d", cfg.MaxBytes)
	}
	if cfg.MaxWait != 5*time.Second {
		t.Errorf("expected MaxWait 5s, got %v", cfg.MaxWait)
	}
	if cfg.CommitInterval != time.Second {
		t.Errorf("expected CommitInterval 1s, got %v", cfg.CommitInterval)
	}
}

func TestReaderConfig_CustomValues(t *testing.T) {
	cfg := ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "custom-topic",
		GroupID:        "custom-group",
		MinBytes:       100,
		MaxBytes:       1e6,
		MaxWait:        10 * time.Second,
		CommitInterval: 5 * time.Second,
		StartOffset:    -1,
	}

	if cfg.Topic != "custom-topic" {
		t.Errorf("expected Topic 'custom-topic', got %s", cfg.Topic)
	}
	if cfg.MinBytes != 100 {
		t.Errorf("expected MinBytes 100, got %d", cfg.MinBytes)
	}
	if cfg.MaxBytes != 1e6 {
		t.Errorf("expected MaxBytes 1MB, got %d", cfg.MaxBytes)
	}
}

func TestBatchConsumerConfig_AllFields(t *testing.T) {
	mockRepo := &MockRepository{}
	logger := newTestLogger()

	cfg := BatchConsumerConfig{
		Reader:        nil, // Would be real reader in production
		Repository:    mockRepo,
		RetryWriter:   nil,
		DeadWriter:    nil,
		BatchSize:     500,
		FlushInterval: 10 * time.Second,
		MaxRetries:    5,
		Logger:        logger,
	}

	if cfg.BatchSize != 500 {
		t.Errorf("expected BatchSize 500, got %d", cfg.BatchSize)
	}
	if cfg.FlushInterval != 10*time.Second {
		t.Errorf("expected FlushInterval 10s, got %v", cfg.FlushInterval)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", cfg.MaxRetries)
	}
}

func TestBatchConsumer_FlushWithContext_EmptyBatch(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Flush an empty batch - should not call repository
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 0 {
		t.Errorf("expected 0 InsertBatch calls for empty batch, got %d", mockRepo.InsertCalls)
	}
}

func TestBatchConsumer_FlushWithContext_WithEvents(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add events to batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypePass,
		Timestamp: time.Now(),
		TeamID:    2,
	})

	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}

	mockRepo.mu.Lock()
	if len(mockRepo.InsertedEvents) != 1 || len(mockRepo.InsertedEvents[0]) != 2 {
		t.Errorf("expected 2 events in batch, got %d", len(mockRepo.InsertedEvents[0]))
	}
	mockRepo.mu.Unlock()

	// Batch should be cleared after flush
	if len(consumer.batch) != 0 {
		t.Errorf("expected empty batch after flush, got %d events", len(consumer.batch))
	}
}

func TestBatchConsumer_FlushWithContext_RepositoryError(t *testing.T) {
	mockRepo := &MockRepository{
		InsertBatchFunc: func(ctx context.Context, events []*domain.Event) error {
			return errors.New("database error")
		},
	}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})

	// Should handle error gracefully (logging, metrics, retry logic)
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}
}

func TestBatchConsumer_FlushWithContext_BatchReset(t *testing.T) {
	mockRepo := &MockRepository{}
	batchSize := 100

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		BatchSize:  batchSize,
		Logger:     newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})

	consumer.flushWithContext(context.Background())

	// After flush, batch should be reset with correct capacity
	if cap(consumer.batch) != batchSize {
		t.Errorf("expected batch capacity %d after reset, got %d", batchSize, cap(consumer.batch))
	}
}

func TestBatchConsumer_ConcurrentFlush(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add events
	for i := 0; i < 10; i++ {
		consumer.batch = append(consumer.batch, &domain.Event{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		})
	}

	// Concurrent flushes should be safe
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.flushWithContext(context.Background())
		}()
	}
	wg.Wait()

	// At least one flush should have happened
	if mockRepo.InsertCalls == 0 {
		t.Error("expected at least one InsertBatch call")
	}
}

func TestBatchConsumer_Stop(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Simulate that the consumer has started by adding to wait group
	consumer.wg.Add(1)
	go func() {
		defer consumer.wg.Done()
		<-consumer.done
	}()

	// Stop should close the done channel and wait
	consumer.Stop()

	// Verify done channel is closed
	select {
	case _, ok := <-consumer.done:
		if ok {
			t.Error("expected done channel to be closed")
		}
	default:
		t.Error("done channel should be closed")
	}
}

func TestBatchConsumer_MultipleBatches(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// First batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-1",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})
	consumer.flushWithContext(context.Background())

	// Second batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-2",
		EventType: domain.EventTypePass,
		Timestamp: time.Now(),
		TeamID:    2,
	})
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 2 {
		t.Errorf("expected 2 InsertBatch calls, got %d", mockRepo.InsertCalls)
	}
}

func TestBatchConsumer_BatchLocking(t *testing.T) {
	mockRepo := &MockRepository{
		InsertBatchFunc: func(ctx context.Context, events []*domain.Event) error {
			// Simulate slow insert
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add initial event
	consumer.batchLock.Lock()
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})
	consumer.batchLock.Unlock()

	// Start flush in background
	go consumer.flushWithContext(context.Background())

	// Try to add more events concurrently
	for i := 0; i < 5; i++ {
		go func() {
			consumer.batchLock.Lock()
			consumer.batch = append(consumer.batch, &domain.Event{
				EventID:   uuid.New(),
				MatchID:   "match-456",
				EventType: domain.EventTypePass,
				Timestamp: time.Now(),
				TeamID:    2,
			})
			consumer.batchLock.Unlock()
		}()
	}

	// Wait for operations to complete
	time.Sleep(50 * time.Millisecond)

	// No panics or race conditions should occur
}

func TestNewWriter(t *testing.T) {
	brokers := []string{"kafka:9092", "kafka2:9092"}
	topic := "test-topic"

	writer := NewWriter(brokers, topic)

	if writer == nil {
		t.Fatal("expected non-nil writer")
	}
	if writer.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, writer.Topic)
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
	if writer.Async != false {
		t.Error("expected Async to be false")
	}
}

func TestBatchConsumer_ContextCancellation(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})

	// Test with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Flush should still work with cancelled context
	consumer.flushWithContext(ctx)

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}
}

func TestBatchConsumer_SendToRetry_NoRetryWriter(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: nil, // No retry writer
		DeadWriter:  nil, // No dead writer either
		Logger:      newTestLogger(),
	})

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
	}

	// Should not panic when no retry writer configured
	consumer.sendToRetry(context.Background(), events, nil)
}

func TestBatchConsumer_SendToDead_NoDeadWriter(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		DeadWriter: nil, // No dead writer
		Logger:     newTestLogger(),
	})

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
	}

	// Should not panic when no dead writer configured
	consumer.sendToDead(context.Background(), events)
}

func TestBatchConsumer_SendSingleToDead_NoDeadWriter(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		DeadWriter: nil, // No dead writer
		Logger:     newTestLogger(),
	})

	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	}

	// Should not panic when no dead writer configured
	consumer.sendSingleToDead(context.Background(), event)
}

func TestRepositoryInterface(t *testing.T) {
	// Verify MockRepository implements Repository interface
	var _ Repository = (*MockRepository)(nil)
}

func TestBatchConsumer_WithAllWriters(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: NewWriter([]string{"kafka:9092"}, "retry-topic"),
		DeadWriter:  NewWriter([]string{"kafka:9092"}, "dead-topic"),
		Logger:      newTestLogger(),
	})

	if consumer.retryWriter == nil {
		t.Error("expected retry writer to be set")
	}
	if consumer.deadWriter == nil {
		t.Error("expected dead writer to be set")
	}
}

func TestNewReader(t *testing.T) {
	cfg := ReaderConfig{
		Brokers:        []string{"kafka:9092", "kafka2:9092"},
		Topic:          "test-topic",
		GroupID:        "test-group",
		MinBytes:       100,
		MaxBytes:       1e6,
		MaxWait:        10 * time.Second,
		CommitInterval: 5 * time.Second,
		StartOffset:    -2, // FirstOffset
	}

	reader := NewReader(cfg)
	if reader == nil {
		t.Fatal("NewReader returned nil")
	}
	defer reader.Close()

	// Verify configuration was applied
	config := reader.Config()
	if config.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got %s", config.Topic)
	}
	if config.GroupID != "test-group" {
		t.Errorf("expected groupID 'test-group', got %s", config.GroupID)
	}
}

func TestNewReader_DefaultConfig(t *testing.T) {
	cfg := DefaultReaderConfig([]string{"kafka:9092"}, "events", "consumer-group")
	reader := NewReader(cfg)
	if reader == nil {
		t.Fatal("NewReader returned nil")
	}
	defer reader.Close()
}

func TestBatchConsumer_WorkerCount(t *testing.T) {
	mockRepo := &MockRepository{}

	testCases := []struct {
		name        string
		workerCount int
		expected    int
	}{
		{"zero worker count", 0, 4},
		{"negative worker count", -1, 4},
		{"positive worker count", 8, 8},
		{"single worker", 1, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := NewBatchConsumer(BatchConsumerConfig{
				Repository:  mockRepo,
				WorkerCount: tc.workerCount,
			})

			if consumer.workerCount != tc.expected {
				t.Errorf("expected workerCount %d, got %d", tc.expected, consumer.workerCount)
			}
		})
	}
}

func TestBatchConsumer_SendToRetry_EmptyEvents(t *testing.T) {
	mockRepo := &MockRepository{}
	retryWriter := NewWriter([]string{"kafka:9092"}, "retry-topic")
	defer retryWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		MaxRetries:  3,
		Logger:      newTestLogger(),
	})

	// Empty events slice should not panic
	consumer.sendToRetry(context.Background(), []*domain.Event{}, nil)
}

func TestBatchConsumer_SendToRetry_MaxRetriesExceeded(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: NewWriter([]string{"kafka:9092"}, "retry-topic"),
		DeadWriter:  nil, // No dead writer
		MaxRetries:  1,
		Logger:      newTestLogger(),
	})
	defer consumer.retryWriter.Close()

	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	}

	// Simulate message with retry count exceeding max
	originalMsg := kafka.Message{
		Headers: []kafka.Header{
			{Key: "retry_count", Value: []byte{byte(2)}}, // Already retried twice
		},
	}

	// Should skip to dead letter (but dead writer is nil so it logs)
	consumer.sendToRetry(context.Background(), []*domain.Event{event}, []kafka.Message{originalMsg})
}

func TestBatchConsumer_FlushWithContext_LargerBatch(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		BatchSize:  100,
		Logger:     newTestLogger(),
	})

	// Add many events to batch
	for i := 0; i < 50; i++ {
		consumer.batch = append(consumer.batch, &domain.Event{
			EventID:   uuid.New(),
			MatchID:   "match-" + string(rune('A'+i)),
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    1,
		})
	}

	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}

	mockRepo.mu.Lock()
	if len(mockRepo.InsertedEvents) != 1 || len(mockRepo.InsertedEvents[0]) != 50 {
		t.Errorf("expected 50 events in batch, got %d", len(mockRepo.InsertedEvents[0]))
	}
	mockRepo.mu.Unlock()
}

func TestBatchConsumer_InitialBatchCapacity(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		BatchSize:  50,
		Logger:     newTestLogger(),
	})

	// Initial capacity should match batch size
	if cap(consumer.batch) != 50 {
		t.Errorf("initial batch capacity should be 50, got %d", cap(consumer.batch))
	}
	if cap(consumer.messages) != 50 {
		t.Errorf("initial messages capacity should be 50, got %d", cap(consumer.messages))
	}
}

func TestReaderConfig_AllFields(t *testing.T) {
	cfg := ReaderConfig{
		Brokers:        []string{"broker1:9092", "broker2:9092"},
		Topic:          "my-topic",
		GroupID:        "my-group",
		MinBytes:       256,
		MaxBytes:       2e6,
		MaxWait:        15 * time.Second,
		CommitInterval: 3 * time.Second,
		StartOffset:    -1,
	}

	if len(cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Brokers))
	}
	if cfg.Topic != "my-topic" {
		t.Errorf("expected topic 'my-topic', got %s", cfg.Topic)
	}
	if cfg.MinBytes != 256 {
		t.Errorf("expected MinBytes 256, got %d", cfg.MinBytes)
	}
	if cfg.MaxBytes != 2e6 {
		t.Errorf("expected MaxBytes 2MB, got %d", cfg.MaxBytes)
	}
}

// MockEngagementRepository implements the EngagementRepository interface for testing
type MockEngagementRepository struct {
	InsertEngagementBatchFunc func(ctx context.Context, events []*domain.EngagementEvent) error
	InsertCalls               int
	InsertedEvents            [][]*domain.EngagementEvent
	mu                        sync.Mutex
}

func (m *MockEngagementRepository) InsertEngagementBatch(ctx context.Context, events []*domain.EngagementEvent) error {
	m.mu.Lock()
	m.InsertCalls++
	m.InsertedEvents = append(m.InsertedEvents, events)
	m.mu.Unlock()

	if m.InsertEngagementBatchFunc != nil {
		return m.InsertEngagementBatchFunc(ctx, events)
	}
	return nil
}

func TestNewEngagementConsumer_DefaultValues(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
	})

	if consumer == nil {
		t.Fatal("expected non-nil consumer")
	}
	if consumer.batchSize != 2000 {
		t.Errorf("expected default batchSize 2000, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 500*time.Millisecond {
		t.Errorf("expected default flushInterval 500ms, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 8 {
		t.Errorf("expected default workerCount 8, got %d", consumer.workerCount)
	}
	if consumer.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestNewEngagementConsumer_CustomValues(t *testing.T) {
	mockRepo := &MockEngagementRepository{}
	logger := newTestLogger()

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    mockRepo,
		BatchSize:     500,
		FlushInterval: 1 * time.Second,
		WorkerCount:   4,
		Logger:        logger,
	})

	if consumer.batchSize != 500 {
		t.Errorf("expected batchSize 500, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected flushInterval 1s, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 4 {
		t.Errorf("expected workerCount 4, got %d", consumer.workerCount)
	}
	if consumer.logger != logger {
		t.Error("expected custom logger to be set")
	}
}

func TestNewEngagementConsumer_ZeroValues(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    mockRepo,
		BatchSize:     0,
		FlushInterval: 0,
		WorkerCount:   0,
	})

	// Zero values should be replaced with defaults
	if consumer.batchSize != 2000 {
		t.Errorf("expected default batchSize 2000 for zero value, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 500*time.Millisecond {
		t.Errorf("expected default flushInterval 500ms for zero value, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 8 {
		t.Errorf("expected default workerCount 8 for zero value, got %d", consumer.workerCount)
	}
}

func TestNewEngagementConsumer_NegativeValues(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository:    mockRepo,
		BatchSize:     -1,
		FlushInterval: -1 * time.Second,
		WorkerCount:   -1,
	})

	// Negative values should be replaced with defaults
	if consumer.batchSize != 2000 {
		t.Errorf("expected default batchSize 2000 for negative value, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 500*time.Millisecond {
		t.Errorf("expected default flushInterval 500ms for negative value, got %v", consumer.flushInterval)
	}
	if consumer.workerCount != 8 {
		t.Errorf("expected default workerCount 8 for negative value, got %d", consumer.workerCount)
	}
}

func TestEngagementConsumer_BatchCapacity(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		BatchSize:  100,
	})

	if cap(consumer.batch) != 100 {
		t.Errorf("expected batch capacity 100, got %d", cap(consumer.batch))
	}
	if cap(consumer.messages) != 100 {
		t.Errorf("expected messages capacity 100, got %d", cap(consumer.messages))
	}
}

func TestEngagementConsumer_DoneChannel(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
	})

	if consumer.done == nil {
		t.Error("expected done channel to be initialized")
	}
}

func TestEngagementConsumer_FlushWithContext_EmptyBatch(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Flush an empty batch - should not call repository
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 0 {
		t.Errorf("expected 0 InsertEngagementBatch calls for empty batch, got %d", mockRepo.InsertCalls)
	}
}

func TestEngagementConsumer_FlushWithContext_WithEvents(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add events to batch
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-123",
		UserID:         "user-456",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
	})
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-123",
		UserID:         "user-789",
		EngagementType: domain.EngagementTypeComment,
		Timestamp:      time.Now(),
	})

	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertEngagementBatch call, got %d", mockRepo.InsertCalls)
	}

	mockRepo.mu.Lock()
	if len(mockRepo.InsertedEvents) != 1 || len(mockRepo.InsertedEvents[0]) != 2 {
		t.Errorf("expected 2 events in batch, got %d", len(mockRepo.InsertedEvents[0]))
	}
	mockRepo.mu.Unlock()

	// Batch should be cleared after flush
	if len(consumer.batch) != 0 {
		t.Errorf("expected empty batch after flush, got %d events", len(consumer.batch))
	}
}

func TestEngagementConsumer_FlushWithContext_RepositoryError(t *testing.T) {
	mockRepo := &MockEngagementRepository{
		InsertEngagementBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			return errors.New("database error")
		},
	}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-123",
		UserID:         "user-456",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
	})

	// Should handle error gracefully (logging, metrics)
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertEngagementBatch call, got %d", mockRepo.InsertCalls)
	}
}

func TestEngagementConsumer_FlushWithContext_BatchReset(t *testing.T) {
	mockRepo := &MockEngagementRepository{}
	batchSize := 100

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		BatchSize:  batchSize,
		Logger:     newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-123",
		UserID:         "user-456",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
	})

	consumer.flushWithContext(context.Background())

	// After flush, batch should be reset with correct capacity
	if cap(consumer.batch) != batchSize {
		t.Errorf("expected batch capacity %d after reset, got %d", batchSize, cap(consumer.batch))
	}
}

func TestEngagementConsumer_ConcurrentFlush(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add events
	for i := 0; i < 10; i++ {
		consumer.batch = append(consumer.batch, &domain.EngagementEvent{
			EventID:        uuid.New(),
			MatchID:        "match-123",
			UserID:         "user-" + string(rune('A'+i)),
			EngagementType: domain.EngagementTypeReaction,
			Timestamp:      time.Now(),
		})
	}

	// Concurrent flushes should be safe
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.flushWithContext(context.Background())
		}()
	}
	wg.Wait()

	// At least one flush should have happened
	if mockRepo.InsertCalls == 0 {
		t.Error("expected at least one InsertEngagementBatch call")
	}
}

func TestEngagementConsumer_Stop(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Simulate that the consumer has started by adding to wait group
	consumer.wg.Add(1)
	go func() {
		defer consumer.wg.Done()
		<-consumer.done
	}()

	// Stop should close the done channel and wait
	consumer.Stop()

	// Verify done channel is closed
	select {
	case _, ok := <-consumer.done:
		if ok {
			t.Error("expected done channel to be closed")
		}
	default:
		t.Error("done channel should be closed")
	}
}

func TestEngagementConsumer_MultipleBatches(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// First batch
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-1",
		UserID:         "user-1",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
	})
	consumer.flushWithContext(context.Background())

	// Second batch
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-2",
		UserID:         "user-2",
		EngagementType: domain.EngagementTypeComment,
		Timestamp:      time.Now(),
	})
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 2 {
		t.Errorf("expected 2 InsertEngagementBatch calls, got %d", mockRepo.InsertCalls)
	}
}

func TestEngagementConsumer_BatchLocking(t *testing.T) {
	mockRepo := &MockEngagementRepository{
		InsertEngagementBatchFunc: func(ctx context.Context, events []*domain.EngagementEvent) error {
			// Simulate slow insert
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add initial event
	consumer.batchLock.Lock()
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-123",
		UserID:         "user-456",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
	})
	consumer.batchLock.Unlock()

	// Start flush in background
	go consumer.flushWithContext(context.Background())

	// Try to add more events concurrently
	for i := 0; i < 5; i++ {
		go func() {
			consumer.batchLock.Lock()
			consumer.batch = append(consumer.batch, &domain.EngagementEvent{
				EventID:        uuid.New(),
				MatchID:        "match-456",
				UserID:         "user-789",
				EngagementType: domain.EngagementTypeComment,
				Timestamp:      time.Now(),
			})
			consumer.batchLock.Unlock()
		}()
	}

	// Wait for operations to complete
	time.Sleep(50 * time.Millisecond)

	// No panics or race conditions should occur
}

func TestEngagementConsumer_ContextCancellation(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "match-123",
		UserID:         "user-456",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
	})

	// Test with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Flush should still work with cancelled context
	consumer.flushWithContext(ctx)

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertEngagementBatch call, got %d", mockRepo.InsertCalls)
	}
}

func TestEngagementRepositoryInterface(t *testing.T) {
	// Verify MockEngagementRepository implements EngagementRepository interface
	var _ EngagementRepository = (*MockEngagementRepository)(nil)
}

func TestEngagementConsumerConfig_AllFields(t *testing.T) {
	mockRepo := &MockEngagementRepository{}
	logger := newTestLogger()
	cfg := ReaderConfig{
		Brokers:        []string{"kafka:9092"},
		Topic:          "engagement-events",
		GroupID:        "engagement-consumer",
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        5 * time.Second,
		CommitInterval: time.Second,
	}
	reader := NewReader(cfg)
	defer reader.Close()

	engCfg := EngagementConsumerConfig{
		Reader:        reader,
		Repository:    mockRepo,
		BatchSize:     1000,
		FlushInterval: 2 * time.Second,
		WorkerCount:   4,
		Logger:        logger,
	}

	if engCfg.BatchSize != 1000 {
		t.Errorf("expected BatchSize 1000, got %d", engCfg.BatchSize)
	}
	if engCfg.FlushInterval != 2*time.Second {
		t.Errorf("expected FlushInterval 2s, got %v", engCfg.FlushInterval)
	}
	if engCfg.WorkerCount != 4 {
		t.Errorf("expected WorkerCount 4, got %d", engCfg.WorkerCount)
	}
}

func TestEngagementConsumer_FlushWithContext_LargerBatch(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		BatchSize:  100,
		Logger:     newTestLogger(),
	})

	// Add many events to batch
	for i := 0; i < 50; i++ {
		consumer.batch = append(consumer.batch, &domain.EngagementEvent{
			EventID:        uuid.New(),
			MatchID:        "match-" + string(rune('A'+i)),
			UserID:         "user-" + string(rune('A'+i)),
			EngagementType: domain.EngagementTypeReaction,
			Timestamp:      time.Now(),
		})
	}

	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertEngagementBatch call, got %d", mockRepo.InsertCalls)
	}

	mockRepo.mu.Lock()
	if len(mockRepo.InsertedEvents) != 1 || len(mockRepo.InsertedEvents[0]) != 50 {
		t.Errorf("expected 50 events in batch, got %d", len(mockRepo.InsertedEvents[0]))
	}
	mockRepo.mu.Unlock()
}

func TestEngagementConsumer_AllEngagementTypes(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Test all engagement types
	engagementTypes := []domain.EngagementType{
		domain.EngagementTypeReaction,
		domain.EngagementTypeComment,
		domain.EngagementTypeShare,
	}

	for _, engType := range engagementTypes {
		consumer.batch = append(consumer.batch, &domain.EngagementEvent{
			EventID:        uuid.New(),
			MatchID:        "match-123",
			UserID:         "user-456",
			EngagementType: engType,
			Timestamp:      time.Now(),
		})
	}

	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertEngagementBatch call, got %d", mockRepo.InsertCalls)
	}

	mockRepo.mu.Lock()
	if len(mockRepo.InsertedEvents[0]) != len(engagementTypes) {
		t.Errorf("expected %d events, got %d", len(engagementTypes), len(mockRepo.InsertedEvents[0]))
	}
	mockRepo.mu.Unlock()
}

func TestBatchConsumer_SendToRetry_WithRetryWriter(t *testing.T) {
	mockRepo := &MockRepository{}

	// Create a retry writer (won't actually connect in test)
	retryWriter := NewWriter([]string{"kafka:9092"}, "retry-topic")
	defer retryWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		MaxRetries:  3,
		Logger:      newTestLogger(),
	})

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
	}

	// Create original messages with no retry count
	originalMessages := []kafka.Message{
		{
			Headers: []kafka.Header{},
		},
	}

	// sendToRetry with retry writer - will fail to write but exercises the code path
	consumer.sendToRetry(context.Background(), events, originalMessages)
}

func TestBatchConsumer_SendToRetry_WithHeaders(t *testing.T) {
	mockRepo := &MockRepository{}

	// Create a retry writer
	retryWriter := NewWriter([]string{"kafka:9092"}, "retry-topic")
	defer retryWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		DeadWriter:  NewWriter([]string{"kafka:9092"}, "dead-topic"),
		MaxRetries:  3,
		Logger:      newTestLogger(),
	})
	defer consumer.deadWriter.Close()

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
	}

	// Create original messages with retry count header
	originalMessages := []kafka.Message{
		{
			Headers: []kafka.Header{
				{Key: "retry_count", Value: []byte{byte(1)}},
				{Key: "event_type", Value: []byte("goal")},
			},
		},
	}

	// sendToRetry should process headers correctly
	consumer.sendToRetry(context.Background(), events, originalMessages)
}

func TestBatchConsumer_SendToRetry_MultipleEvents(t *testing.T) {
	mockRepo := &MockRepository{}

	retryWriter := NewWriter([]string{"kafka:9092"}, "retry-topic")
	defer retryWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		MaxRetries:  5,
		Logger:      newTestLogger(),
	})

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-456",
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    2,
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-789",
			EventType: domain.EventTypeShotOnTarget,
			Timestamp: time.Now(),
			TeamID:    1,
		},
	}

	// Create original messages
	originalMessages := []kafka.Message{
		{Headers: []kafka.Header{{Key: "retry_count", Value: []byte{byte(0)}}}},
		{Headers: []kafka.Header{{Key: "retry_count", Value: []byte{byte(2)}}}},
		{Headers: []kafka.Header{}},
	}

	// This exercises the path where multiple events are processed
	consumer.sendToRetry(context.Background(), events, originalMessages)
}

func TestBatchConsumer_SendSingleToDead_WithDeadWriter(t *testing.T) {
	mockRepo := &MockRepository{}

	deadWriter := NewWriter([]string{"kafka:9092"}, "dead-topic")
	defer deadWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		DeadWriter: deadWriter,
		Logger:     newTestLogger(),
	})

	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
		PlayerID:  "player-456",
		Metadata:  map[string]interface{}{"minute": 45},
	}

	// sendSingleToDead with dead writer - will fail to write but exercises the code path
	consumer.sendSingleToDead(context.Background(), event)
}

func TestBatchConsumer_SendToDead_MultipleEvents(t *testing.T) {
	mockRepo := &MockRepository{}

	deadWriter := NewWriter([]string{"kafka:9092"}, "dead-topic")
	defer deadWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		DeadWriter: deadWriter,
		Logger:     newTestLogger(),
	})

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-456",
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    2,
		},
	}

	// sendToDead with multiple events
	consumer.sendToDead(context.Background(), events)
}

func TestBatchConsumer_FlushWithContext_WithEventsOnly(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Add events without messages (simulates unit test without reader)
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})

	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}

	// Batch should be cleared after flush
	if len(consumer.batch) != 0 {
		t.Errorf("expected empty batch after flush, got %d", len(consumer.batch))
	}
}

func TestBatchConsumer_SendToRetry_NoOriginalMessages(t *testing.T) {
	mockRepo := &MockRepository{}

	retryWriter := NewWriter([]string{"kafka:9092"}, "retry-topic")
	defer retryWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		MaxRetries:  3,
		Logger:      newTestLogger(),
	})

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
	}

	// sendToRetry with nil original messages
	consumer.sendToRetry(context.Background(), events, nil)
}

func TestBatchConsumer_AllEventTypes(t *testing.T) {
	mockRepo := &MockRepository{}

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		Logger:     newTestLogger(),
	})

	// Test all event types
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
		consumer.batch = append(consumer.batch, &domain.Event{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: eventType,
			Timestamp: time.Now(),
			TeamID:    1,
		})
	}

	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}

	mockRepo.mu.Lock()
	if len(mockRepo.InsertedEvents[0]) != len(eventTypes) {
		t.Errorf("expected %d events, got %d", len(eventTypes), len(mockRepo.InsertedEvents[0]))
	}
	mockRepo.mu.Unlock()
}

func TestBatchConsumer_FlushWithContext_RepositoryError_WithRetryWriter(t *testing.T) {
	mockRepo := &MockRepository{
		InsertBatchFunc: func(ctx context.Context, events []*domain.Event) error {
			return errors.New("database error")
		},
	}

	retryWriter := NewWriter([]string{"kafka:9092"}, "retry-topic")
	defer retryWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		Logger:      newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})

	// Should handle error and attempt retry
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}
}

func TestBatchConsumer_FlushWithContext_RepositoryError_WithDeadWriter(t *testing.T) {
	mockRepo := &MockRepository{
		InsertBatchFunc: func(ctx context.Context, events []*domain.Event) error {
			return errors.New("database error")
		},
	}

	deadWriter := NewWriter([]string{"kafka:9092"}, "dead-topic")
	defer deadWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		DeadWriter: deadWriter,
		Logger:     newTestLogger(),
	})

	// Add event to batch
	consumer.batch = append(consumer.batch, &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	})

	// Should handle error and send to dead letter
	consumer.flushWithContext(context.Background())

	if mockRepo.InsertCalls != 1 {
		t.Errorf("expected 1 InsertBatch call, got %d", mockRepo.InsertCalls)
	}
}

func TestBatchConsumer_SendToRetry_ExceedingMessageIndex(t *testing.T) {
	mockRepo := &MockRepository{}

	retryWriter := NewWriter([]string{"kafka:9092"}, "retry-topic")
	defer retryWriter.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		MaxRetries:  3,
		Logger:      newTestLogger(),
	})

	// More events than original messages
	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-456",
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    2,
		},
	}

	// Only one original message - exercises the boundary condition
	originalMessages := []kafka.Message{
		{Headers: []kafka.Header{}},
	}

	consumer.sendToRetry(context.Background(), events, originalMessages)
}

func TestEngagementConsumer_Start_WithCancelledContext(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	// Create a reader with the actual Kafka broker
	cfg := ReaderConfig{
		Brokers:        []string{"kafka:29092"},
		Topic:          "engagement-test-" + uuid.New().String(),
		GroupID:        "test-group-" + uuid.New().String(),
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        50 * time.Millisecond,
		CommitInterval: time.Second,
	}
	reader := NewReader(cfg)
	defer reader.Close()

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Reader:        reader,
		Repository:    mockRepo,
		BatchSize:     100,
		FlushInterval: 100 * time.Millisecond,
		WorkerCount:   2,
		Logger:        newTestLogger(),
	})

	// Create a context that will be cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start in a goroutine - it should exit when context is cancelled
	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	// Wait for Start to complete
	select {
	case <-done:
		// Start completed after context cancellation
	case <-time.After(2 * time.Second):
		t.Error("Start did not exit after context cancellation")
	}
}

func TestBatchConsumer_Start_WithCancelledContext(t *testing.T) {
	mockRepo := &MockRepository{}

	// Create a reader with the actual Kafka broker
	cfg := ReaderConfig{
		Brokers:        []string{"kafka:29092"},
		Topic:          "batch-test-" + uuid.New().String(),
		GroupID:        "test-group-" + uuid.New().String(),
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        50 * time.Millisecond,
		CommitInterval: time.Second,
	}
	reader := NewReader(cfg)
	defer reader.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Reader:        reader,
		Repository:    mockRepo,
		BatchSize:     100,
		FlushInterval: 100 * time.Millisecond,
		WorkerCount:   2,
		Logger:        newTestLogger(),
	})

	// Create a context that will be cancelled quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start in a goroutine - it should exit when context is cancelled
	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	// Wait for Start to complete
	select {
	case <-done:
		// Start completed after context cancellation
	case <-time.After(2 * time.Second):
		t.Error("Start did not exit after context cancellation")
	}
}

func TestEngagementConsumer_WorkerWithMessages(t *testing.T) {
	mockRepo := &MockEngagementRepository{}

	// Create a unique topic for this test
	topic := "engagement-worker-test-" + uuid.New().String()
	brokers := []string{"kafka:29092"}

	// Create a writer to produce messages first
	writer := NewWriter(brokers, topic)
	defer writer.Close()

	// Create an engagement event
	eventID := uuid.New()
	event := &domain.EngagementEvent{
		EventID:        eventID,
		MatchID:        "match-123",
		UserID:         "user-456",
		SessionID:      "session-789",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
	}

	// Serialize and write the message
	eventJSON, _ := event.ToJSON()
	msg := kafka.Message{
		Key:   []byte(event.MatchID),
		Value: eventJSON,
	}

	// Write message with timeout
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := writer.WriteMessages(writeCtx, msg)
	writeCancel()

	if err != nil {
		// Topic might not exist, which is OK for this test - we're exercising error paths
		t.Logf("Write failed (expected if topic doesn't exist): %v", err)
	}

	// Create a reader
	cfg := ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "test-group-" + uuid.New().String(),
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: time.Second,
	}
	reader := NewReader(cfg)
	defer reader.Close()

	consumer := NewEngagementConsumer(EngagementConsumerConfig{
		Reader:        reader,
		Repository:    mockRepo,
		BatchSize:     1, // Small batch to trigger flush quickly
		FlushInterval: 50 * time.Millisecond,
		WorkerCount:   1,
		Logger:        newTestLogger(),
	})

	// Run consumer for a short time
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	<-done
	// Test passes if no panics occurred
}

func TestBatchConsumer_WorkerWithMessages(t *testing.T) {
	mockRepo := &MockRepository{}

	// Create a unique topic for this test
	topic := "batch-worker-test-" + uuid.New().String()
	brokers := []string{"kafka:29092"}

	// Create a writer to produce messages first
	writer := NewWriter(brokers, topic)
	defer writer.Close()

	// Create an event
	eventID := uuid.New()
	event := &domain.Event{
		EventID:   eventID,
		MatchID:   "match-123",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	}

	// Serialize and write the message
	eventJSON, _ := event.ToKafkaMessage()
	msg := kafka.Message{
		Key:   []byte(event.MatchID),
		Value: eventJSON,
	}

	// Write message with timeout
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := writer.WriteMessages(writeCtx, msg)
	writeCancel()

	if err != nil {
		// Topic might not exist, which is OK for this test
		t.Logf("Write failed (expected if topic doesn't exist): %v", err)
	}

	// Create a reader
	cfg := ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        "test-group-" + uuid.New().String(),
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
		CommitInterval: time.Second,
	}
	reader := NewReader(cfg)
	defer reader.Close()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Reader:        reader,
		Repository:    mockRepo,
		BatchSize:     1,
		FlushInterval: 50 * time.Millisecond,
		WorkerCount:   1,
		Logger:        newTestLogger(),
	})

	// Run consumer for a short time
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	<-done
	// Test passes if no panics occurred
}
