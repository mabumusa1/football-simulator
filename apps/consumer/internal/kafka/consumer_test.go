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
	if consumer.flushInterval != 5*time.Second {
		t.Errorf("expected default flushInterval 5s, got %v", consumer.flushInterval)
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
	if consumer.flushInterval != 5*time.Second {
		t.Errorf("expected default flushInterval 5s for zero value, got %v", consumer.flushInterval)
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
	if consumer.flushInterval != 5*time.Second {
		t.Errorf("expected default flushInterval 5s for negative value, got %v", consumer.flushInterval)
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
		select {
		case <-consumer.done:
			return
		}
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
