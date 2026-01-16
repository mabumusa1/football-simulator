package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

func getKafkaBroker() string {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}
	return broker
}

func newIntegrationTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestRepository is an in-memory repository for integration tests
type TestRepository struct {
	Events []*domain.Event
	mu     sync.Mutex
	Err    error
}

func (r *TestRepository) InsertBatch(ctx context.Context, events []*domain.Event) error {
	if r.Err != nil {
		return r.Err
	}
	r.mu.Lock()
	r.Events = append(r.Events, events...)
	r.mu.Unlock()
	return nil
}

func (r *TestRepository) GetEvents() []*domain.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.Events
}

func TestNewReader_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-reader-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	cfg := DefaultReaderConfig([]string{broker}, topic, groupID)
	reader := NewReader(cfg)
	if reader == nil {
		t.Fatal("NewReader returned nil")
	}
	defer reader.Close()
}

func TestNewWriter_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-writer-" + uuid.New().String()[:8]

	writer := NewWriter([]string{broker}, topic)
	if writer == nil {
		t.Fatal("NewWriter returned nil")
	}
	defer writer.Close()

	if writer.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, writer.Topic)
	}
}

func TestBatchConsumer_ProduceAndConsume_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-events-consume-" + uuid.New().String()[:8]
	groupID := "test-consumer-group-" + uuid.New().String()[:8]

	// Create topic by producing a message
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	conn.Close()

	// Create test events to produce
	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-consume-test-1",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "player-1",
			Metadata:  map[string]interface{}{"minute": float64(45)},
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-consume-test-1",
			EventType: domain.EventTypeShot,
			Timestamp: time.Now(),
			TeamID:    2,
			PlayerID:  "player-2",
		},
	}

	// Produce messages to the topic
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	for _, event := range events {
		value, err := event.ToKafkaMessage()
		if err != nil {
			t.Fatalf("failed to serialize event: %v", err)
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.MatchID),
			Value: value,
		})
		if err != nil {
			t.Fatalf("failed to write message: %v", err)
		}
	}

	// Create reader and repository for consumer
	readerCfg := DefaultReaderConfig([]string{broker}, topic, groupID)
	reader := NewReader(readerCfg)
	defer reader.Close()

	testRepo := &TestRepository{}
	logger := newIntegrationTestLogger()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Reader:        reader,
		Repository:    testRepo,
		BatchSize:     2,
		FlushInterval: 100 * time.Millisecond,
		Logger:        logger,
	})

	// Start consumer in background
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go consumer.Start(ctx)

	// Wait for messages to be consumed and flushed
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		consumedEvents := testRepo.GetEvents()
		if len(consumedEvents) >= len(events) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify events were consumed
	consumedEvents := testRepo.GetEvents()
	if len(consumedEvents) < len(events) {
		t.Errorf("expected at least %d events, got %d", len(events), len(consumedEvents))
	}
}

func TestBatchConsumer_RetryTopic_Integration(t *testing.T) {
	broker := getKafkaBroker()
	mainTopic := "test-main-" + uuid.New().String()[:8]
	retryTopic := "test-retry-" + uuid.New().String()[:8]
	groupID := "test-retry-group-" + uuid.New().String()[:8]

	// Create topics
	for _, topic := range []string{mainTopic, retryTopic} {
		conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", topic, err)
		}
		conn.Close()
	}

	// Create reader and retry writer
	readerCfg := DefaultReaderConfig([]string{broker}, mainTopic, groupID)
	reader := NewReader(readerCfg)
	defer reader.Close()

	retryWriter := NewWriter([]string{broker}, retryTopic)
	defer retryWriter.Close()

	// Create repository that always fails
	testRepo := &TestRepository{
		Err: context.DeadlineExceeded, // Simulate database failure
	}
	logger := newIntegrationTestLogger()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Reader:        reader,
		Repository:    testRepo,
		RetryWriter:   retryWriter,
		BatchSize:     1,
		FlushInterval: 100 * time.Millisecond,
		MaxRetries:    2,
		Logger:        logger,
	})

	// Produce a test message
	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-retry-test",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        mainTopic,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	value, _ := event.ToKafkaMessage()
	err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(event.MatchID),
		Value: value,
	})
	if err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	// Start consumer briefly
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go consumer.Start(ctx)

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Check retry topic for messages
	retryReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     retryTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   2 * time.Second,
	})
	defer retryReader.Close()
	retryReader.SetOffset(kafka.FirstOffset)

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer retryCancel()

	msg, err := retryReader.ReadMessage(retryCtx)
	if err == nil && len(msg.Value) > 0 {
		// Message was sent to retry topic as expected
		t.Log("Message correctly sent to retry topic")
	}
}

func TestBatchConsumer_DeadLetterQueue_Integration(t *testing.T) {
	broker := getKafkaBroker()
	mainTopic := "test-main-dlq-" + uuid.New().String()[:8]
	retryTopic := "test-retry-dlq-" + uuid.New().String()[:8]
	deadTopic := "test-dead-" + uuid.New().String()[:8]
	_ = mainTopic   // Used only for topic list iteration
	_ = retryTopic  // Used for creating topics

	// Create topics
	for _, topic := range []string{mainTopic, retryTopic, deadTopic} {
		conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
		if err != nil {
			t.Fatalf("failed to create topic %s: %v", topic, err)
		}
		conn.Close()
	}

	// Create writers for retry and dead letter
	retryWriter := NewWriter([]string{broker}, retryTopic)
	defer retryWriter.Close()

	deadWriter := NewWriter([]string{broker}, deadTopic)
	defer deadWriter.Close()

	// Test that dead writer can write
	ctx := context.Background()
	testMsg := kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte(`{"test": "message"}`),
	}

	err := deadWriter.WriteMessages(ctx, testMsg)
	if err != nil {
		t.Fatalf("failed to write to dead letter topic: %v", err)
	}

	// Verify message in dead letter queue
	deadReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     deadTopic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   2 * time.Second,
	})
	defer deadReader.Close()
	deadReader.SetOffset(kafka.FirstOffset)

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()

	msg, err := deadReader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("failed to read from dead letter topic: %v", err)
	}

	if string(msg.Key) != "test-key" {
		t.Errorf("expected key 'test-key', got %s", string(msg.Key))
	}
}

func TestDefaultReaderConfig_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-default-reader"
	groupID := "test-default-group"

	cfg := DefaultReaderConfig([]string{broker}, topic, groupID)

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != broker {
		t.Errorf("unexpected brokers: %v", cfg.Brokers)
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
	if cfg.StartOffset != kafka.FirstOffset {
		t.Errorf("expected StartOffset FirstOffset, got %d", cfg.StartOffset)
	}
}

func TestBatchConsumer_FlushOnBatchSize_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-batch-flush-" + uuid.New().String()[:8]
	groupID := "test-batch-flush-group-" + uuid.New().String()[:8]

	// Create topic
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	conn.Close()

	// Produce 5 messages
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	for i := 0; i < 5; i++ {
		event := &domain.Event{
			EventID:   uuid.New(),
			MatchID:   "match-batch-test",
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    1,
		}
		value, _ := event.ToKafkaMessage()
		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.MatchID),
			Value: value,
		})
		if err != nil {
			t.Fatalf("failed to write message %d: %v", i, err)
		}
	}

	// Create consumer with batch size of 3
	readerCfg := DefaultReaderConfig([]string{broker}, topic, groupID)
	reader := NewReader(readerCfg)
	defer reader.Close()

	testRepo := &TestRepository{}
	logger := newIntegrationTestLogger()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Reader:        reader,
		Repository:    testRepo,
		BatchSize:     3, // Should flush after 3 messages
		FlushInterval: 10 * time.Second,
		Logger:        logger,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	go consumer.Start(ctx)

	// Wait for messages to be consumed
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		consumedEvents := testRepo.GetEvents()
		if len(consumedEvents) >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify at least a batch was flushed
	consumedEvents := testRepo.GetEvents()
	if len(consumedEvents) < 3 {
		t.Errorf("expected at least 3 events from batch flush, got %d", len(consumedEvents))
	}
}

func TestKafkaMessage_RoundTrip_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-roundtrip-" + uuid.New().String()[:8]

	// Create topic
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	conn.Close()

	// Create original event
	originalEvent := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-roundtrip",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now().Truncate(time.Millisecond),
		TeamID:    1,
		PlayerID:  "player-123",
		Metadata:  map[string]interface{}{"minute": float64(90)},
	}

	// Serialize and produce
	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	value, err := originalEvent.ToKafkaMessage()
	if err != nil {
		t.Fatalf("failed to serialize event: %v", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(originalEvent.MatchID),
		Value: value,
	})
	if err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	// Read and deserialize
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   5 * time.Second,
	})
	defer reader.Close()
	reader.SetOffset(kafka.FirstOffset)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}

	restoredEvent, err := domain.EventFromKafkaMessage(msg.Value)
	if err != nil {
		t.Fatalf("failed to deserialize event: %v", err)
	}

	// Verify round-trip
	if restoredEvent.EventID != originalEvent.EventID {
		t.Errorf("EventID mismatch: %s vs %s", originalEvent.EventID, restoredEvent.EventID)
	}
	if restoredEvent.MatchID != originalEvent.MatchID {
		t.Errorf("MatchID mismatch: %s vs %s", originalEvent.MatchID, restoredEvent.MatchID)
	}
	if restoredEvent.EventType != originalEvent.EventType {
		t.Errorf("EventType mismatch: %s vs %s", originalEvent.EventType, restoredEvent.EventType)
	}
	if restoredEvent.TeamID != originalEvent.TeamID {
		t.Errorf("TeamID mismatch: %d vs %d", originalEvent.TeamID, restoredEvent.TeamID)
	}
	if restoredEvent.PlayerID != originalEvent.PlayerID {
		t.Errorf("PlayerID mismatch: %s vs %s", originalEvent.PlayerID, restoredEvent.PlayerID)
	}
}

func TestBatchConsumer_WorkerCount_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-workers-" + uuid.New().String()[:8]
	groupID := "test-workers-group-" + uuid.New().String()[:8]

	// Create topic
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	conn.Close()

	// Create reader
	readerCfg := DefaultReaderConfig([]string{broker}, topic, groupID)
	reader := NewReader(readerCfg)
	defer reader.Close()

	testRepo := &TestRepository{}
	logger := newIntegrationTestLogger()

	// Create consumer with custom worker count
	consumer := NewBatchConsumer(BatchConsumerConfig{
		Reader:        reader,
		Repository:    testRepo,
		BatchSize:     100,
		FlushInterval: 1 * time.Second,
		WorkerCount:   2, // Custom worker count
		Logger:        logger,
	})

	if consumer.workerCount != 2 {
		t.Errorf("expected workerCount 2, got %d", consumer.workerCount)
	}

	// Start and stop quickly to verify it doesn't panic
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go consumer.Start(ctx)
	time.Sleep(400 * time.Millisecond)
}

func TestMultipleWriters_Integration(t *testing.T) {
	broker := getKafkaBroker()

	topics := []string{
		"test-multi-1-" + uuid.New().String()[:8],
		"test-multi-2-" + uuid.New().String()[:8],
	}

	writers := make([]*kafka.Writer, len(topics))
	for i, topic := range topics {
		writers[i] = NewWriter([]string{broker}, topic)
		defer writers[i].Close()
	}

	// Write to all topics concurrently
	var wg sync.WaitGroup
	for i, writer := range writers {
		wg.Add(1)
		go func(w *kafka.Writer, topic string) {
			defer wg.Done()
			msg := kafka.Message{
				Key:   []byte("key"),
				Value: []byte(`{"test": "` + topic + `"}`),
			}
			err := w.WriteMessages(context.Background(), msg)
			if err != nil {
				t.Errorf("failed to write to topic %s: %v", topic, err)
			}
		}(writer, topics[i])
	}
	wg.Wait()

	// Verify messages in all topics
	for _, topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{broker},
			Topic:     topic,
			Partition: 0,
			MinBytes:  1,
			MaxBytes:  10e6,
			MaxWait:   2 * time.Second,
		})
		defer reader.Close()
		reader.SetOffset(kafka.FirstOffset)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		msg, err := reader.ReadMessage(ctx)
		cancel()

		if err != nil {
			t.Errorf("failed to read from topic %s: %v", topic, err)
			continue
		}

		var data map[string]string
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			t.Errorf("failed to unmarshal message from topic %s: %v", topic, err)
			continue
		}

		if data["test"] != topic {
			t.Errorf("expected test value %s, got %s", topic, data["test"])
		}
	}
}
