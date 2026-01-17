package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
	"github.com/mabumusa1/football-simulator/apps/consumer/internal/repository"
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

// ensureTestTopic explicitly creates a topic and verifies it's ready by doing a test write.
// This is needed because kafka-go's Writer alone doesn't reliably trigger
// topic creation on the broker.
func ensureTestTopic(t *testing.T, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	broker := getKafkaBroker()

	// Connect to Kafka broker to get controller info
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("failed to connect to Kafka: %v", err)
	}

	// Get the controller broker
	controller, err := conn.Controller()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("failed to get controller: %v", err)
	}
	_ = conn.Close()

	// Connect to the controller for topic creation
	controllerAddr := net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port))
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		t.Fatalf("failed to connect to controller at %s: %v", controllerAddr, err)
	}
	defer func() { _ = controllerConn.Close() }()

	// Create topic with explicit configuration
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// Topic might already exist, which is fine - check for known error messages
		errStr := err.Error()
		if !strings.Contains(errStr, "Already Exists") &&
			!strings.Contains(errStr, "already exists") {
			// Log but don't fail - topic auto-creation might handle it
			t.Logf("warning: CreateTopics for %s returned: %v", topic, err)
		}
	}

	// Verify topic is fully operational by doing a test write with retries
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		BatchSize:              1,
		BatchTimeout:           time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	defer func() { _ = writer.Close() }()

	testMsg := kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	// Retry writing until successful or timeout
	for i := 0; i < 30; i++ {
		err = writer.WriteMessages(ctx, testMsg)
		if err == nil {
			return // Topic is ready and writable
		}
		if !strings.Contains(err.Error(), "Unknown Topic Or Partition") {
			t.Fatalf("unexpected error writing to topic %s: %v", topic, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("topic %s was not writable after creation: %v", topic, err)
}

// =============================================================================
// NewBatchConsumer Tests
// =============================================================================

func TestNewBatchConsumer_DefaultValues(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: repo,
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
	if consumer.workerCount != 4 {
		t.Errorf("expected default workerCount 4, got %d", consumer.workerCount)
	}
	if consumer.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestNewBatchConsumer_CustomValues(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		BatchSize:     500,
		FlushInterval: 10 * time.Second,
		MaxRetries:    5,
		WorkerCount:   8,
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
	if consumer.workerCount != 8 {
		t.Errorf("expected workerCount 8, got %d", consumer.workerCount)
	}
}

func TestNewBatchConsumer_ZeroValues(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		BatchSize:     0,
		FlushInterval: 0,
		MaxRetries:    0,
		WorkerCount:   0,
	})

	// Zeros should be replaced with defaults
	if consumer.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000 for zero, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected default flushInterval 1s for zero, got %v", consumer.flushInterval)
	}
	if consumer.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3 for zero, got %d", consumer.maxRetries)
	}
	if consumer.workerCount != 4 {
		t.Errorf("expected default workerCount 4 for zero, got %d", consumer.workerCount)
	}
}

// =============================================================================
// NewReader Tests
// =============================================================================

func TestNewReader(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-consumer-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	cfg := DefaultReaderConfig(brokers, topic, groupID)
	reader := NewReader(cfg)
	defer func() { _ = reader.Close() }()

	if reader == nil {
		t.Fatal("expected non-nil reader")
	}

	// Check reader configuration
	stats := reader.Stats()
	if stats.Topic != topic {
		t.Errorf("expected topic '%s', got '%s'", topic, stats.Topic)
	}
}

func TestDefaultReaderConfig(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-topic"
	groupID := "test-group"

	cfg := DefaultReaderConfig(brokers, topic, groupID)

	if len(cfg.Brokers) != 1 {
		t.Errorf("expected 1 broker, got %d", len(cfg.Brokers))
	}
	if cfg.Topic != topic {
		t.Errorf("expected topic '%s', got '%s'", topic, cfg.Topic)
	}
	if cfg.GroupID != groupID {
		t.Errorf("expected groupID '%s', got '%s'", groupID, cfg.GroupID)
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

// =============================================================================
// NewWriter Tests
// =============================================================================

func TestNewWriter(t *testing.T) {
	brokers := []string{getKafkaBroker()}
	topic := "test-writer-topic-" + uuid.New().String()[:8]

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
}

// =============================================================================
// BatchConsumer Lifecycle Tests
// =============================================================================

func TestBatchConsumer_StartAndStop(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	topic := "test-consumer-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		WorkerCount:   1,
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Start consumer in goroutine
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

// =============================================================================
// Producer-Consumer Integration Tests
// =============================================================================

func TestBatchConsumer_ConsumesProducedMessages(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	topic := "test-consume-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	// Ensure topic exists before writing
	ensureTestTopic(t, topic)

	// Create a producer to write test messages
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

	// Produce some test messages
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
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(string(event.EventType))},
			{Key: "event_id", Value: []byte(event.EventID.String())},
		},
	}

	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}

	// Create consumer
	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		BatchSize:     1,
		FlushInterval: 100 * time.Millisecond,
		WorkerCount:   1,
	})

	consumerCtx, consumerCancel := context.WithCancel(context.Background())

	// Start consumer
	done := make(chan struct{})
	go func() {
		consumer.Start(consumerCtx)
		close(done)
	}()

	// Let consumer process messages
	time.Sleep(2 * time.Second)

	// Stop consumer
	consumerCancel()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop in time")
	}

	_ = reader.Close()
}

func TestBatchConsumer_MultipleBatches(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	topic := "test-batch-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	// Ensure topic exists before writing
	ensureTestTopic(t, topic)

	// Create a producer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    10,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll,
	}
	defer func() { _ = writer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Produce multiple messages
	for i := 0; i < 10; i++ {
		event := createTestEvent()
		value, err := event.ToKafkaMessage()
		if err != nil {
			t.Fatalf("failed to serialize event: %v", err)
		}

		msg := kafka.Message{
			Key:   []byte(event.MatchID),
			Value: value,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(string(event.EventType))},
				{Key: "event_id", Value: []byte(event.EventID.String())},
			},
		}

		err = writer.WriteMessages(ctx, msg)
		if err != nil {
			t.Fatalf("failed to produce message %d: %v", i, err)
		}
	}

	// Create consumer with small batch size to trigger multiple flushes
	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		BatchSize:     3,
		FlushInterval: 500 * time.Millisecond,
		WorkerCount:   1,
	})

	consumerCtx, consumerCancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		consumer.Start(consumerCtx)
		close(done)
	}()

	// Let consumer process messages
	time.Sleep(3 * time.Second)

	consumerCancel()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop in time")
	}

	_ = reader.Close()
}

// =============================================================================
// Retry and Dead Letter Tests
// =============================================================================

func TestBatchConsumer_WithRetryWriter(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	mainTopic := "test-main-topic-" + uuid.New().String()[:8]
	retryTopic := "test-retry-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	reader := NewReader(DefaultReaderConfig(brokers, mainTopic, groupID))
	retryWriter := NewWriter(brokers, retryTopic)

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		RetryWriter:   retryWriter,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
		MaxRetries:    3,
		WorkerCount:   1,
	})

	if consumer.retryWriter == nil {
		t.Error("expected retry writer to be set")
	}

	_ = reader.Close()
	_ = retryWriter.Close()
}

func TestBatchConsumer_WithDeadLetterWriter(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	mainTopic := "test-main-topic-" + uuid.New().String()[:8]
	deadTopic := "test-dead-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	reader := NewReader(DefaultReaderConfig(brokers, mainTopic, groupID))
	deadWriter := NewWriter(brokers, deadTopic)

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  repo,
		Reader:      reader,
		DeadWriter:  deadWriter,
		BatchSize:   10,
		WorkerCount: 1,
	})

	if consumer.deadWriter == nil {
		t.Error("expected dead letter writer to be set")
	}

	_ = reader.Close()
	_ = deadWriter.Close()
}

// =============================================================================
// Domain Event Serialization Tests
// =============================================================================

func TestDomain_EventFromKafkaMessage(t *testing.T) {
	event := createTestEvent()
	value, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("failed to serialize event: %v", err)
	}

	parsedEvent, err := domain.EventFromKafkaMessage(value)
	if err != nil {
		t.Fatalf("failed to parse event: %v", err)
	}

	if parsedEvent.EventID != event.EventID {
		t.Errorf("expected EventID %s, got %s", event.EventID, parsedEvent.EventID)
	}

	if parsedEvent.MatchID != event.MatchID {
		t.Errorf("expected MatchID %s, got %s", event.MatchID, parsedEvent.MatchID)
	}

	if parsedEvent.EventType != event.EventType {
		t.Errorf("expected EventType %s, got %s", event.EventType, parsedEvent.EventType)
	}

	if parsedEvent.TeamID != event.TeamID {
		t.Errorf("expected TeamID %d, got %d", event.TeamID, parsedEvent.TeamID)
	}
}

func TestDomain_EventFromKafkaMessage_InvalidJSON(t *testing.T) {
	_, err := domain.EventFromKafkaMessage([]byte(`{invalid json}`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestDomain_EventFromKafkaMessage_AllEventTypes(t *testing.T) {
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
			event := &domain.Event{
				EventID:   uuid.New(),
				MatchID:   "match-123",
				EventType: eventType,
				Timestamp: time.Now(),
				TeamID:    1,
				PlayerID:  "player-456",
			}

			value, err := event.ToKafkaMessage()
			if err != nil {
				t.Fatalf("failed to serialize event: %v", err)
			}

			parsedEvent, err := domain.EventFromKafkaMessage(value)
			if err != nil {
				t.Fatalf("failed to parse event: %v", err)
			}

			if parsedEvent.EventType != eventType {
				t.Errorf("expected EventType %s, got %s", eventType, parsedEvent.EventType)
			}
		})
	}
}

// =============================================================================
// Configuration Validation Tests
// =============================================================================

func TestBatchConsumer_NegativeValues(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		BatchSize:     -1,
		FlushInterval: -1 * time.Second,
		MaxRetries:    -1,
		WorkerCount:   -1,
	})

	// Negative values should be replaced with defaults
	if consumer.batchSize != 1000 {
		t.Errorf("expected default batchSize 1000 for negative, got %d", consumer.batchSize)
	}
	if consumer.flushInterval != 1*time.Second {
		t.Errorf("expected default flushInterval 1s for negative, got %v", consumer.flushInterval)
	}
	if consumer.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3 for negative, got %d", consumer.maxRetries)
	}
	if consumer.workerCount != 4 {
		t.Errorf("expected default workerCount 4 for negative, got %d", consumer.workerCount)
	}
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func TestBatchConsumer_ConcurrentProducers(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	topic := "test-concurrent-topic-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	// Ensure topic exists before writing
	ensureTestTopic(t, topic)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start multiple producers concurrently
	errChan := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func(producerID int) {
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

			for j := 0; j < 5; j++ {
				event := createTestEvent()
				value, err := event.ToKafkaMessage()
				if err != nil {
					errChan <- err
					return
				}

				msg := kafka.Message{
					Key:   []byte(event.MatchID),
					Value: value,
					Headers: []kafka.Header{
						{Key: "event_type", Value: []byte(string(event.EventType))},
						{Key: "event_id", Value: []byte(event.EventID.String())},
					},
				}

				err = writer.WriteMessages(ctx, msg)
				if err != nil {
					errChan <- err
					return
				}
			}
			errChan <- nil
		}(i)
	}

	// Wait for all producers
	for i := 0; i < 5; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("producer failed: %v", err)
		}
	}

	// Create consumer
	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:    repo,
		Reader:        reader,
		BatchSize:     5,
		FlushInterval: 500 * time.Millisecond,
		WorkerCount:   2,
	})

	consumerCtx, consumerCancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		consumer.Start(consumerCtx)
		close(done)
	}()

	// Let consumer process messages
	time.Sleep(3 * time.Second)

	consumerCancel()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop in time")
	}

	_ = reader.Close()
}

// =============================================================================
// Error Path Tests
// =============================================================================

// MockBatchInserter implements BatchInserter for testing error paths.
type MockBatchInserter struct {
	insertErr   error
	insertCount int
}

func (m *MockBatchInserter) InsertBatch(ctx context.Context, events []*domain.Event) error {
	m.insertCount++
	return m.insertErr
}

func TestBatchConsumer_ExtractRetryCount_EdgeCases(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: repo,
	})

	t.Run("index out of bounds returns 0", func(t *testing.T) {
		messages := []kafka.Message{
			{Value: []byte("test")},
		}
		count := consumer.extractRetryCount(messages, 5)
		if count != 0 {
			t.Errorf("expected 0 for out of bounds index, got %d", count)
		}
	})

	t.Run("empty messages returns 0", func(t *testing.T) {
		count := consumer.extractRetryCount([]kafka.Message{}, 0)
		if count != 0 {
			t.Errorf("expected 0 for empty messages, got %d", count)
		}
	})

	t.Run("no retry_count header returns 0", func(t *testing.T) {
		messages := []kafka.Message{
			{
				Value: []byte("test"),
				Headers: []kafka.Header{
					{Key: "other_header", Value: []byte("value")},
				},
			},
		}
		count := consumer.extractRetryCount(messages, 0)
		if count != 0 {
			t.Errorf("expected 0 for missing retry_count header, got %d", count)
		}
	})

	t.Run("empty retry_count value returns 0", func(t *testing.T) {
		messages := []kafka.Message{
			{
				Value: []byte("test"),
				Headers: []kafka.Header{
					{Key: "retry_count", Value: []byte{}},
				},
			},
		}
		count := consumer.extractRetryCount(messages, 0)
		if count != 0 {
			t.Errorf("expected 0 for empty retry_count value, got %d", count)
		}
	})

	t.Run("valid retry_count returns correct value", func(t *testing.T) {
		messages := []kafka.Message{
			{
				Value: []byte("test"),
				Headers: []kafka.Header{
					{Key: "retry_count", Value: []byte{2}},
				},
			},
		}
		count := consumer.extractRetryCount(messages, 0)
		if count != 2 {
			t.Errorf("expected 2 for valid retry_count, got %d", count)
		}
	})
}

func TestBatchConsumer_FlushWithContext_EmptyBatch(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: repo,
	})

	// Flush empty batch should return early without error
	ctx := context.Background()
	consumer.flushWithContext(ctx) // Should not panic
}

func TestBatchConsumer_FlushWithContext_InsertError(t *testing.T) {
	mockRepo := &MockBatchInserter{
		insertErr: fmt.Errorf("database connection failed"),
	}

	brokers := []string{getKafkaBroker()}
	retryTopic := "test-retry-flush-" + uuid.New().String()[:8]
	deadTopic := "test-dead-flush-" + uuid.New().String()[:8]

	retryWriter := NewWriter(brokers, retryTopic)
	deadWriter := NewWriter(brokers, deadTopic)
	defer func() {
		_ = retryWriter.Close()
		_ = deadWriter.Close()
	}()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		DeadWriter:  deadWriter,
		MaxRetries:  3,
	})

	// Add events to batch manually
	event := createTestEvent()
	consumer.batch = append(consumer.batch, event)
	consumer.messages = append(consumer.messages, kafka.Message{Value: []byte("test")})

	// Flush should handle error and attempt retry
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	consumer.flushWithContext(ctx)

	// Verify insert was attempted
	if mockRepo.insertCount != 1 {
		t.Errorf("expected 1 insert attempt, got %d", mockRepo.insertCount)
	}
}

func TestBatchConsumer_SendToRetry_NoRetryWriter(t *testing.T) {
	mockRepo := &MockBatchInserter{}

	brokers := []string{getKafkaBroker()}
	deadTopic := "test-dead-noretry-" + uuid.New().String()[:8]

	deadWriter := NewWriter(brokers, deadTopic)
	defer func() { _ = deadWriter.Close() }()

	// Create consumer WITHOUT retry writer but WITH dead writer
	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
		DeadWriter: deadWriter,
	})

	events := []*domain.Event{createTestEvent()}
	messages := []kafka.Message{{Value: []byte("test")}}

	// This should send to dead letter instead of retry
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	consumer.sendToRetry(ctx, events, messages)
}

func TestBatchConsumer_SendToRetry_MaxRetriesExceeded(t *testing.T) {
	mockRepo := &MockBatchInserter{}

	brokers := []string{getKafkaBroker()}
	retryTopic := "test-retry-maxretries-" + uuid.New().String()[:8]
	deadTopic := "test-dead-maxretries-" + uuid.New().String()[:8]

	retryWriter := NewWriter(brokers, retryTopic)
	deadWriter := NewWriter(brokers, deadTopic)
	defer func() {
		_ = retryWriter.Close()
		_ = deadWriter.Close()
	}()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  mockRepo,
		RetryWriter: retryWriter,
		DeadWriter:  deadWriter,
		MaxRetries:  3,
	})

	event := createTestEvent()
	events := []*domain.Event{event}

	// Create message with retry count >= max retries
	messages := []kafka.Message{
		{
			Value: []byte("test"),
			Headers: []kafka.Header{
				{Key: "retry_count", Value: []byte{3}}, // Already at max
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	consumer.sendToRetry(ctx, events, messages)
}

func TestBatchConsumer_SendSingleToDead_NoDeadWriter(t *testing.T) {
	mockRepo := &MockBatchInserter{}

	// Create consumer WITHOUT dead writer
	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: mockRepo,
	})

	event := createTestEvent()
	ctx := context.Background()

	// This should log an error but not panic
	consumer.sendSingleToDead(ctx, event)
}

func TestBatchConsumer_PrepareRetryMessage_SerializationError(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	retryTopic := "test-retry-serial-" + uuid.New().String()[:8]
	retryWriter := NewWriter(brokers, retryTopic)
	defer func() { _ = retryWriter.Close() }()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  repo,
		RetryWriter: retryWriter,
		MaxRetries:  3,
	})

	// Create a valid event
	event := createTestEvent()

	// With empty original messages, the index check should handle gracefully
	msg, ok := consumer.prepareRetryMessage(context.Background(), event, []kafka.Message{}, 0)

	// Should succeed with retry count of 1
	if !ok {
		t.Error("expected prepareRetryMessage to succeed for valid event")
	}
	if msg.Key == nil {
		t.Error("expected message key to be set")
	}
}

func TestBatchConsumer_CommitOriginalMessages_EmptyMessages(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: repo,
	})

	// Should return early without error
	ctx := context.Background()
	consumer.commitOriginalMessages(ctx, []kafka.Message{})
}

func TestBatchConsumer_Stop_Clean(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository:  repo,
		WorkerCount: 1,
	})

	// Stop should work even if Start was never called
	// This tests the channel close behavior
	go func() {
		time.Sleep(50 * time.Millisecond)
		consumer.Stop()
	}()

	// Wait for stop to complete
	select {
	case <-consumer.done:
		// Expected
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not complete in time")
	}
}

func TestBatchConsumer_UpdateLagMetric(t *testing.T) {
	repo, cleanup := setupTestRepository(t)
	defer cleanup()

	brokers := []string{getKafkaBroker()}
	topic := "test-lag-metric-" + uuid.New().String()[:8]
	groupID := "test-group-" + uuid.New().String()[:8]

	reader := NewReader(DefaultReaderConfig(brokers, topic, groupID))
	defer func() { _ = reader.Close() }()

	consumer := NewBatchConsumer(BatchConsumerConfig{
		Repository: repo,
		Reader:     reader,
	})

	// Create a test message
	msg := kafka.Message{
		Topic:     topic,
		Partition: 0,
		Offset:    100,
	}

	// Should not panic
	consumer.updateLagMetric(msg)
}

// =============================================================================
// Domain Event Error Path Tests
// =============================================================================

func TestDomain_EventFromKafkaMessage_MissingMatchID(t *testing.T) {
	msg := `{"eventId":"550e8400-e29b-41d4-a716-446655440000","matchId":"","eventType":"goal","timestamp":"2024-01-01T00:00:00Z"}`
	_, err := domain.EventFromKafkaMessage([]byte(msg))
	if err == nil {
		t.Error("expected error for missing matchId")
	}
}

func TestDomain_EventFromKafkaMessage_InvalidEventID(t *testing.T) {
	msg := `{"eventId":"invalid-uuid","matchId":"match-123","eventType":"goal","timestamp":"2024-01-01T00:00:00Z"}`
	_, err := domain.EventFromKafkaMessage([]byte(msg))
	if err == nil {
		t.Error("expected error for invalid eventId")
	}
}

func TestDomain_EventFromKafkaMessage_InvalidTimestamp(t *testing.T) {
	// The code falls back to current time for invalid timestamps
	msg := `{"eventId":"550e8400-e29b-41d4-a716-446655440000","matchId":"match-123","eventType":"goal","timestamp":"invalid-timestamp"}`
	event, err := domain.EventFromKafkaMessage([]byte(msg))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have a valid timestamp (defaulted to now)
	if event.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestDomain_Event_ToKafkaMessage_RoundTrip(t *testing.T) {
	original := createTestEvent()
	original.Metadata = map[string]interface{}{
		"key1": "value1",
		"key2": float64(42),
	}

	value, err := original.ToKafkaMessage()
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	parsed, err := domain.EventFromKafkaMessage(value)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	if original.EventID != parsed.EventID {
		t.Errorf("EventID mismatch: %v != %v", original.EventID, parsed.EventID)
	}
	if original.MatchID != parsed.MatchID {
		t.Errorf("MatchID mismatch: %s != %s", original.MatchID, parsed.MatchID)
	}
	if original.EventType != parsed.EventType {
		t.Errorf("EventType mismatch: %s != %s", original.EventType, parsed.EventType)
	}
}

func TestDomain_Event_MetadataJSON(t *testing.T) {
	t.Run("nil metadata returns empty object", func(t *testing.T) {
		event := createTestEvent()
		event.Metadata = nil
		json := event.MetadataJSON()
		if json != "{}" {
			t.Errorf("expected {}, got %s", json)
		}
	})

	t.Run("valid metadata returns JSON", func(t *testing.T) {
		event := createTestEvent()
		event.Metadata = map[string]interface{}{"key": "value"}
		json := event.MetadataJSON()
		if json != `{"key":"value"}` {
			t.Errorf("expected {\"key\":\"value\"}, got %s", json)
		}
	})
}

func TestDomain_Event_GetEventID(t *testing.T) {
	event := createTestEvent()
	if event.GetEventID() != event.EventID {
		t.Errorf("GetEventID mismatch: %v != %v", event.GetEventID(), event.EventID)
	}
}
