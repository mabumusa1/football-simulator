package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/api/internal/domain"
)

func getKafkaBroker() string {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}
	return broker
}

func TestEventProducer_Ping_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-events-" + uuid.New().String()[:8]

	writer := NewWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEventProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestEventProducer_Produce_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-events-" + uuid.New().String()[:8]

	// Create topic first
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	_ = conn.Close()

	writer := NewWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEventProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "match-integration-test",
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
		PlayerID:  "player-123",
		Metadata:  map[string]interface{}{"minute": 45},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	// Verify message was written by reading it back
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   5 * time.Second,
	})
	defer func() { _ = reader.Close() }()

	_ = reader.SetOffset(kafka.FirstOffset)

	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}

	if string(msg.Key) != event.MatchID {
		t.Errorf("expected key %s, got %s", event.MatchID, string(msg.Key))
	}

	// Verify headers
	headerMap := make(map[string]string)
	for _, h := range msg.Headers {
		headerMap[h.Key] = string(h.Value)
	}

	if headerMap["event_type"] != string(event.EventType) {
		t.Errorf("expected event_type header %s, got %s", event.EventType, headerMap["event_type"])
	}
	if headerMap["event_id"] != event.EventID.String() {
		t.Errorf("expected event_id header %s, got %s", event.EventID.String(), headerMap["event_id"])
	}
}

func TestEventProducer_ProduceBatch_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-events-batch-" + uuid.New().String()[:8]

	// Create topic with proper configuration
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("failed to dial broker: %v", err)
	}
	controller, err := conn.Controller()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("failed to get controller: %v", err)
	}
	_ = conn.Close()

	controllerConn, err := kafka.Dial("tcp", controller.Host+":"+fmt.Sprintf("%d", controller.Port))
	if err != nil {
		t.Fatalf("failed to dial controller: %v", err)
	}
	defer func() { _ = controllerConn.Close() }()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Logf("topic creation returned: %v (may already exist)", err)
	}

	// Wait for topic to be ready
	time.Sleep(500 * time.Millisecond)

	writer := NewWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEventProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-batch-1",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "player-1",
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-batch-1",
			EventType: domain.EventTypeShot,
			Timestamp: time.Now(),
			TeamID:    2,
			PlayerID:  "player-2",
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-batch-2",
			EventType: domain.EventTypeFoul,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "player-3",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed: %v", err)
	}

	// Verify messages were written
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   5 * time.Second,
	})
	defer func() { _ = reader.Close() }()

	_ = reader.SetOffset(kafka.FirstOffset)

	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()

	for i := 0; i < len(events); i++ {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("failed to read message %d: %v", i, err)
		}

		if len(msg.Value) == 0 {
			t.Errorf("message %d has empty value", i)
		}
	}
}

func TestEventProducer_ProduceBatch_EmptyList_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-events-empty-" + uuid.New().String()[:8]

	writer := NewWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEventProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Empty batch should return nil without error
	err := producer.ProduceBatch(ctx, []*domain.Event{})
	if err != nil {
		t.Fatalf("ProduceBatch with empty list should not error: %v", err)
	}
}

func TestEventProducer_Produce_NilEvent_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-events-nil-" + uuid.New().String()[:8]

	writer := NewWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEventProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Produce(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil event")
	}
}

func TestEngagementProducer_Ping_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-engagements-" + uuid.New().String()[:8]

	writer := NewEngagementWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEngagementProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestEngagementProducer_Produce_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-engagements-" + uuid.New().String()[:8]

	// Create topic
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	_ = conn.Close()

	writer := NewEngagementWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEngagementProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	event := &domain.EngagementEvent{
		EventID:        uuid.New(),
		UserID:         "user-123",
		MatchID:        "match-engagement-test",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
		Metadata:       map[string]interface{}{"source": "test"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.Produce(ctx, event)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	// Verify message was written
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   5 * time.Second,
	})
	defer func() { _ = reader.Close() }()

	_ = reader.SetOffset(kafka.FirstOffset)

	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}

	expectedKey := event.MatchID + ":" + event.UserID
	if string(msg.Key) != expectedKey {
		t.Errorf("expected key %s, got %s", expectedKey, string(msg.Key))
	}

	// Verify headers
	headerMap := make(map[string]string)
	for _, h := range msg.Headers {
		headerMap[h.Key] = string(h.Value)
	}

	if headerMap["engagement_type"] != string(event.EngagementType) {
		t.Errorf("expected engagement_type header %s, got %s", event.EngagementType, headerMap["engagement_type"])
	}
	if headerMap["user_id"] != event.UserID {
		t.Errorf("expected user_id header %s, got %s", event.UserID, headerMap["user_id"])
	}
}

func TestEngagementProducer_ProduceBatch_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-engagements-batch-" + uuid.New().String()[:8]

	// Create topic
	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}
	_ = conn.Close()

	writer := NewEngagementWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEngagementProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	events := []*domain.EngagementEvent{
		{
			EventID:        uuid.New(),
			UserID:         "user-1",
			MatchID:        "match-1",
			EngagementType: domain.EngagementTypeReaction,
			Timestamp:      time.Now(),
		},
		{
			EventID:        uuid.New(),
			UserID:         "user-2",
			MatchID:        "match-1",
			EngagementType: domain.EngagementTypeShare,
			Timestamp:      time.Now(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.ProduceBatch(ctx, events)
	if err != nil {
		t.Fatalf("ProduceBatch failed: %v", err)
	}

	// Verify messages
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   5 * time.Second,
	})
	defer func() { _ = reader.Close() }()

	_ = reader.SetOffset(kafka.FirstOffset)

	readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readCancel()

	for i := 0; i < len(events); i++ {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("failed to read message %d: %v", i, err)
		}

		if len(msg.Value) == 0 {
			t.Errorf("message %d has empty value", i)
		}
	}
}

func TestEngagementProducer_Produce_NilEvent_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-engagements-nil-" + uuid.New().String()[:8]

	writer := NewEngagementWriter([]string{broker}, topic)
	defer func() { _ = writer.Close() }()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	producer := NewEngagementProducer(writer, logger)
	defer func() { _ = producer.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := producer.Produce(ctx, nil)
	if err == nil {
		t.Fatal("expected error for nil event")
	}
}

func TestNewWriter_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-writer-" + uuid.New().String()[:8]

	writer := NewWriter([]string{broker}, topic)
	if writer == nil {
		t.Fatal("NewWriter returned nil")
	}
	defer func() { _ = writer.Close() }()

	if writer.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, writer.Topic)
	}
}

func TestNewWriterWithConfig_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-writer-config-" + uuid.New().String()[:8]

	cfg := WriterConfig{
		Brokers:      []string{broker},
		Topic:        topic,
		BatchSize:    50,
		BatchTimeout: 20 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		MaxAttempts:  5,
		Async:        false,
	}

	writer := NewWriterWithConfig(cfg)
	if writer == nil {
		t.Fatal("NewWriterWithConfig returned nil")
	}
	defer func() { _ = writer.Close() }()

	if writer.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, writer.Topic)
	}
	if writer.BatchSize != 50 {
		t.Errorf("expected batch size 50, got %d", writer.BatchSize)
	}
	if writer.MaxAttempts != 5 {
		t.Errorf("expected max attempts 5, got %d", writer.MaxAttempts)
	}
}

func TestDefaultWriterConfig_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-default-config"

	cfg := DefaultWriterConfig([]string{broker}, topic)

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != broker {
		t.Errorf("unexpected brokers: %v", cfg.Brokers)
	}
	if cfg.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, cfg.Topic)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("expected batch size 100, got %d", cfg.BatchSize)
	}
	if cfg.BatchTimeout != 10*time.Millisecond {
		t.Errorf("expected batch timeout 10ms, got %v", cfg.BatchTimeout)
	}
	if cfg.WriteTimeout != 10*time.Second {
		t.Errorf("expected write timeout 10s, got %v", cfg.WriteTimeout)
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("expected max attempts 3, got %d", cfg.MaxAttempts)
	}
}

func TestNewEngagementWriter_Integration(t *testing.T) {
	broker := getKafkaBroker()
	topic := "test-engagement-writer-" + uuid.New().String()[:8]

	writer := NewEngagementWriter([]string{broker}, topic)
	if writer == nil {
		t.Fatal("NewEngagementWriter returned nil")
	}
	defer func() { _ = writer.Close() }()

	if writer.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, writer.Topic)
	}
	if writer.BatchSize != 500 {
		t.Errorf("expected batch size 500, got %d", writer.BatchSize)
	}
}

func TestEventProducer_Close_NilWriter_Integration(t *testing.T) {
	producer := &EventProducer{
		writer: nil,
		logger: slog.Default(),
	}

	err := producer.Close()
	if err != nil {
		t.Fatalf("Close with nil writer should not error: %v", err)
	}
}

func TestEngagementProducer_Close_NilWriter_Integration(t *testing.T) {
	producer := &EngagementProducer{
		writer: nil,
		logger: slog.Default(),
	}

	err := producer.Close()
	if err != nil {
		t.Fatalf("Close with nil writer should not error: %v", err)
	}
}
