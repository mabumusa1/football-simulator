package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/api/internal/domain"
)

var (
	// Prometheus metrics for Kafka producer
	kafkaMessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_producer",
			Name:      "messages_produced_total",
			Help:      "Total number of messages produced to Kafka",
		},
		[]string{"topic", "status"},
	)

	kafkaProduceLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_producer",
			Name:      "produce_duration_seconds",
			Help:      "Histogram of Kafka produce latency in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"topic"},
	)

	kafkaMessageSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_producer",
			Name:      "message_size_bytes",
			Help:      "Histogram of Kafka message sizes in bytes",
			Buckets:   []float64{100, 500, 1000, 5000, 10000, 50000, 100000},
		},
		[]string{"topic"},
	)
)

// EventProducer handles producing events to Kafka.
type EventProducer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewEventProducer creates a new EventProducer instance.
func NewEventProducer(writer *kafka.Writer, logger *slog.Logger) *EventProducer {
	if logger == nil {
		logger = slog.Default()
	}
	return &EventProducer{
		writer: writer,
		logger: logger,
	}
}

// Produce sends an event to Kafka.
// The event is serialized to JSON and sent with the matchId as the key
// to ensure partition ordering for events from the same match.
func (p *EventProducer) Produce(ctx context.Context, event *domain.Event) error {
	if event == nil {
		return fmt.Errorf("event cannot be nil")
	}

	startTime := time.Now()
	topic := p.writer.Topic

	// Serialize event to JSON using domain's serialization method
	value, err := event.ToKafkaMessage()
	if err != nil {
		p.logger.Error("failed to serialize event to JSON",
			slog.String("event_id", event.EventID.String()),
			slog.String("match_id", event.MatchID),
			slog.String("error", err.Error()),
		)
		kafkaMessagesProduced.WithLabelValues(topic, "serialization_error").Inc()
		return fmt.Errorf("failed to serialize event: %w", err)
	}

	// Create Kafka message with headers for efficient filtering
	msg := kafka.Message{
		Key:   []byte(event.MatchID),
		Value: value,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(string(event.EventType))},
			{Key: "event_id", Value: []byte(event.EventID.String())},
		},
		Time: event.Timestamp,
	}

	// Write message synchronously to ensure durability
	err = p.writer.WriteMessages(ctx, msg)
	duration := time.Since(startTime)

	// Record metrics
	kafkaProduceLatency.WithLabelValues(topic).Observe(duration.Seconds())
	kafkaMessageSize.WithLabelValues(topic).Observe(float64(len(value)))

	if err != nil {
		p.logger.Error("failed to produce message to Kafka",
			slog.String("event_id", event.EventID.String()),
			slog.String("match_id", event.MatchID),
			slog.String("event_type", string(event.EventType)),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		kafkaMessagesProduced.WithLabelValues(topic, "error").Inc()
		return fmt.Errorf("failed to produce message: %w", err)
	}

	p.logger.Debug("successfully produced event to Kafka",
		slog.String("event_id", event.EventID.String()),
		slog.String("match_id", event.MatchID),
		slog.String("event_type", string(event.EventType)),
		slog.Duration("duration", duration),
		slog.Int("message_size", len(value)),
	)
	kafkaMessagesProduced.WithLabelValues(topic, "success").Inc()

	return nil
}

// ProduceBatch sends multiple events to Kafka in a single batch.
// All events are serialized and sent together for improved throughput.
func (p *EventProducer) ProduceBatch(ctx context.Context, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	startTime := time.Now()
	topic := p.writer.Topic

	messages := make([]kafka.Message, 0, len(events))

	for _, event := range events {
		if event == nil {
			continue
		}

		value, err := event.ToKafkaMessage()
		if err != nil {
			p.logger.Warn("skipping event due to serialization error",
				slog.String("event_id", event.EventID.String()),
				slog.String("error", err.Error()),
			)
			kafkaMessagesProduced.WithLabelValues(topic, "serialization_error").Inc()
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
		kafkaMessageSize.WithLabelValues(topic).Observe(float64(len(value)))
	}

	if len(messages) == 0 {
		return nil
	}

	err := p.writer.WriteMessages(ctx, messages...)
	duration := time.Since(startTime)

	kafkaProduceLatency.WithLabelValues(topic).Observe(duration.Seconds())

	if err != nil {
		p.logger.Error("failed to produce batch to Kafka",
			slog.Int("batch_size", len(messages)),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		kafkaMessagesProduced.WithLabelValues(topic, "error").Add(float64(len(messages)))
		return fmt.Errorf("failed to produce batch: %w", err)
	}

	p.logger.Debug("successfully produced batch to Kafka",
		slog.Int("batch_size", len(messages)),
		slog.Duration("duration", duration),
	)
	kafkaMessagesProduced.WithLabelValues(topic, "success").Add(float64(len(messages)))

	return nil
}

// Close closes the Kafka writer and releases resources.
func (p *EventProducer) Close() error {
	if p.writer == nil {
		return nil
	}

	p.logger.Info("closing Kafka producer")

	if err := p.writer.Close(); err != nil {
		p.logger.Error("failed to close Kafka writer",
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to close Kafka writer: %w", err)
	}

	p.logger.Info("Kafka producer closed successfully")
	return nil
}

// Ping checks the connectivity to Kafka brokers.
func (p *EventProducer) Ping(ctx context.Context) error {
	startTime := time.Now()

	conn, err := kafka.DialContext(ctx, "tcp", p.writer.Addr.String())
	if err != nil {
		p.logger.Error("Kafka ping failed",
			slog.Duration("duration", time.Since(startTime)),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("kafka ping failed: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Verify connectivity by fetching broker metadata
	_, err = conn.Brokers()
	if err != nil {
		p.logger.Error("Kafka broker check failed",
			slog.Duration("duration", time.Since(startTime)),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("kafka broker check failed: %w", err)
	}

	p.logger.Debug("Kafka ping successful",
		slog.Duration("duration", time.Since(startTime)),
	)
	return nil
}

// NewWriter creates a new Kafka writer with production-ready configuration.
// The writer uses hash-based partitioning by key (matchId) to ensure ordering.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Hash by key (matchId) for partition ordering
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll, // Wait for all replicas for durability
		Async:        false,            // Synchronous writes for reliability
	}
}

// WriterConfig holds configuration for Kafka writer.
type WriterConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	BatchTimeout time.Duration
	WriteTimeout time.Duration
	MaxAttempts  int
	Async        bool
}

// NewWriterWithConfig creates a new Kafka writer with custom configuration.
func NewWriterWithConfig(cfg WriterConfig) *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequireAll,
		Async:        cfg.Async,
	}

	if cfg.MaxAttempts > 0 {
		w.MaxAttempts = cfg.MaxAttempts
	}

	return w
}

// DefaultWriterConfig returns default configuration for the events topic.
func DefaultWriterConfig(brokers []string, topic string) WriterConfig {
	return WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		MaxAttempts:  3,
		Async:        false,
	}
}

// EngagementProducer handles producing engagement events to Kafka.
type EngagementProducer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewEngagementProducer creates a new EngagementProducer instance.
func NewEngagementProducer(writer *kafka.Writer, logger *slog.Logger) *EngagementProducer {
	if logger == nil {
		logger = slog.Default()
	}
	return &EngagementProducer{
		writer: writer,
		logger: logger,
	}
}

// Produce sends an engagement event to Kafka.
func (p *EngagementProducer) Produce(ctx context.Context, event *domain.EngagementEvent) error {
	if event == nil {
		return fmt.Errorf("engagement event cannot be nil")
	}

	startTime := time.Now()
	topic := p.writer.Topic

	value, err := event.ToKafkaMessage()
	if err != nil {
		p.logger.Error("failed to serialize engagement event",
			slog.String("event_id", event.EventID.String()),
			slog.String("match_id", event.MatchID),
			slog.String("error", err.Error()),
		)
		kafkaMessagesProduced.WithLabelValues(topic, "serialization_error").Inc()
		return fmt.Errorf("failed to serialize engagement: %w", err)
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

	err = p.writer.WriteMessages(ctx, msg)
	duration := time.Since(startTime)

	kafkaProduceLatency.WithLabelValues(topic).Observe(duration.Seconds())
	kafkaMessageSize.WithLabelValues(topic).Observe(float64(len(value)))

	if err != nil {
		p.logger.Error("failed to produce engagement to Kafka",
			slog.String("event_id", event.EventID.String()),
			slog.String("user_id", event.UserID),
			slog.String("engagement_type", string(event.EngagementType)),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		kafkaMessagesProduced.WithLabelValues(topic, "error").Inc()
		return fmt.Errorf("failed to produce engagement: %w", err)
	}

	p.logger.Debug("produced engagement event",
		slog.String("event_id", event.EventID.String()),
		slog.String("engagement_type", string(event.EngagementType)),
		slog.Duration("duration", duration),
	)
	kafkaMessagesProduced.WithLabelValues(topic, "success").Inc()

	return nil
}

// ProduceBatch sends multiple engagement events to Kafka in a single batch.
func (p *EngagementProducer) ProduceBatch(ctx context.Context, events []*domain.EngagementEvent) error {
	if len(events) == 0 {
		return nil
	}

	startTime := time.Now()
	topic := p.writer.Topic

	messages := make([]kafka.Message, 0, len(events))

	for _, event := range events {
		if event == nil {
			continue
		}

		value, err := event.ToKafkaMessage()
		if err != nil {
			p.logger.Warn("skipping engagement due to serialization error",
				slog.String("event_id", event.EventID.String()),
				slog.String("error", err.Error()),
			)
			kafkaMessagesProduced.WithLabelValues(topic, "serialization_error").Inc()
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
		kafkaMessageSize.WithLabelValues(topic).Observe(float64(len(value)))
	}

	if len(messages) == 0 {
		return nil
	}

	err := p.writer.WriteMessages(ctx, messages...)
	duration := time.Since(startTime)

	kafkaProduceLatency.WithLabelValues(topic).Observe(duration.Seconds())

	if err != nil {
		p.logger.Error("failed to produce engagement batch",
			slog.Int("batch_size", len(messages)),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		kafkaMessagesProduced.WithLabelValues(topic, "error").Add(float64(len(messages)))
		return fmt.Errorf("failed to produce engagement batch: %w", err)
	}

	p.logger.Debug("produced engagement batch",
		slog.Int("batch_size", len(messages)),
		slog.Duration("duration", duration),
	)
	kafkaMessagesProduced.WithLabelValues(topic, "success").Add(float64(len(messages)))

	return nil
}

// Close closes the engagement producer.
func (p *EngagementProducer) Close() error {
	if p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// Ping checks Kafka connectivity.
func (p *EngagementProducer) Ping(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", p.writer.Addr.String())
	if err != nil {
		return fmt.Errorf("engagement kafka ping failed: %w", err)
	}
	defer func() { _ = conn.Close() }()

	_, err = conn.Brokers()
	if err != nil {
		return fmt.Errorf("engagement kafka broker check failed: %w", err)
	}

	return nil
}

// NewEngagementWriter creates a Kafka writer for engagement events.
func NewEngagementWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    500, // Higher batch size for engagement volume
		BatchTimeout: 50 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireOne, // Lower durability for high-volume engagement
		Async:        false,
	}
}
