package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

var (
	kafkaEngagementConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "lag",
			Help:      "Current engagement consumer lag",
		},
		[]string{"topic", "partition"},
	)

	kafkaEngagementBatchesProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "batches_processed_total",
			Help:      "Total number of engagement batches processed",
		},
		[]string{"status"},
	)

	kafkaEngagementsConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "events_consumed_total",
			Help:      "Total number of engagement events consumed from Kafka",
		},
		[]string{"status"},
	)

	kafkaEngagementConsumeDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "consume_duration_seconds",
			Help:      "Histogram of engagement batch processing duration in seconds",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"operation"},
	)
)

// EngagementBatchInserter defines the interface for engagement batch insertion.
type EngagementBatchInserter interface {
	InsertEngagementBatch(ctx context.Context, events []*domain.EngagementEvent) error
}

// engagementRepoAdapter adapts EngagementBatchInserter to BatchRepository interface.
type engagementRepoAdapter struct {
	repo EngagementBatchInserter
}

func (a *engagementRepoAdapter) InsertBatch(ctx context.Context, events []*domain.EngagementEvent) error {
	return a.repo.InsertEngagementBatch(ctx, events)
}

// EngagementConsumer wraps GenericBatchConsumer for engagement events.
type EngagementConsumer struct {
	consumer *GenericBatchConsumer[domain.EngagementEvent]
}

// EngagementConsumerConfig holds configuration for the engagement consumer.
type EngagementConsumerConfig struct {
	Reader        *kafka.Reader
	Repository    EngagementBatchInserter
	BatchSize     int
	FlushInterval time.Duration
	WorkerCount   int
	Logger        *slog.Logger
}

// NewEngagementConsumer creates a new EngagementConsumer instance.
func NewEngagementConsumer(cfg EngagementConsumerConfig) *EngagementConsumer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 2000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 8
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	genericCfg := GenericBatchConsumerConfig[domain.EngagementEvent]{
		Reader:           cfg.Reader,
		Repository:       &engagementRepoAdapter{repo: cfg.Repository},
		Parser:           domain.EngagementEventFromKafkaMessage,
		BatchSize:        cfg.BatchSize,
		FlushInterval:    cfg.FlushInterval,
		WorkerCount:      cfg.WorkerCount,
		Logger:           cfg.Logger,
		ConsumerName:     "engagement",
		LagMetric:        kafkaEngagementConsumerLag,
		BatchesProcessed: kafkaEngagementBatchesProcessed,
		EventsConsumed:   kafkaEngagementsConsumed,
		ConsumeDuration:  kafkaEngagementConsumeDuration,
	}

	return &EngagementConsumer{
		consumer: NewGenericBatchConsumer(genericCfg),
	}
}

// Start begins consuming engagement messages from Kafka.
func (c *EngagementConsumer) Start(ctx context.Context) {
	c.consumer.Start(ctx)
}

// Stop signals the consumer to stop and waits for it to finish.
func (c *EngagementConsumer) Stop() {
	c.consumer.Stop()
}
